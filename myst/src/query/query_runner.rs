/*
 *
 *  * This file is part of OpenTSDB.
 *  * Copyright (C) 2021  Yahoo.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use log::{debug, error, info};
use std::collections::HashMap;
use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::Hash,
    hash::Hasher,
    thread,
    time::SystemTime,
};

use croaring::Bitmap;
use rayon::prelude::*;

use crate::segment::segment_reader::SegmentReader;
use crate::segment::store::dict::DictHolder;
use crate::utils::myst_error::{MystError, Result};

use super::{filter::FilterType, query::Query, query::QueryType, query_filter::QueryFilter};
use crate::myst_grpc::Dictionary;
use crate::myst_grpc::GroupedTimeseries;
use crate::segment::store::docstore::{DeserializedDocStore, DocStore};
use crate::segment::store::myst_fst::MystFST;
use fasthash::xx::Hash64;
use fasthash::FastHash;

use crate::query::result::StringGroupedTimeseries;
use crate::segment::myst_segment::MystSegmentHeaderKeys::EpochBitmap;
use crate::segment::store::epoch_bitmap;
use crate::segment::store::epoch_bitmap::EpochBitmapHolder;
use crate::utils::config::Config;
use metrics_reporter::MetricsReporter;
use std::fs::File;
use std::io::{Read, Seek};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

const METRIC_PREFIX: &str = crate::utils::config::METRIC;
const TAG_KEY_PREFIX: &str = crate::utils::config::TAG_KEYS;
const TAG_VALUE_PREFIX: &str = crate::utils::config::TAG_VALUES;

/// Runs a query for all segments in a shard.
pub struct QueryRunner<'a, R: Read + Seek + Send + Sync> {
    pub segment_readers: Vec<SegmentReader<R>>,
    pub query: &'a Query,
    pub config: &'a Config,
    pub metrics_reporter: Option<&'a Box<MetricsReporter>>,
}

impl<'a, R: Read + Seek + Send + Sync> QueryRunner<'a, R> {
    pub fn new(
        segment_readers: Vec<SegmentReader<R>>,
        query: &'a Query,
        config: &'a Config,
        metrics_reporter: Option<&'a Box<MetricsReporter>>,
    ) -> Self {
        Self {
            segment_readers,
            query,
            config,
            metrics_reporter,
        }
    }

    pub fn search_timeseries(
        &mut self,
        segment_pool: &rayon::ThreadPool,
        timeseries_response: &mut crate::myst_grpc::TimeseriesResponse,
    ) -> Result<()> {
        let curr_time = SystemTime::now();
        QueryRunner::searh_timeseries_in_segments(
            &self.query.filter,
            &self.query.start,
            &self.query.end,
            &self.query.group,
            &mut self.segment_readers,
            segment_pool,
            &self.config.cache,
            self.config.docstore_block_size,
            timeseries_response,
        )?;
        if self.metrics_reporter.is_some() {
            self.metrics_reporter.unwrap().gauge(
                "segment.query.latency",
                &[
                    "shard",
                    self.segment_readers
                        .get(0)
                        .unwrap()
                        .shard_id
                        .to_string()
                        .as_str(),
                    "segment",
                    self.segment_readers
                        .get(0)
                        .unwrap()
                        .created
                        .to_string()
                        .as_str(),
                    "host",
                    sys_info::hostname().unwrap().as_str(),
                ],
                SystemTime::now()
                    .duration_since(curr_time)
                    .unwrap()
                    .as_millis() as u64,
            );
        }
        Ok(())
    }

    pub fn searh_timeseries_in_segments(
        filter: &QueryFilter,
        query_start: &u64,
        query_end: &u64,
        group_by: &Vec<String>,
        segment_readers: &mut Vec<SegmentReader<R>>,
        segment_pool: &rayon::ThreadPool,
        cache: &Vec<String>,
        docstore_block_size: usize,
        timeseries_response: &mut crate::myst_grpc::TimeseriesResponse,
    ) -> Result<()> {
        let _query_time = SystemTime::now();
        let (tx, rx) = std::sync::mpsc::channel();
        segment_pool.scope(move |s| {
            debug!("Starting scoped segment query");
            for segment_reader in segment_readers {
                let tx = tx.clone();
                s.spawn(move |_s| {
                    let _curr_time = SystemTime::now();
                    let mut grouped_timeseries = HashMap::new();
                    let result = <QueryRunner<'a, R>>::timeseries_for_segment(
                        &filter,
                        query_start,
                        query_end,
                        group_by,
                        segment_reader,
                        cache,
                        docstore_block_size,
                        &mut grouped_timeseries,
                    );
                    match result {
                        Err(e) => {
                            error!("Error fetching timeseries from segment {:?}", e);
                        }
                        Ok(_t) => {}
                    }

                    tx.send(grouped_timeseries).unwrap();
                });
            }
        });
        info!(
            "Waiting for all segment threads with thread {:?}",
            thread::current()
        );
        let curr_time = SystemTime::now();
        let segment_results: Vec<HashMap<u64, crate::query::result::StringGroupedTimeseries>> =
            rx.iter().collect();

        let mut result: HashMap<u64, crate::query::result::StringGroupedTimeseries> =
            HashMap::new();
        for segment_result in segment_results {
            for (k, v) in segment_result {
                if !result.contains_key(&k) {
                    result.insert(k, crate::query::result::StringGroupedTimeseries::default());
                    result.get_mut(&k).unwrap().group.extend(v.group);
                }

                result.get_mut(&k).unwrap().add_all(v.timeseries);
            }
        }

        // Iterate over merged result and form dict and TimeseriesResponse
        let mut grouped_timeseries = Vec::new();
        let mut dict = HashMap::new();
        let _id = 0;
        for (_k, mut v) in result {
            let mut hashes = Vec::with_capacity(v.group.len());
            for tagval in &v.group {
                let tval = tagval.to_string();
                let hash = Hash64::hash(&tval) as i64;
                dict.entry(hash).or_insert(tval);
                hashes.push(hash);
            }

            grouped_timeseries.push(GroupedTimeseries {
                group: hashes,
                timeseries: v.convert(),
            });
        }

        timeseries_response.grouped_timeseries = grouped_timeseries;
        timeseries_response.dict = Some(Dictionary { dict });
        timeseries_response.streams = 0;

        // info!(
        //     "Time took to merge all segment results shard: {:?} is {:?}",
        //     segment_readers.get(0).unwrap().shard_id,
        //     SystemTime::now().duration_since(curr_time).unwrap(),
        // );

        Ok(())
    }

    fn timeseries_for_segment(
        filter: &QueryFilter,
        query_start: &u64,
        query_end: &u64,
        group_by: &Vec<String>,
        segment_reader: &mut SegmentReader<R>,
        cache: &Vec<String>,
        docstore_block_size: usize,
        result: &mut HashMap<u64, crate::query::result::StringGroupedTimeseries>,
    ) -> Result<()> {
        info!(
            "##### Running each segment query for shard {:?} and {} with thread {:?}",
            segment_reader.shard_id,
            segment_reader.created,
            thread::current().id()
        );
        let dict_time = SystemTime::now();
        let dict_cache_enabled = cache.contains(&("dict".to_string()));
        let dict = match dict_cache_enabled {
            true => segment_reader.get_dict_from_cache()?,
            false => segment_reader.get_dict()?,
        };
        info!(
            "Time to get dict {:?} for shard: {} segment: {}",
            SystemTime::now().duration_since(dict_time).unwrap(),
            segment_reader.shard_id,
            segment_reader.created,
        );
        let mut group_key_ids = Vec::new();
        for group in group_by {
            let keys = segment_reader.search_literal(crate::utils::config::TAG_KEYS, group)?;
            for key in keys {
                group_key_ids.push(MystFST::get_id(key.1));
            }
        }
        let fil_time = SystemTime::now();
        let filtered_bitmap = QueryRunner::filter(&filter, query_start, query_end, segment_reader)?;
        if filtered_bitmap.cardinality() == 0 {
            return Ok(());
        }
        info!(
            "Time to get filter bitmap {:?} for shard: {} segment: {}",
            SystemTime::now().duration_since(fil_time).unwrap(),
            segment_reader.shard_id,
            segment_reader.created,
        );
        let mut explicit_filter = false;
        let mut explicit_tags_count = 0;
        if let QueryFilter::ExplicitTags { filter: _, count } = filter {
            explicit_filter = true;
            explicit_tags_count = *count;
        }

        let iter = filtered_bitmap.to_vec();
        let gb_time = SystemTime::now();
        let ts_data_time = Arc::new(AtomicU32::new(0));
        let docstore_blocks = QueryRunner::<R>::get_docstore_blocks(iter, docstore_block_size);
        info!(
            "Number of docstore blocks {} for shard: {} segment: {}",
            docstore_blocks.len(),
            segment_reader.shard_id,
            segment_reader.created
        );

        let chunk_time = SystemTime::now();
        debug!(
            "Processing {:?} docstore blocks for shard: {} segment: {} in thread {:?}",
            docstore_blocks.len(),
            segment_reader.shard_id,
            segment_reader.created,
            thread::current()
        );

        let epoch_bitmap = segment_reader.get_all_ts_bitmaps()?;
        let mut all_groups: Vec<Result<HashMap<u64, StringGroupedTimeseries>>> = docstore_blocks
                .par_iter()
                .map(|(_id, chunk)| {
                    let curr_time = SystemTime::now();
                    let mut groups = HashMap::new();
                    QueryRunner::process_docstore_block(
                        chunk,
                        segment_reader,
                        cache,
                        explicit_filter,
                        explicit_tags_count,
                        &dict.dict,
                        &group_key_ids,
                        docstore_block_size,
                        ts_data_time.clone(),
                        &epoch_bitmap,
                        &mut groups,
                    )?;
                    debug!(
                        "Time took to process docstore block {:?} for shard: {} segment: {} in thread {:?}",
                        SystemTime::now().duration_since(curr_time).unwrap(),
                        segment_reader.shard_id,
                        segment_reader.created,
                        thread::current()
                    );
                    Ok(groups)
                })
                .collect();

        info!(
            "Time took to process all docstore blocks {:?} for shard: {} segment: {}",
            SystemTime::now().duration_since(chunk_time).unwrap(),
            segment_reader.shard_id,
            segment_reader.created
        );

        for groups in all_groups {
            for (k, v) in groups?.into_iter() {
                if result.contains_key(&k) {
                    for (xxhash, ts) in v.timeseries {
                        result.get_mut(&k).unwrap().timeseries.insert(xxhash, ts);
                    }
                } else {
                    result.insert(k, v);
                }
            }
        }

        info!(
            "Time took running segment query for shard: {:?} in segment: {:?} and number of timeseries {:?} is {:?}",
            segment_reader.shard_id,
            segment_reader.created,
            filtered_bitmap.cardinality(),
            SystemTime::now().duration_since(gb_time).unwrap(),
        );
        Ok(())
    }

    fn get_docstore_blocks(
        elements: Vec<u32>,
        docstore_block_size: usize,
    ) -> HashMap<u32, Vec<u32>> {
        let mut result = HashMap::new();
        for element in elements {
            let bucket_id = element / docstore_block_size as u32;
            result
                .entry(bucket_id)
                .or_insert_with(|| Vec::new())
                .push(element);
        }
        result
    }

    fn process_docstore_block(
        elements: &[u32],
        segment_reader: &SegmentReader<R>,
        cache: &Vec<String>,
        explicit_filter: bool,
        explicit_tags_count: u32,
        dict: &HashMap<u32, String>,
        group_key_ids: &Vec<u32>,
        docstore_block_size: usize,
        ts_data_time: Arc<AtomicU32>,
        epoch_bitmap: &HashMap<u64, Arc<EpochBitmapHolder>>,
        groups: &mut HashMap<u64, StringGroupedTimeseries>,
    ) -> Result<()> {
        let mut docstore = Arc::new(DeserializedDocStore::default()); //TODO: Don't initialize
        let mut docstore_result = &Vec::new(); //TODO: Don't initialize
        let mut docstore_offsetlen = &Vec::new(); //TODO: Don't initialize
        let mut ts_file_id = u32::MAX;
        for element in elements {
            let id = element / docstore_block_size as u32;
            if ts_file_id != id {
                ts_file_id = id;

                let curr_time = SystemTime::now();
                let docstore_cache_enabled = cache.contains(&("docstore".to_string()));
                docstore = match docstore_cache_enabled {
                    true => segment_reader.get_docstore_cache(ts_file_id)?,
                    false => segment_reader.get_docstore(ts_file_id)?,
                };

                ts_data_time.fetch_add(
                    SystemTime::now()
                        .duration_since(curr_time)
                        .unwrap()
                        .as_millis() as u32,
                    Ordering::Relaxed,
                );
                if docstore.data.is_none() || docstore.offset_len.is_none() {
                    error!("Should not happen!! No data found");
                    return Ok(());
                }
                docstore_result = docstore.data.as_ref().unwrap();
                docstore_offsetlen = docstore.offset_len.as_ref().unwrap();
            }

            let e = element % docstore_block_size as u32;
            if let Some(offset_len) = docstore_offsetlen.get(e as usize) {
                let offset = offset_len.offset;
                let len = offset_len.len;
                let timeseries_vec = &docstore_result[offset..(offset + len as usize)];
                if explicit_filter
                    && DocStore::count_num_timeseries(timeseries_vec) != explicit_tags_count
                {
                    continue;
                }
                let mut bitmap = Bitmap::create();
                let timeseries = DocStore::deserialize_timeseries(timeseries_vec)?;
                for (epoch, bitmap_holder) in epoch_bitmap {
                    if bitmap_holder.bitmap.contains(*element) {
                        bitmap.add(*epoch as u32);
                    }
                }

                if &group_key_ids.len() > &0 {
                    let mut group_strs_vec = Vec::new();
                    //  let mut group = GroupedTimeseries::default();
                    for id in group_key_ids {
                        if timeseries.tags.contains_key(&id) {
                            let val = timeseries.tags.get(&id).unwrap();
                            let group_str = dict.get(val).unwrap().to_owned();
                            group_strs_vec.push(group_str.clone());
                        }
                    }

                    let mut hasher = DefaultHasher::new();
                    Hash::hash_slice(&group_strs_vec, &mut hasher);
                    let hash = hasher.finish();
                    if !groups.contains_key(&hash) {
                        groups.insert(
                            hash,
                            crate::query::result::StringGroupedTimeseries::default(),
                        );
                        groups.get_mut(&hash).unwrap().group.extend(group_strs_vec);
                    }
                    groups.get_mut(&hash).unwrap().timeseries.insert(
                        timeseries.timeseries_id as i64,
                        crate::query::result::Timeseries {
                            xxhash: timeseries.timeseries_id as i64,
                            bitmap: bitmap,
                        },
                    );
                    //groups.insert(hash, group);
                } else {
                    // let mut group = GroupedTimeseries::default();
                    //group.timeseries.insert(timeseries.timeseries_id);
                    groups
                        .entry(0)
                        .or_insert(crate::query::result::StringGroupedTimeseries::default());
                    groups.get_mut(&0).unwrap().timeseries.insert(
                        timeseries.timeseries_id as i64,
                        crate::query::result::Timeseries {
                            xxhash: timeseries.timeseries_id as i64,
                            bitmap: bitmap,
                        },
                    );
                }
            } else {
                error!(
                    "Should not happen for element {:?} and idx {:?} in file id {:?}",
                    element, e, ts_file_id
                );
                return Ok(());
            }
        }

        Ok(())
    }

    pub fn get_all_metrics(&mut self) -> Result<Vec<String>> {
        let curr_time = SystemTime::now();
        let result: Result<Vec<_>> = self
            .segment_readers
            .par_iter_mut()
            .map(|segment_reader| {
                // Query::getAllMetricsInSegment(segment_reader)
                let metrics = segment_reader.search_regex(&METRIC_PREFIX, ".*")?;
                let result = metrics
                    .into_iter()
                    .map(|met| met.0)
                    .collect::<Vec<String>>();
                Ok(result)
            })
            .collect();

        match result {
            Ok(_) => {
                let mut res = Vec::new();
                for r in result {
                    for er in r {
                        for s in er {
                            res.push(s);
                        }
                    }
                }
                info!(
                    "Time to get all metrics {:?} and num metrics {:?} ",
                    SystemTime::now().duration_since(curr_time).unwrap(),
                    res.len()
                );
                Ok(res)
            }
            Err(e) => Err(e),
        }
    }

    pub fn search(&mut self, _segment_pool: &rayon::ThreadPool) -> Result<()> {
        // let mut results = HashMap::new();
        // for query in self.batch_query.queries.iter() {
        let curr_time = SystemTime::now();
        let _result = match self.query.query_type {
            QueryType::METRICS => QueryRunner::search_in_segment(
                &METRIC_PREFIX,
                &METRIC_PREFIX,
                &self.query.filter,
                &self.query.start,
                &self.query.end,
                &mut self.segment_readers,
            )?,
            QueryType::TAG_KEYS => QueryRunner::search_in_segment(
                &TAG_KEY_PREFIX,
                &TAG_KEY_PREFIX,
                &self.query.filter,
                &self.query.start,
                &self.query.end,
                &mut self.segment_readers,
            )?,
            QueryType::TAG_KEYS_AND_VALUES => QueryRunner::search_in_segment(
                &self.query.group.get(0).unwrap(), //TODO error handling
                &TAG_VALUE_PREFIX,
                &self.query.filter,
                &self.query.start,
                &self.query.end,
                &mut self.segment_readers,
            )?,
            _ => {
                return Err(MystError::new_query_error("Unknown query type"));
            }
        };

        Ok(())
    }

    pub fn search_in_segment(
        key: &str,
        _type: &str,
        filter: &QueryFilter,
        query_start: &u64,
        query_end: &u64,
        segment_readers: &mut Vec<SegmentReader<R>>,
    ) -> Result<()> {
        let segment_results: Result<Vec<Vec<String>>> = segment_readers
            .par_iter_mut()
            .map(|segment_reader| {
                info!(
                    "Running segment query for segment {:?} with thread {:?}",
                    segment_reader.created,
                    thread::current().id()
                );

                let filtered_bitmap =
                    QueryRunner::filter(&filter, query_start, query_end, segment_reader)?;
                let terms = segment_reader.search_regex(key, ".*")?;
                let mut is_tagkey = false;
                if key.eq(TAG_KEY_PREFIX) {
                    is_tagkey = true;
                }
                let terms_bitmap =
                    segment_reader.get_bitmaps_with_terms(_type, is_tagkey, terms)?;
                let mut count = 100;
                let mut result: Vec<String> = Vec::new();
                for bitmap in terms_bitmap {
                    if count < 0 {
                        break;
                    }
                    let matches = filtered_bitmap.and_cardinality(&bitmap.1);
                    if matches > 0 {
                        count = count - 1;
                        result.push(bitmap.0);
                    }
                }
                Ok(result)
            })
            .collect();
        let mut result = HashSet::new();
        for segment_result in segment_results? {
            result.extend(segment_result);
        }
        //Ok(MetaResult::MetricsOrTags { result: result })
        Ok(())
    }

    fn filter(
        query_filter: &QueryFilter,
        start: &u64,
        end: &u64,
        segment_reader: &mut SegmentReader<R>,
    ) -> Result<Bitmap> {
        let mut bitmap = match query_filter {
            QueryFilter::Chain {
                filters: _filters,
                op: _op,
            } => {
                let mut bitmaps: Vec<Bitmap> = Vec::new();
                let mut not_bitmaps: Vec<Bitmap> = Vec::new();
                if let QueryFilter::Chain { filters, op } = query_filter {
                    for filter in filters {
                        if let QueryFilter::NOT { filter } = filter {
                            not_bitmaps.push(QueryRunner::filter(
                                filter,
                                start,
                                end,
                                segment_reader,
                            )?);
                        } else {
                            bitmaps.push(QueryRunner::filter(filter, start, end, segment_reader)?);
                        }
                    }
                    let bitmap = match op.as_str() {
                        "AND" => QueryRunner::<R>::intersection(bitmaps, Some(not_bitmaps)),
                        "OR" => QueryRunner::<R>::union(&mut bitmaps),
                        _ => Err(MystError::new_query_error(
                            "Not a valid operator. AND OR allowed",
                        )),
                    };
                    bitmap
                } else {
                    Err(MystError::new_query_error(
                        "No chain filter found in Chain filter",
                    ))
                }
            }
            QueryFilter::ExplicitTags { filter, count: _ } => {
                return QueryRunner::filter(filter, start, end, segment_reader);
            }
            QueryFilter::NOT { filter } => {
                let bitmap = QueryRunner::filter(filter, start, end, segment_reader)?;
                Ok(bitmap)
            }
            QueryFilter::Metric { metric, _type } => {
                let bitmap = QueryRunner::filter_concrete(
                    segment_reader,
                    &METRIC_PREFIX,
                    &metric,
                    _type,
                    &METRIC_PREFIX,
                )?;
                Ok(bitmap)
            }
            QueryFilter::TagKey { filter, _type } => {
                let bitmap = QueryRunner::filter_concrete(
                    segment_reader,
                    &TAG_KEY_PREFIX,
                    &filter,
                    _type,
                    &TAG_KEY_PREFIX,
                )?;
                Ok(bitmap)
            }
            QueryFilter::TagValue { key, filter, _type } => {
                let bitmap = QueryRunner::filter_concrete(
                    segment_reader,
                    &key,
                    &filter,
                    _type,
                    &TAG_VALUE_PREFIX,
                )?;
                Ok(bitmap)
            }
        };
        QueryRunner::filter_by_time(start, end, segment_reader, bitmap?)
        // bitmap
    }

    fn filter_by_time(
        start: &u64,
        end: &u64,
        segment_reader: &mut SegmentReader<R>,
        mut filtered_bitmap: Bitmap,
    ) -> Result<Bitmap> {
        info!(
            "Before filtering by time, number of elements: {:?}",
            filtered_bitmap.cardinality()
        );
        let mut ts_bitmaps = segment_reader.get_all_ts_bitmaps()?;
        let mut final_ts_bitmap = Bitmap::create();
        for (epoch, ts_bitmap) in &mut ts_bitmaps {
            debug!(
                "Reading epoch from bitmap: {} {}",
                epoch,
                epoch_bitmap::EPOCH_DURATION
            );
            if start <= &(epoch + epoch_bitmap::EPOCH_DURATION) && end >= epoch {
                let ts_card = ts_bitmap.bitmap.cardinality();
                final_ts_bitmap.or_inplace(&ts_bitmap.bitmap);
                info!(
                    "Filtered in epoch from bitmap: {} {} {}",
                    epoch,
                    ts_card,
                    final_ts_bitmap.cardinality()
                );
            }
        }
        filtered_bitmap.and_inplace(&final_ts_bitmap);
        debug!(
            "After filtering by time, number of elements: {:?}",
            filtered_bitmap.cardinality()
        );
        Ok(filtered_bitmap)
    }

    fn filter_concrete(
        segment_reader: &mut SegmentReader<R>,
        key: &str,
        filter: &str,
        _type: &FilterType,
        field_type: &str,
    ) -> Result<Bitmap> {
        let terms = match _type {
            FilterType::Literal => segment_reader.search_literal(key, filter)?,
            FilterType::Regex => segment_reader.search_regex(key, filter)?,
        };
        let mut is_tagkey = false;
        if key.eq(TAG_KEY_PREFIX) {
            is_tagkey = true;
        }
        let mut bitmaps = segment_reader.get_bitmaps(field_type, is_tagkey, terms)?;
        QueryRunner::<R>::union(&mut bitmaps)
    }

    pub fn union(bitmaps: &mut Vec<Bitmap>) -> Result<Bitmap> {
        let curr_time = SystemTime::now();
        let mut union_bitmap = Bitmap::create();
        match bitmaps.len() {
            0 => {}
            1 => {
                union_bitmap = bitmaps.remove(0);
            }
            _ => {
                for i in 0..bitmaps.len() {
                    union_bitmap.or_inplace(&bitmaps.get(i).unwrap());
                }
            }
        }

        info!(
            "Time for union {:?}",
            SystemTime::now().duration_since(curr_time).unwrap()
        );
        Ok(union_bitmap)
    }

    fn intersection(bitmaps: Vec<Bitmap>, not_bitmaps: Option<Vec<Bitmap>>) -> Result<Bitmap> {
        let mut bitmap = match bitmaps.len() {
            0 => {
                let curr_time = SystemTime::now();
                let intersection = Bitmap::create();
                info!(
                    "Time for intersection {:?}",
                    SystemTime::now().duration_since(curr_time).unwrap()
                );
                intersection
            }
            1 => {
                let intersection = bitmaps.get(0).unwrap().clone();
                intersection.clone()
            }
            _ => {
                let curr_time = SystemTime::now();
                let mut intersection = bitmaps.get(0).unwrap().clone();
                intersection.and_inplace(&bitmaps.get(1).unwrap());
                for i in 1..bitmaps.len() {
                    intersection.and_inplace(bitmaps.get(i).unwrap());
                }
                info!(
                    "Time for intersection {:?}",
                    SystemTime::now().duration_since(curr_time).unwrap()
                );
                intersection
            }
        };
        if not_bitmaps.is_some() {
            for nb in not_bitmaps.unwrap() {
                bitmap.andnot_inplace(&nb);
            }
        }
        Ok(bitmap)
    }

    pub fn get_all_tagkeys(&mut self) -> Result<Vec<String>> {
        let result: Result<Vec<_>> = self
            .segment_readers
            .par_iter_mut()
            .map(|segment_reader| {
                // Query::getAllMetricsInSegment(segment_reader)
                let metrics = segment_reader.search_regex(&TAG_KEY_PREFIX, ".*")?;
                let result = metrics
                    .into_iter()
                    .map(|met| met.0)
                    .collect::<Vec<String>>();
                Ok(result)
            })
            .collect();

        match result {
            Ok(_) => {
                let mut res = Vec::new();
                for r in result {
                    for er in r {
                        for s in er {
                            res.push(s);
                        }
                    }
                }
                Ok(res)
            }
            Err(e) => Err(e),
        }
    }

    pub fn get_all_tagvalues(&mut self, key: &str) -> Result<Vec<String>> {
        let result: Result<Vec<_>> = self
            .segment_readers
            .par_iter_mut()
            .map(|segment_reader| {
                // Query::getAllMetricsInSegment(segment_reader)
                let metrics = segment_reader.search_regex(&key, ".*")?;
                let result = metrics
                    .into_iter()
                    .map(|met| met.0)
                    .collect::<Vec<String>>();
                Ok(result)
            })
            .collect();

        match result {
            Ok(_) => {
                let mut res = Vec::new();
                for r in result {
                    for er in r {
                        for s in er {
                            res.push(s);
                        }
                    }
                }
                Ok(res)
            }
            Err(e) => Err(e),
        }
    }
}
