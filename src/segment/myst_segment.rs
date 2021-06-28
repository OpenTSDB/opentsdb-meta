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

use crate::segment::persistence::Builder;
use crate::segment::persistence::Loader;
use crate::segment::store::dict::Dict;
use crate::segment::store::docstore::{DocStore, Timeseries};
use crate::segment::store::metric_bitmap::MetricBitmap;
use crate::segment::store::tag_bitmap::{TagKeysBitmap, TagValuesBitmap};
use crate::segment::store::yamas_fst::{YamasFST, YamasFSTContainer};

use crate::utils::myst_error::{MystError, Result};
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use croaring::Bitmap;
use log::info;
use std::collections::{HashMap, HashSet};

pub use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::rc::Rc;

use std::sync::Arc;

use std::fs;

use crate::segment::segment_reader::SegmentReader;
use crate::segment::store::epoch_bitmap::EpochBitmap;
use num::ToPrimitive;
use num_derive::FromPrimitive;
use num_derive::ToPrimitive;
use std::io::{Seek, SeekFrom};
use yamas_metrics_rs::count;

/// Segment for an epoch and shard.
/// When built, it writes all datastructures into a Writer `W`
/// and stores the offset of each datasturctures in a header `MystSegmentHeader`
pub struct MystSegment {
    pub fsts: YamasFSTContainer,
    pub dict: Dict,
    pub tag_keys_bitmap: TagKeysBitmap,
    pub tag_vals_bitmap: TagValuesBitmap,
    pub metrics_bitmap: MetricBitmap,
    pub epoch_bitmap: EpochBitmap,
    pub data: DocStore,

    header: MystSegmentHeader,

    pub shard_id: u32,
    pub epoch: u64,

    uid: u32,
    segment_timeseries_id: u32,
    tag_key_prefix: Rc<String>,
    metric_prefix: Rc<String>,
    dedup: HashSet<u64>, //TODO remove

    cluster: Option<Clusterer>,
}

/// Map that contains `MystSegmentHeaderKeys` and it's offset in the writer `W`
#[derive(Default)]
pub struct MystSegmentHeader {
    header: HashMap<u32, u32>,
}

/// Maps each datastructure to a integer for efficiency.
#[derive(FromPrimitive, ToPrimitive)]
pub enum MystSegmentHeaderKeys {
    MetricBitmap = 1,
    TagKeysBitmap = 2,
    TagValsBitmap = 3,
    EpochBitmap = 4,
    EpochBitmapHeader = 5,
    Dict = 6,
    Docstore = 7,
    DocstoreHeader = 8,
    Fst = 9,
    FstHeader = 10,
}

impl MystSegmentHeader {
    pub fn deserialize_header(data: &[u8]) -> Result<HashMap<u32, u32>> {
        let mut reader = Cursor::new(data);
        let mut map = HashMap::new();
        for _i in 0..10 {
            map.insert(
                reader.read_u32::<NativeEndian>()?,
                reader.read_u32::<NativeEndian>()?,
            );
        }
        Ok(map)
    }
}

impl<W: Write> Builder<W> for MystSegment {
    /// Builds the MystSegment.
    /// Order of the segment is as follows.
    /// 1. Metric Bitmaps
    /// 2. Tag Keys Bitmaps
    /// 3. Tag Values Bitmaps
    /// 4. Epoch Bitmaps
    /// 5. Epoch Bitmap header
    /// 6. Dictionary
    /// 7. Docstore
    /// 8. Docstore header
    /// 9. FST
    /// 10. FST Header
    /// 11. Myst Segment Header
    /// The offset before start of each structure is written to header `MystSegmentHeader`
    ///
    fn build(mut self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        info!(
            "Building segment for {:?} and {:?}",
            self.shard_id, self.epoch
        );
        &self.drain_clustered_data();

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::MetricBitmap).unwrap(),
            *offset,
        );
        self.metrics_bitmap.fsts = Some(self.fsts.fsts);

        let ret = self
            .metrics_bitmap
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::TagKeysBitmap).unwrap(),
            *offset,
        );
        self.tag_keys_bitmap.fsts = ret.fsts;

        let ret = self
            .tag_keys_bitmap
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::TagValsBitmap).unwrap(),
            *offset,
        );

        self.tag_vals_bitmap.fsts = ret.fsts;
        let mut ret = self
            .tag_vals_bitmap
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.fsts.fsts = ret
            .fsts
            .take()
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::EpochBitmap).unwrap(),
            *offset,
        );
        let ts_bitmap = self
            .epoch_bitmap
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::EpochBitmapHeader).unwrap(),
            *offset,
        );
        ts_bitmap
            .header
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::Dict).unwrap(),
            *offset,
        );
        self.dict.build(buf, offset)?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::Docstore).unwrap(),
            *offset,
        );

        let ret = self
            .data
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;
        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::DocstoreHeader).unwrap(),
            *offset,
        );
        ret.header.build(buf, offset)?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::Fst).unwrap(),
            *offset,
        );
        let ret = self
            .fsts
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::FstHeader).unwrap(),
            *offset,
        );
        ret.header.build(buf, offset)?;
        for (k, v) in self.header.header {
            buf.write_u32::<NativeEndian>(k)?;
            buf.write_u32::<NativeEndian>(v)?;
        }

        info!(
            "Done building segment for {:?} and {:?}",
            self.shard_id, self.epoch
        );
        Ok(None)
    }
}

impl<R: Read + Seek> Loader<R, MystSegment> for MystSegment {
    /// Loads and deserializes the buffer `buf` and returns a MystSegment.
    ///
    fn load(mut self, buf: &mut R, offset: &u32) -> Result<Option<MystSegment>> {
        //        let mut buffered_reader = BufReader::new(buf);
        // Read header first -- seek from end
        let segment_header = SegmentReader::read_segment_header(buf)?;
        // load fst
        let fst_offset = segment_header
            .get(&ToPrimitive::to_u32(&MystSegmentHeaderKeys::FstHeader).unwrap())
            .unwrap();
        let mut fst_container = YamasFSTContainer::new();
        fst_container = fst_container.load(buf, fst_offset)?.unwrap();
        // load metric bit map
        let mut metrics_bitmap = MetricBitmap::new();
        let metric_fst = fst_container.fsts.get(&self.metric_prefix).unwrap();
        for (metric_name, offset_id) in &metric_fst.buf {
            let offset = YamasFST::get_offset(*offset_id);
            let bitmap = SegmentReader::get_bitmap_from_reader(buf, offset as u64)?;
            metrics_bitmap
                .metrics_bitmap
                .insert(metric_name.clone(), bitmap);
        }
        // load tag key bitmap
        let mut tag_keys_bitmap = TagKeysBitmap::new();
        let tag_key_fst = fst_container.fsts.get(&self.tag_key_prefix).unwrap();
        for (tag_key, offset_id) in &tag_key_fst.buf {
            let offset = YamasFST::get_offset(*offset_id);
            let bitmap = SegmentReader::get_bitmap_from_reader(buf, offset as u64)?;
            tag_keys_bitmap
                .tag_keys_bitmap
                .insert(tag_key.clone(), bitmap);
        }
        // load tag val bitmap
        let mut tag_vals_bitmap = TagValuesBitmap::new();
        let tag_key_fst = fst_container.fsts.get(&self.tag_key_prefix).unwrap();
        for (tag_key, offset_id) in &tag_key_fst.buf {
            let tag_val_fst = fst_container.fsts.get(tag_key).unwrap();
            let mut curr_tag_vals_bitmaps = HashMap::new();
            for (tag_val, offset_id) in &tag_val_fst.buf {
                let offset = YamasFST::get_offset(*offset_id);
                let bitmap = SegmentReader::get_bitmap_from_reader(buf, offset as u64)?;
                curr_tag_vals_bitmaps.insert(tag_val.clone(), bitmap);
            }
            tag_vals_bitmap
                .tag_vals_bitmap
                .insert(tag_key.clone(), curr_tag_vals_bitmaps);
        }
        // load dict
        let dict_offset = segment_header
            .get(&ToPrimitive::to_u32(&MystSegmentHeaderKeys::Dict).unwrap())
            .ok_or(MystError::new_query_error("No valid dict offset found"))?;
        let mut dict = Dict::new();
        dict = dict.load(buf, dict_offset)?.unwrap();
        // load doc store
        let docstore_header_offset = segment_header
            .get(&ToPrimitive::to_u32(&MystSegmentHeaderKeys::DocstoreHeader).unwrap())
            .ok_or(MystError::new_query_error(
                "No docstore header offset found",
            ))?;
        let mut docstore = DocStore::new(0);
        docstore = docstore.load(buf, docstore_header_offset)?.unwrap();

        let ts_bitmaps_header_offset = segment_header
            .get(&ToPrimitive::to_u32(&MystSegmentHeaderKeys::EpochBitmapHeader).unwrap())
            .ok_or(MystError::new_query_error(
                "No Timeseries Bitmap header offset found",
            ))?;
        let mut timeseries_bitmap = EpochBitmap::new(0);
        timeseries_bitmap = timeseries_bitmap
            .load(buf, ts_bitmaps_header_offset)?
            .unwrap();
        // TODO: Doc store block entries should be segment specific ? (and not global ?)

        let myst_segment = MystSegment {
            fsts: fst_container,
            dict,
            tag_keys_bitmap,
            tag_vals_bitmap,
            metrics_bitmap,
            epoch_bitmap: timeseries_bitmap,
            data: docstore,
            header: Default::default(),
            shard_id: 0,
            epoch: 0,
            uid: 0,
            segment_timeseries_id: 0,
            metric_prefix: Rc::new(String::from(crate::utils::config::METRIC)),
            tag_key_prefix: Rc::new(String::from(crate::utils::config::TAG_KEYS)),
            dedup: HashSet::new(),
            cluster: None,
        };
        Ok(Some(myst_segment))
    }
}

impl MystSegment {
    /// Creates a new MystSegment
    /// # Arguments
    /// * `shard_id` - Shard id for the segment.
    /// * `epoch` - The start epoch of the segment.
    /// * `docstore_block_entries` - Number of entries in each docstore block
    pub fn new(shard_id: u32, epoch: u64, docstore_block_entries: usize) -> Self {
        info!("Creating new segment for shard id {}", shard_id);
        Self {
            shard_id,
            epoch,

            fsts: YamasFSTContainer::new(),
            dict: Dict::new(),
            tag_keys_bitmap: TagKeysBitmap::new(),
            tag_vals_bitmap: TagValuesBitmap::new(),
            metrics_bitmap: MetricBitmap::new(),
            epoch_bitmap: EpochBitmap::new(epoch),
            data: DocStore::new(docstore_block_entries),

            header: MystSegmentHeader::default(),

            uid: 0,
            segment_timeseries_id: 0,
            metric_prefix: Rc::new(String::from(crate::utils::config::METRIC)),
            tag_key_prefix: Rc::new(String::from(crate::utils::config::TAG_KEYS)),
            dedup: HashSet::new(), // TODO remove
            cluster: Some(Clusterer::default()),
        }
    }

    fn add_tagval(&mut self, key: Rc<String>, val: Rc<String>, doc_id: u32) -> u32 {
        let fst = self
            .fsts
            .fsts
            .entry(key.clone())
            .or_insert_with(|| YamasFST::init(key.clone()));

        if !fst.buf.contains_key(&val) {
            fst.insert(val.clone(), self.uid);
            self.dict.dict.insert(self.uid, val.clone());

            self.uid += 1;
        }
        let tagval_id = YamasFST::get_id(*fst.buf.get_mut(&val).unwrap());

        tagval_id
    }

    fn add_tagkey(&mut self, key: Rc<String>, doc_id: u32) -> u32 {
        let tagkey_prefix = self.tag_key_prefix.clone();
        let fst = self
            .fsts
            .fsts
            .entry(tagkey_prefix.clone())
            .or_insert_with(|| YamasFST::init(tagkey_prefix));

        if !fst.buf.contains_key(&key) {
            fst.insert(key.clone(), self.uid);
            self.dict.dict.insert(self.uid, key.clone());
            self.uid += 1;
        }
        let id = YamasFST::get_id(*fst.buf.get(&key).unwrap());

        id
    }

    fn add_metric(&mut self, metric: Rc<String>, doc_id: u32) -> u32 {
        let metric_prefix = self.metric_prefix.clone();
        let fst = self
            .fsts
            .fsts
            .entry(metric_prefix.clone())
            .or_insert_with(|| YamasFST::init(metric_prefix));

        if !fst.buf.contains_key(&metric) {
            fst.insert(metric.clone(), self.uid);
            self.dict.dict.insert(self.uid, metric.clone());
            self.uid += 1;
        }
        let id = YamasFST::get_id(*fst.buf.get(&metric).unwrap());

        id
    }

    fn add_to_bitmap(&mut self, metric_id: &u32, tags_ids: Arc<HashMap<u32, u32>>, doc_id: u32) {
        let metric = self.dict.dict.get(&metric_id).unwrap().clone();
        self.init_metric(Rc::clone(&metric));
        let bitmap = self.metrics_bitmap.metrics_bitmap.get_mut(&metric);
        if bitmap.is_some() {
            bitmap.unwrap().add(doc_id);
        }

        for (key_id, val_id) in tags_ids.iter() {
            let key = self.dict.dict.get(&key_id).unwrap().clone();
            let val = self.dict.dict.get(&val_id).unwrap().clone();
            self.init_tagkey(Rc::clone(&key));
            let bitmap = self.tag_keys_bitmap.tag_keys_bitmap.get_mut(&key);
            if bitmap.is_some() {
                bitmap.unwrap().add(doc_id);
            }

            self.init_tagval(Rc::clone(&key));
            let values_bitmap = self.tag_vals_bitmap.tag_vals_bitmap.get_mut(&key);
            if values_bitmap.is_some() {
                let bitmap = values_bitmap
                    .unwrap()
                    .entry(Rc::clone(&val))
                    .or_insert_with(|| Bitmap::create());
                bitmap.add(doc_id);
            }
        }
        //println!("Bitmap = {:?}", &bitmap);
    }

    fn init_tagkey(&mut self, key: Rc<String>) {
        self.tag_keys_bitmap
            .tag_keys_bitmap
            .entry(Rc::clone(&key))
            .or_insert_with(|| Bitmap::create());
        self.tag_vals_bitmap
            .tag_vals_bitmap
            .entry(Rc::clone(&key))
            .or_insert_with(|| HashMap::new());
    }

    fn init_tagval(&mut self, key: Rc<String>) {
        self.tag_vals_bitmap
            .tag_vals_bitmap
            .entry(Rc::clone(&key))
            .or_insert_with(|| HashMap::new());
    }

    fn init_metric(&mut self, key: Rc<String>) {
        self.metrics_bitmap
            .metrics_bitmap
            .entry(Rc::clone(&key))
            .or_insert_with(|| Bitmap::create());
    }

    /// Adds the timeseries to the segment
    /// # Arguments
    /// * `metric` - Metric of the timeseries.
    /// * `tags` - Tags of the timeseries.
    /// * `xxhash` - The hash for the timeseries.
    /// * `timestamp` - Timestamp for the timeseries.
    pub fn add_timeseries(
        &mut self,
        metric: Rc<String>,
        tags: HashMap<Rc<String>, Rc<String>>,
        xxhash: u64,
        timestamp: u64,
    ) {
        count!("timeseries_ingested", "host" => "eventsdb-3.yms.gq1.yahoo.com", "shard" => self.shard_id.to_string());
        if !self.dedup.contains(&xxhash) {
            count!("timeseries_ingested_dedup", "host" => "eventsdb-3.yms.gq1.yahoo.com", "shard" => self.shard_id.to_string());
            count!("dedup_size", "host" => "eventsdb-3.yms.gq1.yahoo.com", "shard" => self.shard_id.to_string());
            let _metric_id = self.add_metric(metric.clone(), self.segment_timeseries_id);
            let mut tags_ids = HashMap::new();
            for (k, v) in tags {
                let tagkey_id = self.add_tagkey(k.clone(), self.segment_timeseries_id);
                let tagval_id = self.add_tagval(k.clone(), v.clone(), self.segment_timeseries_id);
                tags_ids.insert(tagkey_id, tagval_id);
                self.add_tagkey(k.clone(), self.segment_timeseries_id);
                self.add_tagval(k.clone(), v.clone(), self.segment_timeseries_id);
            }
            // self.segment_timeseries_id += 1;
            let tags_ids_arc = Arc::new(tags_ids);
            self.cluster.as_mut().unwrap().add_timeseries(
                _metric_id,
                tags_ids_arc.clone(),
                xxhash,
                timestamp,
            );

            self.dedup.insert(xxhash);
        }
    }

    fn drain_clustered_data(&mut self) -> Result<()> {
        let cluster = self.cluster.take().unwrap().cluster;
        for (metric, tags) in cluster {
            for ts in tags {
                let xxhash = ts.xxHash;
                let tags = ts.tags.clone();
                self.add_to_bitmap(&metric, tags.clone(), self.segment_timeseries_id);
                self.epoch_bitmap
                    .add_timeseries(self.segment_timeseries_id, ts.timestamp)?;

                self.data.data.push(
                    // doc_id,
                    Timeseries {
                        timeseries_id: xxhash,
                        tags: tags,
                    },
                );
                self.segment_timeseries_id += 1;
            }
        }
        Ok(())
    }

    pub fn get_segment_filename(shard_id: &u32, created: &u64, data_root_path: String) -> String {
        let mut filename = MystSegment::get_path_prefix(shard_id, created, data_root_path);
        filename.push_str("segment");
        let ext = String::from(".myst");
        filename.push_str(&ext);
        filename
    }

    pub fn get_path_prefix(shard_id: &u32, created: &u64, mut data_path: String) -> String {
        data_path.push_str(&shard_id.to_string());
        data_path.push_str("/");
        data_path.push_str(&created.to_string());
        data_path.push_str(&String::from("/"));
        if !Path::new(&data_path).exists() {
            fs::create_dir_all(Path::new(&data_path)).unwrap();
        }
        data_path
    }
}

/// Groups the timeseries so that all timeseries for a metric are close to each other.
#[derive(Default, Debug)]
struct Clusterer {
    cluster: HashMap<u32, Vec<TagsAndTimestamp>>,
}
#[derive(Default, Debug)]
struct TagsAndTimestamp {
    tags: Arc<HashMap<u32, u32>>,
    timestamp: u64,
    xxHash: u64,
}

impl Clusterer {
    pub fn add_timeseries(
        &mut self,
        metric: u32,
        tags: Arc<HashMap<u32, u32>>,
        xxHash: u64,
        timestamp: u64,
    ) {
        self.cluster
            .entry(metric)
            .or_insert(Vec::new())
            .push(TagsAndTimestamp {
                tags,
                timestamp,
                xxHash,
            });
    }
}
