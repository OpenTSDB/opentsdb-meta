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
use crate::segment::myst_segment::{Clusterer, MystSegment, MystSegmentHeaderKeys};
use crate::segment::persistence::Loader;
use crate::segment::segment_reader::SegmentReader;
use crate::segment::store::dict::Dict;
use crate::segment::store::docstore::DocStore;
use crate::segment::store::epoch_bitmap::EpochBitmap;
use crate::segment::store::metric_bitmap::MetricBitmap;
use crate::segment::store::myst_fst::{MystFST, MystFSTContainer};
use crate::segment::store::tag_bitmap::{TagKeysBitmap, TagValuesBitmap};
use crate::utils::myst_error::{MystError, Result};
use log::info;
use num_traits::ToPrimitive;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek};
use std::rc::Rc;

impl<R: Read + Seek> Loader<R, MystSegment> for MystSegment {
    /// Loads and deserializes the buffer `buf` and returns a MystSegment.
    ///
    fn load(mut self, buf: &mut R, offset: &u32) -> Result<Option<MystSegment>> {
        //        let mut buffered_reader = BufReader::new(buf);
        // Read header first -- seek from end

        let full_segment_header = SegmentReader::read_segment_header(buf)?;

        let segment_header = full_segment_header.header;
        // load fst
        let fst_offset = segment_header
            .get(&ToPrimitive::to_u32(&MystSegmentHeaderKeys::FstHeader).unwrap())
            .unwrap();
        let mut fst_container = MystFSTContainer::new();
        fst_container = fst_container.load(buf, fst_offset)?.unwrap();
        // load metric bit map
        let mut metrics_bitmap = MetricBitmap::new();
        let metric_fst = fst_container.fsts.get(&self.metric_prefix).unwrap();
        for (metric_name, offset_id) in &metric_fst.buf {
            let offset = MystFST::get_offset(*offset_id);
            let bitmap = SegmentReader::get_bitmap_from_reader(buf, offset as u64)?;
            metrics_bitmap
                .metrics_bitmap
                .insert(metric_name.clone(), bitmap);
        }
        // load tag key bitmap
        let mut tag_keys_bitmap = TagKeysBitmap::new();
        let tag_key_fst = fst_container.fsts.get(&self.tag_key_prefix).unwrap();
        for (tag_key, offset_id) in &tag_key_fst.buf {
            let offset = MystFST::get_offset(*offset_id);
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
                let offset = MystFST::get_offset(*offset_id);
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

        /// Load xxhashes into dedup map.
        let mut dedup: HashSet<u64> = HashSet::new();

        let doc_vec = &docstore.data;

        for t in doc_vec {
            dedup.insert(t.timeseries_id);
        }

        let ts_bitmaps_header_offset = segment_header
            .get(&ToPrimitive::to_u32(&MystSegmentHeaderKeys::EpochBitmapHeader).unwrap())
            .ok_or(MystError::new_query_error(
                "No Timeseries Bitmap header offset found",
            ))?;
        let mut timeseries_bitmap = EpochBitmap::new(0);
        timeseries_bitmap = timeseries_bitmap
            .load(buf, ts_bitmaps_header_offset)?
            .unwrap();
        /// TODO: Doc store block entries should be segment specific ? (and not global ?)
        /// NOTE: It is ok to have a new clusterer because,
        /// the exists check is performed while draining the clusters.
        let last_id = full_segment_header.segment_timeseries_id;
        let uid = full_segment_header.uid;
        info!(
            "Loading segment for shard: {} and epoch: {} with last segment id: {} uid: {}",
            self.shard_id, self.epoch, last_id, uid
        );
        let myst_segment = MystSegment {
            fsts: fst_container,
            dict,
            tag_keys_bitmap,
            tag_vals_bitmap,
            metrics_bitmap,
            epoch_bitmap: timeseries_bitmap,
            data: docstore,
            header: Default::default(),
            shard_id: self.shard_id,
            epoch: self.epoch,
            uid: uid,
            segment_timeseries_id: last_id,
            metric_prefix: Rc::new(String::from(crate::utils::config::METRIC)),
            tag_key_prefix: Rc::new(String::from(crate::utils::config::TAG_KEYS)),
            dedup: dedup,
            cluster: Some(Clusterer::default()),
            duration: self.duration,
        };
        Ok(Some(myst_segment))
    }
}
