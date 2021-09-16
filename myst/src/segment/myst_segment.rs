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

use crate::segment::persistence::Loader;
use crate::segment::persistence::TimeSegmented;
use crate::segment::persistence::{Builder, Compactor};
use crate::segment::store::dict::Dict;
use crate::segment::store::docstore::{DocStore, Timeseries};
use crate::segment::store::metric_bitmap::MetricBitmap;
use crate::segment::store::myst_fst::{MystFST, MystFSTContainer};
use crate::segment::store::tag_bitmap::{TagKeysBitmap, TagValuesBitmap};
use crate::utils::config::add_dir;

use crate::utils::myst_error::{MystError, Result};
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use croaring::Bitmap;
use log::{debug, info};
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

/// Segment for an epoch and shard.
/// When built, it writes all datastructures into a Writer `W`
/// and stores the offset of each datasturctures in a header `MystSegmentHeader`
pub struct MystSegment {
    pub fsts: MystFSTContainer,
    pub dict: Dict,
    pub tag_keys_bitmap: TagKeysBitmap,
    pub tag_vals_bitmap: TagValuesBitmap,
    pub metrics_bitmap: MetricBitmap,
    pub epoch_bitmap: EpochBitmap,
    pub data: DocStore,

    pub(crate) header: MystSegmentHeader,

    pub shard_id: u32,
    pub epoch: u64,

    pub(crate) uid: u32,
    pub(crate) segment_timeseries_id: u32,
    pub(crate) tag_key_prefix: Rc<String>,
    pub(crate) metric_prefix: Rc<String>,
    pub(crate) dedup: HashSet<u64>,

    pub(crate) cluster: Option<Clusterer>,

    pub(crate) duration: i32,
}

impl TimeSegmented for MystSegment {
    fn get_duration(&self) -> Option<i32> {
        Some(self.duration)
    }

    fn set_duration(&mut self, duration: i32) {
        self.duration = duration;
    }
}
/// Map that contains `MystSegmentHeaderKeys` and it's offset in the writer `W`

/// TODO: Move reader to the same place as data.
/// TODO: Add version and len.
#[derive(Default)]
pub struct MystSegmentHeader {
    pub header: HashMap<u32, u32>,
    pub segment_timeseries_id: u32,
    pub uid: u32,
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
    pub fn from(data: &[u8]) -> Result<Self> {
        let mut reader = Cursor::new(data);
        let mut map = HashMap::new();
        for _i in 0..10 {
            map.insert(
                reader.read_u32::<NetworkEndian>()?,
                reader.read_u32::<NetworkEndian>()?,
            );
        }
        let segment_timeseries_id = reader.read_u32::<NetworkEndian>()?;
        let uid = reader.read_u32::<NetworkEndian>()?;
        debug!("Read uid {} and segment id: {}", uid, segment_timeseries_id);
        let header = Self {
            header: map,
            segment_timeseries_id: segment_timeseries_id,
            uid: uid,
        };
        Ok(header)
    }
}

impl MystSegment {
    pub fn new(shard_id: u32, epoch: u64) -> Self {
        MystSegment::new_with_block_entries(shard_id, epoch, 50000 as usize)
    }

    pub fn new_with_block_entries(
        shard_id: u32,
        epoch: u64,
        docstore_block_entries: usize,
    ) -> Self {
        MystSegment::new_with_block_entries_duration(
            shard_id,
            epoch,
            docstore_block_entries,
            7200 as i32,
        )
    }

    /// Creates a new MystSegment
    /// # Arguments
    /// * `shard_id` - Shard id for the segment.
    /// * `epoch` - The start epoch of the segment.
    /// * `docstore_block_entries` - Number of entries in each docstore block
    /// * `duration` - The duration of data in the segment in seconds
    pub fn new_with_block_entries_duration(
        shard_id: u32,
        epoch: u64,
        docstore_block_entries: usize,
        duration: i32,
    ) -> Self {
        info!("Creating new segment for shard id {}", shard_id);
        Self {
            shard_id,
            epoch,

            fsts: MystFSTContainer::new(),
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
            duration: duration,
        }
    }

    fn add_tagval(&mut self, key: Rc<String>, val: Rc<String>, doc_id: u32) -> u32 {
        let fst = self
            .fsts
            .fsts
            .entry(key.clone())
            .or_insert_with(|| MystFST::init(key.clone()));

        if !fst.buf.contains_key(&val) {
            fst.insert(val.clone(), self.uid);
            self.dict.dict.insert(self.uid, val.clone());

            self.uid += 1;
        }
        let tagval_id = MystFST::get_id(*fst.buf.get_mut(&val).unwrap());

        tagval_id
    }

    fn add_tagkey(&mut self, key: Rc<String>, doc_id: u32) -> u32 {
        let tagkey_prefix = self.tag_key_prefix.clone();
        let fst = self
            .fsts
            .fsts
            .entry(tagkey_prefix.clone())
            .or_insert_with(|| MystFST::init(tagkey_prefix));

        if !fst.buf.contains_key(&key) {
            fst.insert(key.clone(), self.uid);
            self.dict.dict.insert(self.uid, key.clone());
            self.uid += 1;
        }
        let id = MystFST::get_id(*fst.buf.get(&key).unwrap());

        id
    }

    fn add_metric(&mut self, metric: Rc<String>, doc_id: u32) -> u32 {
        let metric_prefix = self.metric_prefix.clone();
        let fst = self
            .fsts
            .fsts
            .entry(metric_prefix.clone())
            .or_insert_with(|| MystFST::init(metric_prefix));

        if !fst.buf.contains_key(&metric) {
            fst.insert(metric.clone(), self.uid);
            self.dict.dict.insert(self.uid, metric.clone());
            self.uid += 1;
        }
        let id = MystFST::get_id(*fst.buf.get(&metric).unwrap());

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
        if !self.dedup.contains(&xxhash) {
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

    pub(crate) fn drain_clustered_data(&mut self) -> Result<()> {
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
        let mut filename = add_dir(filename, "segment".to_string());
        let ext = String::from(".myst");
        filename.push_str(&ext);
        filename
    }

    pub fn get_path_prefix(shard_id: &u32, created: &u64, mut data_path: String) -> String {
        let data_path = add_dir(data_path, shard_id.to_string());
        let data_path = add_dir(data_path, created.to_string());
        if !Path::new(&data_path).exists() {
            fs::create_dir_all(Path::new(&data_path)).unwrap();
        }
        data_path
    }
}

/// Groups the timeseries so that all timeseries for a metric are close to each other.
#[derive(Default, Debug)]
pub struct Clusterer {
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
