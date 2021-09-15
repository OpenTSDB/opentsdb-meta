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
use crate::segment::myst_segment::{MystSegment, MystSegmentHeaderKeys};
use crate::segment::persistence::Compactor;
use crate::segment::segment_reader::SegmentReader;
use crate::segment::store::dict::Dict;
use crate::segment::store::docstore::{DocStore, Timeseries};
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
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

impl Compactor for MystSegment {
    /// Compacts two segment:
    /// Merges the current segment `self` with the new segment `segment`
    /// 1. FST - Go over the fst's of the new segment, identify missing terms
    /// Add the missing terms to the corresponding fst and dict.
    /// 2. For Metric, Tags and Epoch Bitmaps - Go over the bitmaps and either
    /// add them to the map or merge them with the existing bitmap.
    /// 3.Docstore - Add the current segments timeseries to the dedup set
    /// For each timeseries in the new segment that is not present in the dedup
    /// Create the tags hashmap based on the id's from the dict after they are merged in step 1
    /// add the new segment's timeseries with the new tags map to current segment.
    /// # Arguments
    /// * `segment` - The segment that is to be current with the current.
    fn compact(&mut self, segment: MystSegment) {
        //compact fsts
        for (key, myst_fst) in segment.fsts.fsts {
            if !self.fsts.fsts.contains_key(&key) {
                self.fsts.fsts.insert(key, myst_fst);
            } else {
                let mut curr_myst_fst = self.fsts.fsts.get_mut(&key).unwrap();
                for (key, val) in myst_fst.buf.iter() {
                    if !curr_myst_fst.buf.contains_key(key) {
                        self.uid += 1;
                        let new_val = MystFST::generate_val(self.uid, 0);
                        curr_myst_fst.buf.insert(key.clone(), new_val);
                        self.dict.dict.insert(self.uid, key.clone());
                    }
                }
            }
        }
        // compact bitmaps
        // compact metrics bitmapss
        for (key, bitmap) in segment.metrics_bitmap.metrics_bitmap {
            if !self.metrics_bitmap.metrics_bitmap.contains_key(&key) {
                self.metrics_bitmap.metrics_bitmap.insert(key, bitmap);
            } else {
                let mut curr_bitmap = self.metrics_bitmap.metrics_bitmap.get_mut(&key).unwrap();
                curr_bitmap.add_many(&bitmap.to_vec());
            }
        }

        //compact tagkeys bitmap
        for (key, bitmap) in segment.tag_keys_bitmap.tag_keys_bitmap {
            if !self.tag_keys_bitmap.tag_keys_bitmap.contains_key(&key) {
                self.tag_keys_bitmap.tag_keys_bitmap.insert(key, bitmap);
            } else {
                let mut curr_bitmap = self.tag_keys_bitmap.tag_keys_bitmap.get_mut(&key).unwrap();
                curr_bitmap.add_many(&bitmap.to_vec());
            }
        }

        //compact tag vals bitmap
        for (key, val_bitmap) in segment.tag_vals_bitmap.tag_vals_bitmap {
            if !self.tag_vals_bitmap.tag_vals_bitmap.contains_key(&key) {
                self.tag_vals_bitmap.tag_vals_bitmap.insert(key, val_bitmap);
            } else {
                for (value, bitmap) in val_bitmap {
                    let mut curr_val_bitmap =
                        self.tag_vals_bitmap.tag_vals_bitmap.get_mut(&key).unwrap();
                    if !curr_val_bitmap.contains_key(&value) {
                        curr_val_bitmap.insert(value, bitmap);
                    } else {
                        let curr_bitmap = curr_val_bitmap.get_mut(&value).unwrap();
                        curr_bitmap.add_many(&bitmap.to_vec());
                    }
                }
            }
        }

        //compact epoch bitmap
        for (key, bitmap) in segment.epoch_bitmap.epoch_bitmap {
            if !self.epoch_bitmap.epoch_bitmap.contains_key(&key) {
                self.epoch_bitmap.epoch_bitmap.insert(key, bitmap);
            } else {
                let mut curr_bitmap = self.epoch_bitmap.epoch_bitmap.get_mut(&key).unwrap();
                curr_bitmap.add_many(&bitmap.to_vec());
            }
        }

        //compact docstore
        // Load xxhashes into dedup map.
        let mut dedup: HashSet<u64> = HashSet::new();

        let doc_vec = &self.data.data;

        for t in doc_vec {
            dedup.insert(t.timeseries_id);
        }

        for timeseries in segment.data.data {
            if !dedup.contains(&timeseries.timeseries_id) {
                //convert tags in timeseries that have old dict id's to new dict id's
                let mut tags = HashMap::new();
                for (key, val) in timeseries.tags.iter() {
                    let key_str = segment.dict.dict.get(key).unwrap();
                    let val_str = segment.dict.dict.get(val).unwrap();
                    let mut new_key_id = None;
                    let mut new_val_id = None;

                    let tag_keys_fst = self.fsts.fsts.get(&self.tag_key_prefix).unwrap();
                    if tag_keys_fst.buf.contains_key(key_str) {
                        new_key_id = Some(*tag_keys_fst.buf.get(key_str).unwrap());
                        let tag_vals_fst = self.fsts.fsts.get(key_str).unwrap();
                        if tag_vals_fst.buf.contains_key(val_str) {
                            new_val_id = Some(*tag_vals_fst.buf.get(val_str).unwrap());
                        }
                    }
                    if new_key_id == None || new_val_id == None {
                        panic!("Should not happen!! Either Tag Key or Value is missing in the merged fst. ");
                    }

                    tags.insert(
                        MystFST::get_id(new_key_id.unwrap()),
                        MystFST::get_id(new_val_id.unwrap()),
                    );
                }
                let new_timeseries = Timeseries {
                    timeseries_id: timeseries.timeseries_id,
                    tags: Arc::new(tags),
                };
                self.data.data.push(new_timeseries);
            }
        }
    }
}
