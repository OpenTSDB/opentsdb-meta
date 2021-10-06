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
use crate::segment::myst_segment::MystSegment;
use crate::segment::persistence::{Compactor, TimeSegmented};

use crate::segment::store::docstore::Timeseries;

use crate::utils::myst_error::{MystError, Result};

use std::collections::HashMap;

use std::rc::Rc;

impl Compactor for MystSegment {
    /// Compacts two segment:
    /// Merges the current segment `self` with the new segment `segment`
    /// and returns a compacted segment.
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
    fn compact(segments: Vec<MystSegment>) -> Result<MystSegment> {
        if segments.len() == 0 {
            return Err(MystError::new_write_error(
                "No segments found for compaction",
            ));
        }
        // TODO: Do in-place compaction. Mostly easy, just need to handle timeseries that repeat with multiple timestamps. Note - Do an additional dedup in draining clustered data.
        let first = segments.get(0).unwrap();
        let mut compacted_segment = MystSegment::new_with_block_entries_duration(
            first.shard_id,
            first.epoch,
            first.docstore_block_size as usize,
            0, // will be set after merging.
        );
        let mut duration = 0;
        for segment in segments {
            let mut i = 0;
            duration += segment.duration;
            for ts in &segment.data.data {
                let (metric, tags) = form_timeseries(ts, &segment);
                for (epoch, bitmap) in &segment.epoch_bitmap.epoch_bitmap {
                    if bitmap.contains(i) {
                        compacted_segment.add_timeseries(
                            metric.clone(),
                            tags.clone(),
                            ts.timeseries_id,
                            epoch.clone(),
                        );
                    }
                }
                i += 1;
            }
        }
        compacted_segment.set_duration(duration);
        Ok(compacted_segment)
    }
}

fn form_timeseries(
    timeseries: &Timeseries,
    segment: &MystSegment,
) -> (Rc<String>, HashMap<Rc<String>, Rc<String>>) {
    let mut tags = HashMap::new();
    for (k, v) in timeseries.tags.iter() {
        let key = segment.dict.dict.get(k).unwrap().clone();
        let val = segment.dict.dict.get(v).unwrap().clone();
        tags.insert(key, val);
    }
    let metric = segment.dict.dict.get(&*timeseries.metric).unwrap().clone();

    (metric, tags)
}
