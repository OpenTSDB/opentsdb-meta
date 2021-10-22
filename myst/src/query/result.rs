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

use croaring::Bitmap;

use fasthash::xx::Hash64;
use fasthash::FastHash;
use std::collections::hash_map::IntoIter;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;

#[derive(Debug)]
pub enum MetaResult {
    Timeseries {
        timeseries: crate::myst_grpc::TimeseriesResponse,
    },
    MetricsOrTags {
        result: HashSet<String>,
    },
}

/// Contains the timeseries hashes and the group strings.
#[derive(Default)]
pub struct StringGroupedTimeseries {
    pub group: Vec<String>,
    pub timeseries: HashMap<i64, Timeseries>,
}

pub struct Timeseries {
    pub xxhash: i64,
    pub bitmap: Bitmap,
}

#[derive(Default)]
pub struct StringTimeseriesResponse {
    pub groups: HashMap<u64, StringGroupedTimeseries>,
    pub dict: HashMap<u64, String>,
}

impl FromIterator<StringTimeseriesResponse> for StringTimeseriesResponse {
    fn from_iter<T: IntoIterator<Item = StringTimeseriesResponse>>(iter: T) -> Self {
        let mut result = HashMap::new();
        for str in iter {
            for (k, v) in str.groups {
                if !result.contains_key(&k) {
                    result.insert(k, crate::query::result::StringGroupedTimeseries::default());
                    result.get_mut(&k).unwrap().group.extend(v.group);
                }

                result.get_mut(&k).unwrap().add_all(v.timeseries);
            }
        }

        StringTimeseriesResponse {
            groups: result,
            dict: HashMap::new(),
        }
    }
}

impl IntoIterator for StringTimeseriesResponse {
    type Item = (u64, StringGroupedTimeseries);
    type IntoIter = IntoIter<u64, StringGroupedTimeseries>;

    fn into_iter(self) -> Self::IntoIter {
        self.groups.into_iter()
    }
}

impl StringTimeseriesResponse {
    pub fn extend(&mut self, groups: StringTimeseriesResponse) {
        for (k, v) in groups.groups.into_iter() {
            if self.groups.contains_key(&k) {
                for (xxhash, ts) in v.timeseries {
                    self.groups
                        .get_mut(&k)
                        .unwrap()
                        .timeseries
                        .insert(xxhash, ts);
                }
            } else {
                self.groups.insert(k, v);
            }
        }
    }

    // pub fn convert_groups(&mut self) -> Vec<crate::myst_grpc::GroupedTimeseries> {
    //     let mut grouped_timeseries = Vec::new();
    //     for (_k, mut v) in &mut self.groups {
    //         let mut hashes = Vec::with_capacity(v.group.len());
    //         for tagval in &v.group {
    //             let tval = tagval.to_string();
    //             let hash = Hash64::hash(&tval);
    //             self.dict.entry(hash).or_insert(tval);
    //             hashes.push(hash as i64);
    //         }
    //
    //         grouped_timeseries.push(crate::myst_grpc::GroupedTimeseries {
    //             group: hashes,
    //             timeseries: v.convert(),
    //         });
    //     }
    //     grouped_timeseries
    // }
}

impl StringGroupedTimeseries {
    fn add(&mut self, xxhash: i64, timeseries: Timeseries) {
        if self.timeseries.contains_key(&xxhash) {
            let curr_timeseries = self.timeseries.get_mut(&xxhash).unwrap();
            curr_timeseries.bitmap.add_many(&timeseries.bitmap.to_vec());
        } else {
            self.timeseries.insert(xxhash, timeseries);
        }
    }

    pub fn add_all(&mut self, timeseries: HashMap<i64, Timeseries>) {
        for (xxhash, ts) in timeseries {
            self.add(xxhash, ts);
        }
    }

    pub fn convert(&mut self, size: usize) -> Vec<Vec<crate::myst_grpc::Timeseries>> {
        let mut count = 0;
        let mut final_result = Vec::new();
        let mut result = Vec::with_capacity(size);
        for (xxhash, ts) in &mut self.timeseries {
            ts.bitmap.run_optimize();
            result.push(crate::myst_grpc::Timeseries {
                hash: *xxhash,
                epoch_bitmap: ts.bitmap.serialize(),
            });
            count += 1;
            if count == size {
                final_result.push(result);
                result = Vec::with_capacity(size);
                count = 0;
            }
        }
        if result.len() > 0 {
            final_result.push(result);
        }
        final_result
    }
}
