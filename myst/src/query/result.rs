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
use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

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

impl StringGroupedTimeseries {
    fn add(&mut self, xxhash: i64, timeseries: Timeseries) {
        if self.timeseries.contains_key(&xxhash) {
            let mut curr_timeseries = self.timeseries.get_mut(&xxhash).unwrap();
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

    pub fn convert(&mut self) -> Vec<crate::myst_grpc::Timeseries> {
        let mut result = Vec::with_capacity(self.timeseries.len());
        for (xxhash, ts) in &self.timeseries {
            result.push(crate::myst_grpc::Timeseries {
                hash: *xxhash,
                epoch_bitmap: ts.bitmap.serialize(),
            });
        }
        result
    }
}
