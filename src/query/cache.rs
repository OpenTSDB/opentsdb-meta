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

use crate::segment::store::docstore::DeserializedDocStore;
use crate::utils::myst_error::MystError;

use lru::LruCache;

use std::collections::HashMap;
use std::fs::File;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use std::{fs, thread};

pub struct ShardedCache {
    pub docstore_cache: Arc<Mutex<LruCache<(u64, u32), Arc<DeserializedDocStore>>>>,
    pub dict_cache: Arc<Mutex<LruCache<u64, Arc<HashMap<u32, String>>>>>,
}

impl ShardedCache {
    pub fn new() -> Self {
        Self {
            docstore_cache: Arc::new(Mutex::new(
                LruCache::<(u64, u32), Arc<DeserializedDocStore>>::new(200),
            )),
            dict_cache: Arc::new(Mutex::new(LruCache::<u64, Arc<HashMap<u32, String>>>::new(
                48,
            ))),
        }
    }

    pub fn put(&self, epoch: u64, id: u32, docstore: Arc<DeserializedDocStore>) {
        let mut lock = self.docstore_cache.lock().unwrap();
        lock.put((epoch, id), docstore);
    }

    pub fn put_dict(&self, epoch: u64, dict: Arc<HashMap<u32, String>>) {
        let mut lock = self.dict_cache.lock().unwrap();
        lock.put(epoch, dict);
    }
}

pub struct Cache {
    sharded_cache: Arc<RwLock<HashMap<u32, Arc<ShardedCache>>>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            sharded_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn insert_sharded_cache(&self, shard: u32) -> Arc<ShardedCache> {
        let mut write_guard = self.sharded_cache.write().unwrap();
        let sharded_cache = Arc::new(ShardedCache::new());
        write_guard.insert(shard, sharded_cache.clone());
        return sharded_cache;
    }

    pub fn get_sharded_cache(&self, shard: u32) -> Arc<ShardedCache> {
        let read_guard = self.sharded_cache.read().unwrap();
        let opt = read_guard.get(&shard);
        match opt {
            Some(sharded_cache) => {
                return sharded_cache.clone();
            }
            None => {
                drop(read_guard);
                return self.insert_sharded_cache(shard);
            }
        }
    }
}

struct CacheLoader;

impl CacheLoader {
    pub fn start_loader(data_path: String, _cache: &mut Cache) {
        let _handle = thread::spawn(move || -> Result<(), MystError> {
            let dt = chrono::Utc::now();
            let mut epoch = dt.timestamp() as u64;
            epoch = epoch - (24 * 60 * 60);

            let paths = fs::read_dir(data_path)?;
            for path in paths {
                let p = path?;
                let file_epoch = p.file_name().to_str().unwrap().parse::<u64>().unwrap();
                if file_epoch > epoch {
                    let _file = File::open(p.path())?;
                }
            }
            Ok(())
        });
    }
}
