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

use crate::segment::store::dict::DictHolder;
use crate::segment::store::docstore::DeserializedDocStore;
use crate::utils::myst_error::MystError;

use lru::LruCache;

use crate::segment::store::epoch_bitmap::EpochBitmapHolder;
use croaring::Bitmap;
use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Mutex, RwLock};
use std::{fs, thread};

pub struct ShardedCache {
    pub docstore_cache: Arc<Mutex<LruCache<(u64, u32), Arc<DeserializedDocStore>>>>, //key->(segment_name, docstore_id)
    pub dict_cache: Arc<Mutex<LruCache<u64, Arc<DictHolder>>>>, //key->segment_name
    pub epoch_bitmap_cache: Arc<Mutex<LruCache<(u64, u64), Arc<EpochBitmapHolder>>>>, //key->(segment_name, bitmap_epoch)
}

impl ShardedCache {
    pub fn new() -> Self {
        Self {
            docstore_cache: Arc::new(Mutex::new(
                LruCache::<(u64, u32), Arc<DeserializedDocStore>>::new(200),
            )),
            dict_cache: Arc::new(Mutex::new(LruCache::<u64, Arc<DictHolder>>::new(48))),
            epoch_bitmap_cache: Arc::new(Mutex::new(
                LruCache::<(u64, u64), Arc<EpochBitmapHolder>>::new(48),
            )),
        }
    }

    pub fn put_docstore(&self, segment: u64, id: u32, docstore: Arc<DeserializedDocStore>) {
        let mut lock = self.docstore_cache.lock().unwrap();
        lock.put((segment, id), docstore);
    }

    pub fn put_dict(&self, segment: u64, dict: Arc<DictHolder>) {
        let mut lock = self.dict_cache.lock().unwrap();
        lock.put(segment, dict);
    }

    pub fn put_epoch_bitmap(&self, segment: u64, epoch: u64, bitmap: Arc<EpochBitmapHolder>) {
        let mut lock = self.epoch_bitmap_cache.lock().unwrap();
        lock.put((segment, epoch), bitmap);
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
