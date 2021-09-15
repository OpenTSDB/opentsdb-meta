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

use super::store::docstore::DocStore;
use crate::query::cache::Cache;
use crate::segment::myst_segment::{MystSegmentHeader, MystSegmentHeaderKeys};
use crate::segment::persistence::TimeSegmented;
use crate::segment::store::dict::DictHolder;
use crate::segment::store::docstore::DeserializedDocStore;
use crate::segment::store::myst_fst::MystFST;
use crate::utils::myst_error::{MystError, Result};

use byteorder::{NetworkEndian, ReadBytesExt};
use croaring::Bitmap;
use fst::{IntoStreamer, Map, Streamer};
use log::debug;
use log::info;

use regex_automata::dense;

use crate::segment::store::epoch_bitmap::EpochBitmapHolder;
use lz4::Decoder;
use num::ToPrimitive;
use std::sync::Arc;
use std::time::SystemTime;
use std::{
    collections::HashMap, fs::File, io::BufReader, io::Read, io::Seek, io::SeekFrom, rc::Rc,
};

/// Contains various helper functions to read Myst Segment from a reader R
pub struct SegmentReader<R> {
    pub shard_id: u32,
    pub created: u64,
    pub metric_prefix: String,
    pub tag_key_prefix: String,
    pub tag_value_prefix: String,
    pub segment_header: HashMap<u32, u32>,
    pub fst_header: HashMap<String, u32>,
    pub docstore_header: HashMap<u32, u32>,
    pub ts_bitmap_header: HashMap<u64, u32>,
    pub reader: R,
    pub cache: Arc<Cache>,
    pub file_path: String,
    pub duration: i32,
}

impl<R: Read + Seek> SegmentReader<R> {
    pub fn new(
        shard_id: u32,
        created: u64,
        mut reader: R,
        cache: Arc<Cache>,
        file_path: String,
        duration: i32,
    ) -> Result<Self> {
        let segment_header = SegmentReader::read_segment_header(&mut reader)?.header;
        let fst_header_offset = segment_header
            .get(&ToPrimitive::to_u32(&MystSegmentHeaderKeys::FstHeader).unwrap())
            .ok_or(MystError::new_query_error("No segment header offset found"))?;

        let fst_header = SegmentReader::read_fst_header(&mut reader, fst_header_offset)?;
        let docstore_header_offset = segment_header
            .get(&ToPrimitive::to_u32(&MystSegmentHeaderKeys::DocstoreHeader).unwrap())
            .ok_or(MystError::new_query_error(
                "No docstore header offset found",
            ))?;
        let docstore_header =
            SegmentReader::get_docstore_header(&mut reader, docstore_header_offset)?;
        let ts_bitmap_header_offset = segment_header
            .get(&ToPrimitive::to_u32(&MystSegmentHeaderKeys::EpochBitmapHeader).unwrap())
            .ok_or(MystError::new_query_error(
                "No Timeseries Bitmap header offset found",
            ))?;
        let ts_bitmap_header =
            SegmentReader::get_ts_bitmap_header(&mut reader, ts_bitmap_header_offset)?;
        let s = Self {
            shard_id,
            created,
            metric_prefix: String::from(crate::utils::config::METRIC),
            tag_key_prefix: String::from(crate::utils::config::TAG_KEYS),
            tag_value_prefix: String::from(crate::utils::config::TAG_VALUES),
            segment_header,
            fst_header,
            docstore_header,
            ts_bitmap_header,
            reader,
            cache,
            file_path,
            duration,
        };

        Ok(s)
    }
    pub fn get_ts_bitmap_header(reader: &mut R, offset: &u32) -> Result<HashMap<u64, u32>> {
        reader.seek(SeekFrom::Start(*offset as u64))?;
        let num_elements = reader.read_u32::<NetworkEndian>()?;
        // let start_of_header = offset - (num_elements * (8 + 4));
        // reader.seek(SeekFrom::Start(start_of_header as u64))?;
        let mut header = HashMap::with_capacity(num_elements as usize);
        for _i in 0..num_elements {
            header.insert(
                reader.read_u64::<NetworkEndian>()?,
                reader.read_u32::<NetworkEndian>()?,
            );
        }
        Ok(header)
    }

    pub fn read_segment_header(reader: &mut R) -> Result<MystSegmentHeader> {
        info!("Reading segment header");
        let len = reader.seek(SeekFrom::End(0))?;
        let bytes_header = (4 * 2 * 10) + (4 * 2);
        let header_start = len - bytes_header;
        debug!(
            "Seeking from start: {} {} {}",
            header_start, len, bytes_header
        );
        reader.seek(SeekFrom::Start(header_start))?;
        let mut header_buf = vec![0; bytes_header as usize];
        reader.read_exact(&mut header_buf)?;
        let header = MystSegmentHeader::from(&header_buf)?;
        Ok(header)
    }

    pub fn read_fst_header(reader: &mut R, offset: &u32) -> Result<HashMap<String, u32>> {
        reader.seek(SeekFrom::Start(*offset as u64))?;
        let mut len = reader.read_u32::<NetworkEndian>()?;
        let mut header = HashMap::with_capacity(len as usize);
        while len > 0 {
            let str_size = reader.read_u32::<NetworkEndian>()?;
            let mut str_buf = vec![0; str_size as usize];
            reader.read_exact(&mut str_buf)?;
            let offset = reader.read_u32::<NetworkEndian>()?;
            header.insert(String::from_utf8(str_buf)?, offset);
            len -= 1;
        }
        Ok(header)
    }

    pub fn get_docstore_header(reader: &mut R, offset: &u32) -> Result<HashMap<u32, u32>> {
        info!("Reading docstore header");

        reader.seek(SeekFrom::Start(*offset as u64))?;
        let docstore_header_len = reader.read_u32::<NetworkEndian>()?;

        let mut docstore_header = HashMap::with_capacity(docstore_header_len as usize);
        for _i in 0..docstore_header_len {
            docstore_header.insert(
                reader.read_u32::<NetworkEndian>()?,
                reader.read_u32::<NetworkEndian>()?,
            );
        }
        Ok(docstore_header)
    }

    pub fn read_fst(&mut self, key: &str) -> Result<Map<Vec<u8>>> {
        let offset = self
            .fst_header
            .get(key)
            .ok_or(MystError::new_query_error("No Valid Fst offset found"))?;
        self.reader.seek(SeekFrom::Start(*offset as u64))?;
        SegmentReader::read_fst_from_reader(&mut self.reader)
    }

    pub fn read_fst_from_reader(reader: &mut R) -> Result<Map<Vec<u8>>> {
        let fst_size = reader.read_u32::<NetworkEndian>()?;
        let mut buffer = vec![0; fst_size as usize];
        reader.read_exact(&mut buffer)?;
        let fst = Map::new(buffer)?;
        Ok(fst)
    }

    pub fn search_regex(&mut self, key: &str, regex: &str) -> Result<Vec<(String, u64)>> {
        let curr_time = SystemTime::now();
        // let mmap = unsafe { Mmap::map(&File::open(file)?)? };
        // let map = Map::new(mmap)?;
        let map = self.read_fst(key)?;
        let mut pattern = String::from("(?i)");
        pattern.push_str(&regex);
        let dfa = dense::Builder::new().anchored(false).build(&pattern)?;
        let mut stream = map.search(&dfa).into_stream();
        let mut result = Vec::new();
        while let Some(key) = stream.next() {
            let term = String::from_utf8(key.0.to_vec())?;
            result.push((term, key.1));
        }
        info!(
            "Time to search regex in fst for key {:?} is {:?} for {:?} results ",
            key,
            SystemTime::now().duration_since(curr_time).unwrap(),
            result.len()
        );
        Ok(result)
    }

    pub fn search_literals(
        &mut self,
        key: &str,
        literals: Vec<Rc<String>>,
    ) -> Result<Vec<(String, u64)>> {
        let curr_time = SystemTime::now();
        // let mmap = unsafe { Mmap::map(&File::open(file)?)? };
        // let map = Map::new(mmap)?;
        let map = self.read_fst(key)?;
        let mut result = Vec::new();
        for literal in literals {
            let mut pattern = String::from("(?i)");
            pattern.push_str(&literal);
            let dfa = dense::Builder::new().anchored(true).build(&pattern)?;
            let mut stream = map.search(&dfa).into_stream();
            while let Some(key) = stream.next() {
                let term = String::from_utf8(key.0.to_vec())?;
                result.push((term, key.1));
            }
        }
        info!(
            "Time to search literals in fst for key {:?} is {:?} for {:?} results for shard {}",
            key,
            SystemTime::now().duration_since(curr_time).unwrap(),
            result.len(),
            self.shard_id
        );
        Ok(result)
    }

    pub fn search_literal(&mut self, key: &str, literal: &str) -> Result<Vec<(String, u64)>> {
        let mut vec = Vec::new();
        vec.push(Rc::new(String::from(literal)));
        self.search_literals(key, vec)
    }

    pub fn get_bitmap_from_reader(reader: &mut R, val: u64) -> Result<Bitmap> {
        let offset = MystFST::get_offset(val);
        reader.seek(SeekFrom::Start(offset as u64))?;

        let length = reader.read_u32::<NetworkEndian>()?;
        reader.seek(SeekFrom::Start(offset as u64 + 4))?;
        let mut data = vec![0; length as usize];
        reader.read_exact(&mut data)?;

        let bitmap = Bitmap::deserialize(&data);
        Ok(bitmap)
    }

    pub fn get_tagkeys_bitmaps(&mut self, terms: Vec<(String, u64)>) -> Result<Vec<Bitmap>> {
        let curr_time = SystemTime::now();
        let mut bitmaps: Vec<Bitmap> = Vec::new();
        for (_term, val) in terms.into_iter() {
            let bitmap = SegmentReader::get_bitmap_from_reader(&mut self.reader, val)?;
            bitmaps.push(bitmap);
        }
        info!(
            "Time to get bitmaps for {:?} for tag keys",
            SystemTime::now().duration_since(curr_time).unwrap()
        );
        Ok(bitmaps)
    }

    pub fn get_bitmaps(
        &mut self,
        _type: &str,
        is_tag_key: bool,
        terms: Vec<(String, u64)>,
    ) -> Result<Vec<Bitmap>> {
        let curr_time = SystemTime::now();

        let mut bitmaps: Vec<Bitmap> = Vec::new();
        if is_tag_key {
            return self.get_tagkeys_bitmaps(terms);
        }
        for (_term, val) in terms.into_iter() {
            let bitmap = SegmentReader::get_bitmap_from_reader(&mut self.reader, val)?;

            bitmaps.push(bitmap);
        }
        info!(
            "Time to get bitmaps for {:?} for num bitmaps {:?} is {:?}",
            _type,
            bitmaps.len(),
            SystemTime::now().duration_since(curr_time).unwrap()
        );
        Ok(bitmaps)
    }

    pub fn get_tagkeys_bitmaps_with_terms(
        &mut self,
        _type: &str,
        terms: Vec<(String, u64)>,
    ) -> Result<HashMap<String, Bitmap>> {
        let mut bitmaps: HashMap<String, Bitmap> = HashMap::new();
        for (term, val) in terms.into_iter() {
            let bitmap = SegmentReader::get_bitmap_from_reader(&mut self.reader, val)?;

            bitmaps.insert(term, bitmap);
        }
        Ok(bitmaps)
    }

    pub fn get_bitmaps_with_terms(
        &mut self,
        _type: &str,
        is_tag_key: bool,
        terms: Vec<(String, u64)>,
    ) -> Result<HashMap<String, Bitmap>> {
        let curr_time = SystemTime::now();
        let mut bitmaps: HashMap<String, Bitmap> = HashMap::new();
        if is_tag_key {
            return self.get_tagkeys_bitmaps_with_terms(_type, terms);
        }
        for (term, val) in terms.into_iter() {
            let bitmap = SegmentReader::get_bitmap_from_reader(&mut self.reader, val)?;
            bitmaps.insert(term, bitmap);
        }

        info!(
            "Time to get bitmaps with terms for {:?} is {:?}",
            _type,
            SystemTime::now().duration_since(curr_time).unwrap()
        );

        Ok(bitmaps)
    }

    pub fn get_all_ts_bitmaps(&mut self) -> Result<HashMap<u64, Arc<EpochBitmapHolder>>> {
        let mut bitmaps = HashMap::new();
        let header = &self.ts_bitmap_header.clone();
        for (epoch, offset) in header {
            let bitmap = self.get_ts_bitmap_cache(*epoch)?.clone();
            bitmaps.insert(*epoch, bitmap);
        }
        Ok(bitmaps)
    }

    pub fn get_ts_bitmap(&mut self, epoch: u64) -> Result<Arc<EpochBitmapHolder>> {
        let ts_bitmap_offset = self
            .ts_bitmap_header
            .get(&epoch)
            .ok_or(MystError::new_query_error("TS Bitmap epoch not found"))?;
        let curr_time = SystemTime::now();
        let bitmap = SegmentReader::get_ts_bitmap_from_reader(&mut self.reader, *ts_bitmap_offset)?;
        debug!(
            "Time took to get doc store from disk {:?} for shard: {} segment: {} block: {}",
            SystemTime::now().duration_since(curr_time).unwrap(),
            self.shard_id,
            self.created,
            epoch
        );
        Ok(Arc::new(EpochBitmapHolder {
            bitmap,
            duration: self.duration,
        }))
    }

    pub fn get_ts_bitmap_from_reader(reader: &mut R, offset: u32) -> Result<Bitmap> {
        reader.seek(SeekFrom::Start(offset as u64))?;
        let epoch = reader.read_u64::<NetworkEndian>()?;
        let bitmap_size = reader.read_u32::<NetworkEndian>()?;
        let mut buf = vec![0; bitmap_size as usize];
        reader.read_exact(&mut buf)?;
        let bitmap = Bitmap::deserialize(&mut buf);
        Ok(bitmap)
    }

    pub fn get_ts_bitmap_cache(&mut self, epoch: u64) -> Result<Arc<EpochBitmapHolder>> {
        let curr_time = SystemTime::now();
        let sharded_cache = self.cache.get_sharded_cache(self.shard_id);

        let mut epoch_bitmap_lock = sharded_cache.epoch_bitmap_cache.lock().unwrap();
        let epoch_bitmap = epoch_bitmap_lock.get(&(self.created, epoch));
        match epoch_bitmap {
            Some(epoch_bitmap) => {
                let dur = epoch_bitmap.get_duration().unwrap_or(0 as i32);
                if dur < self.duration {
                    //Refresh cache
                    debug!(
                        "Docstore cache entry will be refreshed as it has old duration {}, new: {} {:?} for shard: {} segment: {} block: {}",
                        dur,
                        self.duration,
                        SystemTime::now().duration_since(curr_time).unwrap(),
                        self.shard_id,
                        self.created,
                        epoch
                    );
                    drop(epoch_bitmap_lock);
                    let bitmap = self.get_ts_bitmap(epoch)?;

                    sharded_cache.put_epoch_bitmap(self.created, epoch, bitmap.clone());
                    return Ok(bitmap);
                } else {
                    let d = epoch_bitmap.clone();
                    drop(epoch_bitmap_lock);
                    return Ok(d);
                }
            }
            None => {
                drop(epoch_bitmap_lock);
                let bitmap = self.get_ts_bitmap(epoch)?;

                sharded_cache.put_epoch_bitmap(self.created, epoch, bitmap.clone());
                return Ok(bitmap);
            }
        }
    }

    pub fn get_docstore_cache(&self, id: u32) -> Result<Arc<DeserializedDocStore>> {
        let curr_time = SystemTime::now();
        let sharded_cache = self.cache.get_sharded_cache(self.shard_id);

        let mut docstore_lock = sharded_cache.docstore_cache.lock().unwrap();
        let docstore = docstore_lock.get(&(self.created, id));
        match docstore {
            Some(docstore) => {
                let dur = docstore.get_duration().unwrap_or(0 as i32);
                if dur < self.duration {
                    //Refresh cache
                    debug!(
                        "Docstore cache entry will be refreshed as it has old duration {}, new: {} {:?} for shard: {} segment: {} block: {}",
                        dur,
                        self.duration,
                        SystemTime::now().duration_since(curr_time).unwrap(),
                        self.shard_id,
                        self.created,
                        id
                    );
                    drop(docstore_lock);
                    let new_d = self.get_docstore(id)?;

                    sharded_cache.put_docstore(self.created, id, new_d.clone());
                    return Ok(new_d);
                } else {
                    let d = docstore.clone();
                    drop(docstore_lock);
                    return Ok(d);
                }
            }
            None => {
                drop(docstore_lock);
                let docstore = self.get_docstore(id)?;

                sharded_cache.put_docstore(self.created, id, docstore.clone());
                return Ok(docstore);
            }
        }
    }

    pub fn get_docstore(&self, id: u32) -> Result<Arc<DeserializedDocStore>> {
        let mut buffered_reader = BufReader::new(File::open(self.file_path.clone())?);
        let offset = *self
            .docstore_header
            .get(&id)
            .ok_or(MystError::new_query_error("Docstore offset not found"))?;
        buffered_reader.seek(SeekFrom::Start(offset as u64))?;
        let curr_time = SystemTime::now();
        let docstore = SegmentReader::get_docstore_from_reader(
            &mut buffered_reader,
            self.shard_id,
            self.created,
            id,
            self.duration,
        );
        debug!(
            "Time took to get doc store from disk {:?} for shard: {} segment: {} block: {}",
            SystemTime::now().duration_since(curr_time).unwrap(),
            self.shard_id,
            self.created,
            id
        );
        docstore
    }

    pub fn get_docstore_from_reader(
        reader: &mut R,
        shard_id: u32,
        created: u64,
        id: u32,
        duration: i32,
    ) -> Result<Arc<DeserializedDocStore>> {
        let mut curr_time = SystemTime::now();
        let size = reader.read_u32::<NetworkEndian>()?;

        let compressed_size = reader.read_u32::<NetworkEndian>()?;
        let mut compressed_data = vec![0; (compressed_size) as usize];

        reader.read_exact(&mut compressed_data)?;
        debug!(
            "Time took to read data file {:?} for shard {} for segment {} with size {} and filename {}",
            SystemTime::now().duration_since(curr_time).unwrap(),
            shard_id,
            created,
            compressed_size,
            id.clone()
        );
        curr_time = SystemTime::now();
        let mut bytes = Vec::with_capacity(size as usize);

        //lz4
        let mut decoder = Decoder::new(&compressed_data[..])?;
        std::io::copy(&mut decoder, &mut bytes)?;
        debug!(
            "Time took to decompress data file {:?} for shard {} for segment {} with size {} and filename {}",
            SystemTime::now().duration_since(curr_time).unwrap(),
            shard_id,
            created,
            bytes.len(),
            id.clone()
        );
        curr_time = SystemTime::now();
        let result = DocStore::deserialize(bytes.as_slice(), duration)?;
        debug!(
            "Time took to deserialize data file {:?} for shard {} for segment {} with size {} and filename {}",
            SystemTime::now().duration_since(curr_time).unwrap(),
            shard_id,
            created,
            bytes.len(),
            id
        );
        return Ok(Arc::new(result));
    }

    pub fn get_dict_from_cache(&mut self) -> Result<Arc<DictHolder>> {
        let curr_time = SystemTime::now();
        let sharded_cache = self.cache.get_sharded_cache(self.shard_id);

        let mut lock = sharded_cache.dict_cache.lock().unwrap();

        let dict = lock.get(&self.created);
        match dict {
            Some(dict) => {
                debug!(
                    "Time took to get dict from cache {:?} for shard: {} segment: {}",
                    SystemTime::now().duration_since(curr_time).unwrap(),
                    self.shard_id,
                    self.created,
                );
                let dur = dict.get_duration().unwrap_or(0 as i32);
                if dict.get_duration().unwrap_or(0 as i32) < self.duration {
                    debug!(
                        "Dict cache entry will be refreshed as it has old duration {}, new: {} {:?} for shard: {} segment: {}",
                        dur,
                        self.duration,
                        SystemTime::now().duration_since(curr_time).unwrap(),
                        self.shard_id,
                        self.created,
                    );
                    drop(lock);
                    let new_dict = self.get_dict()?;
                    debug!(
                        "Time took to get dict from disk {:?} for shard: {} segment: {}",
                        SystemTime::now().duration_since(curr_time).unwrap(),
                        self.shard_id,
                        self.created,
                    );
                    sharded_cache.put_dict(self.created, new_dict.clone());
                    return Ok(new_dict);
                } else {
                    let d = dict.clone();
                    drop(lock);
                    return Ok(d);
                }
            } //Yikes: TODO
            None => {
                drop(lock);
                let dict = self.get_dict()?;
                debug!(
                    "Time took to get dict from disk {:?} for shard: {} segment: {}",
                    SystemTime::now().duration_since(curr_time).unwrap(),
                    self.shard_id,
                    self.created,
                );
                sharded_cache.put_dict(self.created, dict.clone());
                return Ok(dict);
            }
        }
    }

    pub fn get_dict(&mut self) -> Result<Arc<DictHolder>> {
        let curr_time = SystemTime::now();
        let dict_offset = self
            .segment_header
            .get(&ToPrimitive::to_u32(&MystSegmentHeaderKeys::Dict).unwrap())
            .ok_or(MystError::new_query_error("No valid dict offset found"))?;
        self.reader.seek(SeekFrom::Start(*dict_offset as u64))?;
        let dict = SegmentReader::get_dict_from_reader(&mut self.reader, self.duration);
        info!(
            "Time to dict file {:?} for segment: {} for shard: {}",
            SystemTime::now().duration_since(curr_time).unwrap(),
            self.created,
            self.shard_id,
        );
        dict
    }

    pub fn get_dict_from_reader(reader: &mut R, duration: i32) -> Result<Arc<DictHolder>> {
        let dict_len = reader.read_u32::<NetworkEndian>()?;
        let mut dict = HashMap::new();
        for _i in 0..dict_len {
            let id = reader.read_u32::<NetworkEndian>()?;
            let len = reader.read_u32::<NetworkEndian>()?;
            let mut buf = vec![0; len as usize];
            reader.read_exact(&mut buf)?;
            dict.insert(id, String::from_utf8(buf)?);
        }

        Ok(Arc::new(DictHolder::new(dict, duration)))
    }
}
