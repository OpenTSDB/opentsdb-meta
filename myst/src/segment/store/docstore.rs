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

use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use log::{debug, info};
use std::time::SystemTime;
use std::{
    cmp::min,
    collections::HashMap,
    io::Cursor,
    io::{Read, Write},
    sync::Arc,
};

use crate::segment::persistence::Builder;
use crate::segment::persistence::Loader;
use crate::segment::persistence::TimeSegmented;
use crate::segment::segment_reader::SegmentReader;
use crate::utils::myst_error::{MystError, Result};
use lz4::EncoderBuilder;
use std::io::{BufReader, Seek, SeekFrom};

/// Stores the docstore of the segment.
/// Docstore when built could contain multiple docstore blocks.
/// `data` - Is the Timeseries data.
/// `DocstoreHeader` - Stores the id and offset of the docstore block.
/// `compression` - The type of compression used for each docstore block
/// `block_entries` - Number of entries in each docstore block.
#[derive(Debug)]
pub struct DocStore {
    pub data: Vec<Timeseries>,
    pub header: DocStoreHeader,
    pub compression: Option<String>,
    pub block_entries: usize,
}

/// The timeseries that is stored in the docstore.
/// Contains the tags and the hash (id) of the timeseries.
#[derive(Debug, Default)]
pub struct Timeseries {
    pub tags: Arc<HashMap<u32, u32>>, //tags
    pub timeseries_id: u64,           //xxhash
}

/// An efficient datastructure to hold the docstore after reading and deserializing.
#[derive(Debug, Default, Clone)]
pub struct DeserializedDocStore {
    pub data: Option<Vec<u8>>,
    pub offset_len: Option<Vec<OffsetLen>>,
    pub duration: i32,
}

impl TimeSegmented for DeserializedDocStore {
    fn get_duration(&self) -> Option<i32> {
        Some(self.duration)
    }

    fn set_duration(&mut self, duration: i32) {
        self.duration = duration;
    }
}

#[derive(Debug, Clone)]
pub struct OffsetLen {
    pub offset: usize,
    pub len: usize,
}

#[derive(Debug, Default)]
pub struct DocStoreHeader {
    pub header: HashMap<u32, u32>,
}

impl<W: Write> Builder<W> for DocStoreHeader {
    fn build(self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        let mut serialized = Vec::new();
        serialized.write_u32::<NetworkEndian>(self.header.len() as u32)?;

        for (k, v) in self.header.iter() {
            serialized.write_u32::<NetworkEndian>(*k)?;
            serialized.write_u32::<NetworkEndian>(*v)?;
        }
        // *offset += 4;
        // buf.write_u32::<NetworkEndian>(serialized.len() as u32);
        *offset += serialized.len() as u32;
        buf.write_all(&serialized)?;
        Ok(Some(self))
    }
}

impl<W: Write> Builder<W> for DocStore {
    fn build(mut self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        let data = self.serialize_to_arr_multi()?;
        let mut i = 0;
        for datum in data {
            let _tmp_offset = *offset;
            //let mut serialized = Vec::new();
            self.header.header.insert(i, *offset);
            *offset += 4;
            buf.write_u32::<NetworkEndian>(datum.len() as u32)?;

            let compression = self.compression.as_ref().unwrap();

            match compression.as_str() {
                "lz4" => {
                    let mut encoder = EncoderBuilder::new().level(1).build(Vec::new())?;
                    std::io::copy(&mut datum.as_slice(), &mut encoder)?;
                    let (w, _Result) = encoder.finish();

                    *offset += 4;
                    buf.write_u32::<NetworkEndian>(w.len() as u32)?;
                    *offset += w.len() as u32;
                    buf.write_all(&w)?;
                }
                "zstd" => {
                    let mut encoder = zstd::stream::Encoder::new(Vec::new(), 1)?;
                    std::io::copy(&mut datum.as_slice(), &mut encoder)?;
                    let w = encoder.finish()?;
                    *offset += 4;
                    buf.write_u32::<NetworkEndian>(w.len() as u32)?;
                    *offset += w.len() as u32;
                    buf.write_all(&w)?;
                }
                "snappy" => {
                    let mut encoder = snap::write::FrameEncoder::new(Vec::new());
                    std::io::copy(&mut datum.as_slice(), &mut encoder)?;
                    let w = encoder.into_inner().unwrap();

                    *offset += 4;
                    buf.write_u32::<NetworkEndian>(w.len() as u32)?;
                    *offset += w.len() as u32;
                    buf.write_all(&w)?;
                }
                _ => {
                    *offset += 4;
                    buf.write_u32::<NetworkEndian>(datum.len() as u32)?;
                    *offset += datum.len() as u32;
                    buf.write_all(&datum)?;
                }
            }
            i = i + 1;
        }

        info!("Successfully wrote {} docstore blocks ", i);

        Ok(Some(self))
    }
}

impl<R: Read + Seek> Loader<R, DocStore> for DocStore {
    fn load(mut self, buf: &mut R, offset: &u32) -> Result<Option<DocStore>> {
        let docstore_header = SegmentReader::get_docstore_header(buf, offset)?;
        let mut docstore_timeseries = Vec::new();
        let mut block_entries = 0;
        for (id, offset) in docstore_header {
            buf.seek(SeekFrom::Start(offset as u64))?;
            let docstore_deserialized =
                SegmentReader::get_docstore_from_reader(buf, 0, 0, id, 0 as i32)?;
            let offset_length = docstore_deserialized.offset_len.as_ref().unwrap();
            let docstore_result = docstore_deserialized.data.as_ref().unwrap();

            for ol in offset_length {
                let offset = ol.offset;
                let len = ol.len;
                let timeseries_vec = &docstore_result[offset..(offset + len as usize)];
                let timeseries = DocStore::deserialize_timeseries(timeseries_vec)?;
                docstore_timeseries.push(timeseries);
            }
            if block_entries < offset_length.len() {
                block_entries = offset_length.len();
            }
        }
        let docstore = DocStore {
            data: docstore_timeseries,
            header: DocStoreHeader::default(),
            compression: Some(String::from("lz4")),
            block_entries,
        };
        Ok(Some(docstore))
    }
}

impl DocStore {
    pub fn new(block_entries: usize) -> Self {
        Self {
            data: Vec::new(),
            header: DocStoreHeader::default(),
            compression: Some(String::from("lz4")), // writer,
            block_entries,
        }
    }

    fn serialize_to_arr_multi(&mut self) -> Result<Vec<Vec<u8>>> {
        let mut all_serialized = Vec::new();
        let mut serialized = Vec::new();
        let mut written = 0;
        let mut size = min(self.block_entries, self.data.len() - written);
        serialized.write_u32::<NetworkEndian>(size as u32)?;
        for datum in self.data.iter() {
            let ts_ser = DocStore::serialize_timeseries(datum)?;
            serialized.write_u32::<NetworkEndian>(ts_ser.len() as u32)?;
            serialized.extend(ts_ser);
            written = written + 1;
            size = size - 1;
            if size == 0 {
                all_serialized.push(serialized);
                size = min(self.block_entries, self.data.len() - written);
                serialized = Vec::new();
                serialized.write_u32::<NetworkEndian>(size as u32)?;
            }
        }
        Ok(all_serialized)
    }

    fn serialize_timeseries(datum: &Timeseries) -> Result<Vec<u8>> {
        let mut serialized = Vec::new();
        // let map_len = datum.tags.len();
        // serialized.write_u32::<NetworkEndian>((4 + (map_len*8)) as u32 )?;
        serialized.write_u64::<NetworkEndian>(datum.timeseries_id)?;
        for (k, v) in datum.tags.iter() {
            serialized.write_u32::<NetworkEndian>(*k)?;
            serialized.write_u32::<NetworkEndian>(*v)?;
        }
        Ok(serialized)
    }

    pub fn deserialize(data: &[u8], duration: i32) -> Result<DeserializedDocStore> {
        let curr_time = SystemTime::now();
        let mut reader = Cursor::new(data);
        let size = reader.read_u32::<NetworkEndian>()?;
        let result_size = data.len() - (size as usize * 4);
        let mut result = Vec::with_capacity(result_size);
        unsafe {
            result.set_len(result_size);
        }
        let mut sizes = Vec::with_capacity(size as usize);
        let mut offset = 0;
        for _i in 0..size {
            let len = reader.read_u32::<NetworkEndian>()?;
            let offset_len = OffsetLen {
                offset,
                len: len as usize,
            };
            sizes.push(offset_len);

            reader.read_exact(&mut result[offset..(offset + len as usize)])?;
            offset += len as usize;
        }
        debug!(
            "Time to deserialize data file {:?} and result {:?} and size {:?}",
            SystemTime::now().duration_since(curr_time).unwrap(),
            result.len(),
            size
        );
        Ok(DeserializedDocStore {
            data: Some(result),
            offset_len: Some(sizes),
            duration: duration,
        })
    }

    pub fn count_num_timeseries(data: &[u8]) -> u32 {
        let len = data.len() - 8;
        (len / 8) as u32
    }

    pub fn deserialize_timeseries(data: &[u8]) -> Result<Timeseries> {
        let mut len = data.len();
        let mut reader = Cursor::new(data);
        let timeseries_id = reader.read_u64::<NetworkEndian>()?;
        len = len - 8;
        let mut timeseries = HashMap::with_capacity((len / 8) as usize);
        while len > 0 {
            timeseries.insert(
                reader.read_u32::<NetworkEndian>()?,
                reader.read_u32::<NetworkEndian>()?,
            );
            len = len - 8;
        }
        Ok(Timeseries {
            tags: Arc::new(timeseries),
            timeseries_id: timeseries_id,
        })
    }
}

mod test {
    use crate::segment::persistence::Builder;
    use crate::segment::store::docstore::{DocStore, Timeseries};

    use std::collections::HashMap;

    use std::fs::File;

    use super::*;
    use rayon::prelude::*;
    use std::io::BufReader;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;

    pub fn write_data(compression: String) {
        let mut tags = HashMap::new();
        tags.insert(1, 1);
        tags.insert(2, 2);
        tags.insert(3, 3);
        tags.insert(4, 4);
        tags.insert(5, 5);
        tags.insert(6, 6);
        tags.insert(7, 7);
        tags.insert(8, 8);
        let tags_arc = Arc::new(tags);
        let mut docstore = DocStore::new(1000);
        docstore.compression = Some(compression.clone());
        for i in 0..50000 {
            let timeseries = Timeseries {
                tags: tags_arc.clone(),
                timeseries_id: i,
            };
            docstore.data.push(timeseries)
        }
        let mut filename = String::from("./docstore-");
        filename.push_str(&compression);
        let mut writer = File::create(filename).unwrap();
        docstore.build(&mut writer, &mut 0);
    }
    //#[test]
    pub fn test_compression() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_cpus::get())
            .build_global()
            .unwrap();
        write_data(String::from("lz4"));
        write_data(String::from("snappy"));
        write_data(String::from("zstd"));

        let iterations: i32 = 100;
        let none_file_read_time = AtomicU64::new(0);
        let none_file_decompress_time = AtomicU64::new(0);

        let zstd_file_read_time = AtomicU64::new(0);
        let zstd_file_decompress_time = AtomicU64::new(0);

        let lz4_file_read_time = AtomicU64::new(0);
        let lz4_file_decompress_time = AtomicU64::new(0);

        let snappy_file_read_time = AtomicU64::new(0);
        let snappy_file_decompress_time = AtomicU64::new(0);
        (0..iterations).into_par_iter().for_each(|i| {
            //no compression
            let file = String::from("./docstore-none");
            let mut reader = BufReader::new(File::open(file).unwrap());
            let mut data = Vec::new();
            let curr_time = SystemTime::now();
            reader.read_to_end(&mut data).unwrap();
            none_file_read_time.fetch_add(
                SystemTime::now()
                    .duration_since(curr_time)
                    .unwrap()
                    .as_micros() as u64,
                Ordering::SeqCst,
            );
        });

        println!(
            "None Read Time: {} micros, None Decompress Time: {} micros",
            none_file_read_time.load(Ordering::SeqCst) / iterations as u64,
            none_file_decompress_time.load(Ordering::SeqCst) / iterations as u64
        );

        thread::sleep(core::time::Duration::from_secs_f32(5 as f32));
        (0..iterations).into_par_iter().for_each(|i| {
            //zstd
            let mut filename = String::from("./docstore-zstd");
            let mut file = File::open(filename).unwrap();
            let mut reader = BufReader::new(file);

            let mut curr_time = SystemTime::now();

            let size = reader.read_u32::<NetworkEndian>().unwrap();
            let compressed_size = reader.read_u32::<NetworkEndian>().unwrap();
            let mut compressed_data = Vec::with_capacity(compressed_size as usize);
            reader.read_to_end(&mut compressed_data).unwrap();
            zstd_file_read_time.fetch_add(
                SystemTime::now()
                    .duration_since(curr_time)
                    .unwrap()
                    .as_micros() as u64,
                Ordering::SeqCst,
            );
            curr_time = SystemTime::now();
            let mut data = Vec::with_capacity(size as usize);
            let mut decoder = zstd::stream::Decoder::new(&compressed_data[..]).unwrap();
            std::io::copy(&mut decoder, &mut data).unwrap();
            zstd_file_decompress_time.fetch_add(
                SystemTime::now()
                    .duration_since(curr_time)
                    .unwrap()
                    .as_micros() as u64,
                Ordering::SeqCst,
            );
            // write!(
            //     builder,
            //     "{},{},{},",
            //     file_read_time,
            //     file_decompress_time,
            //     (file_read_time + file_decompress_time)
            // )
            //     .unwrap();
        });
        println!(
            "zstd Read Time: {} micros, zstd Decompress Time: {} micros",
            zstd_file_read_time.load(Ordering::SeqCst) / iterations as u64,
            zstd_file_decompress_time.load(Ordering::SeqCst) / iterations as u64
        );

        thread::sleep(core::time::Duration::from_secs_f32(5 as f32));
        (0..iterations).into_par_iter().for_each(|i| {
            //snappy
            let file = String::from("./docstore-snappy");
            let mut reader = BufReader::new(File::open(file).unwrap());
            let mut curr_time = SystemTime::now();

            let size = reader.read_u32::<NetworkEndian>().unwrap();
            let compressed_size = reader.read_u32::<NetworkEndian>().unwrap();
            let mut compressed_data = Vec::with_capacity(compressed_size as usize);
            reader.read_to_end(&mut compressed_data).unwrap();
            snappy_file_read_time.fetch_add(
                SystemTime::now()
                    .duration_since(curr_time)
                    .unwrap()
                    .as_micros() as u64,
                Ordering::SeqCst,
            );
            curr_time = SystemTime::now();
            let mut data = Vec::with_capacity(size as usize);
            let mut decoder = snap::read::FrameDecoder::new(&compressed_data[..]);
            std::io::copy(&mut decoder, &mut data).unwrap();
            snappy_file_decompress_time.fetch_add(
                SystemTime::now()
                    .duration_since(curr_time)
                    .unwrap()
                    .as_micros() as u64,
                Ordering::SeqCst,
            );
            // write!(
            //     builder,
            //     "{},{},{},",
            //     file_read_time,
            //     file_decompress_time,
            //     (file_read_time + file_decompress_time)
            // )
            // .unwrap();
        });
        println!(
            "snappy Read Time: {} micros, snappy Decompress Time: {} micros",
            snappy_file_read_time.load(Ordering::SeqCst) / iterations as u64,
            snappy_file_decompress_time.load(Ordering::SeqCst) / iterations as u64
        );

        thread::sleep(core::time::Duration::from_secs_f32(5 as f32));
        (0..iterations).into_par_iter().for_each(|i| {
            //lz4
            let file = String::from("./docstore-lz4");
            let mut reader = BufReader::new(File::open(file).unwrap());
            let mut curr_time = SystemTime::now();

            let size = reader.read_u32::<NetworkEndian>().unwrap();
            let compressed_size = reader.read_u32::<NetworkEndian>().unwrap();
            let mut compressed_data = Vec::with_capacity(compressed_size as usize);
            reader.read_to_end(&mut compressed_data).unwrap();
            lz4_file_read_time.fetch_add(
                SystemTime::now()
                    .duration_since(curr_time)
                    .unwrap()
                    .as_micros() as u64,
                Ordering::SeqCst,
            );
            curr_time = SystemTime::now();
            let mut data = Vec::with_capacity(size as usize);
            let mut decoder = lz4::Decoder::new(&compressed_data[..]).unwrap();
            std::io::copy(&mut decoder, &mut data).unwrap();
            lz4_file_decompress_time.fetch_add(
                SystemTime::now()
                    .duration_since(curr_time)
                    .unwrap()
                    .as_micros() as u64,
                Ordering::SeqCst,
            );
            // write!(
            //     builder,
            //     "{},{},{}",
            //     file_read_time,
            //     file_decompress_time,
            //     (file_read_time + file_decompress_time)
            // )
            // .unwrap();
            // writeln!(builder).unwrap();
        });

        println!(
            "lz4 Read Time: {} micros, lz4 Decompress Time: {} micros",
            lz4_file_read_time.load(Ordering::SeqCst) / iterations as u64,
            lz4_file_decompress_time.load(Ordering::SeqCst) / iterations as u64
        );

        // let mut f = File::create("./compression.csv").unwrap();
        // f.write_all(builder.as_ref());
        // println!("{:?}", builder);
    }
}
