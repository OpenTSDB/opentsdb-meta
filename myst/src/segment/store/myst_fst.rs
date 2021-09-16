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

use crate::segment::persistence::Builder;
use crate::segment::persistence::Loader;
use crate::utils::myst_error::{MystError, Result};
use byteorder::{NetworkEndian, WriteBytesExt};

use fst::{MapBuilder, Streamer};

use std::collections::{BTreeMap, HashMap};

use std::io::{BufReader, Read, SeekFrom};
use std::io::{Seek, Write};
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::segment::segment_reader::SegmentReader;
use std::fmt;

/// Maintains the FST for each `_type`
pub struct MystFST {
    pub map: Option<MapBuilder<Vec<u8>>>,
    pub buf: BTreeMap<Rc<String>, u64>,
    pub writeable: Arc<AtomicBool>,
    pub _type: Rc<String>,
}

impl fmt::Debug for MystFST {
    fn fmt(&self, f: &mut fmt::Formatter) -> core::fmt::Result {
        write!(f, "file: {}", self._type)
    }
}

/// Encaupsulates MystFst.
#[derive(Debug)]
pub struct MystFSTContainer {
    pub fsts: HashMap<Rc<String>, MystFST>,
    pub header: FSTHeader,
}

/// Stores the `_type` and `offset` for the FST.
#[derive(Debug, Default)]
pub struct FSTHeader {
    pub header: HashMap<Rc<String>, u32>,
}

impl<W: Write> Builder<W> for FSTHeader {
    fn build(self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        let mut serialized = Vec::new();
        serialized.write_u32::<NetworkEndian>(self.header.len() as u32)?;
        for (k, v) in &self.header {
            serialized.write_u32::<NetworkEndian>(k.len() as u32)?;
            serialized.write_all(k.as_bytes())?;
            serialized.write_u32::<NetworkEndian>(*v as u32)?;
        }

        *offset += serialized.len() as u32;
        buf.write_all(&serialized)?;
        Ok(Some(self))
    }
}

impl<W: Write> Builder<W> for MystFSTContainer {
    fn build(mut self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        let mut serialized = Vec::new();

        let fsts = self.serialize_fsts(offset)?;
        serialized.write_u32::<NetworkEndian>(fsts.len() as u32)?;
        serialized.extend(&fsts);
        // *offset += 4;
        // buf.write_u32::<NetworkEndian>(fsts.len() as u32);
        *offset += fsts.len() as u32;
        buf.write_all(&fsts)?;
        Ok(Some(self))
    }
}

impl<R: Read + Seek> Loader<R, MystFSTContainer> for MystFSTContainer {
    fn load(mut self, buf: &mut R, offset: &u32) -> Result<Option<MystFSTContainer>> {
        let fst_header = SegmentReader::read_fst_header(buf, offset)?;
        self.header = FSTHeader::default();
        self.fsts = HashMap::new();

        for (k, v) in fst_header {
            let key = Rc::new(k);
            let mut myst_fst = MystFST::new(key.clone());

            buf.seek(SeekFrom::Start(v as u64))?;
            let fst = SegmentReader::read_fst_from_reader(buf)?;
            let mut stream = fst.stream();

            let mut map = BTreeMap::new();
            while let Some((k, v)) = stream.next() {
                map.insert(Rc::new(String::from_utf8(k.to_vec())?), v);
            }
            myst_fst.buf = map;
            self.fsts.insert(key.clone(), myst_fst);
        }
        Ok(Some(self))
    }
}

impl MystFSTContainer {
    pub fn new() -> Self {
        Self {
            fsts: HashMap::default(),
            header: FSTHeader::default(),
        }
    }

    pub fn serialize_fsts(&mut self, offset: &mut u32) -> Result<Vec<u8>> {
        let mut tmp_offset = *offset;
        let mut serialized = Vec::new();
        for (k, v) in self.fsts.iter_mut() {
            let bytes = v.finish()?;
            self.header.header.insert(k.clone(), tmp_offset);
            tmp_offset += bytes.len() as u32;
            tmp_offset += 4;
            serialized.write_u32::<NetworkEndian>(bytes.len() as u32)?;
            serialized.extend(&bytes);
        }
        Ok(serialized)
    }
}

impl MystFST {
    fn new(_type: Rc<String>) -> Self {
        Self {
            map: Some(MapBuilder::memory()),
            buf: BTreeMap::new(),
            writeable: Arc::new(AtomicBool::new(true)),
            _type,
        }
    }

    /// Initialize MystFst for a type and return
    /// # Arguments
    /// * `_type` - Type of the FST.
    pub fn init(_type: Rc<String>) -> Self {
        let fst = MystFST::new(_type);
        return fst;
    }

    /// Adds a String to the FST and returns it's corresponding value.
    /// # Arguments
    /// * `term` - The string to be added to the FST
    /// * `uid` - A unique id for this string that is stored in the Dictionary structure.
    pub fn insert(&mut self, term: Rc<String>, uid: u32) -> &u64 {
        let ret = self
            .buf
            .entry(term)
            .or_insert(MystFST::generate_val(uid, 0));
        ret
    }

    /// Adds a string to FST with a u64 that is uid + offset of the bitmap for this FST
    /// # Arguments
    /// * `term` - The string to be added to the FST
    /// * `uid` - A unique id for this string that is stored in the Dictionary structure.
    /// * `offset` - The offset of the Bitmap for this String based on the `_type`.
    pub fn insert_with_id_and_offset(
        &mut self,
        term: Rc<String>,
        uid: u32,
        offset: u32,
    ) -> Option<u64> {
        let val = MystFST::generate_val(uid, offset);
        let ret = self.buf.insert(term, val);
        ret
    }

    fn finish(&mut self) -> Result<Vec<u8>> {
        for (k, v) in &self.buf {
            self.map.as_mut().unwrap().insert(k.to_string(), *v)?;
        }
        let serialized = match self.map.take() {
            Some(map) => map.into_inner()?,
            _ => return Err(MystError::new_query_error("Unable to serialize FST")),
        };

        Ok(serialized)
    }

    /// Return the id part of the value of the FST
    /// # Arguments
    /// * `val` - The value of the FST.
    pub fn get_id(val: u64) -> u32 {
        ((val & 0xFFFFFFFF00000000) >> 32) as u32
    }

    /// Return the offset part of the value of the FST
    /// # Arguments
    /// * `val` - The value of the FST.
    pub fn get_offset(val: u64) -> u32 {
        (val & 0xFFFFFFFF) as u32
    }

    /// Generate a u64 value based on the id (u32) and offset(u32) to be stored in the FST
    /// # Arguments
    /// * `id` - The unique id of the string that is stored in the Dictionary.
    /// * `offset` - The offset of the Bitmap for this string for this `_type`.
    pub fn generate_val(id: u32, offset: u32) -> u64 {
        let ret: u64 = (id as u64) << 32 | offset as u64;
        ret
    }
}
