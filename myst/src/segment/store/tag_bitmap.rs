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

use super::myst_fst::MystFST;

use crate::segment::persistence::Builder;
use crate::segment::persistence::Loader;
use crate::utils::myst_error::{MystError, Result};
use byteorder::{NetworkEndian, WriteBytesExt};
use croaring::Bitmap;

use std::io::Seek;
use std::{collections::HashMap, io::Read, io::Write, rc::Rc};

/// Stores the Bitmaps for tag keys
#[derive(Debug)]
pub struct TagKeysBitmap {
    pub tag_keys_bitmap: HashMap<Rc<String>, Bitmap>, // tagkey -> bitmap
    pub fsts: Option<HashMap<Rc<String>, MystFST>>,
}

impl TagKeysBitmap {
    /// Creates a new TagKeysBitmap
    pub fn new() -> Self {
        Self {
            tag_keys_bitmap: HashMap::new(),
            fsts: None,
        }
    }
}

impl<W: Write> Builder<W> for TagKeysBitmap {
    fn build(mut self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        let mut tmp_offset = *offset;
        let mut length: u32 = 0;
        let mut serialized = Vec::new();
        let fst = self
            .fsts
            .as_mut()
            .unwrap()
            .get_mut(&Rc::new(String::from(crate::utils::config::TAG_KEYS)))
            .unwrap();
        for (k, v) in self.tag_keys_bitmap.iter() {
            let val = fst.buf.get(k).ok_or(MystError::new_query_error(
                "Tag not found in FST. Something is wrong",
            ))?;
            let id = MystFST::get_id(*val);

            fst.insert_with_id_and_offset(Rc::clone(&k), id, tmp_offset);
            // data = length +
            length = v.get_serialized_size_in_bytes() as u32;
            tmp_offset += 4;
            serialized.write_u32::<NetworkEndian>(length)?;

            tmp_offset += length;
            serialized.write_all(&mut v.serialize());
        }

        *offset += serialized.len() as u32;
        buf.write_all(&serialized);
        return Ok(Some(self));
    }
}

/// Stores the Bitmaps for tag values
#[derive(Debug)]
pub struct TagValuesBitmap {
    pub tag_vals_bitmap: HashMap<Rc<String>, HashMap<Rc<String>, Bitmap>>, // tagkey -> tagval -> bitmap
    pub fsts: Option<HashMap<Rc<String>, MystFST>>,
}

impl<W: Write> Builder<W> for TagValuesBitmap {
    fn build(mut self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        let mut tmp_offset = *offset;
        let mut length: u32 = 0;
        let mut serialzed = Vec::new();
        for (k, v) in self.tag_vals_bitmap.iter() {
            for (tv, bitmap) in v.iter() {
                let tv_fst = self.fsts.as_mut().unwrap().get_mut(k).unwrap();
                let val = tv_fst.buf.get(tv).unwrap();
                let id = MystFST::get_id(*val);

                tv_fst.insert_with_id_and_offset(Rc::clone(&tv), id, tmp_offset);
                length = bitmap.get_serialized_size_in_bytes() as u32;
                tmp_offset += 4;
                serialzed.write_u32::<NetworkEndian>(length)?;
                tmp_offset += length;
                let data = bitmap.serialize();
                serialzed.extend(&data);
            }
        }

        *offset += serialzed.len() as u32;
        buf.write_all(&serialzed);
        return Ok(Some(self));
    }
}

impl<R: Read + Seek> Loader<R, TagValuesBitmap> for TagValuesBitmap {
    fn load(mut self, buf: &mut R, offset: &u32) -> Result<Option<TagValuesBitmap>> {
        Ok(None)
    }
}

impl TagValuesBitmap {
    pub fn new() -> Self {
        Self {
            tag_vals_bitmap: HashMap::new(),
            fsts: None,
        }
    }
}
