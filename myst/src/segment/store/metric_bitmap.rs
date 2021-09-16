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

use std::rc::Rc;
use std::{collections::HashMap, io::Read, io::Write};

use byteorder::{NetworkEndian, WriteBytesExt};
use croaring::Bitmap;

use super::myst_fst::MystFST;
use crate::segment::persistence::Builder;
use crate::segment::persistence::Loader;
use crate::utils::myst_error::{MystError, Result};
use std::io::Seek;

/// Stores the Bitmaps for each metric.
#[derive(Debug, Default)]
pub struct MetricBitmap {
    pub metrics_bitmap: HashMap<Rc<String>, Bitmap>,
    pub fsts: Option<HashMap<Rc<String>, MystFST>>,
}

impl<W: Write> Builder<W> for MetricBitmap {
    fn build(mut self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        let mut tmp_offset = *offset;
        let mut length = 0;
        let mut serialized = Vec::new();
        let fst = self
            .fsts
            .as_mut()
            .unwrap()
            .get_mut(&Rc::new(String::from(crate::utils::config::METRIC)))
            .unwrap();
        for (k, v) in self.metrics_bitmap.iter() {
            let metric_val = fst.buf.get(k).ok_or(MystError::new_query_error(
                "Metric not found in FST. Something is wrong",
            ))?;
            let metric_id = MystFST::get_id(*metric_val);

            fst.insert_with_id_and_offset(Rc::clone(&k), metric_id, tmp_offset);
            length = v.get_serialized_size_in_bytes() as u32;
            tmp_offset += 4;
            serialized.write_u32::<NetworkEndian>(length)?;

            tmp_offset += length;
            serialized.extend_from_slice(&mut v.serialize());
        }
        // *offset += 4;
        // buf.write_u32::<NetworkEndian>(serialized.len() as u32);
        *offset += serialized.len() as u32;
        buf.write_all(&serialized)?;
        return Ok(Some(self));
    }
}

impl<R: Read + Seek> Loader<R, MetricBitmap> for MetricBitmap {
    fn load(mut self, buf: &mut R, offset: &u32) -> Result<Option<MetricBitmap>> {
        Ok(None)
    }
}

impl MetricBitmap {
    /// Creates a new MetricBitmap
    pub fn new() -> Self {
        Self {
            metrics_bitmap: HashMap::default(),
            fsts: None,
        }
    }

    /// Deserialize a bitmap
    /// # Arguments
    /// * `data` - Data for a bitmap
    pub fn deserialize(data: &[u8]) -> Result<Bitmap> {
        let bitmap = Bitmap::deserialize(&data);
        Ok(bitmap)
    }
}
