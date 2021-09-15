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
use crate::segment::myst_segment::{MystSegment, MystSegmentHeaderKeys};
use crate::segment::persistence::Builder;
use crate::segment::segment_reader::SegmentReader;
use crate::segment::store::dict::Dict;
use crate::segment::store::docstore::DocStore;
use crate::segment::store::epoch_bitmap::EpochBitmap;
use crate::segment::store::metric_bitmap::MetricBitmap;
use crate::segment::store::myst_fst::{MystFST, MystFSTContainer};
use crate::segment::store::tag_bitmap::{TagKeysBitmap, TagValuesBitmap};
use crate::utils::myst_error::{MystError, Result};
use byteorder::{NetworkEndian, WriteBytesExt};
use log::info;
use num_traits::ToPrimitive;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, Write};
use std::rc::Rc;

impl<W: Write> Builder<W> for MystSegment {
    /// Builds the MystSegment.
    /// Order of the segment is as follows.
    /// 1. Metric Bitmaps
    /// 2. Tag Keys Bitmaps
    /// 3. Tag Values Bitmaps
    /// 4. Epoch Bitmaps
    /// 5. Epoch Bitmap header
    /// 6. Dictionary
    /// 7. Docstore
    /// 8. Docstore header
    /// 9. FST
    /// 10. FST Header
    /// 11. Myst Segment Header
    /// The offset before start of each structure is written to header `MystSegmentHeader`
    ///
    fn build(mut self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        info!(
            "Building segment for {:?} and {:?}",
            self.shard_id, self.epoch
        );
        &self.drain_clustered_data();

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::MetricBitmap).unwrap(),
            *offset,
        );
        self.metrics_bitmap.fsts = Some(self.fsts.fsts);

        let ret = self
            .metrics_bitmap
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::TagKeysBitmap).unwrap(),
            *offset,
        );
        self.tag_keys_bitmap.fsts = ret.fsts;

        let ret = self
            .tag_keys_bitmap
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::TagValsBitmap).unwrap(),
            *offset,
        );

        self.tag_vals_bitmap.fsts = ret.fsts;
        let mut ret = self
            .tag_vals_bitmap
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.fsts.fsts = ret
            .fsts
            .take()
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::EpochBitmap).unwrap(),
            *offset,
        );
        let ts_bitmap = self
            .epoch_bitmap
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::EpochBitmapHeader).unwrap(),
            *offset,
        );
        ts_bitmap
            .header
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::Dict).unwrap(),
            *offset,
        );
        self.dict.build(buf, offset)?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::Docstore).unwrap(),
            *offset,
        );

        let ret = self
            .data
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;
        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::DocstoreHeader).unwrap(),
            *offset,
        );
        ret.header.build(buf, offset)?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::Fst).unwrap(),
            *offset,
        );
        let ret = self
            .fsts
            .build(buf, offset)?
            .ok_or(MystError::new_query_error("Unable to build segment"))?;

        self.header.header.insert(
            ToPrimitive::to_u32(&MystSegmentHeaderKeys::FstHeader).unwrap(),
            *offset,
        );
        ret.header.build(buf, offset)?;
        for (k, v) in self.header.header {
            buf.write_u32::<NetworkEndian>(k)?;
            buf.write_u32::<NetworkEndian>(v)?;
        }
        info!(
            "Writing segment timeseries id {} and uid: {} for shard: {} and epoch: {}",
            self.segment_timeseries_id, self.uid, self.shard_id, self.epoch
        );
        buf.write_u32::<NetworkEndian>(self.segment_timeseries_id)?;
        buf.write_u32::<NetworkEndian>(self.uid)?;
        info!(
            "Done building segment for {:?} and {:?}",
            self.shard_id, self.epoch
        );
        Ok(None)
    }
}
