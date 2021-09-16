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

use log::{info, warn, LevelFilter};
use std::{collections::HashMap, io::Read, io::Write, time::UNIX_EPOCH};

use byteorder::{NetworkEndian, WriteBytesExt};
use croaring::Bitmap;

use crate::segment::persistence::Loader;
use crate::segment::persistence::{Builder, TimeSegmented};
use crate::segment::segment_reader::SegmentReader;
use crate::utils::myst_error::{MystError, Result};
use std::collections::BTreeMap;
use std::io::Seek;

/// Duration for each Bitmap. All timeseries that falls from start of epoch to this duration will be in a Bitmap
pub const EPOCH_DURATION: u64 = 6 * 60 * 60;

/// Stores a Bitmap for each epoch rolling every `EPOCH_DURATION` seconds.
#[derive(Debug)]
pub struct EpochBitmap {
    pub epoch_bitmap: BTreeMap<u64, Bitmap>, // epoch -> bitmap
    pub header: EpochBitmapHeader,
    start_epoch: u64,
}
#[derive(Debug, Default)]
pub struct EpochBitmapHeader {
    header: HashMap<u64, u32>,
}
impl<W: Write> Builder<W> for EpochBitmapHeader {
    fn build(mut self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        buf.write_u32::<NetworkEndian>(self.header.len() as u32)?;
        *offset += 4;
        for (k, v) in &self.header {
            buf.write_u64::<NetworkEndian>(*k)?;
            *offset += 8;
            buf.write_u32::<NetworkEndian>(*v)?;
            *offset += 4;
        }

        Ok(Some(self))
    }
}

impl<W: Write> Builder<W> for EpochBitmap {
    fn build(mut self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>> {
        let mut tmp_offset = *offset;
        let mut length: u32 = 0;
        let mut serialized = Vec::new();
        let mut header = HashMap::new();

        for (k, v) in &mut self.epoch_bitmap {
            let cardinality = v.cardinality();
            info!("Flushing cardinality: {} for epoch {}", cardinality, k);
            v.run_optimize();
            header.insert(*k, tmp_offset);
            tmp_offset += 8;
            serialized.write_u64::<NetworkEndian>(*k)?;
            length = v.get_serialized_size_in_bytes() as u32;
            tmp_offset += 4;
            serialized.write_u32::<NetworkEndian>(length)?;

            tmp_offset += length;
            serialized.extend_from_slice(&mut v.serialize());
        }

        *offset += serialized.len() as u32;
        buf.write_all(&serialized)?;

        self.header.header = header;
        return Ok(Some(self));
    }
}

impl<R: Read + Seek> Loader<R, EpochBitmap> for EpochBitmap {
    fn load(mut self, buf: &mut R, offset: &u32) -> Result<Option<EpochBitmap>> {
        let mut bitmaps = BTreeMap::new();
        let ts_bitmap_header = SegmentReader::get_ts_bitmap_header(buf, offset)?;
        info!("Ts bitmap header {:?}", ts_bitmap_header);
        for (epoch, offset) in ts_bitmap_header {
            let bitmap = SegmentReader::get_ts_bitmap_from_reader(buf, offset)?;
            let size = bitmap.cardinality();
            bitmaps.insert(epoch, bitmap);
            info!("Loading epoch bitmap: {} {} {}", epoch, offset, size);
        }
        let ts_bitmaps = EpochBitmap {
            epoch_bitmap: bitmaps,
            header: EpochBitmapHeader::default(),
            start_epoch: 0,
        };
        Ok(Some(ts_bitmaps))
    }
}

impl EpochBitmap {
    /// Creates a new EpochBitmap
    /// # Arguments
    /// * `epoch` - the start epoch for the Bitmaps.
    pub fn new(epoch: u64) -> Self {
        let mut timeseries_bitmap = BTreeMap::default();
        timeseries_bitmap.insert(epoch, Bitmap::create());
        Self {
            epoch_bitmap: timeseries_bitmap,
            header: EpochBitmapHeader::default(),
            start_epoch: epoch,
        }
    }

    /// Adds a timeseries to a Bitmap based on it's timestamp.
    /// # Arguments
    /// * `timeseries_id` - The segment timeseries id for the timeseries.
    /// * `timestamp` - The timestamp for the timeseries
    pub fn add_timeseries(&mut self, timeseries_id: u32, timestamp: u64) -> Result<()> {
        if timestamp < self.start_epoch {
            return Err(MystError::new_write_error(
                "Timestamp cannot be before epoch of segment",
            ));
        }
        let mut last_epoch = self.start_epoch;
        for (epoch, bitmap) in &mut self.epoch_bitmap {
            last_epoch = *epoch;
            if (timestamp >= last_epoch) && (timestamp < last_epoch + EPOCH_DURATION) {
                bitmap.add(timeseries_id);
                return Ok(());
            }
        }
        let next_epoch = last_epoch + EPOCH_DURATION;
        self.epoch_bitmap.insert(next_epoch, Bitmap::create());
        self.epoch_bitmap
            .get_mut(&next_epoch)
            .unwrap()
            .add(timeseries_id);
        Ok(())
    }
}

#[derive(Debug)]
pub struct EpochBitmapHolder {
    pub bitmap: Bitmap,
    pub duration: i32,
}

impl TimeSegmented for EpochBitmapHolder {
    fn get_duration(&self) -> Option<i32> {
        Some(self.duration)
    }

    fn set_duration(&mut self, duration: i32) {
        self.duration = duration;
    }
}

impl EpochBitmapHolder {
    pub fn new(bitmap: Bitmap, duration: i32) -> Self {
        Self {
            bitmap: bitmap,
            duration: duration,
        }
    }
}

mod test {
    use crate::segment::persistence::{Builder, Loader};
    use crate::segment::store::epoch_bitmap::EpochBitmap;
    use std::io::Cursor;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    pub fn test() {
        let mut curr_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut offset = curr_time % 1800;
        let mut epoch = curr_time - offset;
        let mut ts_bitmaps = EpochBitmap::new(epoch);
        curr_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1 * 60 * 60;
        offset = curr_time % 1800;
        epoch = curr_time - offset;
        for i in 0..100 {
            ts_bitmaps.add_timeseries(i, epoch).unwrap();
            if i % 10 == 0 {
                epoch = epoch + 2 * 60 * 60;
            }
        }

        let mut buf = Vec::new();
        let mut offset = 0;
        ts_bitmaps = ts_bitmaps.build(&mut buf, &mut offset).unwrap().unwrap();
        let header = ts_bitmaps.header;
        let header_offset = offset;
        header.build(&mut buf, &mut offset).unwrap();
        println!("Header offset {}", header_offset);
        let mut cursor = Cursor::new(buf);
        let mut loaded_bitmaps = EpochBitmap::new(0);
        loaded_bitmaps = loaded_bitmaps
            .load(&mut cursor, &header_offset)
            .unwrap()
            .unwrap();
        println!("Read bitmaps {:?}", loaded_bitmaps);
    }
}
