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

use crate::utils::myst_error::MystError;
use std::io::Write;
use std::io::{Read, Seek};

pub trait Builder<W: Write> {
    /// A builder that builds the data-structures and add them to a buffer.
    /// The buffer that is passed in the build will have all the data structures, length encoded
    /// # Arguments
    /// * `buf` - The writer that will contain the the full segment after building
    fn build(self, buf: &mut W, offset: &mut u32) -> Result<Option<Self>, MystError>
    where
        Self: Sized;
}

pub trait Loader<R: Read + Seek, T> {
    /// Loads the datastructure from reading a buffer from a offset
    /// # Arguments
    /// * `buf` - The buffer the contains the structure.
    /// * `offset` - The offset to start reading the buffer from.
    fn load(self, buf: &mut R, offset: &u32) -> Result<Option<T>, MystError>
    where
        Self: Sized;
}

pub trait Compactor {
    /// Compacts two segments
    /// # Arguments
    /// * `segment` - The segment that is to be current with the current.
    fn compact(&mut self, segment: Self)
    where
        Self: Sized;
}
pub trait RemoteSegmentStore<W>
where
    W: Write,
{
    fn put(segment: &mut W);

    fn get(shard: u32, epoch: u64) -> Option<W>;
}

/// This trait needs to be implemented
/// by any Data structure needing a dynamic duration parameter.
/// i.e Wherever there is a concept of an updating sub-epoch.
/// Ex: Cached versions of data and MystSegment.
pub trait TimeSegmented {
    fn get_duration(&self) -> Option<i32>;
    fn set_duration(&mut self, duration: i32);
}
