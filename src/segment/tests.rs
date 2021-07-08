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

use crate::query::cache::Cache;
use crate::segment::myst_segment::MystSegment;
use crate::segment::persistence::{Builder, Loader};
use crate::segment::segment_reader::SegmentReader;
use crate::segment::store::docstore::DocStore;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
pub fn test_load() {
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut segment = MystSegment::new_with_block_entries(1, epoch, 200);
    let mut tags = HashMap::new();
    tags.insert(String::from("foo"), String::from("bar"));
    let mut tags_rc = HashMap::new();
    for (k, v) in &tags {
        tags_rc.insert(Rc::new(k.clone()), Rc::new(v.clone()));
    }

    let mut metric = String::from("metric");
    for i in 0..10 {
        metric.push_str(&i.to_string());
        segment.add_timeseries(Rc::new(metric.clone()), tags_rc.clone(), i, epoch);
    }

    let mut buf = Vec::new();
    let mut offset = 0 as u32;
    segment.build(&mut buf, &mut offset);

    let mut loaded_segment = MystSegment::new_with_block_entries(0, 0, 200);
    let mut cursor = Cursor::new(buf.as_slice());
    loaded_segment = loaded_segment.load(&mut cursor, &0).unwrap().unwrap();
    println!("Fst Container {:?}", loaded_segment.fsts);
    println!("Metric Bitmap {:?}", loaded_segment.metrics_bitmap);
    println!("Tag Keys Bitmap {:?}", loaded_segment.tag_keys_bitmap);
    println!("Tag vals Bitmap {:?}", loaded_segment.tag_vals_bitmap);
    println!("Dict {:?}", loaded_segment.dict);
    println!("Docstore {:?}", loaded_segment.data);
    println!("Timeseries Bitmaps {:?}", loaded_segment.epoch_bitmap);
}

#[test]
pub fn test() {
    let data_path = String::from("./data/");
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut segment = MystSegment::new_with_block_entries(1, epoch, 200);

    let mut tags = HashMap::new();
    tags.insert(String::from("foo"), String::from("bar"));
    let mut tags_rc = HashMap::new();
    for (k, v) in &tags {
        tags_rc.insert(Rc::new(k.clone()), Rc::new(v.clone()));
    }

    for i in 0..1000 {
        let mut metric = String::from("metric");
        metric.push_str(&i.to_string());
        segment.add_timeseries(Rc::new(metric.clone()), tags_rc.clone(), i, epoch);
    }

    let mut buf = File::create(MystSegment::get_segment_filename(
        &1,
        &epoch,
        data_path.clone(),
    ))
    .unwrap();
    let mut offset = 0 as u32;
    let fst_header = &segment.fsts.header.header.clone();
    segment.build(&mut buf, &mut offset);
    let cache = Arc::new(Cache::new());
    let file_path = MystSegment::get_segment_filename(&1, &epoch, data_path);
    let file = File::open(file_path.clone()).unwrap();
    let mut segment_reader = SegmentReader::new(1, epoch, file, cache, file_path, 0 as i32).unwrap();
    println!("{:?}", segment_reader.segment_header);
    let fst_header_from_reader = &segment_reader.fst_header;
    for (k, v) in fst_header {
        assert_eq!(v, fst_header_from_reader.get(&k.to_string()).unwrap());
    }

    println!(
        "fst for foo from reader {:?}",
        segment_reader.read_fst("foo")
    );
    println!(
        "fst for foo from reader {:?}",
        segment_reader.read_fst("__metric")
    );
    let terms = segment_reader.search_regex("__tagkey", ".*").unwrap();

    println!(
        "tag keys bitmap {:?}",
        segment_reader.get_tagkeys_bitmaps(terms)
    );
    let terms = segment_reader.search_regex("__metric", ".*").unwrap();

    println!(
        "metrics bitmap {:?}",
        segment_reader.get_bitmaps("__metric", false, terms)
    );

    let terms = segment_reader.search_regex("__tagkey", ".*").unwrap();

    println!(
        "tag keys bitmap {:?}",
        segment_reader.get_bitmaps("__tagkey", true, terms)
    );

    for i in 0..4 {
        println!("Getting docstore for {}", i);
        let deserialized_docstore = segment_reader.get_docstore(i).unwrap();
        let offlen = deserialized_docstore.deref().offset_len.as_ref().unwrap();
        let data = deserialized_docstore.deref().data.as_ref().unwrap();
        for ol in offlen {
            let offset = ol.offset;
            let len = ol.len;
            let timeseries_vec = &data[offset..(offset + len as usize)];
            let timeseries = DocStore::deserialize_timeseries(timeseries_vec).unwrap();
            println!("timeseries {:?}", timeseries);
        }
    }

    println!("Dict {:?}", segment_reader.get_dict());
}
