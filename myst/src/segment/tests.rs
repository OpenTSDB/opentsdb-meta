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
use crate::segment::persistence::{Builder, Compactor, Loader};
use crate::segment::segment_reader::SegmentReader;
use crate::segment::store::docstore::DocStore;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn write_data(
    num_metrics_from: u32,
    num_metrics_to: u32,
    epoch: u64,
    segment: &mut MystSegment,
) {
    let mut tags = HashMap::new();
    tags.insert(String::from("foo"), String::from("bar"));
    let mut tags_rc = HashMap::new();
    for (k, v) in &tags {
        tags_rc.insert(Rc::new(k.clone()), Rc::new(v.clone()));
    }

    for i in num_metrics_from..num_metrics_to {
        let mut metric = String::from("metric");
        metric.push_str(&i.to_string());
        segment.add_timeseries(Rc::new(metric.clone()), tags_rc.clone(), i as u64, epoch);
    }
}
#[test]
pub fn test_load() {
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut segment = MystSegment::new_with_block_entries(1, epoch, 200);

    write_data(0, 10, epoch, &mut segment);
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

    write_data(0, 1000, epoch, &mut segment);

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
    let mut segment_reader =
        SegmentReader::new(1, epoch, file, cache, file_path, 0 as i32).unwrap();
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

#[test]
pub fn test_compaction() {
    let mut epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut segment_one = MystSegment::new_with_block_entries(1, epoch, 200);

    write_data(0, 8, epoch, &mut segment_one);
    let mut buf_one = Vec::new();
    let mut offset = 0 as u32;
    segment_one.build(&mut buf_one, &mut offset);

    epoch = epoch + 2 * 60 * 60;
    let mut segment_two = MystSegment::new_with_block_entries(1, epoch, 200);
    write_data(0, 15, epoch, &mut segment_two);
    let mut buf_two = Vec::new();
    let mut offset = 0 as u32;
    segment_two.build(&mut buf_two, &mut offset).unwrap();

    let mut loaded_segment_one = MystSegment::new_with_block_entries(0, 0, 200);
    let mut cursor = Cursor::new(buf_one.as_slice());
    loaded_segment_one = loaded_segment_one.load(&mut cursor, &0).unwrap().unwrap();

    let mut loaded_segment_two = MystSegment::new_with_block_entries(0, 0, 200);
    let mut cursor = Cursor::new(buf_two.as_slice());
    loaded_segment_two = loaded_segment_two.load(&mut cursor, &0).unwrap().unwrap();

    loaded_segment_one.compact(loaded_segment_two);

    println!("Fst Container {:?}", loaded_segment_one.fsts.fsts);
    assert_eq!(loaded_segment_one.metrics_bitmap.metrics_bitmap.len(), 15);
    assert_eq!(
        loaded_segment_one
            .tag_keys_bitmap
            .tag_keys_bitmap
            .get(&String::from("foo"))
            .unwrap()
            .cardinality(),
        15
    );
    let tag_vals_bitmaps = loaded_segment_one
        .tag_vals_bitmap
        .tag_vals_bitmap
        .get(&String::from("foo"))
        .unwrap();
    assert_eq!(
        tag_vals_bitmaps
            .get(&String::from("bar"))
            .unwrap()
            .cardinality(),
        15
    );
    assert_eq!(loaded_segment_one.dict.dict.len(), 17); //15 metrics and 2 tags
    assert_eq!(loaded_segment_one.data.data.len(), 15);
    //println!("Timeseries Bitmaps {:?}", loaded_segment_one.epoch_bitmap);
}

