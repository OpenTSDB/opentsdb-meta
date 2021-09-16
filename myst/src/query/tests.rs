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

use std::{collections::HashMap, rc::Rc, thread};

use crate::query::query::{Query, QueryType};

use crate::segment::myst_segment::MystSegment;
use crate::segment::persistence::Builder;

use crate::query::cache::Cache;

use crate::query::filter::FilterType;
use crate::query::query_filter::QueryFilter;

use crate::segment::segment_reader::SegmentReader;

use crate::query::query_runner::QueryRunner;
use bloomfilter::Bloom;
use croaring::Bitmap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn write_and_get_segment_readers() -> Vec<SegmentReader<File>> {
    let data_path = String::from("./data/");
    let mut segment_readers = Vec::new();
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut segment = MystSegment::new_with_block_entries(1, epoch, 200);
    let shard_id = segment.shard_id;
    let created = segment.epoch;
    let cache = Arc::new(Cache::new());
    write_data(&mut segment, &epoch, 0, 100); //write 100 timeseries only
    close_segment(segment);
    let file_path = MystSegment::get_segment_filename(&shard_id, &created, data_path);
    let reader = File::open(file_path.clone()).unwrap();
    let segment_reader =
        SegmentReader::new(shard_id, created, reader, cache, file_path, 0 as i32).unwrap();

    segment_readers.push(segment_reader);
    segment_readers
}
pub fn write_data(segment: &mut MystSegment, epoch: &u64, metrics_start: u32, metrics_end: u32) {
    let mut tags = HashMap::new();
    tags.insert(Rc::new(String::from("foo")), Rc::new(String::from("bar")));
    tags.insert(Rc::new(String::from("do")), Rc::new(String::from("re")));
    tags.insert(Rc::new(String::from("hi")), Rc::new(String::from("hello")));
    // let mut event = YmsEvent::default();
    // let mut datum = Datum::default();

    for i in metrics_start..metrics_end {
        let mut metric = String::from("metric");
        metric.push_str(&i.to_string());
        //datum.metric.insert(metric, hash);
        segment.add_timeseries(Rc::new(metric), tags.clone(), i as u64, *epoch);
    }
}
pub fn close_segment(mut segment: MystSegment) {
    println!("Closing");
    let data_path = String::from("./data/");
    let shard_id = segment.shard_id;
    let epoch = segment.epoch;
    let mut writer = File::create(MystSegment::get_segment_filename(
        &shard_id,
        &epoch,
        data_path.clone(),
    ))
    .unwrap();
    segment.build(&mut writer, &mut 0).unwrap();
    let mut lock_file_name = MystSegment::get_path_prefix(&shard_id, &epoch, data_path).clone();
    lock_file_name.push_str("/.lock");
    File::create(lock_file_name).unwrap();
    println!("done Closing");
}

pub fn write_data_large_segment() -> Vec<SegmentReader<BufReader<File>>> {
    let data_path = String::from("./data/");
    let mut segment_readers = Vec::new();
    let mut epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut segment = MystSegment::new_with_block_entries(1, epoch, 200);
    let shard_id = segment.shard_id;
    let created = segment.epoch;
    let cache = Arc::new(Cache::new());
    write_data(&mut segment, &epoch, 0, 1);
    epoch = epoch + 7 * 60 * 60;
    write_data(&mut segment, &epoch, 2, 3);
    epoch = epoch + 7 * 60 * 60;
    write_data(&mut segment, &epoch, 4, 5);
    close_segment(segment);
    let file_path = MystSegment::get_segment_filename(&shard_id, &created, data_path);
    let reader = BufReader::new(File::open(file_path.clone()).unwrap());
    let segment_reader =
        SegmentReader::new(shard_id, created, reader, cache, file_path, 0 as i32).unwrap();

    segment_readers.push(segment_reader);
    segment_readers
}

pub fn build_regex_tag_value_filter() -> QueryFilter {
    // let _metric_filter = QueryFilter::Metric {
    //     metric: String::from("metric0"),
    //     _type: FilterType::Literal,
    // };
    let tag_filter = QueryFilter::TagValue {
        key: String::from("foo"),
        filter: String::from("b.*"),
        _type: FilterType::Regex,
    };
    let filters = vec![tag_filter];
    let query_filter = QueryFilter::Chain {
        filters,
        op: String::from("AND"),
    };
    query_filter
}

pub fn build_literal_tag_value_filter() -> QueryFilter {
    let _metric_filter = QueryFilter::Metric {
        metric: String::from("metric0"),
        _type: FilterType::Literal,
    };
    let tag_filter = QueryFilter::TagValue {
        key: String::from("foo"),
        filter: String::from("bar"),
        _type: FilterType::Literal,
    };
    let filters = vec![tag_filter];
    let query_filter = QueryFilter::Chain {
        filters,
        op: String::from("AND"),
    };
    query_filter
}

pub fn build_not_tag_value_filter() -> QueryFilter {
    let metric_filter = QueryFilter::Metric {
        metric: String::from("metric0"),
        _type: FilterType::Literal,
    };
    let tag_filter = QueryFilter::TagValue {
        key: String::from("foo"),
        filter: String::from("bar"),
        _type: FilterType::Literal,
    };
    let not_filter = QueryFilter::NOT {
        filter: Box::from(tag_filter),
    };
    let filters = vec![metric_filter, not_filter];
    let query_filter = QueryFilter::Chain {
        filters,
        op: String::from("AND"),
    };
    query_filter
}

pub fn build_explicit_tag_filter() -> QueryFilter {
    let metric_filter = QueryFilter::Metric {
        metric: String::from("metric0"),
        _type: FilterType::Literal,
    };
    let tag_filter_1 = QueryFilter::TagValue {
        key: String::from("foo"),
        filter: String::from("bar"),
        _type: FilterType::Literal,
    };
    let tag_filter_2 = QueryFilter::TagValue {
        key: String::from("do"),
        filter: String::from("re"),
        _type: FilterType::Literal,
    };
    let tag_filter_3 = QueryFilter::TagValue {
        key: String::from("hi"),
        filter: String::from("hello"),
        _type: FilterType::Literal,
    };

    let filters = vec![metric_filter, tag_filter_1, tag_filter_2, tag_filter_3];

    let query_filter = QueryFilter::Chain {
        filters,
        op: String::from("AND"),
    };
    let explicit_filter = QueryFilter::ExplicitTags {
        filter: Box::new(query_filter),
        count: 3,
    };
    explicit_filter
}

pub fn build_timeseries_query(filter: QueryFilter) -> Query {
    let query = Query {
        from: 0,
        to: 0,
        start: 0,
        end: 0,
        query_type: QueryType::TIMESERIES,
        limit: 0,
        group: vec![String::from("foo"), String::from("do"), String::from("hi")],
        filter,
    };
    query
}

#[test]
pub fn test_query() {
    //  let data = r#"{"from":0,"to":1,"start":1617815836,"end":1617837436,"order":"ASCENDING","type":"TIMESERIES","group":["foo", "do"],"namespace":"ssp","query":{"filters":[{"filter":"prod","type":"TagValueLiteralOr","tagKey":"corp:Environment"}],"op":"AND","type":"Chain"}}"#;
    let data = r#"{"from":0,"to":1,"start":1630599720,"end":1630621920,"order":"ASCENDING","type":"TIMESERIES","group":[],"namespace":"ssp","query":{"filters":[{"filter":".*","tagKey":"flid","type":"TagValueRegex"},{"filter":".*","tagKey":"InstanceId","type":"TagValueRegex"},{"metric":"exch.auct.Requests","type":"MetricLiteral"}],"op":"AND","type":"Chain"}}"#;
    let q = Query::from_json(data);
    println!("{:?}", q);
    //info!("{:?}", q);
}
#[test]
pub fn search_timeseries() {
    let mut segment_readers = write_and_get_segment_readers();
    let filter = build_regex_tag_value_filter();
    let mut query = build_timeseries_query(filter);
    query.start = segment_readers.get(0).unwrap().created;
    query.end = query.start + 1 * 60 * 60;
    let mut config = crate::utils::config::Config::default();
    config.docstore_block_size = 200;
    let mut query_runner = QueryRunner::new(segment_readers, &query, &config, None);
    let mut curr_time = SystemTime::now();
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build()
        .unwrap();

    let mut ts = crate::myst_grpc::TimeseriesResponse {
        grouped_timeseries: Vec::new(),
        dict: None,
        streams: 0,
    };
    query_runner
        .search_timeseries(&thread_pool, &mut ts)
        .unwrap();
    println!(
        "Time to get all metrics {:?}",
        SystemTime::now().duration_since(curr_time).unwrap()
    );
    println!("{:?}", ts);
    let bitmap_serialized = &ts
        .grouped_timeseries
        .get(0)
        .unwrap()
        .timeseries
        .get(0)
        .unwrap()
        .epoch_bitmap;
    let bitmap = Bitmap::deserialize(bitmap_serialized);
    let epoch = query.start as u32;

    assert_eq!(bitmap.contains(epoch), true);
    assert_eq!(ts.grouped_timeseries.len(), 1);
    assert_eq!(ts.grouped_timeseries.get(0).unwrap().timeseries.len(), 100)
}

#[test]
pub fn search_timeseries_multiple_segments() {
    let mut segment_readers = write_and_get_segment_readers();
    thread::sleep(Duration::from_secs(30));
    let more_segment_readers = write_and_get_segment_readers();
    segment_readers.extend(more_segment_readers);
    let filter = build_regex_tag_value_filter();
    let mut query = build_timeseries_query(filter);
    query.start = segment_readers.get(0).unwrap().created;
    query.end = query.start + 1 * 60 * 60;
    let mut config = crate::utils::config::Config::default();
    config.docstore_block_size = 200;
    let mut query_runner = QueryRunner::new(segment_readers, &query, &config, None);
    let mut curr_time = SystemTime::now();
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build()
        .unwrap();

    let mut ts = crate::myst_grpc::TimeseriesResponse {
        grouped_timeseries: Vec::new(),
        dict: None,
        streams: 0,
    };
    query_runner
        .search_timeseries(&thread_pool, &mut ts)
        .unwrap();
    println!(
        "Time to get all metrics {:?}",
        SystemTime::now().duration_since(curr_time).unwrap()
    );
    println!("{:?}", ts);
    let bitmap_serialized = &ts
        .grouped_timeseries
        .get(0)
        .unwrap()
        .timeseries
        .get(0)
        .unwrap()
        .epoch_bitmap;
    let bitmap = Bitmap::deserialize(bitmap_serialized);
    let epoch = query.start as u32;

    assert_eq!(bitmap.contains(epoch), true);
    assert_eq!(ts.grouped_timeseries.len(), 1);
    assert_eq!(ts.grouped_timeseries.get(0).unwrap().timeseries.len(), 100)
}

#[test]
pub fn search_timeseries_large_segment() {
    let segment_readers = write_data_large_segment();
    let filter = build_regex_tag_value_filter();
    let mut query = build_timeseries_query(filter);
    query.start = segment_readers.get(0).unwrap().created;
    query.end = query.start + 11 * 60 * 60;
    let mut config = crate::utils::config::Config::default();
    config.docstore_block_size = 200;
    println!("Segment Readers {:?}", segment_readers.len());
    let mut query_runner = QueryRunner::new(segment_readers, &query, &config, None);
    let mut curr_time = SystemTime::now();
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build()
        .unwrap();

    let mut ts = crate::myst_grpc::TimeseriesResponse {
        grouped_timeseries: Vec::new(),
        dict: None,
        streams: 0,
    };
    query_runner
        .search_timeseries(&thread_pool, &mut ts)
        .unwrap();
    println!(
        "Time to get all metrics {:?}",
        SystemTime::now().duration_since(curr_time).unwrap()
    );
    println!("{:?}", ts);

    assert_eq!(ts.grouped_timeseries.len(), 1);
}

#[test]
pub fn search_timeseries_with_not_filter() {
    let segment_readers = write_and_get_segment_readers();
    let filter = build_not_tag_value_filter();
    let mut query = build_timeseries_query(filter);
    query.start = segment_readers.get(0).unwrap().created;
    query.end = query.start + 1 * 60 * 60;
    let mut config = crate::utils::config::Config::default();
    config.docstore_block_size = 200;
    let mut query_runner = QueryRunner::new(segment_readers, &query, &config, None);
    let mut curr_time = SystemTime::now();
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();

    let mut ts = crate::myst_grpc::TimeseriesResponse {
        grouped_timeseries: Vec::new(),
        dict: None,
        streams: 0,
    };
    query_runner
        .search_timeseries(&thread_pool, &mut ts)
        .unwrap();
    println!(
        "Time to get all metrics {:?}",
        SystemTime::now().duration_since(curr_time).unwrap()
    );

    assert_eq!(ts.grouped_timeseries.len(), 0);
}

#[test]
pub fn search_timeseries_with_explicit_filter() {
    let segment_readers = write_and_get_segment_readers();
    let filter = build_explicit_tag_filter();
    let mut query = build_timeseries_query(filter);
    query.start = segment_readers.get(0).unwrap().created;
    query.end = query.start + 1 * 60 * 60;
    let mut config = crate::utils::config::Config::default();
    config.docstore_block_size = 200;
    println!("{:?}", query);
    let mut query_runner = QueryRunner::new(segment_readers, &query, &config, None);
    let mut curr_time = SystemTime::now();
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let mut ts = crate::myst_grpc::TimeseriesResponse {
        grouped_timeseries: Vec::new(),
        dict: None,
        streams: 0,
    };
    query_runner
        .search_timeseries(&thread_pool, &mut ts)
        .unwrap();
    println!(
        "Time to get all metrics {:?}",
        SystemTime::now().duration_since(curr_time).unwrap()
    );
    println!("{:?}", ts);

    assert_eq!(ts.grouped_timeseries.len(), 1);
}

#[test]
pub fn test_groupby_ordering() {
    let segment_readers = write_and_get_segment_readers();
    let filter = build_literal_tag_value_filter();
    let mut query = build_timeseries_query(filter);
    query.start = segment_readers.get(0).unwrap().created;
    query.end = query.start + 1 * 60 * 60;
    query.group = vec![String::from("foo"), String::from("do")];
    let mut config = crate::utils::config::Config::default();
    config.docstore_block_size = 200;
    let mut query_runner = QueryRunner::new(segment_readers, &query, &config, None);
    let mut curr_time = SystemTime::now();
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();

    let mut ts = crate::myst_grpc::TimeseriesResponse {
        grouped_timeseries: Vec::new(),
        dict: None,
        streams: 0,
    };
    query_runner
        .search_timeseries(&thread_pool, &mut ts)
        .unwrap();
    println!(
        "Time to get all metrics {:?}",
        SystemTime::now().duration_since(curr_time).unwrap()
    );

    let group = ts.grouped_timeseries;
    assert_eq!((&group).len(), 1);
    let groups = &group.get(0).unwrap().group;
    assert_eq!(groups.len(), 2);
    let dict = ts.dict.unwrap();
    assert_eq!("bar", dict.dict.get(groups.get(0).unwrap()).unwrap());
    assert_eq!("re", dict.dict.get(groups.get(1).unwrap()).unwrap());
}

//#[test]
pub fn test_bloom() {
    let expected_num_items = 100000000000;

    // out of 100 items that are not inserted, expect 1 to return true for contain
    let false_positive_rate = 0.01;

    let mut filter = Bloom::<u32>::compute_bitmap_size(expected_num_items, false_positive_rate);
    println!("{}", filter);
    let mut bitmap = Bitmap::create_with_capacity(expected_num_items as u32);
    for i in 0..expected_num_items {
        bitmap.add(i as u32);
    }
    println!("size of bitmap {:?}", bitmap.get_serialized_size_in_bytes());
    bitmap.run_optimize();
    println!("size of bitmap {:?}", bitmap.get_serialized_size_in_bytes());
}
