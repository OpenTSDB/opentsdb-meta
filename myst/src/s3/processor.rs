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

mod remote_store;
mod segment_writer;
mod timeseries_record;

extern crate myst;
use crate::remote_store::RemoteStore;
use crate::segment_writer::SegmentWriter;
use crate::timeseries_record::ParseRecord;
use crate::timeseries_record::Record;
use flate2::read::GzDecoder;
use futures::TryStreamExt;
use log::info;
use myst::setup_logger;
use rusoto_core::credential::AwsCredentials;
use rusoto_core::credential::StaticProvider;
use rusoto_core::request::HttpClient;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, ListObjectsRequest, Object, S3Client, S3};
use std::collections::HashMap;
use std::io::Read;
use std::io::{Error, ErrorKind};

use myst::utils::config::Config;
use std::panic;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();

    let mut log_path = config.log_file;
    log_path.push_str("-segment-gen.log");
    setup_logger(String::from(log_path));
    let poll_interval = config.polling_interval;
    let segment_duration = config.segment_duration;
    let segment_full_duration = config.segment_full_duration;
    let namespace = config.namespace;
    let start_time = config.start_epoch;
    let bucket = config.input_bucket;
    let processed_bucket = config.processed_bucket;
    let mut key_prefix = format!("{}/{}", namespace, start_time);
    let mut running_time = start_time;

    let region_name = config.aws_region;
    let region = match Region::from_str(&region_name) {
        Ok(region) => region,
        Err(_) => Region::Custom {
            name: region_name,
            endpoint: config.aws_endpoint,
        },
    };
    loop {
        let key = config.aws_key.as_str();
        let secret = config.aws_secret.as_str();
        let creds = AwsCredentials::new(key, secret, None, None);
        let creds_provider = StaticProvider::from(creds);
        let arc_creds_provider = Arc::new(creds_provider);
        let mut s3_client = S3Client::new_with(
            HttpClient::new().expect("Failed to create client"),
            arc_creds_provider.clone(),
            region.clone(),
        );

        let mut list_request = ListObjectsRequest::default();

        list_request.bucket = bucket.to_string();

        list_request.prefix = Some(key_prefix.to_string());

        info!(
            "Listing objects for bucket: {} and key prefix: {}",
            bucket, key_prefix
        );

        let result = s3_client.list_objects(list_request).await?;

        let object_list = result.contents;

        info!(
            "Checked object availability for {} , result is {:?}",
            key_prefix, object_list
        );

        let objects = match object_list {
            Some(obj) => obj,
            None => {
                info!(
                    "Nothing found for {:?} {:?}, will try again in {} secs",
                    bucket, key_prefix, poll_interval
                );
                thread::sleep(tokio::time::Duration::from_secs(poll_interval));
                continue;
            }
        };

        //Calculate if we need a new segement or we write into an older segment

        let arc_namespace = Arc::new(namespace.clone());
        let arc_processed_bucket = Arc::new(processed_bucket.clone());
        let num_shards = config.shards as usize;
        let segment_gen_data_path = config.segment_gen_data_path.clone();
        let rc_segment_gen_data_path = Arc::new(segment_gen_data_path);
        let senders = Arc::new(RwLock::new(HashMap::new()));
        //let segment_writers = Arc::new(RwLock::new(Vec::new()));
        let mut segment_signals = Vec::new();
        let mut handles = vec![];
        for i in 0..num_shards {
            let path = rc_segment_gen_data_path.clone();
            let sender_ref_clone = Arc::clone(&senders);
            //  let segmentwriters_ref_clone = Arc::clone(&segment_writers);
            let signal = Arc::new(AtomicBool::new(false));
            let clone_signal = Arc::clone(&signal);
            segment_signals.push(signal);
            let clone_bucket = Arc::clone(&arc_processed_bucket);
            let clone_namespace = Arc::clone(&arc_namespace);
            let clone_region = region.clone();
            let clone_creds_provider = arc_creds_provider.clone();
            let handle = thread::spawn(move || {
                //Load myst segment
                //Create a new S3 client for each thread. See https://github.com/hyperium/hyper/issues/2112
                let s3_client = Arc::new(create_new_s3_client(clone_creds_provider, clone_region));
                let mut segmentwriter = SegmentWriter::new(
                    i as u32,
                    clone_signal,
                    500,
                    running_time as u64,
                    segment_duration as u64,
                    (running_time - (running_time % segment_full_duration)) as u64,
                    path,
                    clone_namespace,
                    RemoteStore::new(s3_client, clone_bucket),
                );
                let mut guard = futures::executor::block_on(sender_ref_clone.write());
                guard.insert(i as u32, segmentwriter.sender.take().unwrap());
                drop(guard);
                // let mut guard = futures::executor::block_on(segmentwriters_ref_clone.write());
                // guard.push(segmentwriter.);
                // drop(guard);
                segmentwriter.write_to_segment().unwrap();
            });
            handles.push(handle);
        }
        info!(
            "Created segment writers: {} for epoch: {}",
            segment_signals.len(),
            running_time
        );
        //Wait till all senders are created, I guess.
        thread::sleep(tokio::time::Duration::from_secs(10));

        // Create a new S3 client. See https://github.com/hyperium/hyper/issues/2112
        let s3_client = Arc::new(create_new_s3_client(
            arc_creds_provider.clone(),
            region.clone(),
        ));
        process(
            s3_client,
            bucket.as_str(),
            objects,
            senders,
            segment_signals,
        )
        .await?;

        info!("Waiting for segment writers to finish their job...");
        for hdl in handles {
            hdl.join().unwrap();
        }

        info!("Successfully finished generation for {} !", key_prefix);
        running_time += segment_duration;
        key_prefix = format!("{}/{}", namespace, running_time);
        info!("Will begin polling for {}", key_prefix);
    }
}

//abstract senders
async fn process(
    s3_client_arc: Arc<S3Client>,
    bucket_str: &str,
    objects: Vec<Object>,
    senders: Arc<RwLock<HashMap<u32, mpsc::Sender<Record>>>>,
    segment_signals: Vec<Arc<AtomicBool>>,
) -> Result<(), Box<dyn std::error::Error>> {
    //Pass a function
    let curr_time = SystemTime::now();
    let mut handles = Vec::new();
    for object in objects.into_iter() {
        let s3_client = s3_client_arc.clone();
        let bucket = bucket_str.to_string();
        let senders_clone = Arc::clone(&senders);
        let mut num_iteration: u32 = 0;

        let handle = tokio::task::spawn(async move {
            loop {
                let senders_clone_clone = Arc::clone(&senders_clone);
                let s3_client_clone = Arc::clone(&s3_client);
                info!(
                    "Trying to fetch object name: {} : {:?} iteration: {}",
                    bucket, object.key, num_iteration
                );
                //Will be copied ?
                let mut get_object_request = GetObjectRequest::default();
                get_object_request.bucket = bucket.to_string();
                get_object_request.key = object.key.as_ref().unwrap().to_string(); // Wrong pattern

                let result =
                    fetch_and_process(s3_client_clone, get_object_request, senders_clone_clone)
                        .await;

                match result {
                    Ok(()) => {
                        info!(
                            "Successfully fetched and processed {}",
                            object.key.as_ref().unwrap().to_string()
                        );
                        break; // break out of inner loop.
                    }
                    Err(e) => {
                        info!(
                            "Error while fetching from s3: {:?} try: {}",
                            e, num_iteration
                        );
                        if num_iteration > 2 {
                            info!(
                                "Tried fetching {} {} times. Giving up now.",
                                object.key.as_ref().unwrap().to_string(),
                                num_iteration
                            );
                            break;
                        } else {
                            num_iteration += 1;
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }
    let res = futures::executor::block_on(futures::future::try_join_all(handles));
    match res {
        Ok(_res) => {
            info!("Succesfully processed all shards for bucket {}", bucket_str)
        }
        Err(e) => {
            info!("Error while fetching from s3: {:?}", e);
        }
    }
    //Create segment files
    let mut write_guard = senders.write().await;
    for sender in write_guard.drain() {
        info!("Dropping sender {:?}", sender);
        drop(sender.1); // need to drop senders or the receivers will keep blocking
    }
    for i in 0..segment_signals.len() {
        info!("Sending finish signal to writer: {}", i);
        let signal = segment_signals.get(i).unwrap();
        signal.store(true, Ordering::SeqCst);
    }

    info!(
        "Time took to process {:?}",
        SystemTime::now().duration_since(curr_time).unwrap()
    );
    Ok(())
}

//abstract senders, decoding, record parsing
async fn fetch_and_process(
    s3_client: Arc<S3Client>,
    get_object_request: GetObjectRequest,
    senders: Arc<RwLock<HashMap<u32, mpsc::Sender<Record>>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = s3_client.get_object(get_object_request).await;
    let senders_unlocked = senders.read().await;
    let num_shards = senders_unlocked.len();
    //  let mut cluster = Cluster::default();
    info!("Running get from s3 for");
    match result {
        Ok(object) => {
            let len = object.content_length.unwrap();
            info!("Object output: {:?} and length: {}", object.metadata, len);
            let async_read = object.body.unwrap();
            let data = async_read
                .map_ok(|b| bytes::BytesMut::from(&b[..]))
                .try_concat()
                .await
                .unwrap();
            let vec = data.to_vec();
            info!("Read {:?} bytes", data.len());
            let mut decoder = GzDecoder::new(&vec[..]);
            //let mut read_bytes = 0;
            //let mut iter: i64 = 0;
            let mut val = 0;
            let mut parse_errors = 0;
            let mut total_bytes = 0;
            loop {
                let mut small_buffer = [0u8; 4];
                let result = decoder.read_exact(&mut small_buffer);
                match result {
                    Err(e) => {
                        info!("Stopped reading {:?}", e);
                        break;
                    }
                    _ => {}
                }
                let read_len = Record::read_int(&mut small_buffer);
                //info!("New record length: {}", read_len);
                let mut read_buffer = vec![0u8; read_len as usize];
                total_bytes += read_len;
                let mut record = Record::default();
                decoder.read_exact(&mut read_buffer)?;
                match record.parse(&mut read_buffer) {
                    Err(e) => {
                        info!("Error but continuing {:?}", e);
                        parse_errors += 1;
                    }
                    _ => {}
                }
                let sender_index = (record.xx_hash % (num_shards as i64)).abs() as usize;
                let sender = senders_unlocked.get(&(sender_index as u32)).unwrap();
                //info!("Result: {:?} {:?} {:?} {:?} {}", read_len, record.metric, record.tags, record.xx_hash, val);

                sender.send(record).await?;
                //   cluster.cluster_by_metric(record);
                val += 1;
            }

            //  cluster.drain(senders).await?;
            info!(
                "Read {} records {} bytes in shard with {} errors.",
                val, total_bytes, parse_errors
            );

            return Ok(());
        }
        Err(e) => {
            info!("Error fetching object: {:?}", e);
            return Err(Box::new(Error::new(
                ErrorKind::Other,
                "Error fetching from s3",
            )));
        }
    }
}
#[derive(Default, Debug)]
struct Cluster {
    map: HashMap<String, Vec<Record>>,
}

impl Cluster {
    pub fn cluster_by_metric(&mut self, record: Record) {
        self.map
            .entry(record.metric.clone())
            .or_insert(Vec::new())
            .push(record);
    }

    pub async fn drain(
        &mut self,
        senders: Arc<RwLock<HashMap<u32, mpsc::Sender<Record>>>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let senders_unlocked = senders.read().await;
        let num_shards = senders_unlocked.len();
        for (k, records) in self.map.drain() {
            for record in records {
                let sender_index = (record.xx_hash % (num_shards as i64)).abs() as usize;
                let sender = senders_unlocked.get(&(sender_index as u32)).unwrap();
                sender.send(record).await?;
            }
        }
        Ok(())
    }
}

pub fn create_new_s3_client(arc_s3_creds: Arc<StaticProvider>, region: Region) -> S3Client {
    S3Client::new_with(
        HttpClient::new().expect("Failed to create client"),
        arc_s3_creds.clone(),
        region,
    )
}
