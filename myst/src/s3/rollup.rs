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

use log::{error, info};
use myst::s3::remote_store::RemoteStore;
use myst::s3::segment_download::get_remote_shards;
use myst::s3::utils::get_upload_filename;
use myst::segment::myst_segment::MystSegment;
use myst::segment::persistence::{Builder, Compactor, Loader};
use myst::utils::config::Config;
use myst::utils::myst_error::{MystError, Result};

use rayon::prelude::*;
use rusoto_core::credential::AwsCredentials;
use rusoto_core::credential::StaticProvider;
use rusoto_core::{HttpClient, Region};
use rusoto_s3::S3Client;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::io::Cursor;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use myst::{load_metrics_reporter, metrics_gauge, setup_logger};

#[tokio::main]
async fn main() {
    let config = Config::new();
    let lib = libloading::Library::new(&config.plugin_path).expect("load library");
    /// must be loaded in main directly.
    let _metrics_reporter = load_metrics_reporter(&lib);
    info!("Starting rollup");
    let mut log_file = String::from(&config.log_file);
    log_file.push_str("-rollup.log");
    setup_logger(String::from(log_file));
    start_rollup(config).await;
}

pub async fn start_rollup(config: Config) -> Result<()> {
    let rollup_size = config.rollup_size;
    let _rollup_threads = config.rollup_threads;
    let rollup_bucket = config.rollup_bucket;
    let rollup_start_epoch = config.rollup_start_epoch;
    let namespace = config.namespace;
    let key = config.aws_key;
    let secret = config.aws_secret;
    let processed_bucket = config.processed_bucket;
    let arc_processed_bucket = Arc::new(processed_bucket);
    let poll_interval = config.polling_interval;
    let creds = AwsCredentials::new(key, secret, None, None);

    let s3_client = S3Client::new_with(
        HttpClient::new().expect("Failed to create client"),
        StaticProvider::from(creds),
        Region::UsEast2,
    );
    let arc_s3_client = Arc::new(s3_client);
    let remote_store = Arc::new(RemoteStore::new(
        arc_s3_client.clone(),
        arc_processed_bucket.clone(),
    ));
    let _remote_store_rollup = Arc::new(RemoteStore::new(arc_s3_client, Arc::new(rollup_bucket)));
    let shards = get_remote_shards(&namespace.to_string(), remote_store.clone()).await?;

    let mut handles = Vec::new();
    info!("Num shards {:?}", shards);
    for shard in 0..shards {
        let remote_store_clone = remote_store.clone();
        let remote_store_rollup_clone = remote_store_clone.clone();
        let namespace_clone = namespace.clone();
        let processd_bucket_clone = arc_processed_bucket.clone();
        info!("Creating a new task for shard {:?}", shard);
        let handle = tokio::spawn(async move {
            rollup_for_shard(
                namespace_clone,
                shard,
                remote_store_clone,
                remote_store_rollup_clone,
                &processd_bucket_clone,
                &rollup_size,
                rollup_start_epoch.clone(),
                &poll_interval,
            )
            .await;
        });
        handles.push(handle);
    }
    futures::future::join_all(&mut handles).await;

    Ok(())
}

pub async fn rollup_for_shard(
    namespace: String,
    shard: usize,
    remote_store: Arc<RemoteStore>,
    remote_store_rollup: Arc<RemoteStore>,
    processed_bucket: &Arc<String>,
    rollup_size: &u32,
    mut start_time: u64,
    poll_interval: &u64,
) -> Result<()> {
    loop {
        let key_prefix = format!("{}/{}", namespace, shard);
        let files_list = remote_store.list_files(key_prefix.clone()).await?;
        let mut files = match files_list {
            Some(obj) => obj,
            None => {
                info!(
                    "Nothing found for {:?} {:?}, will try again in {} secs",
                    processed_bucket,
                    &key_prefix.clone(),
                    poll_interval
                );
                thread::sleep(tokio::time::Duration::from_secs(*poll_interval));
                continue;
            }
        };
        files.sort();
        let curr_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut to_merge = BTreeMap::new();
        info!("Processing files {:?}", files);
        for file in files {
            let path = file.split("/");
            let path_vec = path.collect::<Vec<&str>>();
            let epoch_str = path_vec.get(path_vec.len() - 2).unwrap();
            let file_epoch: u64 = epoch_str.parse().unwrap();
            if file_epoch < start_time {
                info!(
                    "Skipping file {:?} since it is before the start time for rollup {:?}",
                    file_epoch, start_time
                );
                continue;
            }
            if curr_time - file_epoch > (1 * rollup_size * 24 * 60 * 60) as u64 {
                let file_metadata = remote_store.get_metadata(file.clone()).await?;
                if file_metadata.is_some() {
                    let duration = file_metadata.unwrap().get("duration").unwrap().to_string();
                    let duration_numeric: u64 = duration.parse().unwrap();
                    let rollup_size_secs = rollup_size * 24 * 60 * 60;
                    if duration_numeric < (rollup_size_secs as u64) {
                        to_merge.insert(file.clone(), duration_numeric);
                        if to_merge.len() == (*rollup_size as usize) {
                            info!("Got {:?} segments to rollup. Old start_time {:?}, new start_time {:?}", rollup_size, start_time, file_epoch);
                            start_time = file_epoch;
                            break;
                        }
                    }
                }
            }
        }

        info!("Merging segments {:?}", to_merge);
        let _curr_time = SystemTime::now();
        if to_merge.len() < (*rollup_size as usize) {
            info!(
                "Not enough segments found to merge. Sleeping for {:?}",
                poll_interval
            );
            thread::sleep(tokio::time::Duration::from_secs(*poll_interval));
            continue;
        }
        let merged_segment = merge(shard as u32, to_merge, &remote_store)?;
        info!("Finished merging segment {:?}", merged_segment);
        upload(
            shard as u32,
            &namespace,
            merged_segment,
            &remote_store_rollup,
        );
        info!("Sleeping for {:?}", poll_interval);
        thread::sleep(tokio::time::Duration::from_secs(*poll_interval));
    }
    Ok(())
}

pub fn upload(
    shard: u32,
    namespace: &String,
    merged_segment: RolledupSegment,
    remote_store_rollup: &RemoteStore,
) -> Result<()> {
    let start_time = SystemTime::now();
    let shard_str = shard.to_string();
    let num_segments_str = merged_segment
        .merged_segments
        .len()
        .to_string()
        .len()
        .to_string();
    let host = sys_info::hostname().unwrap();
    let segment_str = merged_segment.myst_segment.epoch.to_string().to_string();
    let tags = [
        "host",
        host.as_str(),
        "shard",
        shard_str.as_str(),
        "segments",
        num_segments_str.as_str(),
        "segment",
        segment_str.as_str(),
    ];
    let mut metadata = HashMap::new();
    metadata.insert("duration".to_string(), merged_segment.duration.to_string());
    metadata.insert(
        "merged_segments".to_string(),
        merged_segment.merged_segments.join(","),
    );
    let upload_filename = get_upload_filename(
        shard,
        merged_segment.myst_segment.epoch,
        String::from(namespace),
    );
    let segment_formatted = format!("{:?}", merged_segment);
    let mut data = Vec::new();
    merged_segment.myst_segment.build(&mut data, &mut 0)?;
    metrics_gauge(&tags, "rollup.segment.size", data.len() as u64);

    let result =
        futures::executor::block_on(remote_store_rollup.upload(upload_filename, data, metadata));
    match result {
        Ok(_i32) => {
            info!(
                "Uploaded rollup segment {:?} for shard {:?}",
                segment_formatted, shard
            );
            metrics_gauge(
                &tags,
                "rollup.upload.ms",
                (SystemTime::now()
                    .duration_since(start_time)
                    .unwrap()
                    .as_millis()) as u64,
            );
            return Ok(());
        }
        Err(e) => {
            error!(
                "Error uploading rollup segment {:?} for shard {:?}",
                segment_formatted, shard
            );
            let error = format!(
                "Error uploading rollup segment {:?} for shard {:?} with error {:?}",
                segment_formatted, shard, e
            );
            return Err(MystError::new_write_error(&error));
        }
    }
}

pub fn merge(
    shard: u32,
    to_merge: BTreeMap<String, u64>,
    remote_store: &RemoteStore,
) -> Result<RolledupSegment> {
    let start_time = SystemTime::now();

    let mut duration = 0;
    let mut merged_segments = Vec::new();
    let mut segments_to_merge = Vec::new();
    for file in &to_merge {
        let mut buffer = Vec::new();
        futures::executor::block_on(remote_store.download(file.0.clone(), &mut buffer));
        info!("Merging file {:?}", file);
        let path = file.0.split("/");
        let path_vec = path.collect::<Vec<&str>>();
        let epoch_str = path_vec.get(path_vec.len() - 2).unwrap();
        let epoch: u64 = epoch_str.parse().unwrap();
        let mut segment_to_merged = MystSegment::new(shard, epoch);
        let mut cursor = Cursor::new(buffer);
        segment_to_merged = segment_to_merged.load(&mut cursor, &0)?.unwrap();
        info!("Loaded segment {:?}", segment_to_merged.epoch);
        segments_to_merge.push(segment_to_merged);
        duration += file.1;

        merged_segments.push(epoch.to_string());
    }

    let merged_segment = MystSegment::compact(segments_to_merge)?;

    let rolledup_segment = RolledupSegment {
        myst_segment: merged_segment,
        duration,
        merged_segments,
    };
    let shard_str = shard.to_string();
    let num_segments_str = to_merge.len().to_string();
    let segment_str = rolledup_segment.myst_segment.epoch.to_string();
    let host = sys_info::hostname().unwrap();
    let tags = [
        "host",
        host.as_str(),
        "shard",
        shard_str.as_str(),
        "segments",
        num_segments_str.as_str(),
        "segment",
        segment_str.as_str(),
    ];

    metrics_gauge(
        &tags,
        "rollup.generation.ms",
        (SystemTime::now()
            .duration_since(start_time)
            .unwrap()
            .as_millis()) as u64,
    );
    Ok(rolledup_segment)
}

pub struct RolledupSegment {
    myst_segment: MystSegment,
    duration: u64,
    merged_segments: Vec<String>,
}

impl Debug for RolledupSegment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RolledupSegment")
            .field("epoch", &self.myst_segment.epoch)
            .field("duration", &self.duration)
            .field("merged_segments", &self.merged_segments)
            .finish()
    }
}
