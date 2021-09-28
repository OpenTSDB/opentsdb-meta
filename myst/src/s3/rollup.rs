use myst::utils::config::Config;
use myst::s3::remote_store::RemoteStore;
use rusoto_core::credential::AwsCredentials;
use rusoto_s3::S3Client;
use rusoto_core::credential::StaticProvider;
use rusoto_core::{HttpClient, Region};
use std::sync::Arc;
use std::{thread, fmt};
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::{BTreeSet, BTreeMap, HashMap};
use myst::segment::myst_segment::MystSegment;
use myst::segment::persistence::{Loader, Compactor, Builder};
use log::{error, info};
use myst::s3::segment_download::{SegmentDownload, get_remote_shards};
use myst::utils::myst_error::{MystError, Result};
use tokio::runtime::Handle;
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Error};
use rayon::iter::IntoParallelRefIterator;
use rayon::prelude::*;
use myst::s3::utils::get_upload_filename;


#[tokio::main]
async fn main() {
    let mut config = Config::new();
    println!("Starting rollup");
    start_rollup(config).await;
}


pub async fn start_rollup(config: Config) -> Result<()> {
    let rollup_size = config.rollup_size;
    let rollup_threads = config.rollup_threads;
    let rollup_bucket = config.rollup_bucket;
    let mut rollup_start_epoch = config.rollup_start_epoch;
    let namespace = config.namespace;
    let key = config.aws_key;
    let secret = config.aws_secret;
    let processed_bucket = config.processed_bucket;
    let arc_processed_bucket = Arc::new(processed_bucket);
    let poll_interval = config.polling_interval;
    let creds = AwsCredentials::new(key, secret, None, None);
    // rayon::ThreadPoolBuilder::new().num_threads(rollup_threads as usize).build_global().unwrap();

    let s3_client = S3Client::new_with(
        HttpClient::new().expect("Failed to create client"),
        StaticProvider::from(creds),
        Region::UsEast2,
    );
    let arc_s3_client = Arc::new(s3_client);
    let remote_store = Arc::new(RemoteStore::new(arc_s3_client.clone(), arc_processed_bucket.clone()));
    let remote_store_rollup = Arc::new(RemoteStore::new(arc_s3_client, Arc::new(rollup_bucket)));
    let shards = get_remote_shards(&namespace.to_string(), remote_store.clone()).await?;
    // (0..shards).into_par_iter().for_each(|shard| {
    //     println!("Running rollup for shard {:?}", shard);
    //     futures::executor::block_on(rollup_for_shard(namespace.to_string(), shard, remote_store.clone(), processed_bucket, &rollup_size, rollup_start_epoch.clone(), &poll_interval));
    // });
    let mut handles = Vec::new();
    println!("Num shards {:?}", shards);
    for shard in (0..shards) {
        let remote_store_clone = remote_store.clone();
        let remote_store_rollup_clone = remote_store_clone.clone();
        let namespace_clone = namespace.clone();
        let processd_bucket_clone = arc_processed_bucket.clone();
        println!("Creating a new task for shard {:?}", shard);
        let handle = tokio::spawn(async move {
            rollup_for_shard(namespace_clone,
                             shard,
                             remote_store_clone,
                             remote_store_rollup_clone,
                             &processd_bucket_clone,
                             &rollup_size,
                             rollup_start_epoch.clone(),
                             &poll_interval).await;
        });
        handles.push(handle);


    }
    futures::future::join_all(&mut handles).await;

    Ok(())
}


pub async fn rollup_for_shard(namespace: String, shard: usize, remote_store: Arc<RemoteStore>, remote_store_rollup: Arc<RemoteStore>, processed_bucket: &Arc<String>, rollup_size: &u32, mut start_time: u64, poll_interval: &u64) -> Result<()> {
    loop {
        let mut key_prefix = format!("{}/{}", namespace, shard);
        let files_list = remote_store.list_files(key_prefix.clone()).await?;
        let mut files = match files_list {
            Some(obj) => obj,
            None => {
                info!(
                    "Nothing found for {:?} {:?}, will try again in {} secs",
                    processed_bucket, &key_prefix.clone(), poll_interval
                );
                thread::sleep(tokio::time::Duration::from_secs(*poll_interval));
                continue;
            }
        };
        files.sort();
        let curr_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut to_merge = BTreeMap::new();
        println!("Processing files {:?}", files);
        for file in files {
            let path = file.split("/");
            let path_vec = path.collect::<Vec<&str>>();
            let epoch_str = path_vec.get(path_vec.len() - 2).unwrap();
            println!("Epoch str {:?}", epoch_str);
            let file_epoch: u64 = epoch_str.parse().unwrap();
            if file_epoch < start_time {
                println!("Skipping file {:?} since it is before the start time for rollup {:?}", file_epoch, start_time);
                continue;
            }
            if curr_time - file_epoch > (2 * rollup_size * 24 * 60 * 60) as u64 {
                let file_metadata = remote_store.get_metadata(file.clone()).await?;
                if file_metadata.is_some() {
                    let duration = file_metadata.unwrap().get("duration").unwrap().to_string();
                    let duration_numeric: u64 = duration.parse().unwrap();
                    let rollup_size_secs = rollup_size * 24 * 60 * 60;
                    if duration_numeric < (rollup_size_secs as u64) {
                        to_merge.insert(file.clone(), duration_numeric);
                        if to_merge.len() == (*rollup_size as usize) {
                            println!("Got {:?} segments to rollup. Old start_time {:?}, new start_time {:?}", rollup_size, start_time, file_epoch);
                            start_time = file_epoch;
                            break;
                        }
                    }
                }
            }
        }

        println!("Merging segments {:?}", to_merge);
        let curr_time = SystemTime::now();
        if to_merge.len() < (*rollup_size as usize) {
            println!("Sleeping for {:?}", poll_interval);
            thread::sleep(tokio::time::Duration::from_secs(*poll_interval));
            continue;
        }
        let merged_segment = merge(shard as u32, to_merge, &remote_store)?;
        println!("Finished merging segment {:?}", merged_segment);
        upload(shard as u32, &namespace , merged_segment, &remote_store_rollup);
        println!("Sleeping for {:?}", poll_interval);
        thread::sleep(tokio::time::Duration::from_secs(*poll_interval));
    }
    Ok(())
}

pub fn upload(shard: u32, namespace: &String, merged_segment: RolledupSegment, remote_store_rollup: &RemoteStore) -> Result<()>{
    let mut metadata = HashMap::new();
    metadata.insert("duration".to_string(), merged_segment.duration.to_string());
    metadata.insert("merged_segments".to_string(), merged_segment.merged_segments.join(","));
    let upload_filename = get_upload_filename(shard, merged_segment.myst_segment.epoch, String::from(namespace));
    let segment_formatted = format!("{:?}", merged_segment);
    let mut data = Vec::new();
    merged_segment.myst_segment.build(&mut data, &mut 0)?;
    let result = futures::executor::block_on(remote_store_rollup.upload(
        upload_filename,
        data,
        metadata,
    ));
    match result {
        Ok(i32) => {
            info!("Uploaded rollup segment {:?} for shard {:?}", segment_formatted, shard);
            return Ok(());
        },
        Err(e) => {
            error!("Error uploading rollup segment {:?} for shard {:?}", segment_formatted, shard);
            let error = format!("Error uploading rollup segment {:?} for shard {:?} with error {:?}", segment_formatted, shard, e);
            return Err(MystError::new_write_error(&error));
        }
    }

}

pub fn merge(shard: u32, mut to_merge: BTreeMap<String, u64>, remote_store: &RemoteStore) -> Result<RolledupSegment> {
    let mut myst_segment = None;
    let mut duration = 0;
    let mut merged_segments = Vec::new();
    for file in to_merge {
        let mut buffer = Vec::new();
        futures::executor::block_on(remote_store.download(file.0.clone(), &mut buffer));
        println!("Merging file {:?}", file);
        let path = file.0.split("/");
        let path_vec = path.collect::<Vec<&str>>();
        let epoch_str = path_vec.get(path_vec.len() - 2).unwrap();
        let epoch: u64 = epoch_str.parse().unwrap();
        let mut segment_to_merged = MystSegment::new(shard, epoch);
        let mut cursor = Cursor::new(buffer);
        segment_to_merged = segment_to_merged.load(&mut cursor, &0)?.unwrap();
        println!("Loaded segment {:?}", segment_to_merged.epoch);
        if myst_segment.is_none() {
            myst_segment = Some(segment_to_merged);
            duration = file.1;
        } else {
            let mut myst_segment_unwrap = myst_segment.unwrap();
            println!("Compacting {:?} and {:?} ", myst_segment_unwrap.epoch, segment_to_merged.epoch);
            myst_segment_unwrap.compact(segment_to_merged);
            myst_segment = Some(myst_segment_unwrap);
            duration += duration + file.1
        }
        merged_segments.push(epoch.to_string());
    }
    let rolledup_segment = RolledupSegment {
        myst_segment: myst_segment.unwrap(),
        duration,
        merged_segments,
    };
    Ok(rolledup_segment)
}

pub struct RolledupSegment {
    myst_segment: MystSegment,
    duration: u64,
    merged_segments: Vec<String>,
}

impl Debug for RolledupSegment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RolledupSegment").
            field("epoch", &self.myst_segment.epoch)
            .field("duration", &self.duration)
            .field("merged_segments", &self.merged_segments)
            .finish()
    }
}