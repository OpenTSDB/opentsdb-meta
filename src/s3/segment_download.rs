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

//Periodically check for and download segments

use crate::s3::remote_store::RemoteStore;
use log::info;
use std::fs::{create_dir_all, read_dir, rename, File};
use std::path::Path;
use std::{
    collections::HashSet, io::Error, io::ErrorKind, io::Write, os::unix::prelude::FileExt,
    sync::Arc, thread,
};
use tokio::sync::RwLock;

//This following imports will move to the main class
use crate::utils::config::Config;
use rusoto_core::credential::AwsCredentials;
use rusoto_core::credential::StaticProvider;
use rusoto_core::request::HttpClient;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, ListObjectsRequest, Object, S3Client, S3};

pub struct SegmentDownload {
    remote_store: Arc<RemoteStore>,
    prefix_i: Arc<String>,
    shard: usize,
    root_path: Arc<String>,
    temp_path: Arc<String>,
    frequency: u64,
}

impl SegmentDownload {
    pub(crate) fn new(
        remote_store: Arc<RemoteStore>,
        prefix_i: Arc<String>,
        shard: usize,
        root_path: Arc<String>,
        temp_path: Arc<String>,
        frequency: u64,
    ) -> SegmentDownload {
        Self {
            remote_store,
            prefix_i,
            shard,
            root_path,
            temp_path,
            frequency,
        }
    }

    async fn download(self) -> Result<(), Error> {
        info!("In download function for {}", &self.prefix_i);
        let prefix = Arc::clone(&self.prefix_i);

        let mut downloaded_set_original: HashSet<String> = HashSet::new();

        let mut shard_path = add_dir(self.root_path.to_string(), self.prefix_i.to_string());

        let root_dir = Path::new(&shard_path);
        if root_dir.exists() {
            info!("Reading for {:?} as dir exists", root_dir);
            let mut files: Vec<String> = Vec::new();
            //Recursively list files
            self.list_files(root_dir, &mut files)?;
            for sfile in files {
                downloaded_set_original.insert(sfile);
            }
        } else {
            info!("Path does not exist, creating {:?}", root_dir);
            create_dir_all(root_dir)?;
        }
        info!("Before loop for {}", &self.prefix_i);
        let downloaded_set: Arc<RwLock<HashSet<String>>> =
            Arc::new(RwLock::new(downloaded_set_original));
        loop {
            info!("Starting loop for {}", &self.prefix_i);
            let store = self.remote_store.clone();
            let store_clone = Arc::clone(&store);
            let result = store_clone.list_files(prefix.to_string().to_owned()).await;
            let files = match result {
                Ok(files_option) => match files_option {
                    Some(f) => f,
                    None => {
                        info!("No files found for {}", prefix);
                        continue;
                    }
                },
                Err(e) => {
                    info!("Error fetching files for prefix: {}", prefix);
                    continue;
                }
            };
            let mut handles = Vec::new();
            for file in files {
                let file_name = Arc::new(file);
                let store = self.remote_store.clone();
                let root_path_clone = self.root_path.clone();
                let temp_path_clone = self.temp_path.clone();
                let downloaded_set_clone = Arc::clone(&downloaded_set);
                //This will parallelize it a file level, we optionally need not do it.
                handles.push(tokio::spawn(async move {
                    let store_clone = Arc::clone(&store);
                    let file_name_clone = Arc::clone(&file_name);
                    let tpath = Path::new(temp_path_clone.as_ref())
                        .join(Path::new(file_name_clone.as_ref()));

                    let fpath = Path::new(root_path_clone.as_ref())
                        .join(Path::new(file_name_clone.as_ref()));

                    let read_lock = futures::executor::block_on(downloaded_set_clone.read());
                    let file_path = fpath.as_path().to_str().unwrap_or("none").to_string();
                    let skip = read_lock.contains(&file_path);
                    drop(read_lock);
                    let fpath_clone = fpath.clone();
                    if !skip {
                        create_dir_all(tpath.parent().unwrap());
                        create_dir_all(fpath.parent().unwrap());
                        info!("Creating path: {:?}", tpath);
                        info!("Downloading file: {}", file_name_clone);
                        let mut f = File::create(tpath.clone()).unwrap();
                        let downloaded = store_clone
                            .download(file_name_clone.to_string().to_owned(), &mut f)
                            .await;
                        match downloaded {
                            Ok(bytes) => {
                                f.flush().unwrap();
                                //Final move
                                info!(
                                    "Moving downloaded file from {:?} to {:?}",
                                    tpath.to_str(),
                                    fpath.to_str()
                                );
                                rename(tpath, fpath);
                                // Wait until lock is acquired. But what is the point of a blocking method, if it doesnt block by itself ?
                                // I guess this is a side effect of async
                                let mut lock =
                                    futures::executor::block_on(downloaded_set_clone.write());
                                lock.insert(file_name_clone.to_string().to_owned());
                                drop(lock);
                            }
                            Err(e) => {
                                info!("Error fetching file {}", file_name);
                            }
                        }
                    } else {
                        info!(
                            "Skipping download for file {}, as it is already there.",
                            &file_path
                        );
                    }

                    //Check and create lock file
                    let lock_file_buf =
                        Path::new(fpath_clone.parent().unwrap()).join(Path::new(".lock"));
                    let lock_file = lock_file_buf.as_path();

                    if !lock_file.exists() {
                        info!("Creating lock file: {:?}", lock_file.to_str());
                        File::create(lock_file).unwrap();
                    }
                }));
            }
            handles.push(tokio::spawn(async move { info!("Dummy closure!") }));
            info!("Before block on");
            /* let result = futures::executor::block_on(futures::future::try_join_all(handles));
                   info!("After block on");
            match result {
                           Ok(v) => info!("Downloaded all shards successfully!"),
                           Err(e) => info!("Error while downloading from s3 {:?}", e),
                      }

                   for rs1 in result {
                       match rs1 {

                       }
                   }*/
            let download_count = handles.len();
            let mut count = 0;
            for h in handles {
                let result = h.await;
                match result {
                    Ok(v) => count += 1,
                    Err(e) => info!("Error while downloading from s3 {:?}", e),
                }
            }
            info!(
                "Successfully downloaded for {}/{} files for shard: {}",
                count, download_count, self.shard
            );
            let sleep_time = tokio::time::Duration::from_secs(self.frequency);
            info!("Will sleep for a while before retrying {:?}", sleep_time);
            thread::sleep(sleep_time);
        }
    }

    fn list_files(&self, root_dir: &Path, files: &mut Vec<String>) -> Result<(), Error> {
        if root_dir.is_dir() {
            let dirs = read_dir(root_dir)?;
            for dir in dirs {
                self.list_files(dir.unwrap().path().as_path(), files)?;
            }
        } else if root_dir.is_file() {
            let full_name = root_dir.to_str().unwrap();
            if !full_name.ends_with(".lock") {
                info!("Inserting path: {} into vector: ", full_name);
                files.push(full_name.to_string());
            }
        }
        Ok(())
    }
}

pub async fn start_download() -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting segment download");
    let mut config = Config::new();

    let processed_bucket = config.processed_bucket;

    let key = config.aws_key;
    let secret = config.aws_secret;
    let creds = AwsCredentials::new(key, secret, None, None);
    let namespace = config.namespace;
    let root_data_path = config.data_path;
    let temp_data_path = config.temp_data_path;
    let frequency = config.download_frequency;
    let s3_client = S3Client::new_with(
        HttpClient::new().expect("Failed to create client"),
        StaticProvider::from(creds),
        Region::UsEast2,
    );

    let arc_s3_client = Arc::new(s3_client);
    let arc_processed_bucket = Arc::new(processed_bucket.clone());
    let num_shards = config.shards as usize;
    info!("Num shards: {} root path: {}", num_shards, root_data_path);

    let mut handles = Vec::new();
    for i in 0..num_shards {
        let clone_s3_client = Arc::clone(&arc_s3_client);
        let clone_processed_bucket = Arc::clone(&arc_processed_bucket);
        let root_path_clone = root_data_path.clone();
        let remote_prefix = format!("{}/{}", &namespace.clone(), i.to_string());
        let temp_data_path_clone = temp_data_path.clone();
        let handle = tokio::spawn(async move {
            let remote_store = RemoteStore::new(clone_s3_client, clone_processed_bucket);
            let segment_download = SegmentDownload::new(
                Arc::new(remote_store),
                Arc::new(remote_prefix.to_owned()),
                i,
                Arc::new(root_path_clone.to_owned()),
                Arc::new(temp_data_path_clone.to_owned()),
                frequency,
            );
            let result = segment_download.download().await;
            info!(
                "Segment download finished for: {} {:?}",
                &remote_prefix, result
            );
        });
        handles.push(handle);
    }

    /*let result = futures::executor::block_on(futures::future::try_join_all(handles));

    match result {
        Ok(v) => info!("Threads submitted successfully - should never happen!"),
        Err(e) => info!("Error while submitting jobs for download from remote store {:?}", e),
    }*/
    Ok(())
}

fn add_dir(mut root: String, child: String) -> String {
    if !root.ends_with("/") {
        root.push_str("/");
    }
    root.push_str(&child);
    root
}
