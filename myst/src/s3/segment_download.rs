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
use log::{error, info};
use std::fs::{create_dir_all, read_dir, rename, File};
use std::path::Path;
use std::{
    collections::HashSet,
    io::Error,
    io::{Read, Write},
    sync::Arc,
    thread,
};
use tokio::sync::RwLock;

//This following imports will move to the main class
use crate::utils::config::{add_dir, Config};
use crate::utils::myst_error::{MystError, Result};
use rusoto_core::credential::AwsCredentials;
use rusoto_core::credential::StaticProvider;
use rusoto_core::request::HttpClient;
use rusoto_core::Region;
use rusoto_s3::S3Client;
use std::str::FromStr;

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

    async fn download(self) -> Result<()> {
        info!("In download function for {}", &self.prefix_i);
        let prefix = Arc::clone(&self.prefix_i);

        let mut downloaded_set_original: HashSet<String> = HashSet::new();

        let shard_path = add_dir(self.root_path.to_string(), self.prefix_i.to_string());

        let root_dir = Path::new(&shard_path);

        info!(
            "Checking if {:?} dir exists, {}",
            root_dir,
            root_dir.exists()
        );
        if root_dir.exists() {
            info!("Reading for {:?} as dir exists", root_dir);
            let mut files: Vec<String> = Vec::new();
            //Recursively list files
            self.list_files(root_dir, &mut files)?;
            for sfile in files {
                info!("Adding file {} to exists set", sfile);
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
                    info!("Error fetching files for prefix: {} {:?}", prefix, e);
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
                    let contains_file = read_lock.contains(&file_path);
                    drop(read_lock);
		    info!("Is file {} present: {}", &file_path, contains_file);	
                    let metadata = store_clone
                        .get_metadata(file_name_clone.to_string().to_owned())
                        .await;
                    if metadata.is_err() {
                        print!(
                            "Error while reading metadata for file: {} {:?}",
                            &file_name_clone,
                            metadata
                        );
                        return;
                    }
                    let mut duration: i32 = -1;
                    let mut fduration: i32 = -1;
                    let unwrapped_meta = metadata.unwrap();
                    if unwrapped_meta.is_some() {
                        let map = unwrapped_meta.unwrap();
                        if map.contains_key("duration") {
                            duration = map
                                .get("duration")
                                .unwrap_or(&"-1".to_string())
                                .parse()
                                .unwrap();
                        }
                    }

                    let duration_file =
                        Path::new(fpath.parent().unwrap()).join(Path::new("duration"));

                    let mut skip = true;
                    // Download the file only if:
                    // either, the file doesnt exist at all or,
                    // the file exists and the duration is < remote file duration.
                    if contains_file {
                        info!("File {} is present. Will check duration file: {:?}", &file_path, duration_file);
                        // Check the duration file
                        if duration_file.exists() {
                            let dur_result = File::open(duration_file.as_path());
                            let mut dur_str = String::new();
                            match dur_result {
                                Ok(mut dur) => {
                                    dur.read_to_string(&mut dur_str);
                                    fduration = dur_str.parse().unwrap_or(-1);
                                    info!("Read duration from file: {:?} string: {} i32: {}", duration_file, dur_str, fduration );
                                    skip = duration <= fduration;
                                },
                                Err(e) => {
                                    error!("Error reading duration from file: {:?} {:?}", duration_file, e);
                                },
                            }
                        }
                    } else {
                        skip = false;
                    }

                    let fpath_clone = fpath.clone();
                    let parent_path = fpath.parent().unwrap();
                    if !skip {
                        create_dir_all(tpath.parent().unwrap());
                        create_dir_all(parent_path);
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
                                rename(tpath, fpath.clone());

                                //Create duration file

                                let duration_tmp_file = Path::new(parent_path)
                                    .join(Path::new("duration_tmp"));
                                {
                                    let duration_tmp_path = duration_tmp_file.as_path();
                                    let mut dt_file_res =
                                        File::create(duration_tmp_path).unwrap();
                                    let dur_as_str= duration.to_string();
                                    let result = dt_file_res.write(dur_as_str.as_bytes());
                                    match result {
                                        Ok(o) => info!("Successfully written duration {} in {:?}. Size: {} bytes",
                                         dur_as_str, duration_tmp_path, o),
                                         Err(e) => error!("Error writing duration file: {:?} {:?}", duration_tmp_path, e),
                                    }
                                    let result = dt_file_res.flush();
                                    match result {
                                        Ok(o) => {},
                                         Err(e) => error!("Error flushing duration file: {:?} {:?}", duration_tmp_path, e),
                                    }
                                } // File should be closed here.
                                info!("Creating duration file: {:?} for duration: {}", &duration_file ,duration);
                                rename(duration_tmp_file, duration_file);
                                // Wait until lock is acquired. But what is the point of a blocking method, if it doesnt block by itself ?
                                // I guess this is a side effect of async
                                let file_name_str = fpath.to_str().unwrap().to_string();
				info!("Writing file {} into cache", file_name_str);
				let mut lock =
                                    futures::executor::block_on(downloaded_set_clone.write());
                                lock.insert(file_name_str);
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
            let mut count: i32 = 0;
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

    fn list_files(&self, root_dir: &Path, files: &mut Vec<String>) -> Result<()> {
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

pub async fn start_download() -> Result<()> {
    info!("Starting segment download");
    let config = Config::new();
    let processed_bucket = config.processed_bucket;

    let key = config.aws_key;
    let secret = config.aws_secret;
    let creds = AwsCredentials::new(key, secret, None, None);
    let namespace = config.namespace;
    let root_data_path = config.data_download_path;
    let temp_data_path = config.temp_data_path;
    let frequency = config.download_frequency;
    let region_name = config.aws_region;
    let region = match Region::from_str(&region_name) {
        Ok(region) => region,
        Err(_) => Region::Custom {
            name: region_name,
            endpoint: config.aws_endpoint,
        },
    };
    let s3_client = S3Client::new_with(
        HttpClient::new().expect("Failed to create client"),
        StaticProvider::from(creds),
        region,
    );

    let arc_s3_client = Arc::new(s3_client);
    let arc_processed_bucket = Arc::new(processed_bucket.clone());
    let remote_store = Arc::new(RemoteStore::new(arc_s3_client, arc_processed_bucket));
    let data_path = add_dir(root_data_path.clone(), namespace.clone());
    let total_num_shards = get_remote_shards(&namespace, remote_store.clone()).await?;
    let num_containers = config.num_containers;
    let container_id = config.container_id;
    let mut shards_to_download = Vec::new();
    for i in 0..total_num_shards {
        if i % num_containers == container_id {
            shards_to_download.push(i);
        }
    }
    info!(
        "Downloading shards: {:?} root path: {}",
        shards_to_download, root_data_path
    );

    let mut handles = Vec::new();
    for i in shards_to_download {
        let root_path_clone = root_data_path.clone();
        let remote_prefix = format!("{}/{}", &namespace.clone(), i.to_string());
        let temp_data_path_clone = temp_data_path.clone();
        let remote_store_clone = remote_store.clone();
        let handle = tokio::spawn(async move {
            let segment_download = SegmentDownload::new(
                remote_store_clone,
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

pub async fn get_remote_shards(
    data_path: &String,
    remote_store: Arc<RemoteStore>,
) -> Result<usize> {
    let mut path = data_path.clone();
    if !path.ends_with("/") {
        path.push_str("/");
    }
    let files = remote_store.list_sub_folders(path).await?;

    if files.is_some() {
        return Ok(files.unwrap().len());
    } else {
        let error = format!("No files found for prefix {:?}", data_path);
        return Err(MystError::new_query_error(&error));
    }
}
