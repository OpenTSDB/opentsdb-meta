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

use crate::remote_store::RemoteStore;
use crate::timeseries_record::Record;
use myst::segment::myst_segment::{MystSegment, Write};
use myst::segment::persistence::{Builder, Loader, TimeSegmented};

use log::error;
use log::info;
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::thread;

use bytes::BufMut;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{
    io,
    io::{Error, ErrorKind},
};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

pub(crate) struct SegmentWriter {
    pub segment: Option<MystSegment>,
    pub shard_id: u32,
    pub epoch: u64,
    pub duration: u64,
    pub epoch_start: u64,
    pub sender: Option<mpsc::Sender<Record>>,
    pub receiver: mpsc::Receiver<Record>,
    pub finish: Arc<AtomicBool>,
    pub segment_gen_data_path: Arc<String>,
    pub upload_root: Arc<String>,
    pub remote_store: Option<RemoteStore>,
    runtime: Runtime,
}

impl SegmentWriter {
    pub(crate) fn new(
        shard_id: u32,
        finish: Arc<AtomicBool>,
        queue_size: usize,
        epoch: u64,
        duration: u64,
        epoch_start: u64,
        segment_gen_data_path: Arc<String>,
        upload_root: Arc<String>,
        remote_store: RemoteStore,
    ) -> Self {
        let myst_duration = epoch - epoch_start + duration;
        let (tx, rx) = mpsc::channel(queue_size);
        let segment_writer = Self {
            segment: Some(MystSegment::new_with_block_entries_duration(
                shard_id,
                epoch_start,
                200,
                myst_duration as i32,
            )),
            shard_id,
            epoch,
            duration,
            epoch_start,
            sender: Some(tx),
            receiver: rx,
            finish,
            segment_gen_data_path,
            upload_root,
            remote_store: Some(remote_store),
            runtime: Runtime::new().unwrap(),
        };
        info!(
            "Sender is {:?} {:?}",
            segment_writer.sender, &segment_writer.sender as *const _
        );
        segment_writer
    }

    pub(crate) fn write_to_segment(&mut self) -> Result<(), std::sync::mpsc::RecvError> {
        let mut count = 0;
        info!("Beginning segment writer for shard: {} epoch: {} epochStart: {} segment duration: {} duration: {}",
                                                                        self.shard_id,
                                                                        self.epoch,
                                                                        self.epoch_start,
                                                                        (self.epoch - self.epoch_start + self.duration),
                                                                        self.duration);

        let mut remote_store = Arc::new(self.remote_store.take().unwrap());
        let sleep_time = tokio::time::Duration::from_secs(2000);
        if self.epoch_start != self.epoch {
            let mut remote_filename = SegmentWriter::get_upload_filename(
                self.shard_id,
                self.epoch_start,
                self.upload_root.to_string(),
            );
            //Check segment_gen_data_path for file locally.
            let file_path =
                Path::new(self.segment_gen_data_path.as_ref()).join(Path::new(&remote_filename));
            create_dir_all(file_path.clone().parent().unwrap());
            let mut use_file = false;
            if !file_path.exists() {
                let mut f = File::create(&file_path).unwrap();
                let result =
                    self.download_file(Arc::clone(&remote_store), &remote_filename, &mut f);
                if result.is_ok() {
                    let opt = result.unwrap();
                    if opt.unwrap_or(-1) == 1 {
                        use_file = true;
                    }
                }
            } else {
                use_file = true;
            }
            if use_file {
                info!("Loading segment for {} into mem", remote_filename);
                let mut f = File::open(&file_path).unwrap();
                let offset: u32 = 0;
                let result = self.segment.take().unwrap().load(&mut f, &offset);

                match result {
                    Ok(o) => {
                        info!(
                            "Loaded segment suecessfully for {} into mem",
                            remote_filename
                        );
                        self.segment = o;
                    }
                    Err(e) => {
                        error!(
                            "Error loading segment for {} into mem {:?}",
                            remote_filename, e
                        );
                    }
                }
            }
        }
        info!("Done with segment init, will begin receiving data for shard: {} epoch: {} epochStart: {} segment duration: {}",
                                                                        self.shard_id,
                                                                        self.epoch,
                                                                        self.epoch_start,
                                                                        (self.epoch - self.epoch_start + self.duration));
        loop {
            let mut result = None;
            let finish_lock = self.finish.load(Ordering::SeqCst);
            result = futures::executor::block_on(self.receiver.recv());

            if finish_lock && result.is_none() {
                if count == 0 {
                    info!(
                        "No records read for shard: {} for epoch: {}",
                        self.shard_id, self.epoch
                    );
                    //Should not happen but, we shouldnt panic because of this.
                    break;
                }
                info!(
                    "Creating segment for shard: {} for epoch: {}",
                    self.shard_id, self.epoch
                );
                let s = self.segment.take().unwrap();
                self.create_and_upload_segment(Arc::clone(&remote_store), s);
                info!(
                    "Stopping shard: {} thread for epoch: {}",
                    self.shard_id, self.epoch
                );

                break;
            }

            //info!("Mutex == {:?}", self.finish);
            match result {
                Some(record) => {
                    count += 1;
                    //for record in records {
                    /* Have to do this because Rc cannot be shared between threads
                    The other option is to have MystSegment impl Arc, but that maybe too costly - not sure.*/
                    let mut hashMap = HashMap::new();
                    let tags = record.tags.clone();
                    for (k, v) in tags {
                        hashMap.insert(Rc::new(k), Rc::new(v));
                    }

                    self.segment.as_mut().unwrap().add_timeseries(
                        Rc::new(record.metric.clone()),
                        hashMap.clone(), //Whats the difference between making a clone and doing a mut hashMap ?
                        record.xx_hash as u64,
                        self.epoch,
                    );
                    // }
                }
                None => {
                    info!(
                        "################ Error in segment writer {:?}",
                        self.shard_id
                    );
                }
            }
        }

        info!("Timeseries written to shard {} is {}", self.shard_id, count);
        Ok(())
    }

    fn download_file(
        &mut self,
        downloader: Arc<RemoteStore>,
        remote_filename: &String,
        mut buf: &mut io::Write,
    ) -> Result<Option<i32>, Error> {
        let sleep_time = tokio::time::Duration::from_millis(2000);
        let mut retries = 0;
        let map_option: Option<HashMap<String, String>>;
        loop {
            let result = self
                .runtime
                .block_on(downloader.get_metadata(remote_filename.clone()));

            if result.is_err() {
                error!(
                    "Getting metadata failed for {} {:?} retries left: {}",
                    remote_filename,
                    result,
                    3 - retries
                );
                if retries == 3 {
                    error!(
                        "Will create a new file for {} as all retried failed",
                        remote_filename
                    );
                    map_option = None;
                    return Err(Error::new(ErrorKind::Other, "Error fetching meta from s3"));
                }
                retries += 1;
            } else {
                info!(
                    "Object meta call sucessfull or Object is not present: {} {:?}",
                    remote_filename, result
                );
                map_option = result.unwrap();
                break;
            }
            thread::sleep(sleep_time);
        }

        if map_option.is_some() {
            //Object is present, download it
            info!("Downloading {}", remote_filename);
            let result = self
                .runtime
                .block_on(downloader.download(remote_filename.clone(), &mut buf));
            if result.is_err() {
                error!(
                    "Error downloading: {} from S3 {:?}",
                    remote_filename, result
                );
                return Err(Error::new(ErrorKind::Other, "Error downloding from s3"));
            }
            return Ok(Some(1 as i32));
        } else {
            return Ok(Some(0 as i32));
        }
    }
    /// This method cannot be called as is. Will need to be worked into the code flow.
    fn create_segment(&mut self, myst_segment: MystSegment) {
        let shard_id = self.shard_id;
        let epoch = myst_segment.epoch;
        let data_path = self.segment_gen_data_path.clone();
        let filename = MystSegment::get_segment_filename(&shard_id, &epoch, data_path.to_string());
        info!("Creating segment file: {:?}", &filename);
        let mut file = File::create(&filename).unwrap();
        myst_segment.build(&mut file, &mut (0 as u32)).unwrap();
        file.flush().unwrap();
        let mut lock_file_name =
            MystSegment::get_path_prefix(&shard_id, &epoch, data_path.to_string());
        lock_file_name.push_str("/.lock");
        File::create(lock_file_name).unwrap();
        info!("Created segment file: {}", &filename);
    }

    fn create_and_upload_segment(&mut self, uploader: Arc<RemoteStore>, myst_segment: MystSegment) {
        let shard_id = self.shard_id;
        let epoch = myst_segment.epoch;
        let dur_result = myst_segment.get_duration();
        let dur_string = dur_result.unwrap().to_string();
        let mut vec_writer = Vec::new().writer();
        myst_segment
            .build(&mut vec_writer, &mut (0 as u32))
            .unwrap();
        let mut upload_root = self.upload_root.clone().to_string();

        let data = vec_writer.get_mut().to_vec();
        let upload_filename = SegmentWriter::get_upload_filename(shard_id, epoch, upload_root);
        //Check segment_gen_data_path for file locally.
        let file_path =
            Path::new(self.segment_gen_data_path.as_ref()).join(Path::new(&upload_filename));

        let parent_dir = file_path.parent();
        create_dir_all(parent_dir.unwrap());
        info!(
            "Creating segment file: {:?} for duration: {}",
            &file_path, &dur_string
        );
        let mut file = File::create(&file_path).unwrap();
        let result = file.write_all(&data);
        match result {
            Ok(()) => info!("Writing to segment file in segment gen: {:?}", &file_path),
            Err(e) => error!("Failed to write to segment file: {:?} {:?}", &file_path, e),
        }

        let result = file.flush();

        match result {
            Ok(()) => info!("Created segment file in segment gen: {:?}", &file_path),
            Err(e) => error!("Failed to create segment file: {:?} {:?}", &file_path, e),
        }

        drop(file);

        info!(
            "Calling the uploader (upload segment)! for filename: {} and duration: {}",
            &upload_filename, &dur_string
        );
        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("duration".to_string(), dur_string);
        let result = self.runtime.block_on(uploader.upload(
            upload_filename.clone().to_owned(),
            data,
            metadata,
        ));
        info!(
            "Uploaded (upload segment) for filename: {} {:?}",
            &upload_filename, result
        );
    }

    fn upload_segment(&mut self, uploader: Arc<RemoteStore>, myst_segment: MystSegment) {
        let shard_id = self.shard_id;
        let epoch = myst_segment.epoch;
        let mut upload_root = self.upload_root.clone().to_string();

        let mut upload_filename = SegmentWriter::get_upload_filename(shard_id, epoch, upload_root);

        let mut vec_writer = Vec::new().writer();
        let dur_string = myst_segment.get_duration().unwrap().to_string();
        myst_segment
            .build(&mut vec_writer, &mut (0 as u32))
            .unwrap();

        let data = vec_writer.get_mut().to_vec();
        info!(
            "Calling the uploader (upload segment)! for filename: {}",
            &upload_filename
        );
        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert("duration".to_string(), dur_string);
        let result = self.runtime.block_on(uploader.upload(
            upload_filename.clone().to_owned(),
            data,
            metadata,
        ));
        info!(
            "Uploaded (upload segment) for filename: {} {:?}",
            &upload_filename, result
        );
    }

    fn get_upload_filename(shard_id: u32, epoch: u64, mut upload_root: String) -> String {
        if !upload_root.ends_with("/") {
            upload_root.push_str("/");
        }
        MystSegment::get_segment_filename(&shard_id, &epoch, upload_root)
    }
}
