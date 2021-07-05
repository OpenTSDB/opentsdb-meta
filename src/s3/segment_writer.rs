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
use myst::segment::persistence::Builder;

use log::info;
use std::collections::HashMap;
use std::fs::{rename, File};
use std::ops::Deref;

use bytes::BufMut;
use std::io::Read;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

pub(crate) struct SegmentWriter {
    pub segment: Option<MystSegment>,
    pub shard_id: u32,
    pub epoch: u64,
    pub sender: Option<mpsc::Sender<Record>>,
    pub receiver: mpsc::Receiver<Record>,
    pub finish: Arc<AtomicBool>,
    pub data_path: Arc<String>,
    pub upload_root: Arc<String>,
    pub remote_store: Option<RemoteStore>,
}

impl SegmentWriter {
    pub(crate) fn new(
        shard_id: u32,
        finish: Arc<AtomicBool>,
        queue_size: usize,
        epoch: u64,
        data_path: Arc<String>,
        upload_root: Arc<String>,
        remote_store: RemoteStore,
    ) -> Self {
        let (tx, rx) = mpsc::channel(queue_size);
        let segment_writer = Self {
            segment: Some(MystSegment::new(shard_id, epoch, 200)),
            shard_id,
            epoch,
            sender: Some(tx),
            receiver: rx,
            finish,
            data_path,
            upload_root,
            remote_store: Some(remote_store),
        };
        info!(
            "Sender is {:?} {:?}",
            segment_writer.sender, &segment_writer.sender as *const _
        );
        segment_writer
    }

    pub(crate) fn write_to_segment(&mut self) -> Result<(), std::sync::mpsc::RecvError> {
        let mut count = 0;
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
                self.upload_segment(s);
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

    fn create_segment(&mut self, myst_segment: MystSegment) {
        let shard_id = self.shard_id;
        let epoch = myst_segment.epoch;
        let data_path = self.data_path.clone();
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

        let filename_clone = Arc::new(filename.clone());
        let uploader = self.remote_store.take().unwrap();
        let mut upload_root = self.upload_root.clone().to_string();
        if !upload_root.ends_with("/") {
            upload_root.push_str("/");
        }
        let upload_filename = MystSegment::get_segment_filename(&shard_id, &epoch, upload_root);
        let move_result = rename(&filename, &upload_filename);

        match move_result {
            Ok(obj) => {
                info!("Calling the uploader! for filename: {}", &upload_filename);
                let mut data: Vec<u8> = Vec::new();
                let mut f = File::open(&upload_filename).unwrap();
                f.read_to_end(&mut data);
                let result = Runtime::new()
                    .unwrap()
                    .block_on(uploader.upload(upload_filename.clone().to_owned(), data));
                info!("Uploaded for filename: {}", &upload_filename);
            }
            Err(e) => info!(
                "Move from {} to {} resulted in error: {:?}",
                &filename, &upload_filename, e
            ),
        };
    }

    fn upload_segment(&mut self, myst_segment: MystSegment) {
        let shard_id = self.shard_id;
        let epoch = myst_segment.epoch;

        let uploader = self.remote_store.take().unwrap();
        let mut upload_root = self.upload_root.clone().to_string();
        if !upload_root.ends_with("/") {
            upload_root.push_str("/");
        }
        let upload_filename = MystSegment::get_segment_filename(&shard_id, &epoch, upload_root);

        let mut vec_writer = Vec::new().writer();

        myst_segment
            .build(&mut vec_writer, &mut (0 as u32))
            .unwrap();

        let data = vec_writer.get_mut().to_vec();
        info!(
            "Calling the uploader (upload segment)! for filename: {}",
            &upload_filename
        );
        let result = Runtime::new()
            .unwrap()
            .block_on(uploader.upload(upload_filename.clone().to_owned(), data));
        info!(
            "Uploaded (upload segment) for filename: {} {:?}",
            &upload_filename, result
        );
    }
}
