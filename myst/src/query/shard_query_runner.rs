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

use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::{fs, path::Path, thread, time::SystemTime};

use log::{error, info};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

use crate::query::cache::Cache;
use crate::segment::myst_segment::MystSegment;
use crate::segment::segment_reader::SegmentReader;
use crate::utils::config::{add_dir, Config};
use crate::utils::myst_error::{MystError, Result};

use super::{query::Query, query_runner::QueryRunner};
use metrics_reporter::MetricsReporter;
use tonic::Code;

/// Runs a query for all shards
pub struct ShardQueryRunner {}

impl ShardQueryRunner {
    /// Run query for all shards and returns a receiver side of the channel with a TimeseriesResponse and Status
    /// # Arguments
    /// * `query` - Query to be run
    /// * `shard_pool` - Thread pool to be used to run the query
    /// * `cache` - The cache to be used.
    /// * `config` - Config to be used.
    pub fn run(
        query: &Query,
        shard_pool: &rayon::ThreadPool,
        cache: Arc<Cache>,
        config: &Config,
        metrics_reporter: Option<&Box<MetricsReporter>>,
    ) -> Result<Receiver<std::result::Result<crate::myst_grpc::TimeseriesResponse, tonic::Status>>>
    {
        let shards = ShardQueryRunner::get_num_shards(config)?;
        let (tx, rx) = mpsc::channel(shards.len());
        let num_shards = shards.len();
        let num_streams = Arc::new(AtomicU32::new(0));
        shard_pool.scope(move |s| {
            for shard_id in shards {
                let _ns = num_streams.clone();
                let c = cache.clone();
                let sender = tx.clone();
                s.spawn(move |_s| {
                    info!(
                        "Creating new thread to run shard query {:?} for shard {}",
                        thread::current().id(),
                        shard_id
                    );
                    let mut timeseries_response = crate::myst_grpc::TimeseriesResponse {
                        grouped_timeseries: Vec::new(),
                        dict: None,
                        streams: 0,
                    };
                    let res = ShardQueryRunner::run_for_shard(
                        query,
                        shard_id,
                        shard_pool,
                        c,
                        config,
                        metrics_reporter,
                        &mut timeseries_response,
                    ); // TODO: panic_handler
                    if res.is_err() {
                        let message = format!("Error running query {:?}", res);
                        sender.try_send(Err(tonic::Status::new(Code::Internal, message)));
                        error!(
                            "Error running query for shard {} {:?} {:?}",
                            shard_id,
                            timeseries_response,
                            res.err()
                        );
                    } else {
                        // timeseries_response.streams = num_shards as i32;
                        let res = sender.try_send(Ok(timeseries_response));
                        if res.is_err() {
                            error!("Error sending query response {:?}", res);
                        }
                    }
                });
            }
        });
        Ok(rx)
    }

    fn get_num_shards(config: &Config) -> Result<Vec<u32>> {
        let mut result = Vec::new();
        let path = String::from(&config.data_read_path);
        let dirs = fs::read_dir(path)?;
        for dir in dirs {
            result.push(
                dir.unwrap()
                    .file_name()
                    .into_string()
                    .unwrap()
                    .parse()
                    .unwrap(),
            );
        }
        Ok(result)
    }

    fn run_for_shard(
        query: &Query,
        shard_id: u32,
        segment_pool: &rayon::ThreadPool,
        cache: Arc<Cache>,
        config: &Config,
        metrics_reporter: Option<&Box<MetricsReporter>>,
        timeseries_response: &mut crate::myst_grpc::TimeseriesResponse,
    ) -> Result<()> {
        let curr_time = SystemTime::now();
        let s_time = SystemTime::now();
        let mut path = String::from(&config.data_read_path);
        let mut path = add_dir(path, shard_id.to_string());
        let dirs = fs::read_dir(path)?;
        let mut segment_readers = Vec::new();

        for dir in dirs {
            let d = dir.unwrap();
            let mut path = d.path();
            path.push(".lock");
            let mut duration_file = d.path();
            duration_file.push("duration");
            if Path::new(&path).exists() {
                let created = d.file_name().to_str().unwrap().parse::<u64>().unwrap();
                let duration = if duration_file.exists() {
                    let dur = File::open(duration_file.as_path());
                    let mut dur_str = String::new();
                    match dur {
                        Ok(mut dur_file) => {
                            dur_file.read_to_string(&mut dur_str)?;
                            //If a duration file is present, it should have the right format.
                            let fduration = dur_str.parse()?;
                            info!(
                                "Read duration: {} for file: {:?}",
                                fduration, &duration_file
                            );
                            fduration
                        }
                        Err(e) => {
                            error!(
                                "Unable to read duration for file: {:?} {:?}",
                                &duration_file, e
                            );
                            0
                        }
                    }
                } else {
                    0
                };
                info!("Duration read for {:?} as {}", &duration_file, duration);

                if query.start <= created && query.end >= created
                    || (duration > 0
                        && query.start >= created
                        && query.end <= (created + duration as u64))
                {
                    let file_path = MystSegment::get_segment_filename(
                        &shard_id,
                        &created,
                        String::from(&config.data_read_path),
                    );
                    let reader = BufReader::new(File::open(file_path.clone())?);
                    let segment_reader = SegmentReader::new(
                        shard_id,
                        created,
                        reader,
                        cache.clone(),
                        file_path,
                        duration,
                    )?;
                    segment_readers.push(segment_reader);
                    timeseries_response.streams = segment_readers.len() as i32;
                }
            }
        }
        info!(
            "Time before starting segment query runner for shard {} is {:?}",
            shard_id,
            SystemTime::now().duration_since(s_time).unwrap()
        );

        if segment_readers.is_empty() {
            return Err(MystError::new_query_error(
                "No valid segments found for this time range",
            ));
        }
        let mut query_runner = QueryRunner::new(segment_readers, query, config, metrics_reporter);
        query_runner.search_timeseries(segment_pool, timeseries_response)?;
        if metrics_reporter.is_some() {
            metrics_reporter.unwrap().gauge(
                "shard.query.latency",
                &[
                    "shard",
                    shard_id.to_string().as_str(),
                    "host",
                    sys_info::hostname().unwrap().as_str(),
                ],
                SystemTime::now()
                    .duration_since(curr_time)
                    .unwrap()
                    .as_millis() as u64,
            );
        }
        info!(
            "Time taken to query in shard: {:?} is {:?} in thread {:?}",
            shard_id,
            SystemTime::now().duration_since(curr_time).unwrap(),
            thread::current().id()
        );
        Ok(())
    }
}
