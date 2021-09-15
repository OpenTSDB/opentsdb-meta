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

use std::path::PathBuf;
/// A constant string that represents tag keys.
pub const TAG_KEYS: &str = "__tagkey";
/// A constant string that represents metrics.
pub const METRIC: &str = "__metric";
/// A constant string that represents tag values.
pub const TAG_VALUES: &str = "__tagvalue";

/// Contains properties for Myst
#[derive(Default)]
pub struct Config {
    // myst properties
    pub shards: usize,
    pub data_download_path: String,
    pub data_read_path: String,
    pub segment_gen_data_path: String,
    pub cache: Vec<String>,
    pub docstore_block_size: usize,
    pub log_file: String,
    pub ssl_key: String,
    pub ssl_cert: String,
    pub ca_cert: String,
    pub polling_interval: u64,
    pub segment_duration: u64,
    pub segment_full_duration: u64,
    pub namespace: String,
    pub start_epoch: u64,
    pub input_bucket: String,
    pub processed_bucket: String,
    pub aws_key: String,
    pub aws_secret: String,
    pub temp_data_path: String,
    pub download_frequency: u64,

    pub plugin_path: String,
    pub ssl_for_metrics: bool,

    pub rollup_size: u32,
    pub num_containers: usize,
    pub container_id: usize,

    pub aws_region: String,
    pub aws_endpoint: String,
}
// TODO: Cleanup
impl Config {
    /// Loads and builds the config.
    pub fn new() -> Self {
        let config = load_config();
        let cache_str = config.get_str("cache").unwrap_or(String::from(""));
        let split = cache_str.split(",");
        let mut cache = Vec::new();
        for s in split {
            cache.push(String::from(s));
        }
        let namespace = config.get_str("namespace").unwrap().to_string();
        let data_download_path = config
            .get_str("data_download_path")
            .unwrap_or(String::from("/var/myst/data/"))
            .to_string();
        let data_read_path_root = data_download_path.clone();
        Self {
            namespace: namespace.clone(),
            data_download_path,
            shards: config.get_int("shards").unwrap_or(10) as usize,
            data_read_path: add_dir(data_read_path_root, namespace),
            segment_gen_data_path: config
                .get_str("segment_gen_data_path")
                .unwrap_or(String::from("/var/myst/segment_gen/data/"))
                .to_string(),
            cache,
            docstore_block_size: config.get_int("docstore_block_size").unwrap_or(200) as usize,
            log_file: config
                .get_str("log_file")
                .unwrap_or(String::from("/var/log/myst/myst.log"))
                .to_string(),

            ssl_key: config.get_str("ssl_key").unwrap(),
            ssl_cert: config.get_str("ssl_cert").unwrap(),
            ca_cert: config.get_str("ca_cert").unwrap(),
            polling_interval: config.get_int("polling_interval").unwrap() as u64,
            segment_duration: config.get_int("segment_duration").unwrap() as u64,
            segment_full_duration: config.get_int("segment_full_duration").unwrap() as u64,
            start_epoch: config.get_int("start_epoch").unwrap() as u64,
            input_bucket: config.get_str("input_bucket").unwrap().to_string(),
            processed_bucket: config.get_str("processed_bucket").unwrap().to_string(),
            aws_key: config.get_str("aws_key").unwrap().to_string(),
            aws_secret: config.get_str("aws_secret").unwrap().to_string(),
            temp_data_path: config.get_str("temp_data_path").unwrap().to_string(),
            download_frequency: config.get_int("download_frequency").unwrap() as u64,
            plugin_path: config
                .get_str("plugin_path")
                .unwrap_or(String::from("/usr/share/myst/plugins/metrics-reporter")),
            ssl_for_metrics: config.get_bool("ssl_for_metrics").unwrap(),
            rollup_size: config.get_int("rollup_size").unwrap_or(7 * 24 * 60 * 60) as u32,
            num_containers: config.get_int("num_containers").unwrap_or(5) as usize,
            container_id: config.get_int("container_id").unwrap_or(1) as usize,
            aws_region: config.get_str("aws_region").unwrap().to_string(),
            aws_endpoint: config.get_str("aws_endpoint").unwrap_or("".to_string()),
        }
    }
}

fn load_config() -> config::Config {
    let path = PathBuf::from("/etc/myst/myst");
    let file = config::File::from(path);
    let mut config = config::Config::new();
    config.merge(file).unwrap();
    let shard_config_path = PathBuf::from("/etc/myst/shard-config/shard-config");
    let shard_config_file = config::File::from(shard_config_path);
    config.merge(shard_config_file).unwrap();
    config
}

pub fn add_dir(mut root: String, child: String) -> String {
    if !root.ends_with("/") {
        root.push_str("/");
    }
    root.push_str(&child);
    root
}
