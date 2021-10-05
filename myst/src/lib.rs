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
/// Myst is OpenTSDB's (https://github.com/opentsdb) metadata storage and query layer.
/// This crate contains one library (myst) and two binaries (server and segment-gen).
extern crate byteorder;
extern crate fasthash;
extern crate tokio_stream;
extern crate tonic;

use crate::utils::config::Config;
use metrics_reporter::MetricsReporter;
use once_cell::sync::OnceCell;

pub mod myst_grpc;
pub mod query;

pub mod s3;
pub mod segment;
pub mod utils;
//pub mod rollup;

static METRICS_REPORTER: OnceCell<Box<dyn MetricsReporter>> = OnceCell::new();

pub fn setup_logger(filename: String) -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S:%f]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(fern::log_file(filename)?)
        // .chain(std::io::stdout())
        .apply()?;
    Ok(())
}

pub fn load_metrics_reporter(lib: &libloading::Library) {
    let config = Config::new();
    println!("Creating metrics reporter");
    let metrics_reporter: Box<dyn MetricsReporter> = match config.ssl_for_metrics {
        true => {
            let new_metrics_reporter: libloading::Symbol<
                fn(&str, &str, &str) -> Box<dyn MetricsReporter>,
            > = unsafe { lib.get(b"new_with_ssl") }.expect("load symbol");
            let metrics_reporter =
                new_metrics_reporter(&config.ssl_key, &config.ssl_cert, &config.ca_cert);
            metrics_reporter
        }
        false => {
            let new_metrics_reporter: libloading::Symbol<fn() -> Box<dyn MetricsReporter>> =
                unsafe { lib.get(b"new") }.expect("load symbol");
            let metrics_reporter = new_metrics_reporter();
            metrics_reporter
        }
    };
    METRICS_REPORTER.set(metrics_reporter);
}

pub fn metrics_count(tags: &[&str], metric: &str, val: u64) {
    if METRICS_REPORTER.get().is_some() {
        METRICS_REPORTER
            .get()
            .as_deref()
            .unwrap()
            .count(metric, tags, val);
    }
}

pub fn metrics_gauge(tags: &[&str], metric: &str, val: u64) {
    if METRICS_REPORTER.get().is_some() {
        METRICS_REPORTER
            .get()
            .as_deref()
            .unwrap()
            .gauge(metric, tags, val);
    }
}
