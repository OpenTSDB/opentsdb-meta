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

use std::time::Duration;

use crate::utils::config::Config;

pub mod myst_grpc;
pub mod query;

pub mod s3;
pub mod segment;
pub mod utils;
//pub mod rollup;

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
