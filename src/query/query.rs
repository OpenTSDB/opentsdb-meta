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

use crate::utils::myst_error::MystError;

use std::str::FromStr;
use strum_macros::*;

use super::{query_filter::QueryFilter, shard_query_runner::ShardQueryRunner};
use serde_json::Value;

use crate::query::cache::Cache;
use crate::utils::config::Config;

use metrics_reporter::MetricsReporter;
use std::sync::Arc;

#[derive(EnumString, Debug, PartialEq)]
pub enum QueryType {
    TAG_KEYS,
    METRICS,
    TAG_KEYS_AND_VALUES,
    TIMESERIES,
}

#[derive(Debug)]
pub struct Query {
    pub from: u32,
    pub to: u32,
    pub start: u64,
    pub end: u64,
    pub query_type: QueryType,
    pub limit: u32,
    pub group: Vec<String>,
    pub filter: QueryFilter,
}

const LIMIT: u64 = 1024;

impl Query {
    pub fn from_json(json: &str) -> Result<Query, MystError> {
        let value = serde_json::from_str(json);
        let json_value: Value = value.unwrap();
        let filter = &json_value["query"];
        let group = json_value["group"]
            .as_array()
            .ok_or(MystError::new_query_error("Group not found in query"))?;
        let mut group_by = Vec::new();
        for g in group {
            group_by
                .push(String::from(g.as_str().ok_or(
                    MystError::new_query_error("Cannnot convert to string"),
                )?));
        }
        let query_type_str = json_value["type"]
            .as_str()
            .ok_or(MystError::new_query_error("type not found in query"))?;
        let query_type = QueryType::from_str(query_type_str).unwrap();
        let mut from = 0;
        let mut to = 0;
        let mut limit = 0;
        if query_type != QueryType::TIMESERIES {
            from = json_value["from"]
                .as_u64()
                .ok_or(MystError::new_query_error("Cannot convert from to long"))?
                as u32;
            to = json_value["to"]
                .as_u64()
                .ok_or(MystError::new_query_error("Cannnot convert `to` to long"))?
                as u32;
            limit = json_value["limit"]
                .as_u64()
                .ok_or(MystError::new_query_error("Cannnot convert limit to int"))?
                as u32;
        }
        let query = Query {
            from,
            to,

            start: {
                let mut start = json_value["start"]
                    .as_u64()
                    .ok_or(MystError::new_query_error("Cannot convert to long"))?
                    as u64;
                start = start - (start % 1800);
                start
            },
            end: {
                let mut end = json_value["end"]
                    .as_u64()
                    .ok_or(MystError::new_query_error("Cannot convert to long"))?
                    as u64;
                end = end - (end % 1800);
                end = end + 1800;
                end
            },
            limit,
            query_type,

            filter: QueryFilter::from_json(filter)?,
            group: group_by,
        };
        if query.query_type == QueryType::TAG_KEYS_AND_VALUES {
            if query.group.is_empty() {
                return Err(MystError::new_query_error(
                    "Group is empty for TAG KEYS AND VALUES QUERY",
                ));
            }
        }
        Ok(query)
    }

    pub fn run_query(
        query: &Query,
        shard_pool: &rayon::ThreadPool,
        cache: Arc<Cache>,
        config: &Config,
        metrics_reporter: Option<&Box<MetricsReporter>>,
    ) -> Result<
        tokio::sync::mpsc::Receiver<
            std::result::Result<crate::myst_grpc::TimeseriesResponse, tonic::Status>,
        >,
        MystError,
    > {
        ShardQueryRunner::run(query, shard_pool, cache, config, metrics_reporter)
    }
}
