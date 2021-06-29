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

use serde_json::Value;

use crate::utils::myst_error::MystError;

use super::filter::FilterType;

#[derive(PartialEq, Debug)]
pub enum QueryFilter {
    Chain {
        filters: Vec<QueryFilter>,
        op: String,
    },
    ExplicitTags {
        filter: Box<QueryFilter>,
        count: u32,
    },
    NOT {
        filter: Box<QueryFilter>,
    },
    Metric {
        metric: String,
        _type: FilterType,
    },
    TagKey {
        filter: String,
        _type: FilterType,
    },
    TagValue {
        key: String,
        filter: String,
        _type: FilterType,
    },
}

impl QueryFilter {
    pub fn from_json(value: &Value) -> Result<QueryFilter, MystError> {
        let _type = value["type"].as_str().ok_or(MystError::new_query_error(
            "Error converting to filter for field: type",
        ))?;
        let filter = match _type.to_lowercase().as_str() {
            "chain" => {
                let op = value["op"].as_str().unwrap_or_else(|| "AND");
                let filters = value["filters"]
                    .as_array()
                    .ok_or(MystError::new_query_error(
                        "Error converting to filter for field: filters",
                    ))?;
                let mut chain_filter = QueryFilter::Chain {
                    filters: Vec::new(),
                    op: String::from(op),
                };
                for filter in filters {
                    if let QueryFilter::Chain { filters, op: _ } = &mut chain_filter {
                        filters.push(QueryFilter::from_json(filter)?);
                    }
                }
                chain_filter
            }
            "explicittags" => {
                let filters = &value["filter"];
                let filter = QueryFilter::from_json(filters)?;
                let count = QueryFilter::count_tag_filters(&filter, 0);
                let explicit_filter = QueryFilter::ExplicitTags {
                    filter: Box::new(filter),
                    count,
                };
                explicit_filter
            }
            "not" => {
                let filters = &value["filter"];
                let not_filter = QueryFilter::NOT {
                    filter: Box::new(QueryFilter::from_json(filters)?),
                };

                not_filter
            }
            "metricliteral" => {
                let metric = value["metric"].as_str().ok_or(MystError::new_query_error(
                    "Error converting to filter field: metric in metricliteral",
                ))?;
                let metric_filter = QueryFilter::Metric {
                    metric: String::from(metric),
                    _type: FilterType::Literal,
                };
                metric_filter
            }
            "tagkeyliteralor" => QueryFilter::TagKey {
                filter: String::from(value["filter"].as_str().ok_or(
                    MystError::new_query_error(
                        "Error converting to filter field: filter for tagkeyliteralor",
                    ),
                )?),
                _type: FilterType::Literal,
            },
            "tagkeyregex" => QueryFilter::TagKey {
                filter: String::from(value["filter"].as_str().ok_or(
                    MystError::new_query_error(
                        "Error converting to filter field: filter for tagkeyregex",
                    ),
                )?),
                _type: FilterType::Regex,
            },
            "tagvalueliteralor" => QueryFilter::TagValue {
                key: String::from(value["tagKey"].as_str().ok_or(MystError::new_query_error(
                    "Error converting to filter field: tagKey for tagvalueliteralor",
                ))?),
                filter: String::from(value["filter"].as_str().ok_or(
                    MystError::new_query_error(
                        "Error converting to filter field: filter for tagvalueliteralor",
                    ),
                )?),
                _type: FilterType::Literal,
            },
            "tagvalueregex" => QueryFilter::TagValue {
                key: String::from(value["tagKey"].as_str().ok_or(MystError::new_query_error(
                    "Error converting to filter field: tagKey for tagvalueregex",
                ))?),
                filter: String::from(value["filter"].as_str().ok_or(
                    MystError::new_query_error(
                        "Error converting to filter field: filter for tagvalueregex",
                    ),
                )?),
                _type: FilterType::Regex,
            },
            _ => return Err(MystError::new_query_error("Invalid Query Filter")),
        };
        Ok(filter)
    }

    fn count_tag_filters(filter: &QueryFilter, mut count: u32) -> u32 {
        match filter {
            QueryFilter::TagValue { .. } => count += 1,
            QueryFilter::TagKey { .. } => count += 1,
            QueryFilter::Chain { filters, .. } => {
                for sub_filter in filters {
                    count = QueryFilter::count_tag_filters(sub_filter, count);
                }
            }
            _ => {}
        };
        count
    }
}
