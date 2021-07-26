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

#[path = "../myst_grpc.rs"]
pub mod myst_grpc;

use myst_grpc::myst_service_client::MystServiceClient;
use myst_grpc::QueryRequest;

use rusoto_core::proto::xml::util::parse_response;
use std::env;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let mut host = Box::new(String::from("http://"));
    host.push_str(args.get(1).unwrap());
    let h: &'static str = Box::leak(host);
    let channel = tonic::transport::Channel::from_static(h).connect().await?;
    let mut client = MystServiceClient::new(channel);

    let s = args.get(2).unwrap();
    let query_request = QueryRequest {
        query: String::from(s),
    };
    println!("Running query {:?}", query_request);
    let mut stream = client
        .get_timeseries(Request::new(query_request))
        .await?
        .into_inner();

    while let Some(resp) = stream.message().await? {
        println!("Got stream {:?}", resp);
    }

    Ok(())
}
