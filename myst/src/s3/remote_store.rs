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

use log::{error, info};
use rusoto_core::RusotoError;
use rusoto_s3::{
    GetObjectRequest, HeadObjectError, HeadObjectRequest, ListObjectsError, ListObjectsOutput,
    ListObjectsRequest, PutObjectRequest, S3Client, StreamingBody, S3,
};
use std::{
    collections::HashMap,
    io,
    io::{Error, ErrorKind},
    sync::Arc,
    time::SystemTime,
};

use tokio::io::AsyncReadExt;

// Ideally, we should have MystSegment implement the std::io::Read interface.
// This is the next best thing.
#[derive(Clone)]
pub struct RemoteStore {
    s3_client: Arc<S3Client>,
    bucket: Arc<String>,
}

impl RemoteStore {
    pub fn new(s3_client: Arc<S3Client>, bucket: Arc<String>) -> RemoteStore {
        Self { s3_client, bucket }
    }

    pub async fn upload(
        &self,
        file_name: String,
        data: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> Result<i32, Error> {
        let b = std::path::Path::new(&file_name).exists();
        info!("Upload Path exists: {} {}", &file_name, b);
        //let stream_from_file = fs::read(file_name.to_string().to_owned())
        //.into_stream()
        //.map_ok(|b| Bytes::from(b));
        let put_request = PutObjectRequest {
            bucket: self.bucket.clone().to_string(),
            key: file_name.clone().to_string(),
            metadata: Some(metadata),
            body: Some(StreamingBody::from(data)),
            ..Default::default()
        };
        let result = self.s3_client.put_object(put_request).await;

        match result {
            Err(e) => {
                info!("Error uploading to S3 {:?}", e);
                return Err(Error::new(ErrorKind::Other, "Error fetching from s3"));
            }
            Ok(_) => {}
        }
        Ok(0)
    }

    //Downloads to a file and returns the buffer.
    //Consider making this a BufReader
    pub async fn download<W: io::Write>(
        &self,
        file_name: String,
        output: &mut W,
    ) -> Result<(), Error> {
        let mut get_object_request = GetObjectRequest::default();
        get_object_request.bucket = self.bucket.to_string().to_owned();
        get_object_request.key = file_name.to_owned();

        let result = self.s3_client.get_object(get_object_request).await;

        let object = match result {
            Ok(obj) => obj,
            Err(e) => {
                info!("Error downloading object for: {} {:?}", file_name, e);
                return Err(Error::new(ErrorKind::Other, "Error downloading object"));
            }
        };

        let len = object.content_length.unwrap();
        let now = SystemTime::now();
        info!(
            "Downloading Object: {:?} and length: {}",
            object.metadata, len
        );
        let mut stream = object.body.unwrap().into_async_read();

        let mut buf = vec![0u8; 1024];
        //Naive Stream read + write
        loop {
            let number = stream.read(&mut buf).await?;
            if number == 0 {
                output.flush()?;
                info!(
                    "Finished Object download: {:?} in: {:?} seconds",
                    object.metadata,
                    now.elapsed().unwrap().as_secs()
                );
                break;
            }
            output.write(&buf[0..number]);
        }
        return Ok(());
    }

    pub async fn get_metadata(
        &self,
        file_name: String,
    ) -> Result<Option<HashMap<String, String>>, Error> {
        let mut head_request = HeadObjectRequest::default();

        head_request.bucket = self.bucket.to_string();

        head_request.key = file_name.to_owned();

        let result = self.s3_client.head_object(head_request).await;
        match result {
            Ok(obj) => {
                let result = obj.metadata;
                if result.is_some() {
                    return Ok(result);
                } else {
                    return Ok(Some(HashMap::new()));
                }
            }
            Err(e) => match e {
                RusotoError::Service(HeadObjectError::NoSuchKey(_)) => {}
                _ => {
                    return Err(Error::new(ErrorKind::Other, e.to_string()));
                }
            },
        }
        Ok(None)
    }

    pub async fn list_files(&self, file_prefix: String) -> Result<Option<Vec<String>>, Error> {
        let result = self.list(&file_prefix, true).await?;
        let object_list = match result.contents {
            Some(obj) => obj,
            None => {
                info!("Didnt find anything in S3 for {}", file_prefix);
                return Ok(None);
            }
        };
        let list_of_objects = object_list
            .iter()
            .filter(|x| x.key.as_ref().is_some())
            .map(|x| x.key.as_ref().unwrap().to_string())
            .collect::<Vec<_>>();

        Ok(Some(list_of_objects))
    }

    pub async fn list_sub_folders(
        &self,
        file_prefix: String,
    ) -> Result<Option<Vec<String>>, Error> {
        let result = self.list(&file_prefix, false).await?;
        let object_list = match result.common_prefixes {
            Some(obj) => obj,
            None => {
                info!("Didnt find anything in S3 for {}", file_prefix);
                return Ok(None);
            }
        };
        let list_of_objects = object_list
            .iter()
            .filter(|x| x.prefix.as_ref().is_some())
            .map(|x| x.prefix.as_ref().unwrap().to_string())
            .collect::<Vec<_>>();

        Ok(Some(list_of_objects))
    }

    async fn list(
        &self,
        file_prefix: &String,
        read_all_files: bool,
    ) -> Result<ListObjectsOutput, Error> {
        let mut list_request = ListObjectsRequest::default();

        list_request.bucket = self.bucket.to_string().to_owned();

        list_request.prefix = Some(file_prefix.to_owned());
        if !read_all_files {
            list_request.delimiter = Some("/".to_string());
        }
        let result = self.s3_client.list_objects(list_request).await;

        if result.is_err() {
            error!(
                "Error listing files for: {} {:?}",
                file_prefix,
                result.err()
            );
            return Err(Error::new(ErrorKind::Other, "Error listing files"));
        }
        Ok(result.unwrap())
    }
}
