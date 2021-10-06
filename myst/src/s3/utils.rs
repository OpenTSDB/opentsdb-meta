use crate::segment::myst_segment::MystSegment;
use rusoto_core::credential::StaticProvider;
use rusoto_core::{HttpClient, Region};
use rusoto_s3::S3Client;
use std::sync::Arc;

pub fn get_upload_filename(shard_id: u32, epoch: u64, mut upload_root: String) -> String {
    if !upload_root.ends_with("/") {
        upload_root.push_str("/");
    }
    MystSegment::get_segment_filename(&shard_id, &epoch, upload_root)
}

pub fn create_new_s3_client(arc_s3_creds: Arc<StaticProvider>, region: Region) -> S3Client {
    S3Client::new_with(
        HttpClient::new().expect("Failed to create client"),
        arc_s3_creds.clone(),
        region,
    )
}
