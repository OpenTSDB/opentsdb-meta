use crate::segment::myst_segment::{MystSegment, MystSegmentHeaderKeys};
use std::rc::Rc;
use std::io::{Read, Seek};
use crate::segment::persistence::Compactor;
use crate::segment::segment_reader::SegmentReader;
use num_traits::ToPrimitive;
use crate::segment::store::myst_fst::{MystFSTContainer, MystFST};
use crate::segment::store::metric_bitmap::MetricBitmap;
use crate::segment::store::tag_bitmap::{TagKeysBitmap, TagValuesBitmap};
use std::collections::{HashMap, HashSet};
use crate::segment::store::dict::Dict;
use crate::segment::store::docstore::{DocStore, Timeseries};
use crate::segment::store::epoch_bitmap::EpochBitmap;
use crate::utils::myst_error::{MystError, Result};
use log::info;
use std::sync::Arc;

impl Compactor for MystSegment {

    fn compact(&mut self, segment: MystSegment) {
        //compact fsts
        for (key, myst_fst) in segment.fsts.fsts {
            if !self.fsts.fsts.contains_key(&key) {
                self.fsts.fsts.insert(key, myst_fst);
            } else {
                let mut curr_myst_fst = self.fsts.fsts.get_mut(&key).unwrap();
                for (key, val) in myst_fst.buf.iter() {
                    if ! curr_myst_fst.buf.contains_key(key) {
                        self.uid += 1;
                        let new_val = MystFST::generate_val(self.uid, 0);
                        curr_myst_fst.buf.insert(key.clone(), new_val);
                        self.dict.dict.insert(self.uid, key.clone());
                    }
                }
            }
        }
        // compact bitmaps
        // compact metrics bitmapss
        for (key, bitmap) in segment.metrics_bitmap.metrics_bitmap {
            if !self.metrics_bitmap.metrics_bitmap.contains_key(&key) {
                self.metrics_bitmap.metrics_bitmap.insert(key, bitmap);
            } else {
                let mut curr_bitmap = self.metrics_bitmap.metrics_bitmap.get_mut(&key).unwrap();
                curr_bitmap.add_many(&bitmap.to_vec());
            }
        }

        //compact tagkeys bitmap
        for (key, bitmap) in segment.tag_keys_bitmap.tag_keys_bitmap {
            if !self.tag_keys_bitmap.tag_keys_bitmap.contains_key(&key) {
                self.tag_keys_bitmap.tag_keys_bitmap.insert(key, bitmap);
            } else {
                let mut curr_bitmap = self.tag_keys_bitmap.tag_keys_bitmap.get_mut(&key).unwrap();
                curr_bitmap.add_many(&bitmap.to_vec());

            }
        }

        //compact tag vals bitmap
        for (key, val_bitmap) in segment.tag_vals_bitmap.tag_vals_bitmap {
            if !self.tag_vals_bitmap.tag_vals_bitmap.contains_key(&key) {
                self.tag_vals_bitmap.tag_vals_bitmap.insert(key, val_bitmap);
            } else {
                for (value, bitmap) in val_bitmap {
                    let mut curr_val_bitmap = self.tag_vals_bitmap.tag_vals_bitmap.get_mut(&key).unwrap();
                    if ! curr_val_bitmap.contains_key(&value) {
                        curr_val_bitmap.insert(value, bitmap);
                    } else {
                        let curr_bitmap = curr_val_bitmap.get_mut(&value).unwrap();
                        curr_bitmap.add_many(&bitmap.to_vec());
                    }
                }
            }
        }

        //compact epoch bitmap
        for (key, bitmap) in segment.epoch_bitmap.epoch_bitmap {
            if !self.epoch_bitmap.epoch_bitmap.contains_key(&key) {
                self.epoch_bitmap.epoch_bitmap.insert(key, bitmap);
            } else {
                let mut curr_bitmap = self.epoch_bitmap.epoch_bitmap.get_mut(&key).unwrap();
                curr_bitmap.add_many(&bitmap.to_vec());
            }
        }

        //compact docstore
        // Load xxhashes into dedup map.
        let mut dedup: HashSet<u64> =  HashSet::new();

        let doc_vec = &self.data.data;

        for t in doc_vec {
            dedup.insert(t.timeseries_id);
        }
        for timeseries in segment.data.data {
            if ! dedup.contains(&timeseries.timeseries_id) {
                //convert tags in timeseries that have old dict id's to new dict id's
                let mut tags = HashMap::new();
                for (key, val) in timeseries.tags.iter() {
                    let key_str = segment.dict.dict.get(key).unwrap();
                    let val_str = segment.dict.dict.get(val).unwrap();
                    let mut new_key_id= 0;
                    let mut new_val_id= 0;
                    for (key, fst) in self.fsts.fsts.iter() {
                        if fst.buf.contains_key(key_str) {
                            new_key_id = *fst.buf.get(key_str).unwrap();
                        }
                        if fst.buf.contains_key(val_str) {
                            new_val_id = *fst.buf.get(val_str).unwrap();
                        }
                    }
                    if new_key_id == 0 || new_val_id == 0 {
                        panic!("Should not happen!! Either Tag Key or Value is missing in the merged fst. ");
                    }
                    tags.insert(MystFST::get_id(new_key_id), MystFST::get_id(new_val_id));
                }
                let new_timeseries = Timeseries{
                    timeseries_id: timeseries.timeseries_id,
                    tags: Arc::new(tags),
                };
                self.data.data.push(new_timeseries);
            }

        }

    }
}