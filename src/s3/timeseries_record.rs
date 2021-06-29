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

use std::collections::HashMap;
use std::io::{Error, ErrorKind};

#[derive(Default, Debug)]
pub(crate) struct Record {
    pub tags: HashMap<String, String>,
    pub metric: String,
    pub xx_hash: i64,
}

pub trait ParseRecord {
    fn parse(&mut self, buf: &mut [u8]) -> Result<i32, std::io::Error>;
}

impl ParseRecord for Record {
    fn parse(&mut self, buf: &mut [u8]) -> Result<i32, std::io::Error> {
        let mut i: usize = 0;
        let _separator: u8 = 0;
        let _record: u8 = 1;
        let _hash: u8 = 2;
        let _tag: u8 = 3;
        let _metric: u8 = 4;

        //Read record header;
        i += 1;
        i += 1;
        i += 1;
        i += 1;
        //Read hash
        self.xx_hash = Record::read_long(buf, i);
        //info!("Hash: {} {}", self.xx_hash, i);
        i += 8;
        i += 1;
        i += 1;
        //Read tags;
        self.tags = HashMap::new();

        loop {
            //info!("Reading next tag: {} buf len: {}", i,buf.len());
            let key = Record::get_next_string(buf, i)?;
            i += key.len();
            i += 1;
            let value = Record::get_next_string(buf, i)?;
            //info!("Read tag: {} {}", key, value);
            i += value.len();
            i += 1;
            self.tags.insert(key, value);
            if i >= buf.len() {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Index is out of bounds, potentially because no metric was written.",
                ));
            }
            if buf[i] == 1 {
                break;
            }
        }

        i += 1;
        i += 1;
        //info!("Reading next metric: {} buf len: {}", i,buf.len());
        let metric = Record::get_next_string(buf, i)?;
        //info!("Read metric: {} {}", metric, i);
        self.metric = metric;
        Ok(0)
    }
}

impl Record {
    fn get_next_string(buf: &mut [u8], pos: usize) -> Result<String, std::io::Error> {
        let str_len = Record::get_len(buf, pos)?;
        let mut str_vec = vec![0u8; str_len];
        str_vec.copy_from_slice(&buf[pos..(pos + str_len)]);
        let result = String::from_utf8(str_vec);
        match result {
            Err(_e) => return Err(Error::new(ErrorKind::Other, "Utf8 error")),
            Ok(next_string) => return Ok(next_string),
        }
        //info!("Parsed result {:?} len: {}", result, str_len);
    }

    pub fn get_len(buf: &mut [u8], pos: usize) -> Result<usize, std::io::Error> {
        let mut index = pos;
        if index >= buf.len() {
            //info!("index: {} len: {}", index, buf.len());
            return Err(Error::new(ErrorKind::Other, "Index out of bounds"));
        }
        while buf[index] != 0x0 {
            index += 1;
            if index == buf.len() {
                break;
            }
        }
        return Ok(index - pos);
    }

    pub fn read_int(buf: &mut [u8]) -> usize {
        let pos: usize = 0;
        let mut x = (buf[pos] as i32 & 0xFF) << 24;
        x = x | (buf[(pos + 1)] as i32 & 0xFF) << 16;
        x = x | (buf[(pos + 2)] as i32 & 0xFF) << 8;
        x = x | (buf[(pos + 3)] as i32 & 0xFF);
        return x as usize;
    }

    pub fn read_long(buf: &mut [u8], pos: usize) -> i64 {
        let mut position = pos;
        let mut x = (buf[position] as i64) << 56;
        position += 1;
        x = x | (buf[position] as i64 & 0xFF) << 48;
        position += 1;
        x = x | ((buf[position] as i64 & 0xFF) as i64) << 40;
        position += 1;
        x = x | ((buf[position] as i64 & 0xFF) as i64) << 32;
        position += 1;
        x = x | ((buf[position] as i64 & 0xFF) as i64) << 24;
        position += 1;
        x = x | (((buf[position] as i64 & 0xFF) as i64) << 16) as i64;
        position += 1;
        x = x | (((buf[position] as i64 & 0xFF) as i64) << 8) as i64;
        position += 1;
        x = x | (buf[position] as i64 & 0xFF) as i64;
        return x;
    }
}
