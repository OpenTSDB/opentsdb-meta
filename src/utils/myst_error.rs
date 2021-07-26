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

use std::{io, num::ParseIntError, string::FromUtf8Error};

/// Result type for myst's error type
pub type Result<T> = std::result::Result<T, MystError>;

/// MystError encapsulates all errors from this crate
#[derive(Debug)]
pub struct MystError {
    kind: ErrorKind,
}
#[derive(Debug)]
pub enum ErrorKind {
    Query(String),
    IO(io::Error),
    Fst(fst::Error),
    Regex(regex_automata::Error),
    UTF8(FromUtf8Error),
    IntParse(ParseIntError),
}

impl MystError {
    /// Creates a new query error
    /// # Arguments
    /// * `err`- A string to represent the error
    ///
    pub fn new_query_error(err: &str) -> MystError {
        Self {
            kind: ErrorKind::Query(String::from(err)),
        }
    }

    /// Creates a new write error. Ususally called when doing segment writes and build.
    /// # Arguments
    /// * `err`- A string to represent the error
    ///
    pub fn new_write_error(err: &str) -> MystError {
        Self {
            kind: ErrorKind::Query(String::from(err)),
        }
    }
}

impl From<io::Error> for MystError {
    fn from(e: io::Error) -> MystError {
        return MystError {
            kind: ErrorKind::IO(e),
        };
    }
}

impl From<fst::Error> for MystError {
    fn from(e: fst::Error) -> MystError {
        return MystError {
            kind: ErrorKind::Fst(e),
        };
    }
}

impl From<regex_automata::Error> for MystError {
    fn from(e: regex_automata::Error) -> MystError {
        return MystError {
            kind: ErrorKind::Regex(e),
        };
    }
}
impl From<FromUtf8Error> for MystError {
    fn from(e: FromUtf8Error) -> MystError {
        return MystError {
            kind: ErrorKind::UTF8(e),
        };
    }
}

impl From<ParseIntError> for MystError {
    fn from(e: ParseIntError) -> MystError {
        MystError {
            kind: ErrorKind::IntParse(e),
        }
    }
}
