use std::path::{Path, PathBuf};

pub mod cli;
pub mod db;
pub mod lines;
mod progress;
pub mod report;
pub mod session;

pub trait Format {
    type Error;

    fn key(line: &str) -> Result<Vec<u8>, Self::Error>;
    fn path(key: &[u8]) -> Result<PathBuf, Self::Error>;

    fn is_input_recursive() -> bool {
        false
    }
    fn include<P: AsRef<Path>>(_path: P) -> bool {
        true
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Location {
    pub path: PathBuf,
    pub line_number: usize,
}

impl Location {
    pub fn new<P: AsRef<Path>>(path: P, line_number: usize) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            line_number,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Replacement {
    pub old_value: String,
    pub new_value: String,
}

/// An instance of a repeated key
///
/// May be either a duplicate (the line values are the same) or a collision (the line values differ)
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Repeat {
    pub location: Location,
    pub replacement: Option<Replacement>,
}

impl Repeat {
    pub fn is_collision(&self) -> bool {
        self.replacement.is_some()
    }
}
