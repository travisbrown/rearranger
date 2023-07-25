use crate::Repeat;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteReport {
    file_counts: HashMap<PathBuf, usize>,
}

impl WriteReport {
    pub fn new(file_counts: HashMap<PathBuf, usize>) -> Self {
        Self { file_counts }
    }

    pub fn file_counts(&self) -> Vec<(&Path, usize)> {
        let mut result = self
            .file_counts
            .iter()
            .map(|(path, count)| (path.as_path(), *count))
            .collect::<Vec<_>>();
        result.sort();
        result
    }

    pub fn file_count(&self) -> usize {
        self.file_counts.len()
    }

    pub fn line_count(&self) -> usize {
        self.file_counts.values().sum()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunReport {
    pub repeats: Vec<Repeat>,
    pub write_report: WriteReport,
}

impl RunReport {
    pub fn duplicate_count(&self) -> usize {
        self.repeats
            .iter()
            .filter(|repeat| !repeat.is_collision())
            .count()
    }

    pub fn collision_count(&self) -> usize {
        self.repeats
            .iter()
            .filter(|repeat| repeat.is_collision())
            .count()
    }
}
