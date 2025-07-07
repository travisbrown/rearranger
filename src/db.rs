use crate::{Format, report::WriteReport};
use rocksdb::{BlockBasedOptions, DBCompressionType, IteratorMode, Options, TransactionDB};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const ZSTD_EXTENSION: &str = "zst";

#[derive(thiserror::Error, Debug)]
pub enum Error<F> {
    #[error("Format error")]
    Format(F),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("UTF-8 decoding error")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("RocksDB error")]
    Db(#[from] rocksdb::Error),
    #[error("Invalid path for key")]
    InvalidPath(PathBuf, Vec<u8>),
    #[error("Invalid database state")]
    InvalidState,
}

#[derive(Clone)]
pub struct LineDb<F> {
    db: Arc<TransactionDB>,
    _format: PhantomData<F>,
}

impl<F: Format> LineDb<F> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error<F::Error>> {
        let mut options = Options::default();
        options.create_if_missing(true);

        let mut block_options = BlockBasedOptions::default();
        block_options.set_ribbon_filter(10.0);

        options.set_block_based_table_factory(&block_options);
        options.set_compression_type(DBCompressionType::None);

        let db = Arc::new(TransactionDB::open(&options, &Default::default(), path)?);

        Ok(Self {
            db,
            _format: PhantomData,
        })
    }

    /// Insert a line, returning the previous value for the key if one existed
    pub fn count(&self) -> usize {
        self.db.iterator(IteratorMode::Start).count()
    }

    /// Insert a line, returning the previous value for the key if one existed
    pub fn insert(&self, line: &str) -> Result<Option<Option<String>>, Error<F::Error>> {
        let key = F::key(line).map_err(Error::Format)?;
        let tx = self.db.transaction();
        let value = tx.get_for_update(&key, true)?;
        let line_bytes = line.as_bytes();

        let result = value
            .map(|bytes| {
                Ok(if *bytes != *line_bytes {
                    Some(std::str::from_utf8(&bytes)?.to_string())
                } else {
                    None
                })
            })
            .map_or(
                Ok(None),
                |value: Result<Option<String>, Error<F::Error>>| value.map(Some),
            )?;

        tx.put(&key, line_bytes)?;
        tx.commit()?;

        Ok(result)
    }

    pub fn write<P: AsRef<Path>>(
        &self,
        base: P,
        compression: Option<u8>,
        progress_bar: Option<indicatif::ProgressBar>,
    ) -> Result<WriteReport, Error<F::Error>> {
        let mut file_counts = HashMap::new();
        let mut last_path = None;
        let mut writer: Option<Box<dyn Write>> = None;

        for result in self.lines() {
            let (key, value) = result?;
            let path = F::path(&key).map_err(Error::Format)?;

            let count = if Some(&path) != last_path.as_ref() {
                let entry = file_counts.entry(path.clone());
                match entry {
                    Entry::Occupied(_) => Err(Error::InvalidPath(path.clone(), key.to_vec())),
                    Entry::Vacant(_) => Ok(()),
                }?;

                match compression {
                    Some(level) => {
                        let extension = path.extension();
                        let mut new_extension = extension.unwrap_or_default().to_os_string();
                        if !new_extension.is_empty() {
                            new_extension.push(".");
                        }
                        new_extension.push(ZSTD_EXTENSION);

                        let mut new_path = path.clone();
                        new_path.set_extension(new_extension);

                        let file = File::create(base.as_ref().join(&new_path))?;
                        writer = Some(Box::new(
                            zstd::stream::write::Encoder::new(file, level as i32)?.auto_finish(),
                        ));
                    }
                    None => {
                        let file = File::create(base.as_ref().join(&path))?;
                        writer = Some(Box::new(BufWriter::new(file)));
                    }
                }

                last_path = Some(path);
                entry
            } else {
                let entry = file_counts.entry(path.clone());
                match entry {
                    Entry::Occupied(_) => Ok(()),
                    Entry::Vacant(_) => Err(Error::InvalidState),
                }?;
                entry
            }
            .or_default();

            match writer {
                Some(ref mut writer) => {
                    *count += 1;
                    Ok(writeln!(writer, "{}", value)?)
                }
                None => Err(Error::InvalidState),
            }?;

            if let Some(progress_bar) = progress_bar.as_ref() {
                progress_bar.inc(1);
            }
        }

        Ok(WriteReport::new(file_counts))
    }

    fn lines(&self) -> impl Iterator<Item = Result<(Box<[u8]>, String), Error<F::Error>>> + '_ {
        self.db.iterator(IteratorMode::Start).map(|result| {
            let (key, value) = result?;
            let value_string = std::str::from_utf8(&value)?.to_string();
            Ok((key, value_string))
        })
    }
}
