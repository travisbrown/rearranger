use crate::Location;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader, Lines};
use std::path::{Path, PathBuf};
use zstd::stream::read::Decoder as ZstDecoder;

type LineResult = Result<(usize, String), Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("File I/O error")]
    File {
        path: PathBuf,
        error: std::io::Error,
    },
    #[error("Line I/O error")]
    Line {
        location: Location,
        error: std::io::Error,
    },
    #[error("Invalid path")]
    InvalidPath(PathBuf),
}

pub fn lines<P: AsRef<Path>>(path: P) -> Result<Box<dyn Iterator<Item = LineResult>>, Error> {
    let path = path.as_ref().to_path_buf();

    if path.is_file() {
        if let Some(extension) = path.extension().and_then(|extension| extension.to_str()) {
            let file = File::open(&path).map_err(|error| Error::File {
                path: path.clone(),

                error,
            })?;

            match extension.to_ascii_lowercase().as_str() {
                "gz" => {
                    let reader = BufReader::new(GzDecoder::new(file));
                    Ok(Box::new(LineReader::new(path, reader)))
                }
                "zst" => {
                    let reader =
                        BufReader::new(ZstDecoder::new(file).map_err(|error| Error::File {
                            path: path.clone(),
                            error,
                        })?);
                    Ok(Box::new(LineReader::new(path, reader)))
                }
                _ => {
                    let reader = BufReader::new(file);
                    Ok(Box::new(LineReader::new(path, reader)))
                }
            }
        } else {
            Err(Error::InvalidPath(path))
        }
    } else {
        Err(Error::InvalidPath(path))
    }
}

struct LineReader<B> {
    lines: Lines<B>,
    path: PathBuf,
    line_number: usize,
}

impl<B: BufRead> LineReader<B> {
    fn new(path: PathBuf, reader: B) -> Self {
        Self {
            lines: reader.lines(),
            path,

            line_number: 0,
        }
    }
}

impl<B: BufRead> Iterator for LineReader<B> {
    type Item = Result<(usize, String), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.lines.next().map(|result| {
            self.line_number += 1;
            match result {
                Ok(line) => Ok((self.line_number, line)),
                Err(error) => Err(Error::Line {
                    location: Location {
                        path: self.path.clone(),
                        line_number: self.line_number,
                    },
                    error,
                }),
            }
        })
    }
}
