use crate::{
    Format, Location, Repeat, Replacement, db::LineDb, progress::ProgressState, report::RunReport,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;

const TEMP_DIR_PREFIX: &str = "lines-db";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileOrder {
    ByName,
    BySizeInterspersed,
}

#[derive(thiserror::Error, Debug)]
pub enum Error<F> {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Task error")]
    Task(#[from] tokio::task::JoinError),
    #[error("Database error")]
    Db(#[from] crate::db::Error<F>),
    #[error("Key parsing error")]
    KeyParsing(crate::db::Error<F>, PathBuf, usize),
    #[error("Input lines error")]
    Lines(#[from] crate::lines::Error),
    #[error("Invalid output directory path")]
    InvalidOutput(PathBuf),
}

pub async fn run<
    F: Format + Clone + Send + 'static,
    I: AsRef<Path>,
    O: AsRef<Path>,
    T: AsRef<Path>,
>(
    input: I,
    output: O,
    temp_base: T,
    file_order: FileOrder,
    parallelism: usize,
    compression: Option<u8>,
    progress_bars: bool,
) -> Result<RunReport, Error<F::Error>>
where
    F::Error: Send,
{
    if output.as_ref().is_dir() {
        let paths = if input.as_ref().is_dir() {
            let mut paths = file_paths::<F, I>(input, F::is_input_recursive())?;
            sort_paths(&mut paths, file_order)?;
            paths
        } else {
            vec![input.as_ref().to_path_buf()]
        };

        let db_dir = tempdir::TempDir::new_in(temp_base, TEMP_DIR_PREFIX)?;
        let db = LineDb::<F>::open(db_dir.path())?;

        let mut progress_state = if progress_bars {
            ProgressState::new()
        } else {
            ProgressState::default()
        };

        progress_state.init_read_bar(|| paths.len());

        let repeats = futures::stream::iter(paths.into_iter())
            .map(|path| {
                let db = db.clone();
                let progress_bar = progress_state.read_bar();
                let action: JoinHandle<Result<_, Error<F::Error>>> = tokio::spawn(async move {
                    let lines = crate::lines::lines(&path)?;
                    let mut repeats = vec![];

                    for result in lines {
                        let (line_number, line) = result?;
                        if let Some(replaced) = db
                            .insert(&line)
                            .map_err(|error| Error::KeyParsing(error, path.clone(), line_number))?
                        {
                            let replacement = replaced.map(|value| Replacement {
                                old_value: value,
                                new_value: line.clone(),
                            });

                            repeats.push(Repeat {
                                location: Location::new(&path, line_number),
                                replacement,
                            });
                        }
                    }

                    if let Some(progress_bar) = progress_bar.as_ref() {
                        progress_bar.inc(1);
                    }

                    Ok(repeats)
                });

                Ok(action.map_ok_or_else(|error| Err(Error::from(error)), |result| result))
            })
            .try_buffer_unordered(parallelism)
            .map_ok(|values| {
                futures::stream::iter(values).map(|value| {
                    let result: Result<Repeat, Error<F::Error>> = Ok(value);
                    result
                })
            })
            .try_flatten()
            .try_collect()
            .await?;

        progress_state.finish_read_bar();
        let write_bar = progress_state.init_write_bar(|| db.count());

        let write_report = db.write(output, compression, write_bar)?;

        progress_state.finish_write_bar();

        Ok(RunReport {
            repeats,
            write_report,
        })
    } else {
        Err(Error::InvalidOutput(output.as_ref().to_path_buf()))
    }
}

fn file_paths<F: Format, P: AsRef<Path>>(
    base: P,
    recursive: bool,
) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut result = vec![];
    file_paths_rec::<F, P>(base, recursive, &mut result)?;
    Ok(result)
}

fn file_paths_rec<F: Format, P: AsRef<Path>>(
    base: P,
    recursive: bool,
    acc: &mut Vec<PathBuf>,
) -> Result<(), std::io::Error> {
    for entry in std::fs::read_dir(base)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if F::include(&path) {
                acc.push(path);
            }
        } else if recursive {
            file_paths_rec::<F, PathBuf>(path, recursive, acc)?;
        }
    }

    Ok(())
}

fn sort_paths(paths: &mut Vec<PathBuf>, file_order: FileOrder) -> Result<(), std::io::Error> {
    match file_order {
        FileOrder::ByName => {
            paths.sort_by_cached_key(|path| path.as_os_str().to_owned());
        }
        FileOrder::BySizeInterspersed => {
            let mut with_size_0 = vec![];

            for path in paths.drain(..) {
                let size = path.metadata()?.len();
                with_size_0.push((size, path));
            }

            with_size_0.sort_by_key(|(len, _)| *len);

            let mut with_size_1 = with_size_0.split_off(with_size_0.len() / 2);
            with_size_1.reverse();

            paths.extend(with_size_0.into_iter().map(|(_, path)| path));
            paths.extend(with_size_1.into_iter().map(|(_, path)| path));
        }
    }
    Ok(())
}
