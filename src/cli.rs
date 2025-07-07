use crate::{Format, report::RunReport, session::FileOrder};
use clap::{Arg, ArgAction, ArgMatches, Command};
use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum Error<F> {
    #[error("Format error")]
    Format(F),
    #[error("Session error")]
    Session(#[from] crate::session::Error<F>),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Argument match error")]
    Matches(#[from] clap::parser::MatchesError),
    #[error("Arguments error")]
    Args(#[from] clap::error::Error<clap::error::RichFormatter>),
}
pub struct App {
    command: Command,
}

impl App {
    pub fn new(name: &str) -> Self {
        let command = Command::new(name.to_string())
            .arg(
                Arg::new("input")
                    .long("input")
                    .short('i')
                    .value_parser(clap::value_parser!(PathBuf))
                    .required(true)
                    .help("Input path"),
            )
            .arg(
                Arg::new("output")
                    .long("output")
                    .short('o')
                    .value_parser(clap::value_parser!(PathBuf))
                    .required(true)
                    .help("Output directory path"),
            )
            .arg(
                Arg::new("tmp")
                    .long("tmp")
                    .short('t')
                    .value_parser(clap::value_parser!(PathBuf))
                    .default_value("/tmp/")
                    .help("Temporary database directory base path"),
            )
            .arg(
                Arg::new("by-size")
                    .long("by-size")
                    .help("Sort input files by size")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("parallel")
                    .long("parallel")
                    .short('p')
                    .value_parser(clap::value_parser!(usize))
                    .default_value("8")
                    .help("Parallelism"),
            )
            .arg(
                Arg::new("zstd")
                    .long("zstd")
                    .short('z')
                    .value_parser(clap::value_parser!(u8))
                    .help("Compress output (ZSTD)"),
            );

        Self { command }
    }

    pub async fn run_from_args<F: Format + Clone + Send + 'static>(
        self,
    ) -> Result<RunReport, Error<F::Error>>
    where
        F::Error: Send,
    {
        let matches = self.command.get_matches();
        Self::run_from_matches::<F>(&matches).await
    }

    async fn run_from_matches<F: Format + Clone + Send + 'static>(
        matches: &ArgMatches,
    ) -> Result<RunReport, Error<F::Error>>
    where
        F::Error: Send,
    {
        let input = matches.try_get_one::<PathBuf>("input")?.unwrap();
        let output = matches.try_get_one::<PathBuf>("output")?.unwrap();
        let temp_dir = matches.try_get_one::<PathBuf>("tmp")?.unwrap();
        let by_size = matches.get_flag("by-size");
        let parallelism = matches.try_get_one::<usize>("parallel")?.unwrap();
        let zstd = matches.try_get_one::<u8>("zstd")?;

        let file_order = if by_size {
            FileOrder::BySizeInterspersed
        } else {
            FileOrder::ByName
        };

        let report = crate::session::run::<F, &PathBuf, &PathBuf, &PathBuf>(
            input,
            output,
            temp_dir,
            file_order,
            *parallelism,
            zstd.copied(),
            true,
        )
        .await?;

        Ok(report)
    }

    pub fn with_command<U: FnOnce(&Command) -> Command>(&self, f: U) -> Self {
        Self {
            command: f(&self.command),
        }
    }

    pub fn show_run_report(report: &RunReport) {
        eprintln!(
            "Wrote {} lines in {} files",
            report.write_report.line_count(),
            report.write_report.file_count()
        );
        eprintln!(
            "Found {} duplicates and {} collisions",
            report.duplicate_count(),
            report.collision_count()
        );
    }
}
