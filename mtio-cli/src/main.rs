use std::path::Path;

use clap::Parser;
use mtio_sys::rayon;

#[derive(Debug, clap::Args)]
pub struct CopyArgs {
    #[clap(long, short)]
    pub input: String,
    #[clap(long, short)]
    pub output: String,
    #[clap(long, short, default_value_t = 1 * 1024 * 1024)]
    pub part_size: u64,
    #[clap(long, short, default_value_t = 2)]
    pub threads: usize,
}

#[derive(Debug, clap::Args)]
pub struct RmArgs {
    #[clap(long, short)]
    pub input: String,
    #[clap(long, short, default_value_t = 2)]
    pub threads: usize,
}

#[derive(Debug, clap::Args)]
pub struct DuArgs {
    #[clap(long, short)]
    pub input: String,
    #[clap(long, short, default_value_t = 2)]
    pub threads: usize,
}

#[derive(Debug, clap::Subcommand)]
pub enum AppCommands {
    Copy(CopyArgs),
    Rm(RmArgs),
    Du(DuArgs),
}

#[derive(Debug, clap::Parser)]
pub struct AppArgs {
    #[clap(subcommand)]
    command: AppCommands,
}

fn main() {
    let args = AppArgs::parse();
    println!("{args:?}");
    let _ = match args.command {
        AppCommands::Copy(_copy_args) => {}
        AppCommands::Rm(_rm_args) => {}
        AppCommands::Du(du_args) => {
            let tp = rayon::ThreadPoolBuilder::default()
                .num_threads(du_args.threads)
                .build()
                .expect("thread pool init failed");
            tp.install(|| mtio_sys::du::du(&du_args.input, None))
                .inspect_err(|e| eprintln!("failed finding sizes: {e}"))
                .ok();
        }
    };
}
