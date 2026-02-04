use std::{io, path::Path};

use clap::Parser;
use mtio_sys::rayon::{
    self,
    iter::{IntoParallelRefIterator, ParallelIterator},
};

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

// static SIZE_LEVELS: &[&str] = &["B", "K", "M", "G", "T"];

fn bytes_to_human(size: u64) -> String {
    let mut rem = size;
    if rem < 1024 {
        return format!("{rem}B - {size}B");
    }
    rem = rem / 1024;
    if rem < 1024 {
        return format!("{rem}K - {size}B");
    }
    rem = rem / 1024;
    if rem < 1024 {
        return format!("{rem}M - {size}B");
    }
    rem = rem / 1024;
    if rem < 1024 {
        return format!("{rem}G - {size}B");
    }
    rem = rem / 1024;
    return format!("{rem}T - {size}B");
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
            let work: Vec<_> = glob::glob(&du_args.input)
                .inspect_err(|e| {
                    eprintln!("cannot resolve any paths for \"{}\": {e}", &du_args.input)
                })
                .expect("cannot proceed")
                .filter_map(|p| p.ok())
                .collect();
            tp.install(|| {
                work.par_iter().for_each(|wp| {
                    println!("starting du of {wp:?}");
                    match mtio_sys::du::du(&wp.to_string_lossy().to_string(), None) {
                        Ok(s) => println!("{} - {wp:?}", bytes_to_human(s)),
                        Err(e) => eprintln!("failed getting size for {wp:?}: {e}"),
                    }
                })
            });
        }
    };
}
