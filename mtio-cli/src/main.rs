use std::path::Path;

use clap::Parser;

#[derive(Debug, clap::Args)]
pub struct CopyArgs {
    #[clap(long, short)]
    pub input: String,
    #[clap(long, short)]
    pub output: String,
    #[clap(long, short, default_value_t = 1 * 1024 * 1024)]
    pub part_size: u64,
    #[clap(long, short, default_value_t = 256)]
    pub max_in_mem_parts: u64,
    #[clap(long, short, default_value_t = 2)]
    pub threads: usize,
    #[clap(long, short, default_value_t = 128)]
    pub files_open_max: usize,
    #[clap(long, short, default_value_t = false)]
    pub preallocate: bool,
}

#[derive(Debug, clap::Args)]
pub struct RmArgs {
    #[clap(long, short, num_args = 1..)]
    pub input: Vec<String>,
    #[clap(long, short, default_value_t = 2)]
    pub threads: usize,
    #[clap(long, short, default_value_t = 128)]
    pub files_open_max: usize,
}

#[derive(Debug, clap::Subcommand)]
pub enum AppCommands {
    Copy(CopyArgs),
    Rm(RmArgs),
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
        AppCommands::Copy(copy_args) => mtio_sys::mt_copy(
            Path::new(&copy_args.input),
            Path::new(&copy_args.output),
            copy_args.part_size,
            copy_args.threads,
            copy_args.files_open_max,
            copy_args.max_in_mem_parts,
            copy_args.preallocate,
        )
        .inspect_err(|e| eprintln!("{e}")),
        AppCommands::Rm(rm_args) => {
            let paths: Vec<_> = rm_args.input.iter().map(|s| Path::new(s)).collect();
            mtio_sys::mt_delete(paths, rm_args.threads, rm_args.files_open_max)
                .inspect_err(|e| eprintln!("{e}"))
        }
    };
}
