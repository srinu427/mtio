use argh::FromArgs;
use rayon::{ThreadPool, ThreadPoolBuildError, ThreadPoolBuilder};

#[derive(Debug, FromArgs)]
/// Copy files/folders in a multithreaded way
#[argh(subcommand, name = "cp")]
pub struct CopyArgs {
    /// source path
    #[argh(option)]
    pub input: String,
    /// destination path
    #[argh(option)]
    pub output: String,
    /// chunk size, multiple chunks will be copied in parallel
    #[argh(option, default = "1024 * 1024")]
    pub part_size: u64,
    /// number of threads to spawn. If none given, it uses the CPU count
    #[argh(option)]
    pub threads: Option<usize>,
}

#[derive(Debug, FromArgs)]
/// Copy files/folders in a multithreaded way
#[argh(subcommand, name = "rm")]
pub struct RmArgs {
    /// source path
    #[argh(option)]
    pub input: String,
    /// destination path
    #[argh(option)]
    pub output: String,
    /// chunk size, multiple chunks will be copied in parallel
    #[argh(option, default = "1024 * 1024")]
    pub part_size: u64,
    /// number of threads to spawn. If none given, it uses the CPU count
    #[argh(option)]
    pub threads: Option<usize>,
}

#[derive(Debug, FromArgs)]
/// Copy files/folders in a multithreaded way
#[argh(subcommand, name = "du")]
pub struct DuArgs {
    /// source path
    #[argh(positional)]
    pub input: String,
    /// number of threads to spawn. If none given, it uses the CPU count
    #[argh(option)]
    pub threads: Option<usize>,
}

#[derive(Debug, FromArgs)]
#[argh(subcommand)]
pub enum AppCommands {
    Copy(CopyArgs),
    Rm(RmArgs),
    Du(DuArgs),
}

#[derive(Debug, FromArgs)]
/// Top-level command
pub struct AppArgs {
    #[argh(subcommand)]
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

fn make_tp(threads: Option<usize>) -> Result<ThreadPool, ThreadPoolBuildError> {
    match threads {
        Some(t) => ThreadPoolBuilder::default().num_threads(t).build(),
        None => ThreadPoolBuilder::default().build(),
    }
}

fn main() {
    let args: AppArgs = argh::from_env();
    env_logger::init();
    let _ = match args.command {
        AppCommands::Copy(_copy_args) => {}
        AppCommands::Rm(_rm_args) => {}
        AppCommands::Du(du_args) => {
            let tp = make_tp(du_args.threads).expect("thread pool init failed");
            let work: Vec<_> = glob::glob(&du_args.input)
                .inspect_err(|e| {
                    eprintln!("cannot resolve any paths for \"{}\": {e}", &du_args.input)
                })
                .expect("cannot proceed")
                .filter_map(|p| p.ok())
                .collect();
            tp.install(|| {
                for wp in &work {
                    match mtio_lib::du::du(&wp.to_string_lossy().to_string()) {
                        Ok(s) => println!("{} - {wp:?}", bytes_to_human(s)),
                        Err(e) => eprintln!("failed getting size for {wp:?}: {e}"),
                    }
                }
            });
        }
    };
}
