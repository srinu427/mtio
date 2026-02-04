use std::{
    collections::HashSet,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

#[derive(Debug)]
pub struct MTDiskUsageProgress {
    pub errors: Vec<(PathBuf, io::Error)>,
    pub being_computed: HashSet<PathBuf>,
    pub total_size: u64,
}

impl MTDiskUsageProgress {
    pub fn new() -> Self {
        Self {
            errors: vec![],
            being_computed: HashSet::new(),
            total_size: 0,
        }
    }

    pub fn new_compute_started(mcp: &Arc<Mutex<Self>>, name: &Path) {
        let mut mcp = match mcp.lock() {
            Ok(w) => w,
            Err(e) => e.into_inner(),
        };
        mcp.being_computed.insert(name.to_path_buf());
    }

    pub fn register_size(mcp: &Arc<Mutex<Self>>, name: &Path, copied: u64) {
        let mut mcp = match mcp.lock() {
            Ok(w) => w,
            Err(e) => e.into_inner(),
        };
        mcp.being_computed.remove(name);
        mcp.total_size += copied;
    }

    pub fn register_error(mcp: &Arc<Mutex<Self>>, name: &Path, error: io::Error) {
        let mut mcp = match mcp.lock() {
            Ok(w) => w,
            Err(e) => e.into_inner(),
        };
        mcp.errors.push((name.to_path_buf(), error));
        mcp.being_computed.remove(name);
    }
}

pub fn du(path: &str, prog: Option<Arc<Mutex<MTDiskUsageProgress>>>) -> io::Result<u64> {
    let mut work: Vec<_> = glob::glob(path)
        .map_err(|e| io::Error::new(io::ErrorKind::NotFound, e))?
        .filter_map(|p| p.ok())
        .collect();
    let mut total_size = 0;
    while work.len() > 0 {
        let (new_work, sizes): (Vec<_>, Vec<_>) = work
            .par_iter()
            .filter_map(|p| {
                let meta = match p.symlink_metadata() {
                    Ok(meta) => meta,
                    Err(e) => {
                        prog.as_ref()
                            .inspect(|mdp| MTDiskUsageProgress::register_error(mdp, p, e));
                        return None;
                    }
                };
                let size = meta.len();
                if meta.is_dir() {
                    let read_dir = match p.read_dir() {
                        Ok(rd) => rd,
                        Err(e) => {
                            prog.as_ref()
                                .inspect(|mdp| MTDiskUsageProgress::register_error(mdp, p, e));
                            return None;
                        }
                    };
                    let new_work: Vec<_> = read_dir
                        .into_iter()
                        .filter_map(|de| de.ok())
                        .map(|de| de.path())
                        .collect();
                    Some((new_work, size))
                } else {
                    Some((vec![], size))
                }
            })
            .unzip();
        work = new_work.into_iter().flatten().collect();
        total_size += sizes.into_iter().sum::<u64>();
    }
    Ok(total_size)
}
