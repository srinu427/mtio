use std::{
    collections::HashMap,
    fs,
    io::{self, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

#[derive(Debug, Clone, Copy)]
pub struct MTFileCopyProgress {
    pub total_size: u64,
    pub completed: u64,
}

#[derive(Debug)]
pub struct MTCopyProgress {
    pub errors: Vec<(PathBuf, io::Error)>,
    pub being_copied: HashMap<PathBuf, MTFileCopyProgress>,
    pub copied: u64,
}

impl MTCopyProgress {
    pub fn new() -> Self {
        Self {
            errors: vec![],
            being_copied: HashMap::new(),
            copied: 0,
        }
    }

    pub fn new_copy_started(mcp: &Mutex<Self>, name: &Path, total_size: u64) {
        let mut mcp = match mcp.lock() {
            Ok(w) => w,
            Err(e) => e.into_inner(),
        };
        mcp.being_copied.insert(
            name.to_path_buf(),
            MTFileCopyProgress {
                total_size,
                completed: 0,
            },
        );
    }

    pub fn register_copy_size(mcp: &Mutex<Self>, name: &Path, copied: u64) {
        let mut mcp = match mcp.lock() {
            Ok(w) => w,
            Err(e) => e.into_inner(),
        };
        let mut exists = false;
        if let Some(fcp) = mcp.being_copied.get_mut(name) {
            exists = true;
            fcp.completed += copied;
        };
        if exists {
            mcp.copied += copied;
        }
    }

    pub fn copy_done(mcp: &Mutex<Self>, name: &Path) {
        let mut mcp = match mcp.lock() {
            Ok(w) => w,
            Err(e) => e.into_inner(),
        };
        mcp.being_copied.remove(name);
    }

    pub fn register_error(mcp: &Mutex<Self>, name: &Path, error: io::Error) {
        let mut mcp = match mcp.lock() {
            Ok(w) => w,
            Err(e) => e.into_inner(),
        };
        mcp.errors.push((name.to_path_buf(), error));
        mcp.being_copied.remove(name);
    }
}

fn read_chunk(path: &Path, offset: u64, size: u64) -> io::Result<Vec<u8>> {
    let mut fr = fs::File::open(path)?;
    fr.seek(io::SeekFrom::Start(offset))?;
    let mut data = vec![0; size as usize];
    fr.read_exact(&mut data)?;
    Ok(data)
}

fn write_chunk(path: &Path, offset: u64, data: &[u8]) -> io::Result<()> {
    let mut fw = fs::File::options().truncate(false).write(true).open(path)?;
    fw.seek(io::SeekFrom::Start(offset))?;
    fw.write_all(data)?;
    Ok(())
}

fn cp_file(
    src: &Path,
    dst: &Path,
    chunk_size: u64,
    prog: &Option<Mutex<MTCopyProgress>>,
) -> io::Result<u64> {
    let src_meta = src.symlink_metadata()?;
    let src_len = src_meta.len();
    prog.as_ref()
        .inspect(|mcp| MTCopyProgress::new_copy_started(mcp, src, src_len));
    let fw = fs::File::create(dst)?;
    if src_len == 0 {
        prog.as_ref()
            .inspect(|mcp| MTCopyProgress::copy_done(mcp, src));
        return Ok(0);
    }
    fw.set_len(src_len)?;
    let chunk_div = (src_len - 1) / chunk_size;
    (0..chunk_div + 1)
        .into_par_iter()
        .map(|c_id| {
            let offset = c_id * chunk_size;
            let size = if c_id == chunk_div {
                src_len - (chunk_size * chunk_div)
            } else {
                chunk_size
            };
            let chunk_data = read_chunk(src, offset, size)?;
            write_chunk(dst, offset, &chunk_data)?;
            prog.as_ref()
                .inspect(|mcp| MTCopyProgress::register_copy_size(mcp, src, size));
            Ok(())
        })
        .collect::<io::Result<()>>()?;
    prog.as_ref()
        .inspect(|mcp| MTCopyProgress::copy_done(mcp, src));
    Ok(src_len)
}

pub fn cp(src: &Path, dst: &Path, chunk_size: u64, prog: Option<Mutex<MTCopyProgress>>) {
    let mut work = vec![(src.to_path_buf(), dst.to_path_buf())];
    while work.len() > 0 {
        work = work
            .par_iter()
            .filter_map(|(s, d)| {
                let s_meta = s
                    .symlink_metadata()
                    .inspect_err(|e| eprintln!("failed getting metadata of {s:?}: {e}"))
                    .ok()?;
                if s_meta.is_file() {
                    match cp_file(&s, &d, chunk_size, &prog) {
                        Ok(_) => Some(vec![]),
                        Err(e) => {
                            prog.as_ref()
                                .inspect(|mcp| MTCopyProgress::register_error(mcp, &s, e));
                            None
                        }
                    }
                } else if s_meta.is_dir() {
                    if let Err(e) = fs::create_dir_all(&d) {
                        prog.as_ref()
                            .inspect(|mcp| MTCopyProgress::register_error(mcp, &s, e));
                        return None;
                    }
                    let read_dir = match s.read_dir() {
                        Ok(rd) => rd,
                        Err(e) => {
                            prog.as_ref()
                                .inspect(|mcp| MTCopyProgress::register_error(mcp, &s, e));
                            return None;
                        }
                    };
                    let new_work = read_dir
                        .into_iter()
                        .filter_map(|p| p.ok())
                        .map(|p| (p.path(), d.join(p.file_name())))
                        .collect();
                    Some(new_work)
                } else {
                    Some(vec![])
                }
            })
            .flatten()
            .collect();
    }
}
