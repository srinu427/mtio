use std::{fs, io};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

pub fn rm(path: &str) -> io::Result<()> {
    let mut work: Vec<_> = glob::glob(path)
        .map_err(|e| io::Error::new(io::ErrorKind::NotFound, e))?
        .filter_map(|p| p.ok())
        .collect();
    while work.len() > 0 {
        work = work
            .par_iter()
            .filter_map(|p| {
                let meta = p
                    .symlink_metadata()
                    .inspect_err(|e| {
                        eprintln!("reading metadata of {p:?} failed: {e}. skipping it")
                    })
                    .ok()?;
                if meta.is_dir() {
                    let new_work: Vec<_> = p
                        .read_dir()
                        .inspect_err(|e| {
                            eprintln!("reading entries in folder {p:?} failed: {e}. skipping it")
                        })
                        .ok()?
                        .into_iter()
                        .filter_map(|de| de.ok())
                        .map(|de| de.path())
                        .collect();
                    Some(new_work)
                } else {
                    fs::remove_file(p)
                        .inspect_err(|e| eprintln!("deleting {p:?} failed: {e}. skipping it"))
                        .ok()?;
                    Some(vec![])
                }
            })
            .flatten()
            .collect();
    }
    Ok(())
}
