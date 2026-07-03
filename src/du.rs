use std::{
    fs::{self, Metadata},
    io,
    ops::AddAssign,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

fn du_inner(entry: PathBuf, metadata: Metadata, results: Arc<Mutex<u64>>) {
    if metadata.is_dir() {
        results
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .add_assign(metadata.len());
        let dir_reader = match fs::read_dir(&entry) {
            Ok(t) => t,
            Err(e) => {
                log::warn!("listing {:?} failed: {e}", &entry);
                return;
            }
        };
        rayon::scope(|s| {
            for ent in dir_reader {
                let Ok(ent) = ent else {
                    continue;
                };
                let ent_path = ent.path();
                let meta = match ent.metadata() {
                    Ok(t) => t,
                    Err(e) => {
                        log::warn!("getting metadata of {:?} failed: {e}", &ent_path);
                        continue;
                    }
                };
                let results_clone = results.clone();
                s.spawn(move |_s| {
                    du_inner(ent_path, meta, results_clone);
                });
            }
        });
    } else {
        results
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .add_assign(metadata.len());
    }
}

pub fn du(path: &str) -> io::Result<u64> {
    let entry = Path::new(path);
    let metadata = entry
        .metadata()
        .map_err(|e| io::Error::new(e.kind(), format!("at getting metadata of {:?}", entry)))?;
    let results = Arc::new(Mutex::new(0));
    let results_clone = results.clone();
    du_inner(entry.to_path_buf(), metadata, results_clone);
    Arc::into_inner(results)
        .map(|m| m.into_inner().unwrap_or_else(|e| e.into_inner()))
        .ok_or(io::Error::new(io::ErrorKind::Other, "runtime sync error"))
}
