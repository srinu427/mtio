use std::{
    fs,
    io::{self, Read, Seek, Write},
    path::Path,
};

use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

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

fn cp_file(src: &Path, dst: &Path, chunk_size: u64) -> io::Result<u64> {
    let src_meta = src.symlink_metadata()?;
    let src_len = src_meta.len();
    let fw = fs::File::create(dst)?;
    if src_len == 0 {
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
            Ok(())
        })
        .collect::<io::Result<()>>()?;
    Ok(src_len)
}

pub fn cp(src: &Path, dst: &Path, chunk_size: u64) {
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
                    match cp_file(&s, &d, chunk_size) {
                        Ok(_) => Some(vec![]),
                        Err(e) => None,
                    }
                } else if s_meta.is_dir() {
                    if let Err(e) = fs::create_dir_all(&d) {
                        return None;
                    }
                    let read_dir = match s.read_dir() {
                        Ok(rd) => rd,
                        Err(e) => {
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
