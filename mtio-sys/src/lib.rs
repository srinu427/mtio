use std::{
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{
    fs,
    io::{self, AsyncWriteExt},
    sync::{AcquireError, Semaphore, TryAcquireError},
    task::JoinSet,
};

fn acquire_to_io_error(e: AcquireError) -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        format!("failed to acquire fs operation permit: {e}"),
    )
}

async fn limit_fs_metadata(semaphore: &Semaphore, path: &Path) -> io::Result<std::fs::Metadata> {
    let limit = semaphore.acquire().await.map_err(acquire_to_io_error)?;
    let result = fs::metadata(path).await;
    drop(limit);
    result
}

async fn limit_create_dir_all(semaphore: &Semaphore, path: &Path) -> io::Result<()> {
    let limit = semaphore.acquire().await.map_err(acquire_to_io_error)?;
    let result = fs::create_dir_all(path).await;
    drop(limit);
    result
}

async fn _limit_remove_file(semaphore: &Semaphore, path: &Path) -> io::Result<()> {
    let limit = semaphore.acquire().await.map_err(acquire_to_io_error)?;
    let result = fs::remove_file(path).await;
    drop(limit);
    result
}

async fn _limit_remove_dir(semaphore: &Semaphore, path: &Path) -> io::Result<()> {
    let limit = semaphore.acquire().await.map_err(acquire_to_io_error)?;
    let result = fs::remove_dir(path).await;
    drop(limit);
    result
}

async fn limit_file_read(
    semaphore: &Semaphore,
    path: PathBuf,
    offset: u64,
    size: u64,
) -> io::Result<Vec<u8>> {
    let limit = semaphore.acquire().await.map_err(acquire_to_io_error)?;
    let buffer = tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(&path)?;
        file.seek(io::SeekFrom::Start(offset))?;
        let mut buffer = vec![0u8; size as usize];
        file.read_exact(&mut buffer)?;
        io::Result::Ok(buffer)
    })
    .await
    .map_err(|e| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            format!("async runtime error: {e}"),
        )
    })??;
    drop(limit);
    Ok(buffer)
}

async fn _limit_file_read_full(semaphore: &Semaphore, path: &Path) -> io::Result<Vec<u8>> {
    let limit = semaphore.acquire().await.map_err(acquire_to_io_error)?;
    let buffer = fs::read(path).await?;
    drop(limit);
    Ok(buffer)
}

async fn _limit_file_set_len(semaphore: &Semaphore, path: &Path, size: u64) -> io::Result<()> {
    let limit = semaphore.acquire().await.map_err(acquire_to_io_error)?;
    let file = fs::File::create(path).await?;
    file.set_len(size).await?;
    drop(limit);
    Ok(())
}

async fn _limit_file_write(semaphore: &Semaphore, path: &Path, data: &[u8]) -> io::Result<()> {
    let limit = semaphore.acquire().await.map_err(acquire_to_io_error)?;
    fs::write(path, data).await?;
    drop(limit);
    Ok(())
}

async fn append_to_file(semaphore: &Semaphore, path: PathBuf, mut data: Vec<u8>) -> io::Result<()> {
    let limit = semaphore.acquire().await.map_err(acquire_to_io_error)?;
    tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::options().append(true).open(&path)?;
        file.write_all(&mut data)?;
        io::Result::Ok(())
    })
    .await
    .map_err(|e| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            format!("async runtime error: {e}"),
        )
    })??;
    drop(limit);
    Ok(())
}

async fn file_copy(
    src: &Path,
    dst: &Path,
    part_size: u64,
    file_open_sem: Arc<Semaphore>,
    data_chunk_sem: Arc<Semaphore>,
) -> io::Result<()> {
    let metadata = limit_fs_metadata(&file_open_sem, src).await?;
    let size = metadata.len();
    let num_parts = (size + part_size - 1) / part_size;

    let mut part_datas: Vec<Option<Vec<u8>>> = vec![None; num_parts as usize];
    let mut all_data_limits = data_chunk_sem
        .acquire_many(0)
        .await
        .map_err(acquire_to_io_error)?;
    let mut join_set = JoinSet::new();

    let mut part_to_write = 0;

    for part in 0..num_parts {
        let data_len = if part == num_parts - 1 {
            size - (part_size * (num_parts - 1))
        } else {
            part_size
        };
        loop {
            match data_chunk_sem.try_acquire() {
                Ok(limit) => {
                    let file_open_sem = file_open_sem.clone();
                    let src = src.to_path_buf();
                    all_data_limits.merge(limit);
                    join_set.spawn(async move {
                        let data = limit_file_read(&file_open_sem, src, part * part_size, data_len)
                            .await?;
                        io::Result::Ok((part, data))
                    });
                    tokio::task::yield_now().await;
                    break;
                }
                Err(TryAcquireError::NoPermits) => {
                    let Some(chunk_res) = join_set.try_join_next() else {
                        tokio::task::yield_now().await;
                        continue;
                    };
                    let (part, data) = chunk_res.map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            format!("error with async runtime: {e}"),
                        )
                    })??;
                    part_datas[part as usize] = Some(data);
                    let mut data_to_write = vec![];
                    while part_to_write < num_parts
                        && let Some(data) = part_datas[part_to_write as usize].take()
                    {
                        data_to_write.extend(data);
                        all_data_limits.split(1);
                        part_to_write += 1;
                    }
                    if data_to_write.len() > 0 {
                        append_to_file(&file_open_sem, dst.to_path_buf(), data_to_write).await?;
                    }
                    tokio::task::yield_now().await;
                }
                Err(TryAcquireError::Closed) => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "Data store semaphore broken",
                    ));
                }
            }
        }
    }

    loop {
        let Some(chunk_res) = join_set.join_next().await else {
            break;
        };
        let (part, data) = chunk_res.map_err(|e| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("error with async runtime: {e}"),
            )
        })??;
        part_datas[part as usize] = Some(data);
        let mut data_to_write = vec![];
        while part_to_write < num_parts
            && let Some(data) = part_datas[part_to_write as usize].take()
        {
            data_to_write.extend(data);
            all_data_limits.split(1);
            part_to_write += 1;
        }
        if data_to_write.len() > 0 {
            append_to_file(&file_open_sem, dst.to_path_buf(), data_to_write).await?;
        }
    }
    drop(all_data_limits);

    Ok(())
}

fn do_copy(
    src: PathBuf,
    dst: PathBuf,
    part_size: u64,
    file_open_sem: Arc<Semaphore>,
    data_chunk_sem: Arc<Semaphore>,
) -> impl Future<Output = io::Result<()>> + Send {
    async move {
        let metadata = limit_fs_metadata(&file_open_sem, &src).await?;
        if metadata.is_dir() {
            limit_create_dir_all(&file_open_sem, &dst).await?;
            let mut limit = file_open_sem
                .acquire_many(3)
                .await
                .map_err(acquire_to_io_error)?;
            limit.split(2);
            let mut dir_reader = fs::read_dir(&src).await?;
            let mut join_set = JoinSet::new();
            while let Some(dir_entry) = dir_reader.next_entry().await? {
                let entry_name = dir_entry.file_name();
                let dst_entry = dst.join(&entry_name);
                join_set.spawn(do_copy(
                    dir_entry.path(),
                    dst_entry,
                    part_size,
                    file_open_sem.clone(),
                    data_chunk_sem.clone(),
                ));
            }
            join_set.join_all().await;
            drop(limit);
        } else if metadata.is_file() {
            file_copy(
                &src,
                &dst,
                part_size,
                file_open_sem.clone(),
                file_open_sem.clone(),
            )
            .await?
        }
        Ok(())
    }
}

pub fn mt_copy(
    src: &Path,
    dst: &Path,
    part_size: u64,
    cores: usize,
    max_open_files: usize,
    max_parts_in_mem: u64,
) -> io::Result<()> {
    let file_open_sem = Arc::new(Semaphore::new(max_open_files as usize));
    let data_chunk_sem = Arc::new(Semaphore::new(max_parts_in_mem as usize));
    let thread_pool = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores)
        .max_blocking_threads(max_open_files)
        .enable_all()
        .build()
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::HostUnreachable,
                format!("thread pool init failed: {e}"),
            )
        })?;
    thread_pool.block_on(async {
        do_copy(
            src.to_path_buf(),
            dst.to_path_buf(),
            part_size,
            file_open_sem,
            data_chunk_sem,
        )
        .await
    })
}
