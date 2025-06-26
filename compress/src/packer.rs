use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use blake3;
use rayon::prelude::*;
use walkdir::WalkDir;

use zstd_sys::{
    ZSTD_createCCtx,
    ZSTD_createCCtxParams,
    ZSTD_compress2,
    ZSTD_compressBound,
    ZSTD_CCtxParams_set_compressionLevel,
    ZSTD_CCtxParams_set_nbWorkers,
    ZSTD_freeCCtxParams,
    ZSTD_freeCCtx,
    ZSTD_isError,
};

#[derive(Debug)]
pub struct FileEntry {
    pub path: String,
    pub offset: u64,
    pub size: u64,
    pub hash: [u8; 32],
}

pub fn create_snippy_archive<P: AsRef<Path>>(input_dir: P, output_file: P) -> anyhow::Result<()> {
    let input_dir = input_dir.as_ref();

    let files: Vec<PathBuf> = WalkDir::new(input_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect();

    let archive_file = Arc::new(Mutex::new(BufWriter::new(File::create(&output_file)?)));
    let index = Arc::new(Mutex::new(Vec::new()));
    let offset = Arc::new(Mutex::new(0u64));

    files.par_iter().try_for_each(|path| -> anyhow::Result<()> {
        let rel_path = path.strip_prefix(input_dir)?.to_str().unwrap().replace("\\", "/");
        let mut file = BufReader::new(File::open(path)?);
        let mut input_data = Vec::new();
        file.read_to_end(&mut input_data)?;

        let hash = *blake3::hash(&input_data).as_bytes();

        let max_len = unsafe { ZSTD_compressBound(input_data.len()) };
        let mut compressed = vec![0u8; max_len];

        let cctx = unsafe { ZSTD_createCCtx() };
        if cctx.is_null() {
            anyhow::bail!("Failed to create compression context");
        }

        let params = unsafe { ZSTD_createCCtxParams() };
        if params.is_null() {
            unsafe { ZSTD_freeCCtx(cctx) };
            anyhow::bail!("Failed to create compression parameters");
        }

        let threads = std::thread::available_parallelism()?.get() as i32;
        unsafe {
            ZSTD_CCtxParams_set_compressionLevel(params, 3);
            ZSTD_CCtxParams_set_nbWorkers(params, threads);
        }

        let compressed_size = unsafe {
            ZSTD_compress2(
                cctx,
                compressed.as_mut_ptr() as *mut _,
                compressed.len(),
                input_data.as_ptr() as *const _,
                input_data.len(),
                params,
            )
        };

        unsafe {
            ZSTD_freeCCtxParams(params);
            ZSTD_freeCCtx(cctx);
        }

        if unsafe { ZSTD_isError(compressed_size) } != 0 {
            anyhow::bail!("Compression failed");
        }

        compressed.truncate(compressed_size);

        let mut archive = archive_file.lock().unwrap();
        let mut idx = index.lock().unwrap();
        let mut off = offset.lock().unwrap();

        archive.write_all(&compressed)?;

        idx.push(FileEntry {
            path: rel_path,
            offset: *off,
            size: compressed.len() as u64,
            hash,
        });

        *off += compressed.len() as u64;

        Ok(())
    })?;

    let mut archive = archive_file.lock().unwrap();
    let index = index.lock().unwrap();
    let index_offset = archive.stream_position()?;

    for entry in &*index {
        let path_bytes = entry.path.as_bytes();
        let path_len = path_bytes.len() as u32;

        archive.write_all(&path_len.to_le_bytes())?;
        archive.write_all(path_bytes)?;
        archive.write_all(&entry.offset.to_le_bytes())?;
        archive.write_all(&entry.size.to_le_bytes())?;
        archive.write_all(&entry.hash)?;
    }

    archive.write_all(&index_offset.to_le_bytes())?;
    archive.write_all(&(index.len() as u32).to_le_bytes())?;

    Ok(())
}
