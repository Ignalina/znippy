// compress/src/packer.rs

use std::fs::{self, File};
use std::io::{BufWriter, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use blake3::Hasher;
use rayon::prelude::*;
use serde::{Serialize, Deserialize};
use zstd_sys::*;

use bincode::config::standard;
use bincode::encode_to_vec;

#[derive(Debug, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: PathBuf,
    pub original_size: u64,
    pub compressed_size: u64,
    pub checksum: [u8; 32],
    pub offset: u64,
}

pub fn compress_dir<P: AsRef<Path>>(input_dir: P, output_file: P) -> Result<()> {
    let input_dir = input_dir.as_ref();
    let output_file = output_file.as_ref();

    let mut file_list = Vec::new();
    for entry in walkdir::WalkDir::new(input_dir) {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let rel_path = path.strip_prefix(input_dir)?.to_path_buf();
            file_list.push(rel_path);
        }
    }

    let output = File::create(output_file)?;
    let writer = Arc::new(Mutex::new(BufWriter::new(output)));
    let mut entries = Vec::new();

    file_list.par_iter().map(|rel_path| {
        let abs_path = input_dir.join(rel_path);
        let data = fs::read(&abs_path)?;

        let mut hasher = Hasher::new();
        hasher.update(&data);
        let checksum = hasher.finalize();

        unsafe {
            let cctx = ZSTD_createCCtx();
            let cctx_params = ZSTD_createCCtxParams();
            ZSTD_CCtxParams_init(cctx_params);
            ZSTD_CCtxParams_setParameter(cctx_params, ZSTD_c_nbWorkers, 4);
            ZSTD_CCtx_setParametersUsingCCtxParams(cctx, cctx_params);

            let max_compressed_size = ZSTD_compressBound(data.len());
            let mut compressed = vec![0u8; max_compressed_size];
            let compressed_size = ZSTD_compress2(
                cctx,
                compressed.as_mut_ptr() as *mut _,
                max_compressed_size,
                data.as_ptr() as *const _,
                data.len(),
            );

            ZSTD_freeCCtx(cctx);
            ZSTD_freeCCtxParams(cctx_params);

            if ZSTD_isError(compressed_size) != 0 {
                anyhow::bail!("ZSTD compression error");
            }

            compressed.truncate(compressed_size);

            let mut writer = writer.lock().unwrap();
            let offset = writer.seek(SeekFrom::Current(0))?;
            writer.write_all(&compressed)?;

            Ok(FileEntry {
                path: rel_path.clone(),
                original_size: data.len() as u64,
                compressed_size: compressed.len() as u64,
                checksum: *checksum.as_bytes(),
                offset,
            })
        }
    }).collect::<Result<Vec<_>>>()?
        .into_iter()
        .for_each(|entry| entries.push(entry));

    let mut writer = writer.lock().unwrap();
    let index_offset = writer.seek(SeekFrom::Current(0))?;
    let (encoded, _len) = encode_to_vec(&entries, standard())?;
    writer.write_all(&encoded)?;

    let mut hasher = Hasher::new();
    hasher.update(&encoded);
    let hash = hasher.finalize();
    writer.write_all(hash.as_bytes())?;

    Ok(())
}
