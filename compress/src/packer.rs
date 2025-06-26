use std::fs::{self, File};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Result, Context};
use rayon::prelude::*;
use zstd_sys::*;
use blake3::Hasher;

use snippy_common::{FileEntry, should_skip_compression};


/// Compress a directory into a `.snippy` archive.
pub fn compress_dir(input_dir: &Path, output_file: &Path, skip_compression: bool) -> Result<()> {
    let mut entries = Vec::new();
    let mut writer = BufWriter::new(File::create(output_file)?);

    // Förbered indexposterna utan offset/längd ännu
    let paths: Vec<_> = walkdir::WalkDir::new(input_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .collect();

    for entry in &paths {
        let rel_path = entry.path().strip_prefix(input_dir)?.to_path_buf();
        entries.push(FileEntry {
            relative_path: rel_path,
            offset: 0,
            length: 0,
            checksum: [0u8; 32],
            compressed: false,
            uncompressed_size: 0,
        });
    }

    // Serialisera och skriv index (med dummy offset/längd)
    let data = bincode::encode_to_vec(&entries, bincode::config::standard())?;

    writer.write_all(&(data.len() as u64).to_le_bytes())?;
    writer.write_all(&data)?;

    // Gå tillbaka och skriv data, uppdatera offset/längd/checksum
    for (entry, original) in entries.iter_mut().zip(paths.iter()) {
        let offset = writer.seek(SeekFrom::Current(0))?;
        let mut file = File::open(original.path())?;

        let should_compress = !should_skip_compression(&entry.relative_path);
        let mut buffer = Vec::new();
        std::io::copy(&mut file, &mut buffer)?;

        let (written_data, compressed) = if should_compress {
            let cctx = unsafe { ZSTD_createCCtx() };
            assert!(!cctx.is_null());

            let cctx_params = unsafe { ZSTD_createCCtxParams() };
            unsafe {
                ZSTD_CCtxParams_init(cctx_params, 0);
                ZSTD_CCtxParams_setParameter(cctx_params, ZSTD_cParameter_ZSTD_c_nbWorkers, 4);
                ZSTD_CCtx_setParametersUsingCCtxParams(cctx, cctx_params);
            }

            let bound = unsafe { ZSTD_compressBound(buffer.len()) };
            let mut compressed = vec![0u8; bound];

            let compressed_size = unsafe {
                ZSTD_compressCCtx(
                    cctx,
                    compressed.as_mut_ptr() as *mut _,
                    compressed.len(),
                    buffer.as_ptr() as *const _,
                    buffer.len(),
                    3,
                )
            };

            unsafe {
                ZSTD_freeCCtxParams(cctx_params);
                ZSTD_freeCCtx(cctx);
            }

            if unsafe { ZSTD_isError(compressed_size) } != 0 {
                anyhow::bail!("Compression failed: {}", compressed_size);
            }

            compressed.truncate(compressed_size);
            (compressed, true)
        } else {
            (buffer, false)
        };

        writer.write_all(&written_data)?;

        entry.offset = offset;
        entry.length = written_data.len() as u64;
        entry.checksum = *blake3::hash(&written_data).as_bytes();
        entry.compressed = compressed;
    }

    Ok(())
}
