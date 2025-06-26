use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{Result, Context};
use zstd_sys::*;
use snippy_common::{read_snippy_index, FileEntry};

/// Decompress all files from a `.snippy` archive into a target directory.
pub fn decompress_archive(archive_path: &Path, output_dir: &Path) -> Result<()> {
    let mut file = File::open(archive_path)?;
    let entries = read_snippy_index(&mut file)?;

    for entry in &entries {
        let output_path = output_dir.join(&entry.relative_path);
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        file.seek(SeekFrom::Start(entry.offset))?;
        let mut buffer = vec![0u8; entry.length as usize];
        file.read_exact(&mut buffer)?;

        let data = if entry.compressed {
            let dctx = unsafe { ZSTD_createDCtx() };
            assert!(!dctx.is_null());

            let decompressed_bound = entry.length * 50; // generous overestimate
            let mut decompressed = vec![0u8; decompressed_bound as usize];

            let size = unsafe {
                ZSTD_decompressDCtx(
                    dctx,
                    decompressed.as_mut_ptr() as *mut _,
                    decompressed.len(),
                    buffer.as_ptr() as *const _,
                    buffer.len(),
                )
            };

            unsafe { ZSTD_freeDCtx(dctx) };

            if unsafe { ZSTD_isError(size) } != 0 {
                anyhow::bail!("Decompression failed for {:?}", entry.relative_path);
            }

            decompressed.truncate(size);
            decompressed
        } else {
            buffer
        };

        let mut out_file = File::create(&output_path)?;
        out_file.write_all(&data)?;
    }

    Ok(())
}
