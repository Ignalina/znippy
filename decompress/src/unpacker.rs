use std::fs::File;
use std::io::{BufWriter, Read, Seek, Write};
use std::path::Path;

use anyhow::{Context, Result};
use blake3;
use hex;
use zstd_sys;
use std::ffi::c_int;

#[derive(Debug)]
pub struct FileEntry {
    pub path: String,
    pub offset: u64,
    pub size: u64,
    pub hash: [u8; 32],
}

pub fn read_snippy_index<P: AsRef<Path>>(archive_path: P) -> Result<Vec<FileEntry>> {
    let mut file = File::open(archive_path)?;
    file.seek(std::io::SeekFrom::End(-12))?;
    let mut buf = [0u8; 12];
    file.read_exact(&mut buf)?;
    let index_offset = u64::from_le_bytes(buf[0..8].try_into()?);
    let num_entries = u32::from_le_bytes(buf[8..12].try_into()?);

    file.seek(std::io::SeekFrom::Start(index_offset))?;
    let mut index = Vec::with_capacity(num_entries as usize);

    for _ in 0..num_entries {
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let path_len = u32::from_le_bytes(len_buf);

        let mut path_buf = vec![0u8; path_len as usize];
        file.read_exact(&mut path_buf)?;
        let path = String::from_utf8(path_buf)?;

        let mut offset_buf = [0u8; 8];
        file.read_exact(&mut offset_buf)?;
        let offset = u64::from_le_bytes(offset_buf);

        let mut size_buf = [0u8; 8];
        file.read_exact(&mut size_buf)?;
        let size = u64::from_le_bytes(size_buf);

        let mut hash_buf = [0u8; 32];
        file.read_exact(&mut hash_buf)?;

        index.push(FileEntry { path, offset, size, hash: hash_buf });
    }

    Ok(index)
}

pub fn list_snippy_archive<P: AsRef<Path>>(archive_path: P) -> Result<()> {
    let index = read_snippy_index(archive_path)?;
    for entry in index {
        println!("{:<60} {:>10} bytes  {}", entry.path, entry.size, hex::encode(entry.hash));
    }
    Ok(())
}

pub fn extract_snippy_archive<P: AsRef<Path>>(archive_path: P, output_dir: P) -> Result<()> {
    let index = read_snippy_index(&archive_path)?;
    let mut file = File::open(&archive_path)?;

    for entry in index {
        let mut output_path = output_dir.as_ref().to_path_buf();
        output_path.push(&entry.path);

        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        file.seek(std::io::SeekFrom::Start(entry.offset))?;
        let mut compressed = vec![0u8; entry.size as usize];
        file.read_exact(&mut compressed)?;

        let max_decompressed_size = zstd_sys::ZSTD_getFrameContentSize(compressed.as_ptr() as *const _, compressed.len());
        if max_decompressed_size == zstd_sys::ZSTD_CONTENTSIZE_ERROR || max_decompressed_size == zstd_sys::ZSTD_CONTENTSIZE_UNKNOWN {
            anyhow::bail!("Could not determine decompressed size");
        }

        let mut decompressed = vec![0u8; max_decompressed_size as usize];
        let decompressed_size = unsafe {
            zstd_sys::ZSTD_decompress(
                decompressed.as_mut_ptr() as *mut _,
                max_decompressed_size,
                compressed.as_ptr() as *const _,
                compressed.len(),
            )
        };

        if zstd_sys::ZSTD_isError(decompressed_size) != 0 {
            anyhow::bail!("ZSTD decompression failed");
        }

        decompressed.truncate(decompressed_size as usize);

        let actual_hash = blake3::hash(&decompressed);
        if actual_hash.as_bytes() != &entry.hash {
            anyhow::bail!("Hash mismatch for file: {}", entry.path);
        }

        let mut out_file = BufWriter::new(File::create(output_path)?);
        out_file.write_all(&decompressed)?;
    }

    Ok(())
}
