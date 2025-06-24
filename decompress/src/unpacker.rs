use std::fs::{File, create_dir_all};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use rayon::prelude::*;
use blake3;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub path: String,
    pub offset: u64,
    pub size: u64,
    pub hash: [u8; 32],
}

pub fn read_snippy_index<P: AsRef<Path>>(archive_path: P) -> anyhow::Result<Vec<FileEntry>> {
    let archive_path = archive_path.as_ref();
    let mut file = File::open(archive_path)?;

    file.seek(SeekFrom::End(-12))?;
    let mut buf = [0u8; 12];
    file.read_exact(&mut buf)?;

    let index_offset = u64::from_le_bytes(buf[0..8].try_into()?);
    let num_entries = u32::from_le_bytes(buf[8..12].try_into()?);

    file.seek(SeekFrom::Start(index_offset))?;

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

pub fn extract_snippy_archive_filtered<P: AsRef<Path> + Sync>(
    archive_path: P,
    output_dir: P,
    include_only: Option<&HashSet<[u8; 32]>>,
    exclude_hashes: Option<&HashSet<[u8; 32]>>,
) -> anyhow::Result<()> {
    let archive_path = archive_path.as_ref();
    let index = read_snippy_index(archive_path)?;

    index.par_iter()
        .filter(|entry| {
            let include_ok = include_only.map_or(true, |set| set.contains(&entry.hash));
            let not_excluded = exclude_hashes.map_or(true, |set| !set.contains(&entry.hash));
            include_ok && not_excluded
        })
        .try_for_each(|entry| -> anyhow::Result<()> {
            let mut file = File::open(archive_path)?;
            file.seek(SeekFrom::Start(entry.offset))?;
            let mut limited = file.take(entry.size);
            let mut decoder = zstd::Decoder::new(&mut limited)?;

            let mut data = Vec::new();
            decoder.read_to_end(&mut data)?;

            let actual_hash = blake3::hash(&data);
            if actual_hash.as_bytes() != &entry.hash {
                anyhow::bail!("Checksum mismatch for {}", entry.path);
            }

            let out_path = output_dir.as_ref().join(&entry.path);
            if let Some(parent) = out_path.parent() {
                create_dir_all(parent)?;
            }

            std::fs::write(out_path, data)?;

            Ok(())
        })?;

    Ok(())
}

pub fn extract_snippy_archive<P: AsRef<Path> + Sync>(
    archive_path: P,
    output_dir: P,
) -> anyhow::Result<()> {
    extract_snippy_archive_filtered(archive_path, output_dir, None, None)
}
