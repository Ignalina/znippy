use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use anyhow::{Context, Result};
use blake3::Hasher;
use bincode::{config::standard, decode_from_slice};

use crate::file_entry::FileEntry;

pub fn read_snippy_index(file: &mut File) -> Result<Vec<FileEntry>> {
    let mut reader = BufReader::new(file);
    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf)?;
    let index_len = u64::from_le_bytes(len_buf);

    let mut index_data = vec![0u8; index_len as usize];
    reader.read_exact(&mut index_data)?;

    let (entries, _): (Vec<FileEntry>, usize) =
        decode_from_slice(&index_data, standard())?;

    Ok(entries)
}

pub fn verify_archive_integrity(mut file: File) -> Result<()> {
    let entries = read_snippy_index(&mut file)?;

    for entry in &entries {
        file.seek(SeekFrom::Start(entry.offset))?;
        let mut buffer = vec![0u8; entry.uncompressed_size as usize];
        file.read_exact(&mut buffer)?;

        let hash = blake3::hash(&buffer);
        if hash.as_bytes() != &entry.checksum {
            anyhow::bail!(
                "Checksum mismatch for file {:?}",
                entry.relative_path
            );
        }
    }

    Ok(())
}

pub fn list_archive_contents(mut file: File) -> Result<()> {
    let entries = read_snippy_index(&mut file)?;

    for entry in entries {
        println!(
            "{} ({} bytes) [{}]",
            entry.relative_path.display(),
            entry.uncompressed_size,
            if entry.compressed { "compressed" } else { "stored" }
        );
    }

    Ok(())
}
