use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use anyhow::{Context, Result, bail};
use blake3::Hasher;
use bincode::{config::standard, decode_from_slice};
use log::debug;

use crate::FileEntry;

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
    debug!("[verify] Läser index...");
    let entries = read_snippy_index(&mut file)?;
    debug!("[verify] Läste {} poster", entries.len());

    let mut verified_count = 0;

    for (i, entry) in entries.iter().enumerate() {
        file.seek(SeekFrom::Start(entry.offset))?;
        let mut reader = (&mut file).take(entry.length);

        let mut hasher = Hasher::new();
        let mut buffer = [0u8; 8192];

        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }

        let actual = hasher.finalize();
        if actual.as_bytes() != &entry.checksum {
            bail!(
                "❌ Felaktig kontrollsumma för {:?} (index {}):\nFörväntad: {}\nFick:      {}",
                entry.relative_path,
                i,
                hex::encode(entry.checksum),
                actual.to_hex()
            );
        }

        verified_count += 1;
        debug!("[verify] OK: {:?} (index {})", entry.relative_path, i);
    }

    println!("✅ Arkivet är korrekt (alla {} checksummor matchar).", verified_count);
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
