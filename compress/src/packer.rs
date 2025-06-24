// compress/src/packer.rs
use std::fs::File;
use std::io::{BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;
use rayon::prelude::*;
use blake3;
use hex;

#[derive(Debug)]
struct FileEntry {
    path: String,
    offset: u64,
    size: u64,
    hash: [u8; 32],
}

struct CompressedFile {
    path: String,
    compressed: Vec<u8>,
    hash: [u8; 32],
}

pub fn create_snippy_archive<P: AsRef<Path>>(input_dir: P, output_file: P) -> anyhow::Result<()> {
    let input_dir = input_dir.as_ref();

    let files: Vec<PathBuf> = WalkDir::new(input_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect();

    let compressed_files: Vec<CompressedFile> = files
        .par_iter()
        .map(|path| {
            let rel_path = path.strip_prefix(input_dir).unwrap().to_str().unwrap().replace("\\", "/");
            let mut file = File::open(path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            let hash = *blake3::hash(&buffer).as_bytes();

            let mut compressed = Vec::new();
            zstd::stream::copy_encode(&buffer[..], &mut compressed, 3)?;

            Ok(CompressedFile { path: rel_path, compressed, hash })
        })
        .collect::<Result<_, anyhow::Error>>()?;

    let mut archive = BufWriter::new(File::create(&output_file)?);
    let mut index = Vec::new();
    let mut offset = 0u64;

    for file in &compressed_files {
        archive.write_all(&file.compressed)?;
        index.push(FileEntry {
            path: file.path.clone(),
            offset,
            size: file.compressed.len() as u64,
            hash: file.hash,
        });
        offset += file.compressed.len() as u64;
    }

    let index_offset = archive.stream_position()?;
    for entry in &index {
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

pub fn list_snippy_archive<P: AsRef<Path>>(archive_path: P) -> anyhow::Result<()> {
    let index = read_index(archive_path)?;
    for entry in index {
        println!("{:<60} {:>10} bytes  {}", entry.path, entry.size, hex::encode(entry.hash));
    }
    Ok(())
}

pub fn export_snippy_hashlist<P: AsRef<Path>>(archive_path: P, out_file: P) -> anyhow::Result<()> {
    let index = read_index(archive_path)?;
    let mut out = BufWriter::new(File::create(out_file)?);
    for entry in index {
        writeln!(out, "{}", hex::encode(entry.hash))?;
    }
    Ok(())
}

fn read_index<P: AsRef<Path>>(archive_path: P) -> anyhow::Result<Vec<FileEntry>> {
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
