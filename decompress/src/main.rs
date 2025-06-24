use clap::Parser;
use decompress::{extract_snippy_archive_filtered, read_snippy_index};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Parser)]
struct Args {
    #[arg(help = "Input .snippy file")]
    archive_file: String,

    #[arg(help = "Output directory")]
    output_dir: String,

    #[arg(long, help = "Path to file containing hex-encoded hashes to include")]
    include_hashes: Option<String>,

    #[arg(long, help = "Path to file containing hex-encoded hashes to exclude")]
    exclude_hashes: Option<String>,

    #[arg(long, help = "Only list archive contents with hash")]
    list_only: bool,
}

fn parse_hashes(path: &str) -> anyhow::Result<HashSet<[u8; 32]>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut set = HashSet::new();

    for line in reader.lines() {
        let line = line?;
        let bytes = hex::decode(line.trim())?;
        if bytes.len() != 32 {
            anyhow::bail!("Invalid hash length: {}", line);
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes);
        set.insert(hash);
    }
    Ok(set)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.list_only {
        let index = read_snippy_index(&args.archive_file)?;
        for entry in &index {
            println!("{:<60} {:>10} bytes  {}", entry.path, entry.size, blake3::Hash::from(entry.hash));
        }
        return Ok(());
    }

    let include_hashes = if let Some(path) = &args.include_hashes {
        Some(parse_hashes(path)?)
    } else {
        None
    };

    let exclude_hashes = if let Some(path) = &args.exclude_hashes {
        Some(parse_hashes(path)?)
    } else {
        None
    };

    extract_snippy_archive_filtered(&args.archive_file, &args.output_dir, include_hashes.as_ref(), exclude_hashes.as_ref())
}
