use clap::{Parser, Subcommand};
use std::path::PathBuf;
use anyhow::Result;

use compress::compress_dir;
use decompress::decompress_archive;
use snippy_common::{verify_archive_integrity, list_archive_contents};

#[derive(Parser)]
#[command(name = "snippy")]
#[command(about = "Snippy: fast archive format with per-file compression", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Compress a directory into a .snippy archive
    Compress {
        /// Input directory
        #[arg(short, long)]
        input: PathBuf,

        /// Output archive file (.snippy)
        #[arg(short, long)]
        output: PathBuf,

        /// Disable skipping compression for already compressed files
        #[arg(long)]
        no_skip: bool,
    },

    /// Decompress a .snippy archive
    Decompress {
        /// Input archive file (.snippy)
        #[arg(short, long)]
        input: PathBuf,

        /// Output directory
        #[arg(short, long)]
        output: PathBuf,
    },

    /// List contents of a .snippy archive
    List {
        /// Input archive file (.snippy)
        #[arg(short, long)]
        input: PathBuf,
    },

    /// Verify archive integrity (checksum)
    Verify {
        /// Input archive file (.snippy)
        #[arg(short, long)]
        input: PathBuf,
    },
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Compress { input, output, no_skip } => {
            let report = compress_dir(&input, &output, !no_skip)?;
            println!("\n✅ Komprimering klar:");
            println!("📁 Totalt antal filer:         {}", report.total_files);
            println!("📂 Totalt antal kataloger:     {}", report.total_dirs);
            println!("📦 Filer komprimerade:         {}", report.compressed_files);
            println!("📄 Filer ej komprimerade:      {}", report.uncompressed_files);
            println!("📥 Totalt inlästa bytes:       {}", report.total_bytes_in);
            println!("📤 Totalt skrivna bytes:       {}", report.total_bytes_out);
            println!("📉 Bytes som komprimerades:    {}", report.compressed_bytes);
            println!("📃 Bytes ej komprimerade:      {}", report.uncompressed_bytes);
            println!("📊 Komprimeringsgrad:          {:.2}%", report.compression_ratio);
        }
        Commands::Decompress { input, output } => {
            decompress_archive(&input, &output)?;
        }
        Commands::List { input } => {
            let file = std::fs::File::open(&input)?;
            list_archive_contents(file)?;
        }
        Commands::Verify { input } => {
            let file = std::fs::File::open(&input)?;
            verify_archive_integrity(file)?;
        }
    }

    Ok(())
}
