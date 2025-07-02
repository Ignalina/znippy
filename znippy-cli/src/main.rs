// znippy-cli/src/main.rs

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use anyhow::Result;

use compress::compress_dir;
use decompress::decompress_archive;
use znippy_common::{verify_archive_integrity, list_archive_contents, VerifyReport};

#[derive(Parser)]
#[command(name = "znippy")]
#[command(about = "Znippy: fast archive format with per-file compression", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Compress a directory into a .znippy archive
    Compress {
        #[arg(short, long)]
        input: PathBuf,

        #[arg(short, long)]
        output: PathBuf,

        #[arg(long)]
        no_skip: bool,
    },

    /// Decompress a .znippy archive
    Decompress {
        #[arg(short, long)]
        input: PathBuf,

        #[arg(short, long)]
        output: PathBuf,
    },

    /// List contents of a .znippy archive
    List {
        #[arg(short, long)]
        input: PathBuf,
    },

    /// Verify archive integrity (checksum)
    Verify {
        #[arg(short, long)]
        input: PathBuf,
    },
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Compress { input, output, no_skip } => {
            let report = compress_dir(&input, &output)?;
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
            let report: VerifyReport = decompress_archive(&input,  &output)?;
            println!("\n✅ Dekomprimering och verifiering klar:");
            println!("📁 Totala filer:       {}", report.total_files);
            println!("🔐 Verifierade filer:  {}", report.verified_files);
            println!("❌ Korrupta filer:     {}", report.corrupt_files);
            println!("📥 Totala bytes:       {}", report.total_bytes);
            println!("📤 Verifierade bytes:  {}", report.verified_bytes);
            println!("⚠️  Korrupta bytes:    {}", report.corrupt_bytes);
        }

        Commands::List { input } => {
            list_archive_contents(&input)?;
        }

        Commands::Verify { input } => {
            let report: VerifyReport = verify_archive_integrity(&input)?;
            println!("\n🔍 Verifiering klar:");
            println!("📁 Totala filer:       {}", report.total_files);
            println!("🔐 Verifierade filer:  {}", report.verified_files);
            println!("❌ Korrupta filer:     {}", report.corrupt_files);
            println!("📥 Totala bytes:       {}", report.total_bytes);
            println!("📤 Verifierade bytes:  {}", report.verified_bytes);
            println!("⚠️  Korrupta bytes:    {}", report.corrupt_bytes);
        }
    }

    Ok(())
}
