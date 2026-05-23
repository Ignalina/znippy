// znippy-cli/src/main.rs

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use znippy_common::{VerifyReport, list_archive_contents, verify_archive_integrity};
use znippy_common::plugin::PluginRegistry;
use znippy_common::plugins::wasm_loader::WasmPlugin;
use znippy_compress::compress_dir;
use znippy_decompress::decompress_archive;
use znippy_plugin_maven::NativeMavenPlugin;

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

        /// Path to a .wasm plugin for metadata extraction (replaces native maven plugin)
        #[arg(long)]
        plugin: Option<PathBuf>,

        /// DenseUnion type_id for the WASM plugin (default: 1 = maven)
        #[arg(long, default_value_t = 1)]
        plugin_type_id: i8,
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

pub fn run() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Compress {
            input,
            output,
            no_skip,
            plugin,
            plugin_type_id,
        } => {
            let registry = match plugin {
                Some(wasm_path) => {
                    let wp = WasmPlugin::load(&wasm_path.to_string_lossy(), "wasm-plugin", plugin_type_id)?;
                    PluginRegistry::with_plugin(Box::new(wp))
                }
                None => PluginRegistry::with_plugin(Box::new(NativeMavenPlugin)),
            };
            let report = compress_dir(&input, &output, no_skip, Some(&registry), None)?;
            println!("\n✅ Komprimering klar:");
            println!("📁 Totalt antal filer:         {}", report.total_files);
            println!("📁 Totalt antal chunks:         {}", report.chunks);

            println!("📂 Totalt antal kataloger:     {}", report.total_dirs);
            println!("📦 Filer komprimerade:         {}", report.compressed_files);
            println!(
                "📄 Filer ej komprimerade:      {}",
                report.uncompressed_files
            );
            println!("📥 Totalt inlästa bytes:       {}", report.total_bytes_in);
            println!("📤 Totalt skrivna bytes:       {}", report.total_bytes_out);
            println!("📉 Bytes som komprimerades:    {}", report.compressed_bytes);
            println!(
                "📃 Bytes ej komprimerade:      {}",
                report.uncompressed_bytes
            );
            println!(
                "📊 Komprimeringsgrad:          {:.2}%",
                report.compression_ratio
            );
        }

        Commands::Decompress { input, output } => {
            let report: VerifyReport = decompress_archive(&input, &output)?;
            println!("\n✅ Dekomprimering och verifiering klar:");
            println!("📁 Totala filer:       {}", report.total_files);
            println!("🔐 Verifierade filer:  {}", report.verified_files);
            println!("📥  chunks:    {}", report.chunks);
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
