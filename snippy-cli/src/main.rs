// snippy-cli/src/main.rs

use clap::{Parser, Subcommand};
use anyhow::Result;

#[derive(Parser)]
#[command(name = "snippy")]
#[command(about = "Snippy: streamande, flertrådad komprimering och dekomprimering", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Compress {
        input: String,
        output: String,
    },
    Decompress {
        input: String,
        output: String,
    },
    List {
        input: String,
    },
    Verify {
        input: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Compress { input, output } => {
            compress::compress_dir(input, output)?;
        }
        Commands::Decompress { input, output } => {
            decompress::decompress_snippy(input, output)?;
        }
        Commands::List { input } => {
            let (entries, _) = decompress::read_snippy_index(input)?;
            for entry in entries {
                println!("{} ({} bytes)", entry.path.display(), entry.original_size);
            }
        }
        Commands::Verify { input } => {
            decompress::verify_archive_integrity(input)?;
            println!("Archive OK.");
        }
    }

    Ok(())
}
