use clap::{Parser, Subcommand};
use anyhow::Result;
use decompress::unpacker::{extract_snippy_archive, list_snippy_archive};

#[derive(Parser)]
#[command(name = "decompress")]
#[command(about = "Snippy decompressor", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Unpack a .snippy archive
    Unpack {
        #[arg(help = "Input .snippy archive")]
        input: String,
        #[arg(help = "Output directory")]
        output: String,
    },

    /// List contents of a .snippy archive
    List {
        #[arg(help = "Input .snippy archive")]
        input: String,
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Unpack { input, output } => {
            extract_snippy_archive(input, output)?;
        }
        Commands::List { input } => {
            list_snippy_archive(input)?;
        }
    }

    Ok(())
}
