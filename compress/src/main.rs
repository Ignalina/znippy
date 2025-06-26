use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "compress")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Pack {
        input_dir: String,
        output_file: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Pack { input_dir, output_file } => {
            compress::packer::create_snippy_archive(input_dir, output_file)?;
        }
    }
    Ok(())
}
