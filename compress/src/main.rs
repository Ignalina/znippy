// compress/src/main.rs
use clap::{Parser, Subcommand};
use compress::{create_snippy_archive, list_snippy_archive, export_snippy_hashlist};

#[derive(Parser)]
#[command(name = "snippy-compress")]
#[command(about = "Create or inspect a .snippy archive", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Pack {
        #[arg(help = "Input directory")]
        input_dir: String,

        #[arg(help = "Output .snippy file")]
        output_file: String,
    },

    List {
        #[arg(help = "Path to .snippy archive")]
        archive_file: String,
    },

    ExportHashes {
        #[arg(help = "Path to .snippy archive")]
        archive_file: String,

        #[arg(help = "Output path for hash list file")]
        out_file: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Pack { input_dir, output_file } => {
            if let Err(e) = create_snippy_archive(&input_dir, &output_file) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::List { archive_file } => {
            if let Err(e) = list_snippy_archive(&archive_file) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::ExportHashes { archive_file, out_file } => {
            if let Err(e) = export_snippy_hashlist(&archive_file, &out_file) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
