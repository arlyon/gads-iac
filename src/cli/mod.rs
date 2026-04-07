use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about = "Google Ads IaC CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Fetch state from Google Ads and write local YAML files
    Import {
        #[arg(short, long)]
        account_id: String,
    },
    /// Compare local YAML state against live remote state. Outputs a computed difference.
    Plan,
    /// Execute the differences, mutating the remote state to match the local files, detecting out-of-band drift.
    Apply,
}
