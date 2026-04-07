use clap::Parser;

mod api;
mod cli;
mod commands;
mod engine;
mod models;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    
    let cli_parsed = cli::Cli::parse();

    match &cli_parsed.command {
        cli::Commands::Import { account_id } => commands::import::run(account_id).await?,
        cli::Commands::Plan => commands::plan::run().await?,
        cli::Commands::Apply => commands::apply::run().await?,
    }

    Ok(())
}
