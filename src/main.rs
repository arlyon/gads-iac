use clap::Parser;
use miette::IntoDiagnostic;

mod api;
mod cli;
mod commands;
mod engine;
mod models;

fn main() -> miette::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()
        .into_diagnostic()?;

    runtime.block_on(async move {
        let cli_parsed = cli::Cli::parse();

        if let cli::Commands::ExportSchema = &cli_parsed.command {
            return commands::export_schema::run().map_err(|error| miette::miette!("{error:?}"));
        }

        let config =
            engine::config::Config::from_env().map_err(|error| miette::miette!("{error:?}"))?;

        match &cli_parsed.command {
            cli::Commands::Import { account_id } => commands::import::run(account_id, &config)
                .await
                .map_err(|error| miette::miette!("{error:?}"))?,
            cli::Commands::Plan => commands::plan::run(&config)
                .await
                .map_err(|error| miette::miette!("{error:?}"))?,
            cli::Commands::Apply => commands::apply::run(&config).await?,
            cli::Commands::ExportSchema => unreachable!(),
        }

        Ok(())
    })
}
