use crate::api::client::{GoogleAdsClient, ads};
use crate::commands::import::fetch_remote_campaigns;
use crate::engine::config::Config;
use crate::engine::diff::compute_diff;
use crate::models::schema::Campaign;
use anyhow::Result;
use colored::Colorize;
use std::fs;
use std::fs::File;
use std::io::{self, Write};
use tracing::{debug, trace};

pub async fn run(config: &Config) -> Result<()> {
    println!("{}", "Loading local YAML files...".blue());

    let mut local_campaigns: Vec<Campaign> = Vec::new();
    let mut account_id_opt = None;

    for entry in fs::read_dir(".")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
            && name.ends_with("_campaign.yaml")
        {
            let parts: Vec<&str> = name.split('_').collect();
            if parts.len() >= 2 {
                account_id_opt = Some(parts[0].to_string());
            }

            let file = File::open(&path)?;
            let campaign: Campaign = serde_yaml::from_reader(file)?;
            trace!("Loaded campaign from file: {}", path.display());
            local_campaigns.push(campaign);
        }
    }

    if local_campaigns.is_empty() {
        println!(
            "{}",
            "No local YAML files found. Try running `import` first.".yellow()
        );
        return Ok(());
    }
    let account_id_str = account_id_opt.unwrap_or_else(|| "593-530-0129".to_string());
    let account_id =
        crate::models::account::AccountId::new(&account_id_str).map_err(|e| anyhow::anyhow!(e))?;

    println!(
        "Found {} local campaigns for account {}.",
        local_campaigns.len().to_string().green(),
        account_id.hyphenated().cyan()
    );
    debug!("Total campaigns loaded: {}", local_campaigns.len());

    println!("{}", "Fetching remote state...".blue());
    let mut remote_map = fetch_remote_campaigns(&account_id, config).await?;

    let mut clean = true;

    for local in &mut local_campaigns {
        local.normalize();
        if let Some(camp_id) = local.id {
            if let Some(remote) = remote_map.get_mut(&camp_id) {
                remote.normalize();
                let diffs = compute_diff(local, remote);
                if !diffs.is_empty() {
                    clean = false;
                    println!(
                        "{} Campaign {} ({}) has drifted:",
                        "~".yellow(),
                        local.name.bold(),
                        camp_id
                    );
                    for diff in diffs {
                        if diff.starts_with('+') {
                            println!("{}", diff.green());
                        } else if diff.starts_with('-') {
                            println!("{}", diff.red());
                        } else if diff.starts_with('~') {
                            println!("{}", diff.yellow());
                        } else {
                            println!("  {}", diff);
                        }
                    }
                    println!();
                }
            } else {
                clean = false;
                println!(
                    "{} Campaign {} ({}) will be {}",
                    "+".green(),
                    local.name.bold(),
                    camp_id,
                    "CREATED".green()
                );
            }
        } else {
            clean = false;
            println!(
                "{} Campaign {} (NEW) will be {}",
                "+".green(),
                local.name.bold(),
                "CREATED".green()
            );
        }
    }

    // Removed destructive DESTROY logic as per requirements.

    if clean {
        println!(
            "{}",
            "No drift detected. Local state matches remote. Nothing to apply.".green()
        );
        return Ok(());
    }

    // Check for CI environment
    if std::env::var("CI").is_ok() {
        eprintln!(
            "{}",
            "ERROR: CI environment detected and drift was found. Aborting to prevent un-interactive overwrites.".red().bold()
        );
        std::process::exit(1);
    }

    let mut operations = Vec::new();
    for local in &local_campaigns {
        let remote_opt = local.id.and_then(|id| remote_map.get(&id));
        debug!("Processing diffs for campaign: {}", local.name);
        let mut ops = crate::engine::diff::build_mutations(local, remote_opt, &account_id);
        trace!("Generated {} mutations for campaign", ops.len());
        operations.append(&mut ops);
    }

    if operations.is_empty() {
        println!(
            "{}",
            "No structural mutations could be built for the detected drift. (Only Campaign name/status are currently mapped in MVP).".yellow()
        );
        return Ok(());
    }

    // Prompt for confirmation locally
    print!(
        "Do you want to apply these {} mapped mutation(s) to the live API? (y/N): ",
        operations.len().to_string().yellow()
    );
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    if input.trim().to_lowercase() != "y" {
        println!("Apply aborted by user.");
        return Ok(());
    }

    println!("\n{}", "Applying changes to remote state...".cyan().bold());
    println!(
        "Executing {} mutation(s) concurrently...",
        operations.len().to_string().cyan()
    );

    let mut client = GoogleAdsClient::new(config).await?;

    let request = ads::services::MutateGoogleAdsRequest {
        customer_id: account_id.unhyphenated(),
        mutate_operations: operations.clone(),
        partial_failure: true,
        validate_only: false,
        response_content_type: 0,
    };

    let response = match client.client.mutate(request).await {
        Ok(res) => res,
        Err(status) => {
            eprintln!(
                "{} Fatal API Error during dispatch: {:?}",
                "✘".red(),
                status
            );
            return Ok(());
        }
    };

    let inner = response.into_inner();

    let mut aggregator = crate::engine::errors::ErrorAggregator::new();
    if let Some(status) = inner.partial_failure_error {
        println!("{}", "Partial Failures Detected!".red().bold());
        aggregator.parse_partial_failures(&status.details);
        for error in &aggregator.errors {
            println!("  {} {}", "-".red(), error);
        }
    }

    let total = operations.len();
    let mut successes = 0;
    for res in &inner.mutate_operation_responses {
        if res.response.is_some() {
            successes += 1;
        }
    }
    let failures = total.saturating_sub(successes);

    println!(
        "\n{} Apply Summary: {} attempted, {} succeeded, {} failed.",
        "ℹ".blue().bold(),
        total,
        successes.to_string().green(),
        failures.to_string().red()
    );

    if failures > 0 {
        return Err(anyhow::anyhow!(
            "Apply completed with {} partial failures.",
            failures
        ));
    }

    Ok(())
}
