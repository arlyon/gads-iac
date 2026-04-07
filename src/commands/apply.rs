use anyhow::Result;
use std::fs;
use std::fs::File;
use std::io::{self, Write};
use crate::models::schema::Campaign;
use crate::engine::diff::compute_diff;
use crate::commands::import::fetch_remote_campaigns;
use tracing::{debug, info, trace};

pub async fn run() -> Result<()> {
    println!("\x1b[1;34mLoading local YAML files...\x1b[0m");
    
    let mut local_campaigns: Vec<Campaign> = Vec::new();
    let mut account_id_opt = None;
    
    for entry in fs::read_dir(".")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with("_campaign.yaml") {
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
        }
    }
    
    if local_campaigns.is_empty() {
        println!("\x1b[33mNo local YAML files found. Try running `import` first.\x1b[0m");
        return Ok(());
    }
    let account_id_str = account_id_opt.unwrap_or_else(|| "593-530-0129".to_string());
    let account_id = crate::models::account::AccountId::new(&account_id_str)
        .map_err(|e| anyhow::anyhow!(e))?;
    
    println!("Found \x1b[1;32m{}\x1b[0m local campaigns for account \x1b[1;36m{}\x1b[0m.", local_campaigns.len(), account_id.hyphenated());
    debug!("Total campaigns loaded: {}", local_campaigns.len());
    
    println!("\x1b[1;34mFetching remote state...\x1b[0m");
    let remote_map = fetch_remote_campaigns(&account_id).await?;
    
    let mut clean = true;
    let mut total_diffs = 0;
    
    for local in &local_campaigns {
        if let Some(camp_id) = local.id {
            if let Some(remote) = remote_map.get(&camp_id) {
                let diffs = compute_diff(local, remote);
                if !diffs.is_empty() {
                    clean = false;
                    total_diffs += diffs.len();
                    println!("\x1b[1;33m~ Campaign {} ({}) has drifted:\x1b[0m", local.name, camp_id);
                    for diff in diffs {
                        if diff.starts_with('+') {
                            println!("\x1b[32m  {}\x1b[0m", diff);
                        } else if diff.starts_with('-') {
                            println!("\x1b[31m  {}\x1b[0m", diff);
                        } else if diff.starts_with('~') {
                            println!("\x1b[33m  {}\x1b[0m", diff);
                        } else {
                            println!("  {}", diff);
                        }
                    }
                    println!("");
                }
            } else {
                clean = false;
                total_diffs += 1;
                println!("\x1b[1;32m+ Campaign {} ({}) will be CREATED\x1b[0m", local.name, camp_id);
            }
        } else {
            clean = false;
            total_diffs += 1;
            println!("\x1b[1;32m+ Campaign {} (NEW) will be CREATED\x1b[0m", local.name);
        }
    }
    
    for (remote_id, remote) in &remote_map {
        if !local_campaigns.iter().any(|c| c.id == Some(*remote_id)) {
            clean = false;
            total_diffs += 1;
            println!("\x1b[1;31m- Campaign {} ({}) will be DESTROYED (exists in remote but not local)\x1b[0m", remote.name, remote_id);
        }
    }
    
    if clean {
        println!("\x1b[1;32mNo drift detected. Local state matches remote. Nothing to apply.\x1b[0m");
        return Ok(());
    }
    
    // Check for CI environment
    if std::env::var("CI").is_ok() {
        eprintln!("\x1b[1;31mERROR: CI environment detected and drift was found. Aborting to prevent un-interactive overwrites.\x1b[0m");
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

    // Connect MutateGoogleAdsRequest field-mask updates
    if operations.is_empty() {
        println!("\x1b[1;33mNo structural mutations could be built for the detected drift. (Only Campaign name/status are currently mapped in MVP).\x1b[0m");
        return Ok(());
    }

    // Prompt for confirmation locally
    print!("\x1b[1;33mDo you want to apply these {} mapped mutation(s) to the live API? (y/N): \x1b[0m", operations.len());
    io::stdout().flush()?;
    
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    
    if input.trim().to_lowercase() != "y" {
        println!("Apply aborted by user.");
        return Ok(());
    }

    println!("\n\x1b[1;36mApplying changes to remote state...\x1b[0m");
    println!("Executing {} mutation(s) concurrently...", operations.len());

    let mut client = crate::api::client::GoogleAdsClient::new().await?;
    
    let request = googleads_rs::google::ads::googleads::v23::services::MutateGoogleAdsRequest {
        customer_id: account_id.unhyphenated(), 
        mutate_operations: operations.clone(),
        partial_failure: true,
        validate_only: false,
        response_content_type: 0,
    };

    let response = match client.client.mutate(request).await {
        Ok(res) => res,
        Err(status) => {
            eprintln!("\x1b[1;31mFatal API Error during dispatch: {:?}\x1b[0m", status);
            return Ok(());
        }
    };

    let inner = response.into_inner();
    
    let mut aggregator = crate::engine::errors::ErrorAggregator::new();
    if let Some(status) = inner.partial_failure_error {
        // Here we could extract `status` using our `parse_partial_failures` but for MVP:
        println!("\x1b[1;31mPartial Failures Detected!\x1b[0m");
        // Decode details or log status
        // tonic status details can be unpacked from any prost payload
        println!("Details: {:?}", status.details);
    }
    
    let successes = inner.mutate_operation_responses.len();
    let total = operations.len();
    let failures = total.saturating_sub(successes);
    
    println!("\x1b[1;32mApply Summary: {} attempted, {} succeeded, {} failed.\x1b[0m", total, successes, failures);

    Ok(())
}
