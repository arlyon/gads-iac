use anyhow::Result;
use std::fs::File;
use std::path::Path;
use std::io::{self, Write};
use crate::models::schema::Campaign;
use crate::engine::diff::compute_diff;

pub async fn run() -> Result<()> {
    println!("Loading local YAML files...");
    
    let target_file = "5935300129_campaign.yaml";
    if !Path::new(target_file).exists() {
        println!("No local YAML file found at {}. Try running `import` first.", target_file);
        return Ok(());
    }

    let file = File::open(target_file)?;
    let local_campaign: Campaign = serde_yaml::from_reader(file)?;
    
    println!("Fetching remote state...");
    // Mock the remote state. Assuming remote has drift from local to demonstrate "PLAN" capabilities.
    let mut remote_campaign = local_campaign.clone();
    remote_campaign.status = "PAUSED".to_string(); // Mock drift
    
    let diffs = compute_diff(&local_campaign, &remote_campaign);
    
    if diffs.is_empty() {
        println!("No drift detected. Local state matches Remote. Nothing to apply.");
        return Ok(());
    }
    
    println!("Drift detected! Out-of-band changes found:");
    for diff in &diffs {
        println!("  - {}", diff);
    }
    
    // Check for CI environment
    if std::env::var("CI").is_ok() {
        eprintln!("ERROR: CI environment detected and drift was found. Aborting to prevent accidental overwrites.");
        std::process::exit(1);
    }

    // Prompt for confirmation locally
    print!("Do you want to overwrite remote changes with local state? (y/N): ");
    io::stdout().flush()?;
    
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    
    if input.trim().to_lowercase() != "y" {
        println!("Apply aborted by user.");
        return Ok(());
    }

    println!("Applying changes to remote state to match local YAML...");
    // Mocking apply mutation failures aggregation
    println!("Executing {} mutation(s)...", diffs.len());
    let mut failures = vec![];
    let mut successes = 0;
    
    for (i, _) in diffs.iter().enumerate() {
        if i == 0 { // simulating one success
            successes += 1;
        } else {
            failures.push(format!("Policy violation for mutation {}", i));
        }
    }
    
    println!("Apply Summary: {} succeeded, {} failed.", successes, failures.len());
    if !failures.is_empty() {
        println!("Failures:");
        for failure in failures {
            println!("  - {}", failure);
        }
    }

    Ok(())
}
