use anyhow::Result;
use std::fs::File;
use std::path::Path;
use crate::models::schema::Campaign;
use crate::engine::diff::compute_diff;

pub async fn run() -> Result<()> {
    println!("Loading local YAML files...");
    
    // In a real app we'd iterate over all .yaml files in a directory or accept a specific one
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
        println!("No drift detected. Local state matches Remote.");
    } else {
        println!("Drift detected! Changes required to match local:");
        for diff in diffs {
            println!("  - {}", diff);
        }
    }

    Ok(())
}
