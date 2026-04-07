use anyhow::Result;
use std::fs;
use std::fs::File;
use crate::models::schema::Campaign;
use crate::engine::diff::compute_diff;
use crate::commands::import::fetch_remote_campaigns;

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
    
    println!("\x1b[1;34mFetching remote state...\x1b[0m");
    let remote_map = fetch_remote_campaigns(&account_id).await?;
    
    println!("\n\x1b[1;36m================ GENERATING PLAN ================\x1b[0m\n");
    let mut clean = true;
    for local in &local_campaigns {
        if let Some(camp_id) = local.id {
            if let Some(remote) = remote_map.get(&camp_id) {
                let diffs = compute_diff(local, remote);
                if !diffs.is_empty() {
                    clean = false;
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
                println!("\x1b[1;32m+ Campaign {} ({}) will be CREATED\x1b[0m", local.name, camp_id);
            }
        } else {
            clean = false;
            println!("\x1b[1;32m+ Campaign {} (NEW) will be CREATED\x1b[0m", local.name);
        }
    }
    
    for (remote_id, remote) in &remote_map {
        if !local_campaigns.iter().any(|c| c.id == Some(*remote_id)) {
            clean = false;
            println!("\x1b[1;31m- Campaign {} ({}) will be DESTROYED (exists in remote but not local)\x1b[0m", remote.name, remote_id);
        }
    }
    
    if clean {
        println!("\x1b[1;32mNo drift detected! Local infrastructure matches remote state.\x1b[0m");
    }
    
    println!("\n\x1b[1;36m=================================================\x1b[0m");

    Ok(())
}
