use crate::commands::import::fetch_remote_campaigns;
use crate::engine::config::Config;
use crate::engine::diff::compute_diff;
use crate::models::schema::Campaign;
use anyhow::Result;
use colored::Colorize;
use std::fs;
use std::fs::File;

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

    println!("{}", "Fetching remote state...".blue());
    let remote_map = fetch_remote_campaigns(&account_id, config).await?;

    println!(
        "\n{}",
        "================ GENERATING PLAN ================"
            .cyan()
            .bold()
    );
    let mut clean = true;
    for local in &local_campaigns {
        if let Some(camp_id) = local.id {
            if let Some(remote) = remote_map.get(&camp_id) {
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
            "No drift detected! Local infrastructure matches remote state.".green()
        );
    }

    println!(
        "\n{}",
        "================================================="
            .cyan()
            .bold()
    );

    Ok(())
}
