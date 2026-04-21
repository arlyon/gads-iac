use crate::commands::{load_campaigns_by_account, print_diff_lines};
use crate::commands::import::fetch_remote_campaigns;
use crate::engine::config::Config;
use crate::engine::diff::compute_diff;
use anyhow::Result;
use colored::Colorize;

pub async fn run(config: &Config) -> Result<()> {
    println!("{}", "Loading local YAML files...".blue());

    let campaigns_by_account = load_campaigns_by_account()?;

    if campaigns_by_account.is_empty() {
        println!(
            "{}",
            "No local YAML files found. Try running `import` first.".yellow()
        );
        return Ok(());
    }

    for (account_id_str, local_campaigns) in &campaigns_by_account {
        let account_id = crate::models::account::AccountId::new(account_id_str)
            .map_err(|e| anyhow::anyhow!(e))?;

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
        for local in local_campaigns {
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
                        print_diff_lines(&diffs);
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
    }

    Ok(())
}
