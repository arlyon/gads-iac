pub mod apply;
pub mod export_schema;
pub mod import;
pub mod plan;

use crate::models::schema::Campaign;
use anyhow::Result;
use colored::Colorize;
use std::collections::HashMap;
use std::fs;
use std::fs::File;

/// Load all `*_campaign.yaml` files in the current directory, grouped by account ID.
pub fn load_campaigns_by_account() -> Result<HashMap<String, Vec<Campaign>>> {
    let mut by_account: HashMap<String, Vec<Campaign>> = HashMap::new();

    for entry in fs::read_dir(".")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
            && name.ends_with("_campaign.yaml")
        {
            if let Some(account_id) = name.split('_').next().map(|s| s.to_string()) {
                let file = File::open(&path)?;
                let campaign: Campaign = serde_yaml::from_reader(file)?;
                by_account.entry(account_id).or_default().push(campaign);
            }
        }
    }

    Ok(by_account)
}

/// Print a list of diff lines with colour coding.
pub fn print_diff_lines(diffs: &[String]) {
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
}
