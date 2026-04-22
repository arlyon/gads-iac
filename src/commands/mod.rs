pub mod apply;
pub mod export_schema;
pub mod import;
pub mod plan;

use crate::models::schema::Campaign;
use anyhow::Result;
use colored::Colorize;
use std::collections::HashMap;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;

/// A local campaign loaded from YAML, including the source metadata needed for
/// diagnostics that point back into the original file.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LocalCampaign {
    pub campaign: Campaign,
    pub source_path: PathBuf,
    pub source: String,
}

impl Deref for LocalCampaign {
    type Target = Campaign;

    fn deref(&self) -> &Self::Target {
        &self.campaign
    }
}

impl DerefMut for LocalCampaign {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.campaign
    }
}

/// Load all `*_campaign.yaml` files in the current directory, grouped by account ID.
pub fn load_local_campaigns_by_account() -> Result<HashMap<String, Vec<LocalCampaign>>> {
    let mut by_account: HashMap<String, Vec<LocalCampaign>> = HashMap::new();

    for entry in fs::read_dir(".")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file()
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
            && name.ends_with("_campaign.yaml")
        {
            if let Some(account_id) = name.split('_').next().map(|s| s.to_string()) {
                let source = fs::read_to_string(&path)?;
                let campaign: Campaign = serde_yaml::from_str(&source)?;
                by_account
                    .entry(account_id)
                    .or_default()
                    .push(LocalCampaign {
                        campaign,
                        source_path: path,
                        source,
                    });
            }
        }
    }

    Ok(by_account)
}

/// Load all `*_campaign.yaml` files in the current directory, grouped by account ID.
pub fn load_campaigns_by_account() -> Result<HashMap<String, Vec<Campaign>>> {
    Ok(load_local_campaigns_by_account()?
        .into_iter()
        .map(|(account_id, campaigns)| {
            (
                account_id,
                campaigns
                    .into_iter()
                    .map(|local_campaign| local_campaign.campaign)
                    .collect(),
            )
        })
        .collect())
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
