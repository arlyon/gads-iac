use crate::models::schema::Campaign;
use googleads_rs::google::ads::googleads::v23::enums::campaign_status_enum::CampaignStatus;
use googleads_rs::google::ads::googleads::v23::resources::Campaign as RemoteCampaign;
use googleads_rs::google::ads::googleads::v23::services::CampaignOperation;
use googleads_rs::google::ads::googleads::v23::services::MutateOperation;
use prost_types::FieldMask;
use tracing::{debug, trace};

/// Computes the difference between local YAML definition and remote Google Ads API state.
pub fn compute_diff(local: &Campaign, remote: &Campaign) -> Vec<String> {
    let mut differences = Vec::new();

    let mut local_norm = local.clone();
    local_norm.normalize();
    let mut remote_norm = remote.clone();
    remote_norm.normalize();

    let local_yml = serde_yaml::to_string(&local_norm).unwrap_or_default();
    let remote_yml = serde_yaml::to_string(&remote_norm).unwrap_or_default();

    if local_yml == remote_yml {
        return differences;
    }

    let l_lines: Vec<&str> = local_yml.lines().collect();
    let r_lines: Vec<&str> = remote_yml.lines().collect();

    use std::collections::HashMap;
    let mut l_counts = HashMap::new();
    for line in &l_lines {
        *l_counts.entry(line).or_insert(0) += 1;
    }
    let mut r_counts = HashMap::new();
    for line in &r_lines {
        *r_counts.entry(line).or_insert(0) += 1;
    }

    let mut added = Vec::new();
    let mut r_counts_temp = r_counts.clone();
    for line in &l_lines {
        let r_c = r_counts_temp.entry(line).or_insert(0);
        if *r_c > 0 {
            *r_c -= 1;
        } else {
            added.push(format!("+ {}", line));
        }
    }

    let mut removed = Vec::new();
    let mut l_counts_temp = l_counts.clone();
    for line in &r_lines {
        let l_c = l_counts_temp.entry(line).or_insert(0);
        if *l_c > 0 {
            *l_c -= 1;
        } else {
            removed.push(format!("- {}", line));
        }
    }

    differences.extend(added);
    differences.extend(removed);

    if differences.is_empty() && local_yml != remote_yml {
        differences.push("~ State difference detected (order/formatting only)".to_string());
    }

    differences
}

/// Translates structural differences into a set of precise gRPC update operations.
pub fn build_mutations(
    local: &Campaign,
    remote: Option<&Campaign>,
    account_id: &crate::models::account::AccountId,
) -> Vec<MutateOperation> {
    let mut operations = Vec::new();
    let clean_customer_id = account_id.unhyphenated();

    if let Some(r) = remote {
        debug!(
            "Comparing remote and local state for campaign: {}",
            local.name
        );
        // Update handling
        let mut update_mask = FieldMask::default();
        let mut has_changes = false;

        let mut campaign = RemoteCampaign {
            resource_name: format!(
                "customers/{}/campaigns/{}",
                clean_customer_id,
                local.id.unwrap_or(0)
            ),
            ..Default::default()
        };

        if local.name != r.name {
            campaign.name = local.name.clone();
            update_mask.paths.push("name".to_string());
            has_changes = true;
        }

        if local.status != r.status {
            campaign.status = match local.status.as_str() {
                "ENABLED" => CampaignStatus::Enabled as i32,
                "PAUSED" => CampaignStatus::Paused as i32,
                "REMOVED" => CampaignStatus::Removed as i32,
                _ => CampaignStatus::Unspecified as i32,
            };
            update_mask.paths.push("status".to_string());
            has_changes = true;
        }

        if has_changes {
            trace!("Generated FieldMask with paths: {:?}", update_mask.paths);
            let op = CampaignOperation {
                update_mask: Some(update_mask),
                operation: Some(googleads_rs::google::ads::googleads::v23::services::campaign_operation::Operation::Update(campaign)),
            };
            operations.push(MutateOperation {
                operation: Some(googleads_rs::google::ads::googleads::v23::services::mutate_operation::Operation::CampaignOperation(op)),
            });
        }

        // Sub-entity Diffing (Sitelinks)
        for (l_site, r_site) in local.sitelinks.iter().zip(r.sitelinks.iter()) {
            if let Some(asset_id) = r_site.asset_id {
                let mut site_mask = FieldMask::default();
                let mut has_site_changes = false;

                let mut sl_asset =
                    googleads_rs::google::ads::googleads::v23::common::SitelinkAsset::default();

                if l_site.link_text != r_site.link_text {
                    sl_asset.link_text = l_site.link_text.clone();
                    site_mask.paths.push("sitelink_asset.link_text".to_string());
                    has_site_changes = true;
                }

                if l_site.line1 != r_site.line1 {
                    sl_asset.description1 = l_site.line1.clone().unwrap_or_default();
                    site_mask
                        .paths
                        .push("sitelink_asset.description1".to_string());
                    has_site_changes = true;
                }

                if l_site.line2 != r_site.line2 {
                    sl_asset.description2 = l_site.line2.clone().unwrap_or_default();
                    site_mask
                        .paths
                        .push("sitelink_asset.description2".to_string());
                    has_site_changes = true;
                }

                if has_site_changes {
                    debug!(
                        "Sitelink '{}' drift detected. Queuing mutation.",
                        l_site.link_text
                    );
                    trace!("Sitelink mask: {:?}", site_mask.paths);
                    let asset = googleads_rs::google::ads::googleads::v23::resources::Asset {
                        resource_name: format!("customers/{}/assets/{}", clean_customer_id, asset_id),
                        asset_data: Some(googleads_rs::google::ads::googleads::v23::resources::asset::AssetData::SitelinkAsset(sl_asset)),
                        ..Default::default()
                    };

                    let op = googleads_rs::google::ads::googleads::v23::services::AssetOperation {
                        update_mask: Some(site_mask),
                        operation: Some(googleads_rs::google::ads::googleads::v23::services::asset_operation::Operation::Update(asset)),
                    };

                    operations.push(MutateOperation {
                        operation: Some(googleads_rs::google::ads::googleads::v23::services::mutate_operation::Operation::AssetOperation(op)),
                    });
                }
            }
        }
        // Sub-entity Diffing (Callouts)
        for (l_co, r_co) in local.callouts.iter().zip(r.callouts.iter()) {
            if let Some(asset_id) = r_co.asset_id {
                let mut co_mask = FieldMask::default();
                let mut has_co_changes = false;

                let mut co_asset =
                    googleads_rs::google::ads::googleads::v23::common::CalloutAsset::default();

                if l_co.text != r_co.text {
                    co_asset.callout_text = l_co.text.clone();
                    co_mask.paths.push("callout_asset.callout_text".to_string());
                    has_co_changes = true;
                }

                if has_co_changes {
                    debug!("Callout '{}' drift detected. Queuing mutation.", l_co.text);
                    let asset = googleads_rs::google::ads::googleads::v23::resources::Asset {
                        resource_name: format!("customers/{}/assets/{}", clean_customer_id, asset_id),
                        asset_data: Some(googleads_rs::google::ads::googleads::v23::resources::asset::AssetData::CalloutAsset(co_asset)),
                        ..Default::default()
                    };

                    let op = googleads_rs::google::ads::googleads::v23::services::AssetOperation {
                        update_mask: Some(co_mask),
                        operation: Some(googleads_rs::google::ads::googleads::v23::services::asset_operation::Operation::Update(asset)),
                    };

                    operations.push(MutateOperation {
                        operation: Some(googleads_rs::google::ads::googleads::v23::services::mutate_operation::Operation::AssetOperation(op)),
                    });
                }
            }
        }
    } else {
        // Create handling (simplified for this scope)
        let campaign = RemoteCampaign {
            name: local.name.clone(),
            status: match local.status.as_str() {
                "ENABLED" => CampaignStatus::Enabled as i32,
                "PAUSED" => CampaignStatus::Paused as i32,
                "REMOVED" => CampaignStatus::Removed as i32,
                _ => CampaignStatus::Unspecified as i32,
            },
            ..Default::default()
        };

        let op = CampaignOperation {
            operation: Some(googleads_rs::google::ads::googleads::v23::services::campaign_operation::Operation::Create(campaign)),
            ..Default::default()
        };

        operations.push(MutateOperation {
            operation: Some(googleads_rs::google::ads::googleads::v23::services::mutate_operation::Operation::CampaignOperation(op)),
        });
    }

    operations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::schema::{Campaign, Keyword};

    #[test]
    fn test_compute_diff_ignores_keyword_order() {
        let local = Campaign {
            id: Some(1),
            name: "Test Campaign".to_string(),
            status: "ENABLED".to_string(),
            budget_id: None,
            daily_budget: None,
            bidding_strategy: None,
            start_date: None,
            end_date: None,
            locations: vec![],
            callouts: vec![],
            sitelinks: vec![],
            negative_keywords: vec![
                Keyword {
                    criterion_id: None,
                    text: "kw1".to_string(),
                    match_type: "EXACT".to_string(),
                },
                Keyword {
                    criterion_id: None,
                    text: "kw2".to_string(),
                    match_type: "EXACT".to_string(),
                },
            ],
            ad_groups: vec![],
        };

        let remote = Campaign {
            id: Some(1),
            name: "Test Campaign".to_string(),
            status: "ENABLED".to_string(),
            budget_id: None,
            daily_budget: None,
            bidding_strategy: None,
            start_date: None,
            end_date: None,
            locations: vec![],
            callouts: vec![],
            sitelinks: vec![],
            negative_keywords: vec![
                Keyword {
                    criterion_id: None,
                    text: "kw2".to_string(),
                    match_type: "EXACT".to_string(),
                },
                Keyword {
                    criterion_id: None,
                    text: "kw1".to_string(),
                    match_type: "EXACT".to_string(),
                },
            ],
            ad_groups: vec![],
        };

        let diffs = compute_diff(&local, &remote);
        assert!(diffs.is_empty(), "Differences found: {:?}", diffs);
    }

    #[test]
    fn test_compute_diff_ignores_sitelink_and_url_order() {
        let local = Campaign {
            id: Some(1),
            name: "Test Campaign".to_string(),
            status: "ENABLED".to_string(),
            budget_id: None,
            daily_budget: None,
            bidding_strategy: None,
            start_date: None,
            end_date: None,
            locations: vec![],
            callouts: vec![],
            sitelinks: vec![
                crate::models::schema::Sitelink {
                    asset_id: None,
                    link_text: "Site 1".to_string(),
                    final_urls: vec!["url2".to_string(), "url1".to_string()],
                    line1: None,
                    line2: None,
                },
                crate::models::schema::Sitelink {
                    asset_id: None,
                    link_text: "Site 2".to_string(),
                    final_urls: vec!["url3".to_string()],
                    line1: None,
                    line2: None,
                },
            ],
            negative_keywords: vec![],
            ad_groups: vec![],
        };

        let remote = Campaign {
            id: Some(1),
            name: "Test Campaign".to_string(),
            status: "ENABLED".to_string(),
            budget_id: None,
            daily_budget: None,
            bidding_strategy: None,
            start_date: None,
            end_date: None,
            locations: vec![],
            callouts: vec![],
            sitelinks: vec![
                crate::models::schema::Sitelink {
                    asset_id: None,
                    link_text: "Site 2".to_string(),
                    final_urls: vec!["url3".to_string()],
                    line1: None,
                    line2: None,
                },
                crate::models::schema::Sitelink {
                    asset_id: None,
                    link_text: "Site 1".to_string(),
                    final_urls: vec!["url1".to_string(), "url2".to_string()],
                    line1: None,
                    line2: None,
                },
            ],
            negative_keywords: vec![],
            ad_groups: vec![],
        };

        let diffs = compute_diff(&local, &remote);
        assert!(diffs.is_empty(), "Differences found: {:?}", diffs);
    }
}
