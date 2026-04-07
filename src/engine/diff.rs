use crate::models::schema::Campaign;
use tracing::{debug, trace};
use googleads_rs::google::ads::googleads::v23::services::MutateOperation;
use googleads_rs::google::ads::googleads::v23::services::CampaignOperation;
use googleads_rs::google::ads::googleads::v23::resources::Campaign as RemoteCampaign;
use googleads_rs::google::ads::googleads::v23::enums::campaign_status_enum::CampaignStatus;
use prost_types::FieldMask;

/// Computes the difference between local YAML definition and remote Google Ads API state.
pub fn compute_diff(local: &Campaign, remote: &Campaign) -> Vec<String> {
    let mut differences = Vec::new();
    
    let local_yml = serde_yaml::to_string(local).unwrap_or_default();
    let remote_yml = serde_yaml::to_string(remote).unwrap_or_default();
    
    if local_yml == remote_yml {
        return differences;
    }
    
    let l_lines: Vec<&str> = local_yml.lines().collect();
    let r_lines: Vec<&str> = remote_yml.lines().collect();
    
    let mut l_adds = 0;
    for line in &l_lines {
        if !r_lines.contains(line) {
            differences.push(format!("+ {}", line));
            l_adds += 1;
        }
    }
    
    for line in &r_lines {
        if !l_lines.contains(line) {
            differences.push(format!("- {}", line));
            l_adds += 1;
        }
    }
    
    if l_adds == 0 {
        differences.push("~ State difference detected (likely array sorting/sequence)".to_string());
    }
    
    differences
}

/// Translates structural differences into a set of precise gRPC update operations.
pub fn build_mutations(local: &Campaign, remote: Option<&Campaign>, account_id: &crate::models::account::AccountId) -> Vec<MutateOperation> {
    let mut operations = Vec::new();
    let clean_customer_id = account_id.unhyphenated();

    if let Some(r) = remote {
        debug!("Comparing remote and local state for campaign: {}", local.name);
        // Update handling
        let mut update_mask = FieldMask::default();
        let mut has_changes = false;

        let mut campaign = RemoteCampaign {
            resource_name: format!("customers/{}/campaigns/{}", clean_customer_id, local.id.unwrap_or(0)),
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
                ..Default::default()
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
                
                let mut sl_asset = googleads_rs::google::ads::googleads::v23::common::SitelinkAsset::default();
                
                if l_site.link_text != r_site.link_text {
                    sl_asset.link_text = l_site.link_text.clone();
                    site_mask.paths.push("sitelink_asset.link_text".to_string());
                    has_site_changes = true;
                }
                
                if l_site.line1 != r_site.line1 {
                    sl_asset.description1 = l_site.line1.clone().unwrap_or_default();
                    site_mask.paths.push("sitelink_asset.description1".to_string());
                    has_site_changes = true;
                }
                
                if l_site.line2 != r_site.line2 {
                    sl_asset.description2 = l_site.line2.clone().unwrap_or_default();
                    site_mask.paths.push("sitelink_asset.description2".to_string());
                    has_site_changes = true;
                }
                
                if has_site_changes {
                    debug!("Sitelink '{}' drift detected. Queuing mutation.", l_site.link_text);
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

