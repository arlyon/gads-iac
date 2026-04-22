use crate::models::schema::{AdText, BiddingStrategy, Campaign, Keyword, Sitelink};
use googleads_rs::google::ads::googleads::v23 as ads;
use googleads_rs::google::ads::googleads::v23::common::{AdTextAsset, KeywordInfo};
use googleads_rs::google::ads::googleads::v23::enums::ad_group_ad_status_enum::AdGroupAdStatus;
use googleads_rs::google::ads::googleads::v23::enums::ad_group_status_enum::AdGroupStatus;
use googleads_rs::google::ads::googleads::v23::enums::asset_field_type_enum::AssetFieldType;
use googleads_rs::google::ads::googleads::v23::enums::campaign_status_enum::CampaignStatus;
use googleads_rs::google::ads::googleads::v23::enums::keyword_match_type_enum::KeywordMatchType;
use googleads_rs::google::ads::googleads::v23::enums::served_asset_field_type_enum::ServedAssetFieldType;
use googleads_rs::google::ads::googleads::v23::resources::Ad as RemoteAd;
use googleads_rs::google::ads::googleads::v23::resources::AdGroup as RemoteAdGroup;
use googleads_rs::google::ads::googleads::v23::resources::AdGroupAd as RemoteAdGroupAd;
use googleads_rs::google::ads::googleads::v23::resources::AdGroupCriterion;
use googleads_rs::google::ads::googleads::v23::resources::Asset;
use googleads_rs::google::ads::googleads::v23::resources::Campaign as RemoteCampaign;
use googleads_rs::google::ads::googleads::v23::resources::CampaignAsset;
use googleads_rs::google::ads::googleads::v23::resources::CampaignBudget;
use googleads_rs::google::ads::googleads::v23::resources::CampaignCriterion;
use googleads_rs::google::ads::googleads::v23::resources::ad;
use googleads_rs::google::ads::googleads::v23::resources::ad_group_criterion;
use googleads_rs::google::ads::googleads::v23::resources::asset;
use googleads_rs::google::ads::googleads::v23::resources::campaign_criterion;
use googleads_rs::google::ads::googleads::v23::services::AdGroupAdOperation;
use googleads_rs::google::ads::googleads::v23::services::AdGroupCriterionOperation;
use googleads_rs::google::ads::googleads::v23::services::AdGroupOperation;
use googleads_rs::google::ads::googleads::v23::services::AssetOperation;
use googleads_rs::google::ads::googleads::v23::services::CampaignAssetOperation;
use googleads_rs::google::ads::googleads::v23::services::CampaignBudgetOperation;
use googleads_rs::google::ads::googleads::v23::services::CampaignCriterionOperation;
use googleads_rs::google::ads::googleads::v23::services::CampaignOperation;
use googleads_rs::google::ads::googleads::v23::services::MutateOperation;
use googleads_rs::google::ads::googleads::v23::services::ad_group_ad_operation;
use googleads_rs::google::ads::googleads::v23::services::ad_group_criterion_operation;
use googleads_rs::google::ads::googleads::v23::services::asset_operation;
use googleads_rs::google::ads::googleads::v23::services::campaign_asset_operation;
use googleads_rs::google::ads::googleads::v23::services::campaign_budget_operation;
use googleads_rs::google::ads::googleads::v23::services::campaign_criterion_operation;
use prost_types::FieldMask;
use std::collections::HashSet;
use tracing::{debug, trace};

fn keyword_match_type(kw: &Keyword) -> i32 {
    match kw.match_type.as_str() {
        "EXACT" => KeywordMatchType::Exact as i32,
        "PHRASE" => KeywordMatchType::Phrase as i32,
        _ => KeywordMatchType::Broad as i32,
    }
}

fn micros(value: f64) -> i64 {
    (value * 1_000_000.0).round() as i64
}

fn campaign_resource(customer_id: &str, campaign_id: i64) -> String {
    format!("customers/{}/campaigns/{}", customer_id, campaign_id)
}

fn ad_group_resource(customer_id: &str, ad_group_id: i64) -> String {
    format!("customers/{}/adGroups/{}", customer_id, ad_group_id)
}

fn asset_resource(customer_id: &str, asset_id: i64) -> String {
    format!("customers/{}/assets/{}", customer_id, asset_id)
}

fn ad_text_asset(text: &AdText, index: usize, headline: bool) -> AdTextAsset {
    let pinned_field = if text.pinned {
        match (headline, index) {
            (true, 0) => ServedAssetFieldType::Headline1,
            (true, 1) => ServedAssetFieldType::Headline2,
            (true, 2) => ServedAssetFieldType::Headline3,
            (false, 0) => ServedAssetFieldType::Description1,
            (false, 1) => ServedAssetFieldType::Description2,
            _ => ServedAssetFieldType::Unspecified,
        }
    } else {
        ServedAssetFieldType::Unspecified
    };

    AdTextAsset {
        text: text.text.clone(),
        pinned_field: pinned_field as i32,
        ..Default::default()
    }
}

fn bidding_strategy(
    strategy: &BiddingStrategy,
) -> ads::resources::campaign::CampaignBiddingStrategy {
    match strategy {
        BiddingStrategy::TargetCpa { target_cpa } => {
            ads::resources::campaign::CampaignBiddingStrategy::TargetCpa(ads::common::TargetCpa {
                target_cpa_micros: micros(*target_cpa),
                ..Default::default()
            })
        }
        BiddingStrategy::TargetRoas { target_roas } => {
            ads::resources::campaign::CampaignBiddingStrategy::TargetRoas(ads::common::TargetRoas {
                target_roas: *target_roas,
                ..Default::default()
            })
        }
        BiddingStrategy::MaximizeConversions { target_cpa } => {
            ads::resources::campaign::CampaignBiddingStrategy::MaximizeConversions(
                ads::common::MaximizeConversions {
                    target_cpa_micros: target_cpa.map(micros).unwrap_or_default(),
                    ..Default::default()
                },
            )
        }
        BiddingStrategy::MaximizeConversionValue { target_roas } => {
            ads::resources::campaign::CampaignBiddingStrategy::MaximizeConversionValue(
                ads::common::MaximizeConversionValue {
                    target_roas: target_roas.unwrap_or_default(),
                    ..Default::default()
                },
            )
        }
        BiddingStrategy::ManualCpc {
            enhanced_cpc_enabled,
        } => ads::resources::campaign::CampaignBiddingStrategy::ManualCpc(ads::common::ManualCpc {
            enhanced_cpc_enabled: *enhanced_cpc_enabled,
        }),
    }
}

fn bidding_strategy_mask_path(strategy: &BiddingStrategy) -> &'static str {
    match strategy {
        BiddingStrategy::TargetCpa { .. } => "target_cpa",
        BiddingStrategy::TargetRoas { .. } => "target_roas",
        BiddingStrategy::MaximizeConversions { .. } => "maximize_conversions",
        BiddingStrategy::MaximizeConversionValue { .. } => "maximize_conversion_value",
        BiddingStrategy::ManualCpc { .. } => "manual_cpc",
    }
}

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

/// Diffs a list of ad group keywords (positive or negative) and appends removes then creates
/// so each changed keyword forms an atomic pair within the batch.
fn diff_ad_group_keywords(
    operations: &mut Vec<MutateOperation>,
    customer_id: &str,
    ag_id: i64,
    local_kws: &[Keyword],
    remote_kws: &[Keyword],
    negative: bool,
) {
    let remote_set: HashSet<String> = remote_kws.iter().map(|k| k.to_string()).collect();
    let local_set: HashSet<String> = local_kws.iter().map(|k| k.to_string()).collect();

    // Removes first.
    for kw in remote_kws {
        if !local_set.contains(&kw.to_string()) {
            if let Some(criterion_id) = kw.criterion_id {
                debug!(
                    "Removing ad group {} keyword '{}'",
                    if negative { "negative" } else { "positive" },
                    kw
                );
                let op = AdGroupCriterionOperation {
                    operation: Some(ad_group_criterion_operation::Operation::Remove(format!(
                        "customers/{}/adGroupCriteria/{}~{}",
                        customer_id, ag_id, criterion_id
                    ))),
                    ..Default::default()
                };
                operations.push(MutateOperation {
                    operation: Some(googleads_rs::google::ads::googleads::v23::services::mutate_operation::Operation::AdGroupCriterionOperation(op)),
                });
            }
        }
    }

    // Creates second.
    for kw in local_kws {
        if !remote_set.contains(&kw.to_string()) {
            debug!(
                "Adding ad group {} keyword '{}'",
                if negative { "negative" } else { "positive" },
                kw
            );
            let op = AdGroupCriterionOperation {
                operation: Some(ad_group_criterion_operation::Operation::Create(
                    AdGroupCriterion {
                        ad_group: format!("customers/{}/adGroups/{}", customer_id, ag_id),
                        negative,
                        criterion: Some(ad_group_criterion::Criterion::Keyword(KeywordInfo {
                            text: kw.text.clone(),
                            match_type: keyword_match_type(kw),
                        })),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            };
            operations.push(MutateOperation {
                operation: Some(googleads_rs::google::ads::googleads::v23::services::mutate_operation::Operation::AdGroupCriterionOperation(op)),
            });
        }
    }
}

fn diff_campaign_locations(
    operations: &mut Vec<MutateOperation>,
    customer_id: &str,
    campaign_id: i64,
    local: &Campaign,
    remote: &Campaign,
) {
    let remote_set: HashSet<String> = remote
        .locations
        .iter()
        .map(|l| l.geo_target_constant.clone())
        .collect();
    let local_set: HashSet<String> = local
        .locations
        .iter()
        .map(|l| l.geo_target_constant.clone())
        .collect();

    for location in &remote.locations {
        if !local_set.contains(&location.geo_target_constant)
            && let Some(criterion_id) = location.criterion_id
        {
            operations.push(MutateOperation {
                operation: Some(
                    ads::services::mutate_operation::Operation::CampaignCriterionOperation(
                        CampaignCriterionOperation {
                            operation: Some(campaign_criterion_operation::Operation::Remove(
                                format!(
                                    "customers/{}/campaignCriteria/{}~{}",
                                    customer_id, campaign_id, criterion_id
                                ),
                            )),
                            ..Default::default()
                        },
                    ),
                ),
            });
        }
    }

    for location in &local.locations {
        if !remote_set.contains(&location.geo_target_constant) {
            operations.push(MutateOperation {
                operation: Some(
                    ads::services::mutate_operation::Operation::CampaignCriterionOperation(
                        CampaignCriterionOperation {
                            operation: Some(campaign_criterion_operation::Operation::Create(
                                CampaignCriterion {
                                    campaign: campaign_resource(customer_id, campaign_id),
                                    criterion: Some(campaign_criterion::Criterion::Location(
                                        ads::common::LocationInfo {
                                            geo_target_constant: location
                                                .geo_target_constant
                                                .clone(),
                                        },
                                    )),
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        },
                    ),
                ),
            });
        }
    }
}

fn create_callout_asset_operation(resource_name: String, text: String) -> AssetOperation {
    AssetOperation {
        operation: Some(asset_operation::Operation::Create(Asset {
            resource_name,
            asset_data: Some(asset::AssetData::CalloutAsset(ads::common::CalloutAsset {
                callout_text: text,
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn create_sitelink_asset_operation(resource_name: String, sitelink: &Sitelink) -> AssetOperation {
    AssetOperation {
        operation: Some(asset_operation::Operation::Create(Asset {
            resource_name,
            final_urls: sitelink.final_urls.clone(),
            asset_data: Some(asset::AssetData::SitelinkAsset(
                ads::common::SitelinkAsset {
                    link_text: sitelink.link_text.clone(),
                    description1: sitelink.line1.clone().unwrap_or_default(),
                    description2: sitelink.line2.clone().unwrap_or_default(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn create_campaign_asset_link_operation(
    customer_id: &str,
    campaign_id: i64,
    asset: String,
    field_type: AssetFieldType,
) -> CampaignAssetOperation {
    CampaignAssetOperation {
        operation: Some(campaign_asset_operation::Operation::Create(CampaignAsset {
            campaign: campaign_resource(customer_id, campaign_id),
            asset,
            field_type: field_type as i32,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn diff_campaign_callouts(
    operations: &mut Vec<MutateOperation>,
    customer_id: &str,
    campaign_id: i64,
    local: &Campaign,
    remote: &Campaign,
    next_temp_asset: &mut i64,
) {
    let remote_set: HashSet<String> = remote.callouts.iter().map(|c| c.text.clone()).collect();
    let local_set: HashSet<String> = local.callouts.iter().map(|c| c.text.clone()).collect();

    for callout in &remote.callouts {
        if !local_set.contains(&callout.text)
            && let Some(asset_id) = callout.asset_id
        {
            operations.push(MutateOperation {
                operation: Some(
                    ads::services::mutate_operation::Operation::CampaignAssetOperation(
                        CampaignAssetOperation {
                            operation: Some(campaign_asset_operation::Operation::Remove(format!(
                                "customers/{}/campaignAssets/{}~{}~{}",
                                customer_id,
                                campaign_id,
                                asset_id,
                                AssetFieldType::Callout as i32
                            ))),
                            ..Default::default()
                        },
                    ),
                ),
            });
        }
    }

    for callout in &local.callouts {
        if !remote_set.contains(&callout.text) {
            let temp_resource = format!("customers/{}/assets/{}", customer_id, *next_temp_asset);
            *next_temp_asset -= 1;
            operations.push(MutateOperation {
                operation: Some(ads::services::mutate_operation::Operation::AssetOperation(
                    create_callout_asset_operation(temp_resource.clone(), callout.text.clone()),
                )),
            });
            operations.push(MutateOperation {
                operation: Some(
                    ads::services::mutate_operation::Operation::CampaignAssetOperation(
                        create_campaign_asset_link_operation(
                            customer_id,
                            campaign_id,
                            temp_resource,
                            AssetFieldType::Callout,
                        ),
                    ),
                ),
            });
        }
    }
}

fn diff_campaign_sitelinks(
    operations: &mut Vec<MutateOperation>,
    customer_id: &str,
    campaign_id: i64,
    local: &Campaign,
    remote: &Campaign,
    next_temp_asset: &mut i64,
) {
    let remote_set: HashSet<String> = remote
        .sitelinks
        .iter()
        .map(|s| s.link_text.clone())
        .collect();
    let local_set: HashSet<String> = local
        .sitelinks
        .iter()
        .map(|s| s.link_text.clone())
        .collect();

    for sitelink in &remote.sitelinks {
        if !local_set.contains(&sitelink.link_text)
            && let Some(asset_id) = sitelink.asset_id
        {
            operations.push(MutateOperation {
                operation: Some(
                    ads::services::mutate_operation::Operation::CampaignAssetOperation(
                        CampaignAssetOperation {
                            operation: Some(campaign_asset_operation::Operation::Remove(format!(
                                "customers/{}/campaignAssets/{}~{}~{}",
                                customer_id,
                                campaign_id,
                                asset_id,
                                AssetFieldType::Sitelink as i32
                            ))),
                            ..Default::default()
                        },
                    ),
                ),
            });
        }
    }

    for sitelink in &local.sitelinks {
        if !remote_set.contains(&sitelink.link_text) {
            let temp_resource = format!("customers/{}/assets/{}", customer_id, *next_temp_asset);
            *next_temp_asset -= 1;
            operations.push(MutateOperation {
                operation: Some(ads::services::mutate_operation::Operation::AssetOperation(
                    create_sitelink_asset_operation(temp_resource.clone(), sitelink),
                )),
            });
            operations.push(MutateOperation {
                operation: Some(
                    ads::services::mutate_operation::Operation::CampaignAssetOperation(
                        create_campaign_asset_link_operation(
                            customer_id,
                            campaign_id,
                            temp_resource,
                            AssetFieldType::Sitelink,
                        ),
                    ),
                ),
            });
        }
    }
}

fn rsa_ad(ad: &crate::models::schema::TextAd) -> RemoteAd {
    RemoteAd {
        final_urls: ad.final_urls.clone(),
        ad_data: Some(ad::AdData::ResponsiveSearchAd(
            ads::common::ResponsiveSearchAdInfo {
                headlines: ad
                    .headlines
                    .iter()
                    .enumerate()
                    .map(|(idx, h)| ad_text_asset(h, idx, true))
                    .collect(),
                descriptions: ad
                    .descriptions
                    .iter()
                    .enumerate()
                    .map(|(idx, d)| ad_text_asset(d, idx, false))
                    .collect(),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn add_rsa_ad_operation(ad_group: String, ad: &crate::models::schema::TextAd) -> MutateOperation {
    MutateOperation {
        operation: Some(
            ads::services::mutate_operation::Operation::AdGroupAdOperation(AdGroupAdOperation {
                operation: Some(ad_group_ad_operation::Operation::Create(RemoteAdGroupAd {
                    ad_group,
                    status: AdGroupAdStatus::Enabled as i32,
                    ad: Some(rsa_ad(ad)),
                    ..Default::default()
                })),
                ..Default::default()
            }),
        ),
    }
}

/// Translates structural differences into a set of precise gRPC update operations.
pub fn build_mutations(
    local: &Campaign,
    remote: Option<&Campaign>,
    account_id: &crate::models::account::AccountId,
) -> Vec<MutateOperation> {
    let mut operations = Vec::new();
    let clean_customer_id = account_id.unhyphenated();
    let mut next_temp_asset = -1;
    let mut next_temp_ad_group = -10_000;

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

        if local.start_date != r.start_date
            && let Some(start_date) = &local.start_date
        {
            campaign.start_date_time = start_date.clone();
            update_mask.paths.push("start_date_time".to_string());
            has_changes = true;
        }

        if local.end_date != r.end_date {
            campaign.end_date_time = local.end_date.clone().unwrap_or_default();
            update_mask.paths.push("end_date_time".to_string());
            has_changes = true;
        }

        if local.bidding_strategy != r.bidding_strategy
            && let Some(strategy) = &local.bidding_strategy
        {
            campaign.campaign_bidding_strategy = Some(bidding_strategy(strategy));
            update_mask
                .paths
                .push(bidding_strategy_mask_path(strategy).to_string());
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

        if local.daily_budget != r.daily_budget
            && let (Some(budget), Some(budget_id)) = (local.daily_budget, r.budget_id)
        {
            debug!("Campaign budget drift detected. Queuing budget update.");
            let op = CampaignBudgetOperation {
                update_mask: Some(FieldMask {
                    paths: vec!["amount_micros".to_string()],
                }),
                operation: Some(campaign_budget_operation::Operation::Update(
                    CampaignBudget {
                        resource_name: format!(
                            "customers/{}/campaignBudgets/{}",
                            clean_customer_id, budget_id
                        ),
                        amount_micros: micros(budget),
                        ..Default::default()
                    },
                )),
            };
            operations.push(MutateOperation {
                operation: Some(
                    ads::services::mutate_operation::Operation::CampaignBudgetOperation(op),
                ),
            });
        }

        diff_campaign_locations(
            &mut operations,
            &clean_customer_id,
            local.id.unwrap_or(0),
            local,
            r,
        );
        diff_campaign_callouts(
            &mut operations,
            &clean_customer_id,
            local.id.unwrap_or(0),
            local,
            r,
            &mut next_temp_asset,
        );
        diff_campaign_sitelinks(
            &mut operations,
            &clean_customer_id,
            local.id.unwrap_or(0),
            local,
            r,
            &mut next_temp_asset,
        );

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

        // Campaign negative keywords — removes before creates so the pair is atomic in the batch.
        let campaign_id = local.id.unwrap_or(0);
        let remote_neg: HashSet<String> =
            r.negative_keywords.iter().map(|k| k.to_string()).collect();
        let local_neg: HashSet<String> = local
            .negative_keywords
            .iter()
            .map(|k| k.to_string())
            .collect();

        for kw in &r.negative_keywords {
            if !local_neg.contains(&kw.to_string()) {
                if let Some(criterion_id) = kw.criterion_id {
                    debug!("Removing campaign negative keyword '{}'", kw);
                    let op = CampaignCriterionOperation {
                        operation: Some(campaign_criterion_operation::Operation::Remove(format!(
                            "customers/{}/campaignCriteria/{}~{}",
                            clean_customer_id, campaign_id, criterion_id
                        ))),
                        ..Default::default()
                    };
                    operations.push(MutateOperation {
                        operation: Some(googleads_rs::google::ads::googleads::v23::services::mutate_operation::Operation::CampaignCriterionOperation(op)),
                    });
                }
            }
        }
        for kw in &local.negative_keywords {
            if !remote_neg.contains(&kw.to_string()) {
                debug!("Adding campaign negative keyword '{}'", kw);
                let op = CampaignCriterionOperation {
                    operation: Some(campaign_criterion_operation::Operation::Create(
                        CampaignCriterion {
                            campaign: format!(
                                "customers/{}/campaigns/{}",
                                clean_customer_id, campaign_id
                            ),
                            negative: true,
                            criterion: Some(campaign_criterion::Criterion::Keyword(KeywordInfo {
                                text: kw.text.clone(),
                                match_type: keyword_match_type(kw),
                            })),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                };
                operations.push(MutateOperation {
                    operation: Some(googleads_rs::google::ads::googleads::v23::services::mutate_operation::Operation::CampaignCriterionOperation(op)),
                });
            }
        }

        // Ad group diffing — match by ID, then diff keywords and status/name.
        let remote_ag_map: std::collections::HashMap<i64, &crate::models::schema::AdGroup> = r
            .ad_groups
            .iter()
            .filter_map(|ag| ag.id.map(|id| (id, ag)))
            .collect();

        for l_ag in &local.ad_groups {
            let ag_id = match l_ag.id {
                Some(id) => id,
                None => {
                    let temp_resource = ad_group_resource(&clean_customer_id, next_temp_ad_group);
                    next_temp_ad_group -= 1;
                    debug!("Creating ad group '{}'", l_ag.name);
                    let op = AdGroupOperation {
                        operation: Some(ads::services::ad_group_operation::Operation::Create(
                            RemoteAdGroup {
                                resource_name: temp_resource.clone(),
                                campaign: campaign_resource(
                                    &clean_customer_id,
                                    local.id.unwrap_or(0),
                                ),
                                name: l_ag.name.clone(),
                                status: match l_ag.status.as_str() {
                                    "ENABLED" => AdGroupStatus::Enabled as i32,
                                    "PAUSED" => AdGroupStatus::Paused as i32,
                                    "REMOVED" => AdGroupStatus::Removed as i32,
                                    _ => AdGroupStatus::Unspecified as i32,
                                },
                                ..Default::default()
                            },
                        )),
                        ..Default::default()
                    };
                    operations.push(MutateOperation {
                        operation: Some(
                            ads::services::mutate_operation::Operation::AdGroupOperation(op),
                        ),
                    });

                    for kw in &l_ag.keywords {
                        operations.push(MutateOperation {
                            operation: Some(ads::services::mutate_operation::Operation::AdGroupCriterionOperation(
                                AdGroupCriterionOperation {
                                    operation: Some(ad_group_criterion_operation::Operation::Create(
                                        AdGroupCriterion {
                                            ad_group: temp_resource.clone(),
                                            negative: false,
                                            criterion: Some(ad_group_criterion::Criterion::Keyword(KeywordInfo {
                                                text: kw.text.clone(),
                                                match_type: keyword_match_type(kw),
                                            })),
                                            ..Default::default()
                                        },
                                    )),
                                    ..Default::default()
                                },
                            )),
                        });
                    }
                    for kw in &l_ag.negative_keywords {
                        operations.push(MutateOperation {
                            operation: Some(ads::services::mutate_operation::Operation::AdGroupCriterionOperation(
                                AdGroupCriterionOperation {
                                    operation: Some(ad_group_criterion_operation::Operation::Create(
                                        AdGroupCriterion {
                                            ad_group: temp_resource.clone(),
                                            negative: true,
                                            criterion: Some(ad_group_criterion::Criterion::Keyword(KeywordInfo {
                                                text: kw.text.clone(),
                                                match_type: keyword_match_type(kw),
                                            })),
                                            ..Default::default()
                                        },
                                    )),
                                    ..Default::default()
                                },
                            )),
                        });
                    }
                    for ad in &l_ag.ads {
                        operations.push(add_rsa_ad_operation(temp_resource.clone(), ad));
                    }
                    continue;
                }
            };
            let r_ag = match remote_ag_map.get(&ag_id) {
                Some(ag) => ag,
                None => continue,
            };

            // Ad group field updates (name, status).
            let mut ag_mask = FieldMask::default();
            let mut ag_has_changes = false;
            let mut remote_ag = RemoteAdGroup {
                resource_name: format!("customers/{}/adGroups/{}", clean_customer_id, ag_id),
                ..Default::default()
            };
            if l_ag.name != r_ag.name {
                remote_ag.name = l_ag.name.clone();
                ag_mask.paths.push("name".to_string());
                ag_has_changes = true;
            }
            if l_ag.status != r_ag.status {
                remote_ag.status = match l_ag.status.as_str() {
                    "ENABLED" => AdGroupStatus::Enabled as i32,
                    "PAUSED" => AdGroupStatus::Paused as i32,
                    "REMOVED" => AdGroupStatus::Removed as i32,
                    _ => AdGroupStatus::Unspecified as i32,
                };
                ag_mask.paths.push("status".to_string());
                ag_has_changes = true;
            }
            if ag_has_changes {
                debug!("Ad group '{}' field drift detected.", l_ag.name);
                let op = AdGroupOperation {
                    update_mask: Some(ag_mask),
                    operation: Some(googleads_rs::google::ads::googleads::v23::services::ad_group_operation::Operation::Update(remote_ag)),
                };
                operations.push(MutateOperation {
                    operation: Some(googleads_rs::google::ads::googleads::v23::services::mutate_operation::Operation::AdGroupOperation(op)),
                });
            }

            // Ad group keywords — diff positive and negative together using the same
            // removes-before-creates ordering so each changed keyword is an atomic pair.
            diff_ad_group_keywords(
                &mut operations,
                &clean_customer_id,
                ag_id,
                &l_ag.keywords,
                &r_ag.keywords,
                false,
            );
            diff_ad_group_keywords(
                &mut operations,
                &clean_customer_id,
                ag_id,
                &l_ag.negative_keywords,
                &r_ag.negative_keywords,
                true,
            );

            let remote_ads_by_id: std::collections::HashMap<i64, &crate::models::schema::TextAd> =
                r_ag.ads
                    .iter()
                    .filter_map(|ad| ad.id.map(|id| (id, ad)))
                    .collect();

            for l_ad in &l_ag.ads {
                match l_ad.id {
                    Some(ad_id) => {
                        if let Some(r_ad) = remote_ads_by_id.get(&ad_id)
                            && l_ad != *r_ad
                        {
                            debug!("Responsive search ad {} drift detected.", ad_id);
                            let mut ad = rsa_ad(l_ad);
                            ad.resource_name =
                                format!("customers/{}/ads/{}", clean_customer_id, ad_id);
                            let op = AdGroupAdOperation {
                                update_mask: Some(FieldMask {
                                    paths: vec![
                                        "ad.final_urls".to_string(),
                                        "ad.responsive_search_ad.headlines".to_string(),
                                        "ad.responsive_search_ad.descriptions".to_string(),
                                    ],
                                }),
                                operation: Some(ad_group_ad_operation::Operation::Update(
                                    RemoteAdGroupAd {
                                        resource_name: format!(
                                            "customers/{}/adGroupAds/{}~{}",
                                            clean_customer_id, ag_id, ad_id
                                        ),
                                        ad: Some(ad),
                                        ..Default::default()
                                    },
                                )),
                                ..Default::default()
                            };
                            operations.push(MutateOperation {
                                operation: Some(
                                    ads::services::mutate_operation::Operation::AdGroupAdOperation(
                                        op,
                                    ),
                                ),
                            });
                        }
                    }
                    None => operations.push(add_rsa_ad_operation(
                        ad_group_resource(&clean_customer_id, ag_id),
                        l_ad,
                    )),
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
