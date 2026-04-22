use crate::api::client::{GoogleAdsClient, ads};
use crate::commands::import::fetch_remote_campaigns;
use crate::commands::{LocalCampaign, load_local_campaigns_by_account, print_diff_lines};
use crate::engine::config::Config;
use crate::engine::diff::compute_diff;
use crate::engine::errors::OperationSourceContext;
use colored::Colorize;
use googleads_rs::google::ads::googleads::v23::resources::{ad, asset};
use googleads_rs::google::ads::googleads::v23::services::{
    ad_group_ad_operation, ad_group_operation, asset_operation, campaign_asset_operation,
    campaign_operation, mutate_operation,
};
use miette::{IntoDiagnostic, SourceSpan};
use std::collections::HashMap;
use std::io::{self, Write};
use tracing::{debug, trace};

pub async fn run(config: &Config) -> miette::Result<()> {
    println!("{}", "Loading local YAML files...".blue());

    let campaigns_by_account =
        load_local_campaigns_by_account().map_err(|error| miette::miette!("{error:?}"))?;

    if campaigns_by_account.is_empty() {
        println!(
            "{}",
            "No local YAML files found. Try running `import` first.".yellow()
        );
        return Ok(());
    }

    for (account_id_str, mut local_campaigns) in campaigns_by_account {
        let account_id = crate::models::account::AccountId::new(&account_id_str)
            .map_err(|e| miette::miette!("{}", e))?;

        println!(
            "Found {} local campaigns for account {}.",
            local_campaigns.len().to_string().green(),
            account_id.hyphenated().cyan()
        );
        debug!("Total campaigns loaded: {}", local_campaigns.len());

        println!("{}", "Fetching remote state...".blue());
        let mut remote_map = fetch_remote_campaigns(&account_id, config)
            .await
            .map_err(|error| miette::miette!("{error:?}"))?;

        let mut clean = true;

        for local in &mut local_campaigns {
            local.normalize();
            if let Some(camp_id) = local.id {
                if let Some(remote) = remote_map.get_mut(&camp_id) {
                    remote.normalize();
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
                "No drift detected. Local state matches remote. Nothing to apply.".green()
            );
            continue;
        }

        // Check for CI environment
        if std::env::var("CI").is_ok() {
            eprintln!(
                "{}",
                "ERROR: CI environment detected and drift was found. Aborting to prevent un-interactive overwrites.".red().bold()
            );
            std::process::exit(1);
        }

        let mut operations = Vec::new();
        let mut operation_sources = Vec::new();
        for local in &local_campaigns {
            let remote_opt = local.id.and_then(|id| remote_map.get(&id));
            debug!("Processing diffs for campaign: {}", local.name);
            let mut ops = crate::engine::diff::build_mutations(local, remote_opt, &account_id);
            operation_sources.extend(source_contexts_for_operations(local, &ops));
            trace!("Generated {} mutations for campaign", ops.len());
            operations.append(&mut ops);
        }

        if operations.is_empty() {
            println!(
                "{}",
                "No structural mutations could be built for the detected drift with the currently mapped resource types.".yellow()
            );
            continue;
        }

        print!(
            "Do you want to apply these {} mapped mutation(s) to account {} in the live API? (y/N): ",
            operations.len().to_string().yellow(),
            account_id.hyphenated().cyan()
        );
        io::stdout().flush().into_diagnostic()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input).into_diagnostic()?;

        if input.trim().to_lowercase() != "y" {
            println!("Apply aborted by user.");
            continue;
        }

        println!("\n{}", "Applying changes to remote state...".cyan().bold());
        println!(
            "Executing {} mutation(s)...",
            operations.len().to_string().cyan()
        );

        let mut client = GoogleAdsClient::new(config)
            .await
            .map_err(|error| miette::miette!("{error:?}"))?;

        let request = ads::services::MutateGoogleAdsRequest {
            customer_id: account_id.unhyphenated(),
            mutate_operations: operations.clone(),
            partial_failure: true,
            validate_only: false,
            response_content_type: 0,
        };

        let response = client.client.mutate(request).await.map_err(|source| {
            crate::engine::errors::GoogleAdsDispatchDiagnostic {
                account_id: account_id.hyphenated(),
                source,
            }
        })?;

        let inner = response.into_inner();

        let mut aggregator = crate::engine::errors::ErrorAggregator::new();
        if let Some(status) = inner.partial_failure_error {
            println!("{}", "Partial Failures Detected!".red().bold());
            aggregator.parse_partial_failures(&status.details);
            if aggregator.errors.is_empty() {
                aggregator
                    .errors
                    .push(crate::engine::errors::GoogleAdsFailureItem {
                        message: if status.message.is_empty() {
                            "Google Ads reported partial failures, but no decodable failure details were returned.".to_string()
                        } else {
                            status.message
                        },
                        location: None,
                        operation_index: None,
                    });
            }
        }

        let total = operations.len();
        let failures = aggregator.failed_operation_count(total);
        let successes = total.saturating_sub(failures);

        println!(
            "\n{} Apply Summary: {} attempted, {} succeeded, {} failed.",
            "i".blue().bold(),
            total,
            successes.to_string().green(),
            failures.to_string().red()
        );

        if !aggregator.errors.is_empty() {
            return Err(aggregator
                .into_diagnostic(account_id.hyphenated(), total, &operation_sources)
                .into());
        }
    }

    Ok(())
}

fn source_contexts_for_operations(
    local: &LocalCampaign,
    operations: &[ads::services::MutateOperation],
) -> Vec<OperationSourceContext> {
    let source_name = local.source_path.to_string_lossy().to_string();
    let source = local.source.clone();
    let mut temp_asset_contexts = HashMap::new();

    operations
        .iter()
        .map(|operation| {
            let mut context = source_context_for_operation(&source_name, &source, operation);

            match operation.operation.as_ref() {
                Some(mutate_operation::Operation::AssetOperation(asset_op)) => {
                    if let Some(asset_operation::Operation::Create(asset)) =
                        asset_op.operation.as_ref()
                    {
                        if asset.resource_name.contains("/assets/-") {
                            temp_asset_contexts
                                .insert(asset.resource_name.clone(), context.clone());
                        }
                    }
                }
                Some(mutate_operation::Operation::CampaignAssetOperation(campaign_asset_op)) => {
                    if let Some(campaign_asset_operation::Operation::Create(campaign_asset)) =
                        campaign_asset_op.operation.as_ref()
                    {
                        if let Some(asset_context) = temp_asset_contexts.get(&campaign_asset.asset)
                        {
                            context = asset_context.clone();
                            context.label =
                                "this asset link depends on the asset value highlighted here"
                                    .to_string();
                        }
                    }
                }
                _ => {}
            }

            context
        })
        .collect()
}

fn source_context_for_operation(
    source_name: &str,
    source: &str,
    operation: &ads::services::MutateOperation,
) -> OperationSourceContext {
    let (span, label) = match operation.operation.as_ref() {
        Some(mutate_operation::Operation::CampaignOperation(op)) => {
            source_context_for_campaign_operation(source, op)
        }
        Some(mutate_operation::Operation::AssetOperation(op)) => {
            source_context_for_asset_operation(source, op)
        }
        Some(mutate_operation::Operation::AdGroupOperation(op)) => {
            source_context_for_ad_group_operation(source, op)
        }
        Some(mutate_operation::Operation::AdGroupAdOperation(op)) => {
            source_context_for_ad_group_ad_operation(source, op)
        }
        _ => (None, "operation generated from this YAML file".to_string()),
    };

    OperationSourceContext {
        source_name: source_name.to_string(),
        source: source.to_string(),
        span,
        label,
    }
}

fn source_context_for_campaign_operation(
    source: &str,
    operation: &ads::services::CampaignOperation,
) -> (Option<SourceSpan>, String) {
    match operation.operation.as_ref() {
        Some(campaign_operation::Operation::Update(_)) => {
            let span = line_span_containing(source, "bidding_strategy:")
                .or_else(|| line_span_containing(source, "type: ManualCpc"))
                .or_else(|| line_span_containing(source, "id:"))
                .or_else(|| whole_file_span(source));
            (
                span,
                "campaign field update came from this campaign YAML".to_string(),
            )
        }
        _ => (
            line_span_containing(source, "name:").or_else(|| whole_file_span(source)),
            "campaign create came from this campaign YAML".to_string(),
        ),
    }
}

fn source_context_for_asset_operation(
    source: &str,
    operation: &ads::services::AssetOperation,
) -> (Option<SourceSpan>, String) {
    let asset = match operation.operation.as_ref() {
        Some(asset_operation::Operation::Create(asset))
        | Some(asset_operation::Operation::Update(asset)) => asset,
        _ => {
            return (
                whole_file_span(source),
                "asset operation generated from this campaign YAML".to_string(),
            );
        }
    };

    match asset.asset_data.as_ref() {
        Some(asset::AssetData::CalloutAsset(callout)) => (
            scalar_value_span(source, "text", &callout.callout_text)
                .or_else(|| text_span(source, &callout.callout_text)),
            "callout text sent to Google Ads".to_string(),
        ),
        Some(asset::AssetData::SitelinkAsset(sitelink)) => {
            let span = if !sitelink.description2.is_empty() {
                scalar_value_span(source, "line2", &sitelink.description2)
                    .or_else(|| text_span(source, &sitelink.description2))
            } else if !sitelink.description1.is_empty() {
                scalar_value_span(source, "line1", &sitelink.description1)
                    .or_else(|| text_span(source, &sitelink.description1))
            } else {
                scalar_value_span(source, "link_text", &sitelink.link_text)
                    .or_else(|| text_span(source, &sitelink.link_text))
            };
            (span, "sitelink value sent to Google Ads".to_string())
        }
        _ => (
            whole_file_span(source),
            "asset operation generated from this campaign YAML".to_string(),
        ),
    }
}

fn source_context_for_ad_group_operation(
    source: &str,
    operation: &ads::services::AdGroupOperation,
) -> (Option<SourceSpan>, String) {
    let ad_group = match operation.operation.as_ref() {
        Some(ad_group_operation::Operation::Create(ad_group))
        | Some(ad_group_operation::Operation::Update(ad_group)) => ad_group,
        _ => {
            return (
                whole_file_span(source),
                "ad group operation generated from this campaign YAML".to_string(),
            );
        }
    };

    (
        scalar_value_span(source, "name", &ad_group.name)
            .or_else(|| text_span(source, &ad_group.name)),
        "ad group name sent to Google Ads".to_string(),
    )
}

fn source_context_for_ad_group_ad_operation(
    source: &str,
    operation: &ads::services::AdGroupAdOperation,
) -> (Option<SourceSpan>, String) {
    let ad_group_ad = match operation.operation.as_ref() {
        Some(ad_group_ad_operation::Operation::Create(ad_group_ad))
        | Some(ad_group_ad_operation::Operation::Update(ad_group_ad)) => ad_group_ad,
        _ => {
            return (
                whole_file_span(source),
                "ad operation generated from this campaign YAML".to_string(),
            );
        }
    };

    let span = ad_group_ad
        .ad
        .as_ref()
        .and_then(|ad| ad.ad_data.as_ref())
        .and_then(|ad_data| match ad_data {
            ad::AdData::ResponsiveSearchAd(rsa) => rsa
                .descriptions
                .iter()
                .map(|description| &description.text)
                .find_map(|text| text_span(source, text)),
            _ => None,
        })
        .or_else(|| whole_file_span(source));

    (
        span,
        "responsive search ad text sent to Google Ads".to_string(),
    )
}

fn text_span(source: &str, needle: &str) -> Option<SourceSpan> {
    if needle.is_empty() {
        return None;
    }

    source
        .find(needle)
        .map(|offset| (offset, needle.len()).into())
}

fn scalar_value_span(source: &str, key: &str, value: &str) -> Option<SourceSpan> {
    if value.is_empty() {
        return None;
    }

    let key_prefix = format!("{key}:");
    source
        .lines()
        .scan(0, |offset, line| {
            let line_offset = *offset;
            *offset += line.len() + 1;
            Some((line_offset, line))
        })
        .find_map(|(line_offset, line)| {
            let trimmed = line.trim_start();
            if !trimmed.starts_with(&key_prefix) || !line.contains(value) {
                return None;
            }

            let value_offset = line.find(value)?;
            Some((line_offset + value_offset, value.len()).into())
        })
}

fn line_span_containing(source: &str, needle: &str) -> Option<SourceSpan> {
    source
        .lines()
        .scan(0, |offset, line| {
            let line_offset = *offset;
            *offset += line.len() + 1;
            Some((line_offset, line))
        })
        .find_map(|(line_offset, line)| {
            line.contains(needle)
                .then(|| (line_offset, line.len().max(1)).into())
        })
}

fn whole_file_span(source: &str) -> Option<SourceSpan> {
    (!source.is_empty())
        .then(|| (0, source.lines().next().map(str::len).unwrap_or(1).max(1)).into())
}
