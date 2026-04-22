use crate::api::client::{GoogleAdsClient, ads};
use crate::engine::config::Config;
use crate::models::schema::{
    AdGroup, AdText, BiddingStrategy, Callout, Campaign, Keyword, Location, Sitelink, TextAd,
};
use ads::enums::ad_group_status_enum::AdGroupStatus;
use ads::enums::campaign_status_enum::CampaignStatus;
use ads::enums::keyword_match_type_enum::KeywordMatchType;
use ads::enums::served_asset_field_type_enum::ServedAssetFieldType;
use ads::resources as gads_resources;
use ads::services::{SearchGoogleAdsRequest, SearchGoogleAdsResponse};
use anyhow::Result;
use colored::Colorize;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum QueryType {
    Campaign,
    AdGroup,
    Keyword,
    Ad,
    CampaignSitelink,
    AdGroupSitelink,
    CampaignCallout,
    AdGroupCallout,
    CampaignNegativeKeyword,
    CampaignLocation,
}

const QUERIES: [(QueryType, &str); 10] = [
    (
        QueryType::Campaign,
        "SELECT campaign.id, campaign.name, campaign.status, campaign.start_date_time, campaign.end_date_time, campaign.bidding_strategy_type, campaign.manual_cpc.enhanced_cpc_enabled, campaign.target_cpa.target_cpa_micros, campaign.target_roas.target_roas, campaign.maximize_conversions.target_cpa_micros, campaign.maximize_conversion_value.target_roas, campaign_budget.id, campaign_budget.amount_micros FROM campaign WHERE campaign.status != 'REMOVED'",
    ),
    (
        QueryType::AdGroup,
        "SELECT campaign.id, ad_group.id, ad_group.name, ad_group.status FROM ad_group WHERE ad_group.status != 'REMOVED'",
    ),
    (
        QueryType::Keyword,
        "SELECT ad_group.id, ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, ad_group_criterion.negative FROM ad_group_criterion WHERE ad_group_criterion.type = 'KEYWORD' AND ad_group_criterion.status != 'REMOVED'",
    ),
    (
        QueryType::Ad,
        "SELECT ad_group.id, ad_group_ad.ad.id, ad_group_ad.ad.final_urls, ad_group_ad.ad.responsive_search_ad.headlines, ad_group_ad.ad.responsive_search_ad.descriptions FROM ad_group_ad WHERE ad_group_ad.status != 'REMOVED'",
    ),
    (
        QueryType::CampaignSitelink,
        "SELECT campaign.id, asset.id, asset.sitelink_asset.link_text, asset.sitelink_asset.description1, asset.sitelink_asset.description2, asset.final_urls FROM campaign_asset WHERE campaign_asset.field_type = 'SITELINK' AND campaign_asset.status != 'REMOVED'",
    ),
    (
        QueryType::AdGroupSitelink,
        "SELECT ad_group.id, asset.id, asset.sitelink_asset.link_text, asset.sitelink_asset.description1, asset.sitelink_asset.description2, asset.final_urls FROM ad_group_asset WHERE ad_group_asset.field_type = 'SITELINK' AND ad_group_asset.status != 'REMOVED'",
    ),
    (
        QueryType::CampaignCallout,
        "SELECT campaign.id, asset.id, asset.callout_asset.callout_text FROM campaign_asset WHERE campaign_asset.field_type = 'CALLOUT' AND campaign_asset.status != 'REMOVED'",
    ),
    (
        QueryType::AdGroupCallout,
        "SELECT ad_group.id, asset.id, asset.callout_asset.callout_text FROM ad_group_asset WHERE ad_group_asset.field_type = 'CALLOUT' AND ad_group_asset.status != 'REMOVED'",
    ),
    (
        QueryType::CampaignNegativeKeyword,
        "SELECT campaign.id, campaign_criterion.criterion_id, campaign_criterion.keyword.text, campaign_criterion.keyword.match_type, campaign_criterion.negative FROM campaign_criterion WHERE campaign_criterion.type = 'KEYWORD'",
    ),
    (
        QueryType::CampaignLocation,
        "SELECT campaign.id, campaign_criterion.criterion_id, campaign_criterion.location.geo_target_constant FROM campaign_criterion WHERE campaign_criterion.type = 'LOCATION' AND campaign_criterion.negative = false AND campaign_criterion.status != 'REMOVED'",
    ),
];

pub async fn fetch_remote_campaigns(
    account_id: &crate::models::account::AccountId,
    config: &Config,
) -> Result<HashMap<i64, Campaign>> {
    let ga_client = GoogleAdsClient::new(config).await?;
    let customer_id = account_id.unhyphenated();

    // Limit concurrency to avoid 429s. 5 is a safe default for most CLI use cases.
    let semaphore = Arc::new(Semaphore::new(5));

    let mut handles = Vec::with_capacity(QUERIES.len());
    for (query_type, query) in QUERIES {
        let mut client = ga_client.client.clone();
        let cid = customer_id.clone();
        let q = query.to_string();
        let permit = semaphore.clone().acquire_owned().await?;

        handles.push(tokio::spawn(async move {
            let _permit = permit; // Hold permit until query finishes
            let res = client
                .search(SearchGoogleAdsRequest {
                    customer_id: cid,
                    query: q,
                    ..Default::default()
                })
                .await?;
            Ok::<(QueryType, SearchGoogleAdsResponse), anyhow::Error>((
                query_type,
                res.into_inner(),
            ))
        }));
    }

    let mut results = HashMap::with_capacity(QUERIES.len());
    for h in handles {
        let (query_type, resp) = h.await.map_err(|e| anyhow::anyhow!(e))??;
        results.insert(query_type, resp);
    }

    assemble_campaigns(results)
}

fn assemble_campaigns(
    mut results: HashMap<QueryType, SearchGoogleAdsResponse>,
) -> Result<HashMap<i64, Campaign>> {
    let mut campaigns = HashMap::new();
    let mut ad_groups_map = HashMap::new();

    if let Some(res) = results.remove(&QueryType::Campaign) {
        process_campaigns(&mut campaigns, res);
    }
    if let Some(res) = results.remove(&QueryType::AdGroup) {
        process_ad_groups(&mut ad_groups_map, res);
    }
    if let Some(res) = results.remove(&QueryType::Keyword) {
        process_keywords(&mut ad_groups_map, res);
    }
    if let Some(res) = results.remove(&QueryType::Ad) {
        process_ads(&mut ad_groups_map, res);
    }
    if let Some(res) = results.remove(&QueryType::CampaignSitelink) {
        process_campaign_sitelinks(&mut campaigns, res);
    }
    if let Some(res) = results.remove(&QueryType::AdGroupSitelink) {
        process_ad_group_sitelinks(&mut ad_groups_map, res);
    }
    if let Some(res) = results.remove(&QueryType::CampaignCallout) {
        process_campaign_callouts(&mut campaigns, res);
    }
    if let Some(res) = results.remove(&QueryType::AdGroupCallout) {
        process_ad_group_callouts(&mut ad_groups_map, res);
    }
    if let Some(res) = results.remove(&QueryType::CampaignNegativeKeyword) {
        process_campaign_negative_keywords(&mut campaigns, res);
    }
    if let Some(res) = results.remove(&QueryType::CampaignLocation) {
        process_campaign_locations(&mut campaigns, res);
    }

    // Final Assembly
    for (camp_id, ad_group) in ad_groups_map.into_values() {
        if let Some(camp) = campaigns.get_mut(&camp_id) {
            camp.ad_groups.push(ad_group);
        }
    }

    Ok(campaigns)
}

fn process_campaigns(campaigns: &mut HashMap<i64, Campaign>, res: SearchGoogleAdsResponse) {
    for row in res.results {
        if let Some(c) = row.campaign {
            let camp_id = c.id;
            let status_str = match CampaignStatus::try_from(c.status) {
                Ok(CampaignStatus::Enabled) => "ENABLED",
                Ok(CampaignStatus::Paused) => "PAUSED",
                Ok(CampaignStatus::Removed) => "REMOVED",
                _ => "UNKNOWN",
            };

            let mut budget_id = None;
            let mut daily_budget = None;
            if let Some(cb) = row.campaign_budget {
                budget_id = Some(cb.id);
                daily_budget = Some(cb.amount_micros as f64 / 1_000_000.0);
            }

            let bidding_strategy = match c.campaign_bidding_strategy {
                Some(gads_resources::campaign::CampaignBiddingStrategy::TargetCpa(t)) => {
                    Some(BiddingStrategy::TargetCpa {
                        target_cpa: t.target_cpa_micros as f64 / 1_000_000.0,
                    })
                }
                Some(gads_resources::campaign::CampaignBiddingStrategy::TargetRoas(t)) => {
                    Some(BiddingStrategy::TargetRoas {
                        target_roas: t.target_roas,
                    })
                }
                Some(gads_resources::campaign::CampaignBiddingStrategy::MaximizeConversions(t)) => {
                    Some(BiddingStrategy::MaximizeConversions {
                        target_cpa: if t.target_cpa_micros > 0 {
                            Some(t.target_cpa_micros as f64 / 1_000_000.0)
                        } else {
                            None
                        },
                    })
                }
                Some(
                    gads_resources::campaign::CampaignBiddingStrategy::MaximizeConversionValue(t),
                ) => Some(BiddingStrategy::MaximizeConversionValue {
                    target_roas: if t.target_roas > 0.0 {
                        Some(t.target_roas)
                    } else {
                        None
                    },
                }),
                Some(gads_resources::campaign::CampaignBiddingStrategy::ManualCpc(t)) => {
                    Some(BiddingStrategy::ManualCpc {
                        enhanced_cpc_enabled: t.enhanced_cpc_enabled,
                    })
                }
                _ => None,
            };

            campaigns.insert(
                camp_id,
                Campaign {
                    id: Some(c.id),
                    name: c.name,
                    status: status_str.to_string(),
                    budget_id,
                    daily_budget,
                    bidding_strategy,
                    start_date: Some(c.start_date_time).filter(|s: &String| !s.is_empty()),
                    end_date: Some(c.end_date_time).filter(|s: &String| !s.is_empty()),
                    locations: vec![],
                    callouts: vec![],
                    sitelinks: vec![],
                    negative_keywords: vec![],
                    ad_groups: vec![],
                },
            );
        }
    }
}

fn process_ad_groups(
    ad_groups_map: &mut HashMap<i64, (i64, AdGroup)>,
    res: SearchGoogleAdsResponse,
) {
    for row in res.results {
        if let (Some(c), Some(ag)) = (row.campaign, row.ad_group) {
            let ad_group = AdGroup {
                id: Some(ag.id),
                name: ag.name,
                status: match AdGroupStatus::try_from(ag.status) {
                    Ok(AdGroupStatus::Enabled) => "ENABLED",
                    Ok(AdGroupStatus::Paused) => "PAUSED",
                    _ => "UNKNOWN",
                }
                .to_string(),
                demographics: None,
                ads: vec![],
                keywords: vec![],
                negative_keywords: vec![],
                callouts: vec![],
                sitelinks: vec![],
            };
            ad_groups_map.insert(ag.id, (c.id, ad_group));
        }
    }
}

fn process_keywords(
    ad_groups_map: &mut HashMap<i64, (i64, AdGroup)>,
    res: SearchGoogleAdsResponse,
) {
    for row in res.results {
        if let (Some(ag), Some(agc)) = (row.ad_group, row.ad_group_criterion)
            && let Some(entry) = ad_groups_map.get_mut(&ag.id)
            && let Some(gads_resources::ad_group_criterion::Criterion::Keyword(k)) = agc.criterion
        {
            let kw = Keyword {
                criterion_id: Some(agc.criterion_id),
                text: k.text,
                match_type: match KeywordMatchType::try_from(k.match_type) {
                    Ok(KeywordMatchType::Exact) => "EXACT",
                    Ok(KeywordMatchType::Phrase) => "PHRASE",
                    Ok(KeywordMatchType::Broad) => "BROAD",
                    _ => "UNKNOWN",
                }
                .to_string(),
            };
            if agc.negative {
                entry.1.negative_keywords.push(kw);
            } else {
                entry.1.keywords.push(kw);
            }
        }
    }
}

fn process_ads(ad_groups_map: &mut HashMap<i64, (i64, AdGroup)>, res: SearchGoogleAdsResponse) {
    for row in res.results {
        if let (Some(ag), Some(aga)) = (row.ad_group, row.ad_group_ad)
            && let Some(ad) = aga.ad
            && let Some(entry) = ad_groups_map.get_mut(&ag.id)
            && let Some(gads_resources::ad::AdData::ResponsiveSearchAd(rsa)) = ad.ad_data
        {
            entry.1.ads.push(TextAd {
                id: Some(ad.id),
                final_urls: ad.final_urls,
                headlines: rsa
                    .headlines
                    .into_iter()
                    .map(|h| ad_text_from_remote(h, true))
                    .collect(),
                descriptions: rsa
                    .descriptions
                    .into_iter()
                    .map(|d| ad_text_from_remote(d, false))
                    .collect(),
            });
        }
    }
}

fn ad_text_from_remote(asset: ads::common::AdTextAsset, headline: bool) -> AdText {
    let pinned = match ServedAssetFieldType::try_from(asset.pinned_field) {
        Ok(ServedAssetFieldType::Headline1)
        | Ok(ServedAssetFieldType::Headline2)
        | Ok(ServedAssetFieldType::Headline3)
        | Ok(ServedAssetFieldType::Headline)
            if headline =>
        {
            true
        }
        Ok(ServedAssetFieldType::Description1)
        | Ok(ServedAssetFieldType::Description2)
        | Ok(ServedAssetFieldType::Description)
            if !headline =>
        {
            true
        }
        _ => false,
    };

    if pinned {
        AdText::pinned(asset.text)
    } else {
        AdText::plain(asset.text)
    }
}

fn process_campaign_sitelinks(
    campaigns: &mut HashMap<i64, Campaign>,
    res: SearchGoogleAdsResponse,
) {
    for row in res.results {
        if let (Some(c), Some(asset)) = (row.campaign, row.asset)
            && let Some(camp) = campaigns.get_mut(&c.id)
            && let Some(gads_resources::asset::AssetData::SitelinkAsset(sl)) = asset.asset_data
        {
            camp.sitelinks.push(Sitelink {
                asset_id: Some(asset.id),
                link_text: sl.link_text,
                final_urls: asset.final_urls,
                line1: Some(sl.description1),
                line2: Some(sl.description2),
            });
        }
    }
}

fn process_ad_group_sitelinks(
    ad_groups_map: &mut HashMap<i64, (i64, AdGroup)>,
    res: SearchGoogleAdsResponse,
) {
    for row in res.results {
        if let (Some(ag), Some(asset)) = (row.ad_group, row.asset)
            && let Some(entry) = ad_groups_map.get_mut(&ag.id)
            && let Some(gads_resources::asset::AssetData::SitelinkAsset(sl)) = asset.asset_data
        {
            entry.1.sitelinks.push(Sitelink {
                asset_id: Some(asset.id),
                link_text: sl.link_text,
                final_urls: asset.final_urls,
                line1: Some(sl.description1),
                line2: Some(sl.description2),
            });
        }
    }
}

fn process_campaign_callouts(campaigns: &mut HashMap<i64, Campaign>, res: SearchGoogleAdsResponse) {
    for row in res.results {
        if let (Some(c), Some(asset)) = (row.campaign, row.asset)
            && let Some(camp) = campaigns.get_mut(&c.id)
            && let Some(gads_resources::asset::AssetData::CalloutAsset(co)) = asset.asset_data
        {
            camp.callouts.push(Callout {
                asset_id: Some(asset.id),
                text: co.callout_text,
            });
        }
    }
}

fn process_ad_group_callouts(
    ad_groups_map: &mut HashMap<i64, (i64, AdGroup)>,
    res: SearchGoogleAdsResponse,
) {
    for row in res.results {
        if let (Some(ag), Some(asset)) = (row.ad_group, row.asset)
            && let Some(entry) = ad_groups_map.get_mut(&ag.id)
            && let Some(gads_resources::asset::AssetData::CalloutAsset(co)) = asset.asset_data
        {
            entry.1.callouts.push(Callout {
                asset_id: Some(asset.id),
                text: co.callout_text,
            });
        }
    }
}

fn process_campaign_negative_keywords(
    campaigns: &mut HashMap<i64, Campaign>,
    res: SearchGoogleAdsResponse,
) {
    for row in res.results {
        if let (Some(c), Some(cc)) = (row.campaign, row.campaign_criterion)
            && let Some(camp) = campaigns.get_mut(&c.id)
            && let Some(gads_resources::campaign_criterion::Criterion::Keyword(k)) = cc.criterion
        {
            camp.negative_keywords.push(Keyword {
                criterion_id: Some(cc.criterion_id),
                text: k.text,
                match_type: match KeywordMatchType::try_from(k.match_type) {
                    Ok(KeywordMatchType::Exact) => "EXACT",
                    Ok(KeywordMatchType::Phrase) => "PHRASE",
                    Ok(KeywordMatchType::Broad) => "BROAD",
                    _ => "UNKNOWN",
                }
                .to_string(),
            });
        }
    }
}

fn process_campaign_locations(
    campaigns: &mut HashMap<i64, Campaign>,
    res: SearchGoogleAdsResponse,
) {
    for row in res.results {
        if let (Some(c), Some(cc)) = (row.campaign, row.campaign_criterion)
            && let Some(camp) = campaigns.get_mut(&c.id)
            && let Some(gads_resources::campaign_criterion::Criterion::Location(l)) = cc.criterion
        {
            camp.locations.push(Location {
                criterion_id: Some(cc.criterion_id),
                geo_target_constant: l.geo_target_constant,
            });
        }
    }
}

pub async fn run(account_id_str: &str, config: &Config) -> Result<()> {
    let account_id =
        crate::models::account::AccountId::new(account_id_str).map_err(|e| anyhow::anyhow!(e))?;
    let campaigns = fetch_remote_campaigns(&account_id, config).await?;

    for (camp_id, campaign) in campaigns.into_iter() {
        let filename = format!("{}_{}_campaign.yaml", account_id.hyphenated(), camp_id);
        let mut file = File::create(&filename)?;
        let yaml_string = serde_yaml::to_string(&campaign)?;
        file.write_all(yaml_string.as_bytes())?;
        println!(
            "{} Successfully exported YAML to {}",
            "✔".green(),
            filename.bright_blue()
        );
    }
    Ok(())
}
