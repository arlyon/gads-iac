use crate::api::client::GoogleAdsClient;
use crate::models::schema::{
    AdGroup, BiddingStrategy, Callout, Campaign, Keyword, Sitelink, TextAd,
};
use anyhow::Result;
use googleads_rs::google::ads::googleads::v23::resources as gads_resources;
use googleads_rs::google::ads::googleads::v23::services::{
    SearchGoogleAdsRequest, SearchGoogleAdsResponse,
};
use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::pin::Pin;

type SearchResults = (
    SearchGoogleAdsResponse,
    SearchGoogleAdsResponse,
    SearchGoogleAdsResponse,
    SearchGoogleAdsResponse,
    SearchGoogleAdsResponse,
    SearchGoogleAdsResponse,
    SearchGoogleAdsResponse,
    SearchGoogleAdsResponse,
    SearchGoogleAdsResponse,
);

pub fn fetch_remote_campaigns(
    account_id: &crate::models::account::AccountId,
) -> Pin<Box<dyn Future<Output = Result<HashMap<i64, Campaign>>> + Send + '_>> {
    let account_id = account_id.clone();
    Box::pin(async move {
        let ga_client = GoogleAdsClient::new().await?;
        let customer_id = account_id.unhyphenated();

        let queries = [
            "SELECT campaign.id, campaign.name, campaign.status, campaign.start_date_time, campaign.end_date_time, campaign.bidding_strategy_type, campaign.target_cpa.target_cpa_micros, campaign.target_roas.target_roas, campaign_budget.id, campaign_budget.amount_micros FROM campaign WHERE campaign.status != 'REMOVED'",
            "SELECT campaign.id, ad_group.id, ad_group.name, ad_group.status FROM ad_group WHERE ad_group.status != 'REMOVED'",
            "SELECT ad_group.id, ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, ad_group_criterion.negative FROM ad_group_criterion WHERE ad_group_criterion.type = 'KEYWORD' AND ad_group_criterion.status != 'REMOVED'",
            "SELECT ad_group.id, ad_group_ad.ad.id, ad_group_ad.ad.final_urls, ad_group_ad.ad.responsive_search_ad.headlines, ad_group_ad.ad.responsive_search_ad.descriptions FROM ad_group_ad WHERE ad_group_ad.status != 'REMOVED'",
            "SELECT campaign.id, asset.id, asset.sitelink_asset.link_text, asset.sitelink_asset.description1, asset.sitelink_asset.description2, asset.final_urls FROM campaign_asset WHERE campaign_asset.field_type = 'SITELINK' AND campaign_asset.status != 'REMOVED'",
            "SELECT ad_group.id, asset.id, asset.sitelink_asset.link_text, asset.sitelink_asset.description1, asset.sitelink_asset.description2, asset.final_urls FROM ad_group_asset WHERE ad_group_asset.field_type = 'SITELINK' AND ad_group_asset.status != 'REMOVED'",
            "SELECT campaign.id, asset.id, asset.callout_asset.callout_text FROM campaign_asset WHERE campaign_asset.field_type = 'CALLOUT' AND campaign_asset.status != 'REMOVED'",
            "SELECT ad_group.id, asset.id, asset.callout_asset.callout_text FROM ad_group_asset WHERE ad_group_asset.field_type = 'CALLOUT' AND ad_group_asset.status != 'REMOVED'",
            "SELECT campaign.id, campaign_criterion.criterion_id, campaign_criterion.keyword.text, campaign_criterion.keyword.match_type, campaign_criterion.negative FROM campaign_criterion WHERE campaign_criterion.type = 'KEYWORD'",
        ];

        let mut handles = Vec::new();
        for query in queries {
            let mut client = ga_client.client.clone();
            let cid = customer_id.clone();
            let q = query.to_string();
            handles.push(tokio::spawn(async move {
                client
                    .search(SearchGoogleAdsRequest {
                        customer_id: cid,
                        query: q,
                        ..Default::default()
                    })
                    .await
            }));
        }

        let mut results = Vec::new();
        for h in handles {
            results.push(
                h.await
                    .map_err(|e| anyhow::anyhow!(e))?
                    .map_err(|e| anyhow::anyhow!(e))?
                    .into_inner(),
            );
        }

        // Drop huge response objects as we extract what we need in a separate sync step
        // to keep the async state machine small.
        assemble_campaigns(results)
    })
}

fn assemble_campaigns(results: Vec<SearchGoogleAdsResponse>) -> Result<HashMap<i64, Campaign>> {
    if results.len() != 9 {
        return Err(anyhow::anyhow!("Expected 9 query results"));
    }
    let mut results = results.into_iter();

    let c_res = results.next().unwrap();
    let ag_res = results.next().unwrap();
    let kw_res = results.next().unwrap();
    let ad_res = results.next().unwrap();
    let cs_res = results.next().unwrap();
    let ags_res = results.next().unwrap();
    let cc_res = results.next().unwrap();
    let agc_res = results.next().unwrap();
    let ckw_res = results.next().unwrap();

    let mut campaigns = HashMap::new();

    // 1. Process Campaigns
    for row in c_res.results {
        if let Some(c) = row.campaign {
            let camp_id = c.id;
            let status_str = match c.status {
                2 => "ENABLED",
                3 => "PAUSED",
                4 => "REMOVED",
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
                Some(gads_resources::campaign::CampaignBiddingStrategy::MaximizeConversions(_)) => {
                    Some(BiddingStrategy::MaximizeConversions { target_cpa: None })
                }
                Some(
                    gads_resources::campaign::CampaignBiddingStrategy::MaximizeConversionValue(_),
                ) => Some(BiddingStrategy::MaximizeConversionValue { target_roas: None }),
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

    // 2. Index AdGroups
    let mut ad_groups_map = HashMap::new();
    for row in ag_res.results {
        if let (Some(c), Some(ag)) = (row.campaign, row.ad_group) {
            let ad_group = AdGroup {
                id: Some(ag.id),
                name: ag.name,
                status: match ag.status {
                    2 => "ENABLED",
                    3 => "PAUSED",
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

    // 3. Process Keywords
    for row in kw_res.results {
        if let (Some(ag), Some(agc)) = (row.ad_group, row.ad_group_criterion) {
            if let Some(entry) = ad_groups_map.get_mut(&ag.id) {
                if let Some(gads_resources::ad_group_criterion::Criterion::Keyword(k)) =
                    agc.criterion
                {
                    let kw = Keyword {
                        criterion_id: Some(agc.criterion_id),
                        text: k.text,
                        match_type: match k.match_type {
                            2 => "EXACT",
                            3 => "PHRASE",
                            4 => "BROAD",
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
    }

    // 4. Process Ads
    for row in ad_res.results {
        if let (Some(ag), Some(aga)) = (row.ad_group, row.ad_group_ad) {
            if let Some(ad) = aga.ad {
                if let Some(entry) = ad_groups_map.get_mut(&ag.id) {
                    if let Some(gads_resources::ad::AdData::ResponsiveSearchAd(rsa)) = ad.ad_data {
                        entry.1.ads.push(TextAd {
                            id: Some(ad.id),
                            final_urls: ad.final_urls,
                            headlines: rsa.headlines.into_iter().map(|h| h.text).collect(),
                            descriptions: rsa.descriptions.into_iter().map(|d| d.text).collect(),
                        });
                    }
                }
            }
        }
    }

    // 5. Process Campaign Sitelinks
    for row in cs_res.results {
        if let (Some(c), Some(asset)) = (row.campaign, row.asset) {
            if let Some(camp) = campaigns.get_mut(&c.id) {
                if let Some(gads_resources::asset::AssetData::SitelinkAsset(sl)) = asset.asset_data
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
    }

    // 6. Process AdGroup Sitelinks
    for row in ags_res.results {
        if let (Some(ag), Some(asset)) = (row.ad_group, row.asset) {
            if let Some(entry) = ad_groups_map.get_mut(&ag.id) {
                if let Some(gads_resources::asset::AssetData::SitelinkAsset(sl)) = asset.asset_data
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
    }

    // 7. Process Campaign Callouts
    for row in cc_res.results {
        if let (Some(c), Some(asset)) = (row.campaign, row.asset) {
            if let Some(camp) = campaigns.get_mut(&c.id) {
                if let Some(gads_resources::asset::AssetData::CalloutAsset(co)) = asset.asset_data {
                    camp.callouts.push(Callout {
                        asset_id: Some(asset.id),
                        text: co.callout_text,
                    });
                }
            }
        }
    }

    // 8. Process AdGroup Callouts
    for row in agc_res.results {
        if let (Some(ag), Some(asset)) = (row.ad_group, row.asset) {
            if let Some(entry) = ad_groups_map.get_mut(&ag.id) {
                if let Some(gads_resources::asset::AssetData::CalloutAsset(co)) = asset.asset_data {
                    entry.1.callouts.push(Callout {
                        asset_id: Some(asset.id),
                        text: co.callout_text,
                    });
                }
            }
        }
    }

    // 9. Process Campaign Negative Keywords
    for row in ckw_res.results {
        if let (Some(c), Some(cc)) = (row.campaign, row.campaign_criterion) {
            if let Some(camp) = campaigns.get_mut(&c.id) {
                if let Some(gads_resources::campaign_criterion::Criterion::Keyword(k)) =
                    cc.criterion
                {
                    camp.negative_keywords.push(Keyword {
                        criterion_id: Some(cc.criterion_id),
                        text: k.text,
                        match_type: match k.match_type {
                            2 => "EXACT",
                            3 => "PHRASE",
                            4 => "BROAD",
                            _ => "UNKNOWN",
                        }
                        .to_string(),
                    });
                }
            }
        }
    }

    // Final Assembly
    for (camp_id, ad_group) in ad_groups_map.into_values() {
        if let Some(camp) = campaigns.get_mut(&camp_id) {
            camp.ad_groups.push(ad_group);
        }
    }

    Ok(campaigns)
}

pub async fn run(account_id_str: &str) -> Result<()> {
    let account_id =
        crate::models::account::AccountId::new(account_id_str).map_err(|e| anyhow::anyhow!(e))?;
    let campaigns = fetch_remote_campaigns(&account_id).await?;

    for (camp_id, campaign) in campaigns.into_iter() {
        let filename = format!("{}_{}_campaign.yaml", account_id.hyphenated(), camp_id);
        let mut file = File::create(&filename)?;
        let yaml_string = serde_yaml::to_string(&campaign)?;
        file.write_all(yaml_string.as_bytes())?;
        println!("Successfully exported YAML to {}", filename);
    }
    Ok(())
}
