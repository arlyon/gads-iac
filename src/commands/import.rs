use crate::api::client::GoogleAdsClient;
use crate::models::schema::{AdGroup, BiddingStrategy, Campaign};
use anyhow::Result;
use googleads_rs::google::ads::googleads::v23::resources as gads_resources;
use googleads_rs::google::ads::googleads::v23::services::SearchGoogleAdsRequest;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

pub async fn fetch_remote_campaigns(
    account_id: &crate::models::account::AccountId,
) -> Result<HashMap<i64, Campaign>> {
    println!(
        "Fetching remote state for account: {}...",
        account_id.hyphenated()
    );

    let mut ga_client = GoogleAdsClient::new().await?;
    ga_client.customer_id = account_id.unhyphenated();

    // 1. Fetch Campaigns, Budgets and core settings
    let camp_query = "SELECT campaign.id, campaign.name, campaign.status, campaign.start_date_time, \
                      campaign.end_date_time, campaign.bidding_strategy_type, \
                      campaign.target_cpa.target_cpa_micros, campaign.target_roas.target_roas, \
                      campaign_budget.id, campaign_budget.amount_micros \
                      FROM campaign WHERE campaign.status != 'REMOVED'";
    let camp_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: camp_query.to_string(),
        ..Default::default()
    };

    let mut campaigns = HashMap::new();

    match ga_client.client.search(camp_req).await {
        Ok(response) => {
            for row in response.into_inner().results {
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
                        Some(gads_resources::campaign::CampaignBiddingStrategy::TargetCpa(t)) => Some(BiddingStrategy::TargetCpa { target_cpa: t.target_cpa_micros as f64 / 1_000_000.0 }),
                        Some(gads_resources::campaign::CampaignBiddingStrategy::TargetRoas(t)) => Some(BiddingStrategy::TargetRoas { target_roas: t.target_roas }),
                        Some(gads_resources::campaign::CampaignBiddingStrategy::MaximizeConversions(_)) => Some(BiddingStrategy::MaximizeConversions { target_cpa: None }),
                        Some(gads_resources::campaign::CampaignBiddingStrategy::MaximizeConversionValue(_)) => Some(BiddingStrategy::MaximizeConversionValue { target_roas: None }),
                        Some(gads_resources::campaign::CampaignBiddingStrategy::ManualCpc(t)) => Some(BiddingStrategy::ManualCpc { enhanced_cpc_enabled: t.enhanced_cpc_enabled }),
                        _ => None,
                    };

                    campaigns.insert(
                        camp_id,
                        Campaign {
                            id: Some(c.id),
                            name: c.name.clone(),
                            status: status_str.to_string(),
                            budget_id,
                            daily_budget,
                            bidding_strategy,
                            start_date: Some(c.start_date_time.clone()).filter(|s| !s.is_empty()),
                            end_date: Some(c.end_date_time.clone()).filter(|s| !s.is_empty()),
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
        Err(e) => {
            println!("GRPC Error Status: {:?}", e);
            println!("Detailed Error: {}", String::from_utf8_lossy(e.details()));
            return Err(e.into());
        }
    }

    if campaigns.is_empty() {
        println!(
            "No campaigns found for account {}.",
            account_id.hyphenated()
        );
        return Ok(campaigns);
    }

    // 2. Fetch AdGroups
    let ag_query = "SELECT campaign.id, ad_group.id, ad_group.name, ad_group.status FROM ad_group WHERE ad_group.status != 'REMOVED'";
    let ag_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: ag_query.to_string(),
        ..Default::default()
    };

    if let Ok(response) = ga_client.client.search(ag_req).await {
        for row in response.into_inner().results {
            if let (Some(c), Some(ag)) = (row.campaign, row.ad_group)
                && let Some(campaign) = campaigns.get_mut(&c.id)
            {
                let status_str = match ag.status {
                    2 => "ENABLED",
                    3 => "PAUSED",
                    4 => "REMOVED",
                    _ => "UNKNOWN",
                };

                campaign.ad_groups.push(AdGroup {
                    id: Some(ag.id),
                    name: ag.name.clone(),
                    status: status_str.to_string(),
                    demographics: None,
                    ads: vec![],
                    sitelinks: vec![],
                    callouts: vec![],
                    keywords: vec![],
                    negative_keywords: vec![],
                });
            }
        }
    }

    // 3. Fetch Keywords
    let kw_query = "SELECT ad_group.id, ad_group_criterion.criterion_id, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, ad_group_criterion.negative FROM ad_group_criterion WHERE ad_group_criterion.type = 'KEYWORD' AND ad_group_criterion.status != 'REMOVED'";
    let kw_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: kw_query.to_string(),
        ..Default::default()
    };

    if let Ok(response) = ga_client.client.search(kw_req).await {
        for row in response.into_inner().results {
            if let (Some(ag), Some(agc)) = (row.ad_group, row.ad_group_criterion) {
                for camp in campaigns.values_mut() {
                    if let Some(ad_group) = camp.ad_groups.iter_mut().find(|a| a.id == Some(ag.id))
                        && let Some(gads_resources::ad_group_criterion::Criterion::Keyword(
                            ref kw_info,
                        )) = agc.criterion
                    {
                        let match_type_str = match kw_info.match_type {
                            2 => "EXACT",
                            3 => "PHRASE",
                            4 => "BROAD",
                            _ => "UNKNOWN",
                        };
                        let kw_obj = crate::models::schema::Keyword {
                            criterion_id: Some(agc.criterion_id),
                            text: kw_info.text.clone(),
                            match_type: match_type_str.to_string(),
                        };
                        if agc.negative {
                            ad_group.negative_keywords.push(kw_obj);
                        } else {
                            ad_group.keywords.push(kw_obj);
                        }
                    }
                }
            }
        }
    }

    // 4. Fetch Ads
    let ad_query = "SELECT ad_group.id, ad_group_ad.ad.id, ad_group_ad.ad.final_urls, ad_group_ad.ad.responsive_search_ad.headlines, ad_group_ad.ad.responsive_search_ad.descriptions FROM ad_group_ad WHERE ad_group_ad.status != 'REMOVED'";
    let ad_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: ad_query.to_string(),
        ..Default::default()
    };

    if let Ok(response) = ga_client.client.search(ad_req).await {
        for row in response.into_inner().results {
            if let (Some(ag), Some(aga)) = (row.ad_group, row.ad_group_ad) {
                for camp in campaigns.values_mut() {
                    if let Some(ad_group) = camp.ad_groups.iter_mut().find(|a| a.id == Some(ag.id))
                        && let Some(ref ad) = aga.ad
                    {
                        let mut headlines = vec![];
                        let mut descriptions = vec![];

                        if let Some(gads_resources::ad::AdData::ResponsiveSearchAd(ref rsa)) =
                            ad.ad_data
                        {
                            for hl in &rsa.headlines {
                                headlines.push(hl.text.clone());
                            }
                            for desc in &rsa.descriptions {
                                descriptions.push(desc.text.clone());
                            }
                        }

                        ad_group.ads.push(crate::models::schema::TextAd {
                            id: Some(ad.id),
                            headlines,
                            descriptions,
                            final_urls: ad.final_urls.clone(),
                        });
                    }
                }
            }
        }
    }

    // 5. Fetch Campaign Sitelinks
    let cs_query = "SELECT campaign.id, asset.id, asset.sitelink_asset.link_text, asset.sitelink_asset.description1, asset.sitelink_asset.description2, asset.final_urls FROM campaign_asset WHERE campaign_asset.field_type = 'SITELINK' AND campaign_asset.status != 'REMOVED'";
    let cs_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: cs_query.to_string(),
        ..Default::default()
    };

    if let Ok(response) = ga_client.client.search(cs_req).await {
        for row in response.into_inner().results {
            if let (Some(c), Some(asset)) = (row.campaign, row.asset)
                && let Some(camp) = campaigns.get_mut(&c.id)
                && let Some(gads_resources::asset::AssetData::SitelinkAsset(ref sl)) =
                    asset.asset_data
            {
                camp.sitelinks.push(crate::models::schema::Sitelink {
                    asset_id: Some(asset.id),
                    link_text: sl.link_text.clone(),
                    final_urls: asset.final_urls.clone(),
                    line1: if sl.description1.is_empty() {
                        None
                    } else {
                        Some(sl.description1.clone())
                    },
                    line2: if sl.description2.is_empty() {
                        None
                    } else {
                        Some(sl.description2.clone())
                    },
                });
            }
        }
    }

    // 6. Fetch AdGroup Sitelinks
    let ags_query = "SELECT ad_group.id, asset.id, asset.sitelink_asset.link_text, asset.sitelink_asset.description1, asset.sitelink_asset.description2, asset.final_urls FROM ad_group_asset WHERE ad_group_asset.field_type = 'SITELINK' AND ad_group_asset.status != 'REMOVED'";
    let ags_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: ags_query.to_string(),
        ..Default::default()
    };

    if let Ok(response) = ga_client.client.search(ags_req).await {
        for row in response.into_inner().results {
            if let (Some(ag), Some(asset)) = (row.ad_group, row.asset) {
                for camp in campaigns.values_mut() {
                    if let Some(ad_group) = camp.ad_groups.iter_mut().find(|a| a.id == Some(ag.id))
                        && let Some(gads_resources::asset::AssetData::SitelinkAsset(ref sl)) =
                            asset.asset_data
                    {
                        ad_group.sitelinks.push(crate::models::schema::Sitelink {
                            asset_id: Some(asset.id),
                            link_text: sl.link_text.clone(),
                            final_urls: asset.final_urls.clone(),
                            line1: if sl.description1.is_empty() {
                                None
                            } else {
                                Some(sl.description1.clone())
                            },
                            line2: if sl.description2.is_empty() {
                                None
                            } else {
                                Some(sl.description2.clone())
                            },
                        });
                    }
                }
            }
        }
    }

    // 7. Fetch Campaign Callouts
    let cc_query = "SELECT campaign.id, asset.id, asset.callout_asset.callout_text FROM campaign_asset WHERE campaign_asset.field_type = 'CALLOUT' AND campaign_asset.status != 'REMOVED'";
    let cc_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: cc_query.to_string(),
        ..Default::default()
    };

    if let Ok(response) = ga_client.client.search(cc_req).await {
        for row in response.into_inner().results {
            if let (Some(c), Some(asset)) = (row.campaign, row.asset)
                && let Some(camp) = campaigns.get_mut(&c.id)
                && let Some(gads_resources::asset::AssetData::CalloutAsset(ref co)) =
                    asset.asset_data
            {
                camp.callouts.push(crate::models::schema::Callout {
                    asset_id: Some(asset.id),
                    text: co.callout_text.clone(),
                });
            }
        }
    }

    // 8. Fetch AdGroup Callouts
    let agc_query = "SELECT ad_group.id, asset.id, asset.callout_asset.callout_text FROM ad_group_asset WHERE ad_group_asset.field_type = 'CALLOUT' AND ad_group_asset.status != 'REMOVED'";
    let agc_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: agc_query.to_string(),
        ..Default::default()
    };

    if let Ok(response) = ga_client.client.search(agc_req).await {
        for row in response.into_inner().results {
            if let (Some(ag), Some(asset)) = (row.ad_group, row.asset) {
                for camp in campaigns.values_mut() {
                    if let Some(ad_group) = camp.ad_groups.iter_mut().find(|a| a.id == Some(ag.id))
                        && let Some(gads_resources::asset::AssetData::CalloutAsset(ref co)) =
                            asset.asset_data
                    {
                        ad_group.callouts.push(crate::models::schema::Callout {
                            asset_id: Some(asset.id),
                            text: co.callout_text.clone(),
                        });
                    }
                }
            }
        }
    }

    // 9. Fetch Campaign Negative Keywords
    let ckw_query = "SELECT campaign.id, campaign_criterion.criterion_id, campaign_criterion.keyword.text, campaign_criterion.keyword.match_type, campaign_criterion.negative FROM campaign_criterion WHERE campaign_criterion.type = 'KEYWORD'";
    let ckw_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: ckw_query.to_string(),
        ..Default::default()
    };

    if let Ok(response) = ga_client.client.search(ckw_req).await {
        for row in response.into_inner().results {
            if let (Some(c), Some(cc)) = (row.campaign, row.campaign_criterion)
                && let Some(camp) = campaigns.get_mut(&c.id)
                && let Some(gads_resources::campaign_criterion::Criterion::Keyword(ref kw_info)) =
                    cc.criterion
            {
                let match_type_str = match kw_info.match_type {
                    2 => "EXACT",
                    3 => "PHRASE",
                    4 => "BROAD",
                    _ => "UNKNOWN",
                };
                let kw_obj = crate::models::schema::Keyword {
                    criterion_id: Some(cc.criterion_id),
                    text: kw_info.text.clone(),
                    match_type: match_type_str.to_string(),
                };
                if cc.negative {
                    camp.negative_keywords.push(kw_obj);
                }
            }
        }
    }

    Ok(campaigns)
}

pub async fn run(account_id_str: &str) -> Result<()> {
    let account_id =
        crate::models::account::AccountId::new(account_id_str).map_err(|e| anyhow::anyhow!(e))?;
    let campaigns = fetch_remote_campaigns(&account_id).await?;

    // Export to YAML
    for (camp_id, campaign) in campaigns.into_iter() {
        let filename = format!("{}_{}_campaign.yaml", account_id.hyphenated(), camp_id);
        let mut file = File::create(&filename)?;

        let yaml_string = serde_yaml::to_string(&campaign)?;
        file.write_all(yaml_string.as_bytes())?;

        println!("Successfully exported YAML to {}", filename);
    }

    Ok(())
}
