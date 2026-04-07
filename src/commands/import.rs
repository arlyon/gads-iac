use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use crate::api::client::GoogleAdsClient;
use crate::models::schema::{Campaign, AdGroup};
use googleads_rs::google::ads::googleads::v23::services::SearchGoogleAdsRequest;

pub async fn fetch_remote_campaigns(account_id: &crate::models::account::AccountId) -> Result<HashMap<i64, Campaign>> {
    println!("Fetching remote state for account: {}...", account_id.hyphenated());
    
    let mut ga_client = GoogleAdsClient::new().await?;
    ga_client.customer_id = account_id.unhyphenated(); 
    
    // 1. Fetch Campaigns
    let camp_query = "SELECT campaign.id, campaign.name, campaign.status FROM campaign WHERE campaign.status != 'REMOVED'";
    let mut camp_req = SearchGoogleAdsRequest {
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

                    campaigns.insert(camp_id, Campaign {
                        id: Some(c.id),
                        name: c.name,
                        status: status_str.to_string(),
                        negative_keywords: vec![],
                        ad_groups: vec![],
                        sitelinks: vec![],
                    });
                }
            }
        },
        Err(e) => {
            println!("GRPC Error Status: {:?}", e);
            println!("Detailed Error: {}", String::from_utf8_lossy(e.details()));
            return Err(e.into());
        }
    }
    
    if campaigns.is_empty() {
        println!("No campaigns found for account {}.", account_id.hyphenated());
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
            if let (Some(c), Some(ag)) = (row.campaign, row.ad_group) {
                if let Some(campaign) = campaigns.get_mut(&c.id) {
                    let status_str = match ag.status {
                        2 => "ENABLED",
                        3 => "PAUSED",
                        4 => "REMOVED",
                        _ => "UNKNOWN",
                    };
                    
                    campaign.ad_groups.push(AdGroup {
                        id: Some(ag.id),
                        name: ag.name,
                        status: status_str.to_string(),
                        ads: vec![],
                        keywords: vec![],
                        negative_keywords: vec![],
                        sitelinks: vec![],
                    });
                }
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
    
    use googleads_rs::google::ads::googleads::v23::resources::ad_group_criterion::Criterion;
    use googleads_rs::google::ads::googleads::v23::resources::ad::AdData;

    if let Ok(response) = ga_client.client.search(kw_req).await {
        for row in response.into_inner().results {
            if let (Some(ag), Some(agc)) = (row.ad_group, row.ad_group_criterion) {
                for camp in campaigns.values_mut() {
                    if let Some(ad_group) = camp.ad_groups.iter_mut().find(|a| a.id == Some(ag.id)) {
                        if let Some(Criterion::Keyword(ref kw_info)) = agc.criterion {
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
                    if let Some(ad_group) = camp.ad_groups.iter_mut().find(|a| a.id == Some(ag.id)) {
                        if let Some(ref ad) = aga.ad {
                            let mut headlines = vec![];
                            let mut descriptions = vec![];
                            
                            if let Some(AdData::ResponsiveSearchAd(ref rsa)) = ad.ad_data {
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
    }

    // 5. Fetch Campaign Sitelinks
    use googleads_rs::google::ads::googleads::v23::resources::asset::AssetData;
    
    let cs_query = "SELECT campaign.id, asset.id, asset.sitelink_asset.link_text, asset.sitelink_asset.description1, asset.sitelink_asset.description2, asset.final_urls FROM campaign_asset WHERE campaign_asset.field_type = 'SITELINK' AND campaign_asset.status != 'REMOVED'";
    let cs_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: cs_query.to_string(),
        ..Default::default()
    };
    
    if let Ok(response) = ga_client.client.search(cs_req).await {
        for row in response.into_inner().results {
            if let (Some(c), Some(asset)) = (row.campaign, row.asset) {
                if let Some(camp) = campaigns.get_mut(&c.id) {
                    if let Some(AssetData::SitelinkAsset(ref sl)) = asset.asset_data {
                        camp.sitelinks.push(crate::models::schema::Sitelink {
                            asset_id: Some(asset.id),
                            link_text: sl.link_text.clone(),
                            final_urls: asset.final_urls.clone(),
                            line1: if sl.description1.is_empty() { None } else { Some(sl.description1.clone()) },
                            line2: if sl.description2.is_empty() { None } else { Some(sl.description2.clone()) },
                        });
                    }
                }
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
                    if let Some(ad_group) = camp.ad_groups.iter_mut().find(|a| a.id == Some(ag.id)) {
                        if let Some(AssetData::SitelinkAsset(ref sl)) = asset.asset_data {
                            ad_group.sitelinks.push(crate::models::schema::Sitelink {
                                asset_id: Some(asset.id),
                                link_text: sl.link_text.clone(),
                                final_urls: asset.final_urls.clone(),
                                line1: if sl.description1.is_empty() { None } else { Some(sl.description1.clone()) },
                                line2: if sl.description2.is_empty() { None } else { Some(sl.description2.clone()) },
                            });
                        }
                    }
                }
            }
        }
    }

    // 7. Fetch Campaign Negative Keywords
    use googleads_rs::google::ads::googleads::v23::resources::campaign_criterion::Criterion as CampaignCriterionEnum;

    let ckw_query = "SELECT campaign.id, campaign_criterion.criterion_id, campaign_criterion.keyword.text, campaign_criterion.keyword.match_type, campaign_criterion.negative FROM campaign_criterion WHERE campaign_criterion.type = 'KEYWORD'";
    let ckw_req = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: ckw_query.to_string(),
        ..Default::default()
    };
    
    if let Ok(response) = ga_client.client.search(ckw_req).await {
        for row in response.into_inner().results {
            if let (Some(c), Some(cc)) = (row.campaign, row.campaign_criterion) {
                if let Some(camp) = campaigns.get_mut(&c.id) {
                    if let Some(CampaignCriterionEnum::Keyword(ref kw_info)) = cc.criterion {
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
        }
    }

    Ok(campaigns)
}

pub async fn run(account_id_str: &str) -> Result<()> {
    let account_id = crate::models::account::AccountId::new(account_id_str)
        .map_err(|e| anyhow::anyhow!(e))?;
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
