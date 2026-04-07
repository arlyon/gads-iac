use anyhow::Result;
use std::fs::File;
use std::io::Write;
use crate::api::client::GoogleAdsClient;
use crate::models::schema::Campaign;
use googleads_rs::google::ads::googleads::v23::services::SearchGoogleAdsRequest;

pub async fn run(account_id: &str) -> Result<()> {
    println!("Fetching remote state for account: {}...", account_id);
    
    let mut ga_client = GoogleAdsClient::new().await?;
    ga_client.customer_id = account_id.replace("-", ""); // Ensure clean ID
    
    let query = "SELECT campaign.id, campaign.name, campaign.status FROM campaign WHERE campaign.status != 'REMOVED'";
    
    let request = SearchGoogleAdsRequest {
        customer_id: ga_client.customer_id.clone(),
        query: query.to_string(),
        ..Default::default()
    };
    
    let response = match ga_client.client.search(request).await {
        Ok(r) => r,
        Err(e) => {
            println!("GRPC Error Status: {:?}", e);
            println!("Detailed Error payload: {}", String::from_utf8_lossy(e.details()));
            return Err(e.into());
        }
    };
    
    let rows = response.into_inner().results;
    
    if rows.is_empty() {
        println!("No campaigns found for account {}.", account_id);
        return Ok(());
    }
    
    for row in rows {
        if let Some(c) = row.campaign {
            let camp_id = c.id;
            let status_str = match c.status {
                2 => "ENABLED",
                3 => "PAUSED",
                4 => "REMOVED",
                _ => "UNKNOWN",
            };

            let mock_campaign = Campaign {
                id: Some(c.id),
                name: c.name,
                status: status_str.to_string(),
                ad_groups: vec![],
            };
            
            let filename = format!("{}_{}_campaign.yaml", account_id, camp_id);
            let mut file = File::create(&filename)?;
            
            let yaml_string = serde_yaml::to_string(&mock_campaign)?;
            file.write_all(yaml_string.as_bytes())?;
            
            println!("Successfully exported YAML to {}", filename);
        }
    }
    
    Ok(())
}
