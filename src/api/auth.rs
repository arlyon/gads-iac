use anyhow::{Context, Result};
use std::env;
use yup_oauth2::{ServiceAccountAuthenticator, read_service_account_key};

#[derive(Debug, Clone)]
pub struct GoogleAdsAuth {
    pub developer_token: String,
    pub access_token: String,
    pub customer_id: String,
    pub login_customer_id: String,
}

pub async fn get_auth_token() -> Result<GoogleAdsAuth> {
    let dev_token =
        env::var("GOOGLE_ADS_DEVELOPER_TOKEN").context("Missing GOOGLE_ADS_DEVELOPER_TOKEN")?;

    let customer_id = env::var("GOOGLE_PROJECT_ID")
        .context("Missing GOOGLE_PROJECT_ID")?
        .replace("-", "");

    let login_customer_id = env::var("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
        .unwrap_or_else(|_| customer_id.clone())
        .replace("-", "");

    let creds_path = env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .context("Missing GOOGLE_APPLICATION_CREDENTIALS")?;

    let secret = read_service_account_key(&creds_path)
        .await
        .context("Failed to read service account JSON key file")?;

    let auth = ServiceAccountAuthenticator::builder(secret).build().await?;
    let scopes = &["https://www.googleapis.com/auth/adwords"];

    let token = auth.token(scopes).await?;

    // Fallback unwrap string extraction
    let access_token = token.token().unwrap_or_default().to_string();

    Ok(GoogleAdsAuth {
        developer_token: dev_token,
        access_token,
        customer_id,
        login_customer_id,
    })
}
