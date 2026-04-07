use crate::engine::config::Config;
use anyhow::{Context, Result};
use std::fmt;
use yup_oauth2::{ServiceAccountAuthenticator, read_service_account_key};

#[derive(Clone)]
pub struct GoogleAdsAuth {
    pub developer_token: String,
    pub access_token: String,
    pub login_customer_id: String,
}

impl fmt::Debug for GoogleAdsAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GoogleAdsAuth")
            .field("developer_token", &self.developer_token)
            .field("access_token", &"***MASKED***")
            .field("login_customer_id", &self.login_customer_id)
            .finish()
    }
}

pub async fn get_auth_token(config: &Config) -> Result<GoogleAdsAuth> {
    let secret = read_service_account_key(&config.creds_path)
        .await
        .context("Failed to read service account JSON key file")?;

    let auth = ServiceAccountAuthenticator::builder(secret).build().await?;
    let scopes = &["https://www.googleapis.com/auth/adwords"];

    let token = auth.token(scopes).await?;

    // Fallback unwrap string extraction
    let access_token = token.token().unwrap_or_default().to_string();

    Ok(GoogleAdsAuth {
        developer_token: config.developer_token.clone(),
        access_token,
        login_customer_id: config.login_customer_id.clone(),
    })
}
