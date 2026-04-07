use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub developer_token: String,
    pub _project_id: String,
    pub login_customer_id: String,
    pub creds_path: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let developer_token =
            env::var("GOOGLE_ADS_DEVELOPER_TOKEN").context("Missing GOOGLE_ADS_DEVELOPER_TOKEN")?;

        let project_id = env::var("GOOGLE_PROJECT_ID")
            .context("Missing GOOGLE_PROJECT_ID")?
            .replace("-", "");

        let login_customer_id = env::var("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
            .unwrap_or_else(|_| project_id.clone())
            .replace("-", "");

        let creds_path = env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .context("Missing GOOGLE_APPLICATION_CREDENTIALS")?;

        Ok(Self {
            developer_token,
            _project_id: project_id,
            login_customer_id,
            creds_path,
        })
    }
}
