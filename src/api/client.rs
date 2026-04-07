use crate::api::auth::get_auth_token;
use anyhow::Result;
use tonic::codegen::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Request, Status, metadata::MetadataValue};

// Assuming v13 based on googleads-rs v0.13.0 cargo versions
pub use googleads_rs::google::ads::googleads::v23::services::google_ads_service_client::GoogleAdsServiceClient;

pub type GAdsClient = GoogleAdsServiceClient<InterceptedService<Channel, GAdsInterceptor>>;

#[derive(Clone)]
pub struct GAdsInterceptor {
    developer_token: MetadataValue<tonic::metadata::Ascii>,
    login_customer_id: MetadataValue<tonic::metadata::Ascii>,
    access_token: MetadataValue<tonic::metadata::Ascii>,
}

impl tonic::service::Interceptor for GAdsInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("developer-token", self.developer_token.clone());
        request
            .metadata_mut()
            .insert("login-customer-id", self.login_customer_id.clone());
        request
            .metadata_mut()
            .insert("authorization", self.access_token.clone());
        Ok(request)
    }
}

pub struct GoogleAdsClient {
    pub client: GAdsClient,
}

impl GoogleAdsClient {
    pub async fn new() -> Result<Self> {
        let auth = get_auth_token().await?;

        // Ensure that tonic rustls is correctly setup with native roots
        let endpoint = Channel::from_static("https://googleads.googleapis.com")
            .tls_config(ClientTlsConfig::new().with_native_roots())?;

        let channel = endpoint.connect().await?;

        let token = format!("Bearer {}", auth.access_token);
        let interceptor = GAdsInterceptor {
            developer_token: auth.developer_token.parse()?,
            login_customer_id: auth.login_customer_id.parse()?,
            access_token: token.parse()?,
        };

        let client = GoogleAdsServiceClient::with_interceptor(channel, interceptor);

        Ok(Self {
            client,
        })
    }
}
