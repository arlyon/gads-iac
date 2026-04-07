pub struct ErrorAggregator {
    #[allow(dead_code)]
    pub errors: Vec<String>,
}

impl ErrorAggregator {
    pub fn new() -> Self {
        Self { errors: Vec::new() }
    }

    #[allow(dead_code)]
    pub fn parse_partial_failures(
        &mut self,
        _operations_count: usize,
        _response: &tonic::Response<
            googleads_rs::google::ads::googleads::v23::services::MutateGoogleAdsResponse,
        >,
    ) {
        // Advanced mapping logic can go here. For now we will just assume we want to mock it out or implement a basic structure.
        // The MutateGoogleAdsResponse has partial_failure_error which is a `Status`.
        // Tonic response exposes the payload.
    }
}
