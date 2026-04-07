use prost::Message;
use std::fmt;

#[derive(Clone, PartialEq, Message)]
pub struct GoogleAdsFailure {
    #[prost(message, repeated, tag = "1")]
    pub errors: Vec<GoogleAdsError>,
    #[prost(string, tag = "2")]
    pub request_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GoogleAdsError {
    #[prost(message, optional, tag = "1")]
    pub error_code: Option<ErrorCode>,
    #[prost(string, tag = "2")]
    pub message: String,
    // trigger (tag 3) skipped
    #[prost(message, optional, tag = "4")]
    pub location: Option<ErrorLocation>,
    #[prost(message, optional, tag = "5")]
    pub details: Option<ErrorDetails>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ErrorCode {
    // We just need the struct to exist for decoding to continue,
    // we don't need to parse the oneof unless we want specific codes.
}

#[derive(Clone, PartialEq, Message)]
pub struct ErrorDetails {
    // Placeholder for additional error details
}

#[derive(Clone, PartialEq, Message)]
pub struct ErrorLocation {
    #[prost(message, repeated, tag = "2")]
    pub field_path_elements: Vec<FieldPathElement>,
}

#[derive(Clone, PartialEq, Message)]
pub struct FieldPathElement {
    #[prost(string, tag = "1")]
    pub field_name: String,
    #[prost(int32, optional, tag = "3")]
    pub index: Option<i32>,
}

pub struct ErrorAggregator {
    pub errors: Vec<String>,
}

impl ErrorAggregator {
    pub fn new() -> Self {
        Self { errors: Vec::new() }
    }

    pub fn parse_partial_failures(&mut self, details: &[prost_types::Any]) {
        for any in details {
            // Check if it's a GoogleAdsFailure. Different versions might have different URLs.
            if any.type_url.contains("GoogleAdsFailure") {
                if let Ok(failure) = GoogleAdsFailure::decode(&any.value[..]) {
                    self.add_failure(failure);
                }
            }
        }
    }

    pub fn add_failure(&mut self, failure: GoogleAdsFailure) {
        for err in failure.errors {
            let mut msg = err.message;
            if let Some(loc) = err.location {
                let path: Vec<String> = loc
                    .field_path_elements
                    .iter()
                    .map(|e| {
                        if let Some(i) = e.index {
                            format!("{}[{}]", e.field_name, i)
                        } else {
                            e.field_name.clone()
                        }
                    })
                    .collect();
                if !path.is_empty() {
                    msg = format!("{} (at {})", msg, path.join("."));
                }
            }
            self.errors.push(msg);
        }
    }
}

impl fmt::Display for ErrorAggregator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for err in &self.errors {
            writeln!(f, " - {}", err)?;
        }
        Ok(())
    }
}
