use miette::{Diagnostic, NamedSource, SourceSpan};
use prost::Message;
use std::collections::BTreeSet;
use std::fmt;
use thiserror::Error;

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

#[derive(Debug, Clone)]
pub struct GoogleAdsFailureItem {
    pub message: String,
    pub location: Option<String>,
    pub operation_index: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct OperationSourceContext {
    pub source_name: String,
    pub source: String,
    pub span: Option<SourceSpan>,
    pub label: String,
}

impl fmt::Display for GoogleAdsFailureItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.operation_index, &self.location) {
            (Some(index), Some(location)) => {
                write!(f, "operation {}: {} (at {})", index, self.message, location)
            }
            (Some(index), None) => write!(f, "operation {}: {}", index, self.message),
            (None, Some(location)) => write!(f, "{} (at {})", self.message, location),
            (None, None) => write!(f, "{}", self.message),
        }
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("{message}")]
#[diagnostic(code(gads_iac::google_ads::operation_failure))]
pub struct GoogleAdsOperationDiagnostic {
    message: String,
    #[source_code]
    source_code: Option<NamedSource<String>>,
    #[label(primary, "{label}")]
    span: Option<SourceSpan>,
    label: String,
}

impl GoogleAdsOperationDiagnostic {
    fn from_failure(
        value: &GoogleAdsFailureItem,
        source_context: Option<&OperationSourceContext>,
    ) -> Self {
        let refined_span = source_context
            .and_then(|context| {
                value.location.as_deref().and_then(|location| {
                    yaml_span_for_google_ads_location(&context.source, location)
                })
            })
            .or_else(|| source_context.and_then(|context| context.span));

        let source_code = source_context
            .and_then(|context| refined_span.map(|_| context))
            .map(|context| {
                NamedSource::new(context.source_name.clone(), context.source.clone())
                    .with_language("yaml")
            });
        let label = source_context
            .map(|context| context.label.clone())
            .unwrap_or_else(|| {
                value
                    .location
                    .clone()
                    .unwrap_or_else(|| value.message.clone())
            });

        Self {
            message: value.to_string(),
            source_code,
            span: refined_span,
            label,
        }
    }
}

fn yaml_span_for_google_ads_location(source: &str, location: &str) -> Option<SourceSpan> {
    if let Some(index) = indexed_path_component(location, "descriptions") {
        return nth_yaml_sequence_value_span(source, "descriptions", index);
    }

    None
}

fn indexed_path_component(location: &str, component: &str) -> Option<usize> {
    let start = location.find(&format!("{component}["))? + component.len() + 1;
    let end = location[start..].find(']')? + start;
    location[start..end].parse().ok()
}

fn nth_yaml_sequence_value_span(
    source: &str,
    sequence_key: &str,
    index: usize,
) -> Option<SourceSpan> {
    let sequence = yaml_sequence_line_range(source, sequence_key)?;
    source[sequence.start..sequence.end]
        .lines()
        .scan(sequence.start, |offset, line| {
            let line_offset = *offset;
            *offset += line.len() + 1;
            Some((line_offset, line))
        })
        .filter_map(|(line_offset, line)| {
            let trimmed = line.trim_start();
            if !trimmed.starts_with("- ") {
                return None;
            }
            let value_start = line.find("- ")? + 2;
            let value = line[value_start..].trim_start();
            let leading_ws = line[value_start..].len() - value.len();
            Some((line_offset + value_start + leading_ws, value.len().max(1)))
        })
        .nth(index)
        .map(Into::into)
}

fn yaml_sequence_line_range(source: &str, key: &str) -> Option<std::ops::Range<usize>> {
    let header = format!("{key}:");
    let mut start = None;
    let mut end = source.len();

    for (offset, line) in source.lines().scan(0, |offset, line| {
        let line_offset = *offset;
        *offset += line.len() + 1;
        Some((line_offset, line))
    }) {
        if start.is_none() {
            if line.trim_start() == header {
                start = Some(offset + line.len() + 1);
            }
            continue;
        }

        if !line.trim().is_empty() && !line.starts_with(' ') && !line.starts_with('\t') {
            end = offset;
            break;
        }
    }

    start.map(|start| start..end)
}

#[derive(Debug, Error, Diagnostic)]
#[error(
    "Google Ads apply failed for account {account_id}: {failed} of {attempted} mutation(s) failed"
)]
#[diagnostic(
    code(gads_iac::apply::partial_failure),
    help(
        "The request reached Google Ads, but the API rejected individual mutate operations. Fix the listed local YAML values or generated mutations, then run `plan` before applying again."
    )
)]
pub struct GoogleAdsPartialFailureDiagnostic {
    pub account_id: String,
    pub attempted: usize,
    pub succeeded: usize,
    pub failed: usize,
    #[related]
    pub errors: Vec<GoogleAdsOperationDiagnostic>,
}

#[derive(Debug, Error, Diagnostic)]
#[error("Google Ads API dispatch failed for account {account_id}")]
#[diagnostic(
    code(gads_iac::apply::dispatch_failed),
    help("This is a request-level failure before Google Ads could apply per-operation mutations.")
)]
pub struct GoogleAdsDispatchDiagnostic {
    pub account_id: String,
    #[source]
    pub source: tonic::Status,
}

pub struct ErrorAggregator {
    pub errors: Vec<GoogleAdsFailureItem>,
}

impl ErrorAggregator {
    pub fn new() -> Self {
        Self { errors: Vec::new() }
    }

    pub fn parse_partial_failures(&mut self, details: &[prost_types::Any]) {
        for any in details {
            // Check if it's a GoogleAdsFailure. Different versions might have different URLs.
            if any.type_url.contains("GoogleAdsFailure")
                && let Ok(failure) = GoogleAdsFailure::decode(&any.value[..])
            {
                self.add_failure(failure);
            }
        }
    }

    pub fn add_failure(&mut self, failure: GoogleAdsFailure) {
        for err in failure.errors {
            let (location, operation_index) = if let Some(loc) = err.location {
                let operation_index = loc
                    .field_path_elements
                    .iter()
                    .find(|element| element.field_name == "mutate_operations")
                    .and_then(|element| element.index)
                    .and_then(|index| usize::try_from(index).ok());

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

                let location = (!path.is_empty()).then(|| path.join("."));
                (location, operation_index)
            } else {
                (None, None)
            };

            self.errors.push(GoogleAdsFailureItem {
                message: err.message,
                location,
                operation_index,
            });
        }
    }

    pub fn failed_operation_indices(&self) -> BTreeSet<usize> {
        self.errors
            .iter()
            .filter_map(|error| error.operation_index)
            .collect()
    }

    pub fn failed_operation_count(&self, attempted: usize) -> usize {
        let failed_operation_indices = self.failed_operation_indices();
        if failed_operation_indices.is_empty() {
            self.errors.len().min(attempted)
        } else {
            failed_operation_indices.len()
        }
    }

    pub fn into_diagnostic(
        self,
        account_id: String,
        attempted: usize,
        operation_sources: &[OperationSourceContext],
    ) -> GoogleAdsPartialFailureDiagnostic {
        let failed = self.failed_operation_count(attempted);
        let succeeded = attempted.saturating_sub(failed);

        GoogleAdsPartialFailureDiagnostic {
            account_id,
            attempted,
            succeeded,
            failed,
            errors: self
                .errors
                .iter()
                .map(|error| {
                    let source_context = error
                        .operation_index
                        .and_then(|index| operation_sources.get(index));
                    GoogleAdsOperationDiagnostic::from_failure(error, source_context)
                })
                .collect(),
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
