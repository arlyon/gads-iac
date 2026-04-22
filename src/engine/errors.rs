use miette::{Diagnostic, NamedSource, SourceSpan};
use prost::{Enumeration, Message};
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
    #[prost(oneof = "error_code::ErrorCode", tags = "10")]
    pub error_code: Option<error_code::ErrorCode>,
}

pub mod error_code {
    use super::StringLengthError;
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum ErrorCode {
        #[prost(enumeration = "StringLengthError", tag = "10")]
        StringLengthError(i32),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
#[repr(i32)]
pub enum StringLengthError {
    Unspecified = 0,
    Unknown = 1,
    Empty = 4,
    TooShort = 2,
    TooLong = 3,
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
    pub length_limit: Option<FieldLengthLimit>,
}

#[derive(Debug, Clone, Copy)]
pub struct FieldLengthLimit {
    pub field: &'static str,
    pub max: usize,
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
        write!(f, "{}", self.display_message())
    }
}

impl GoogleAdsFailureItem {
    fn display_message(&self) -> String {
        if let Some(limit) = self.length_limit {
            format!(
                "{}: maximum {} characters for {}",
                self.message.trim_end_matches('.'),
                limit.max,
                limit.field
            )
        } else {
            self.message.clone()
        }
    }

    fn is_dependent_asset_link_failure(&self) -> bool {
        self.message == "Resource was not found."
            && self.location.as_deref().is_some_and(|location| {
                location.ends_with(".campaign_asset_operation.create.asset")
            })
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
    fn from_failures(
        values: &[&GoogleAdsFailureItem],
        source_context: Option<&OperationSourceContext>,
    ) -> Self {
        let value = values[0];
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
        let label =
            label_with_length_limit(label, value.length_limit, &source_context, refined_span);

        Self {
            message: grouped_failure_message(values, source_context),
            source_code,
            span: refined_span,
            label,
        }
    }
}

fn grouped_failure_message(
    values: &[&GoogleAdsFailureItem],
    _source_context: Option<&OperationSourceContext>,
) -> String {
    values[0].display_message()
}

fn label_with_length_limit(
    label: String,
    limit: Option<FieldLengthLimit>,
    source_context: &Option<&OperationSourceContext>,
    span: Option<SourceSpan>,
) -> String {
    let Some(limit) = limit else {
        return label;
    };

    let current = source_context
        .and_then(|context| span_text(&context.source, span))
        .map(|text| text.chars().count());

    if let Some(current) = current {
        format!("{label} (max {} characters, got {current})", limit.max)
    } else {
        format!("{label} (max {} characters)", limit.max)
    }
}

fn span_text(source: &str, span: Option<SourceSpan>) -> Option<&str> {
    let span = span?;
    let start = span.offset();
    let end = start.checked_add(span.len())?;
    source.get(start..end)
}

fn yaml_span_for_google_ads_location(source: &str, location: &str) -> Option<SourceSpan> {
    if let Some(index) = indexed_path_component(location, "headlines") {
        return nth_yaml_sequence_value_span(source, "headlines", index);
    }

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
            let is_too_long = matches!(
                err.error_code
                    .as_ref()
                    .and_then(|code| code.error_code.as_ref()),
                Some(error_code::ErrorCode::StringLengthError(
                    value
                )) if StringLengthError::try_from(*value) == Ok(StringLengthError::TooLong)
            ) || err.message == "Too long.";

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
                length_limit: location
                    .as_deref()
                    .and_then(|location| field_length_limit(location, is_too_long)),
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
            errors: grouped_operation_diagnostics(&self.errors, operation_sources),
        }
    }
}

fn grouped_operation_diagnostics(
    errors: &[GoogleAdsFailureItem],
    operation_sources: &[OperationSourceContext],
) -> Vec<GoogleAdsOperationDiagnostic> {
    let mut groups: Vec<(String, Vec<&GoogleAdsFailureItem>)> = Vec::new();

    for error in errors {
        if error.is_dependent_asset_link_failure()
            && error
                .operation_index
                .and_then(|index| operation_sources.get(index))
                .is_some_and(|context| context.label.starts_with("this asset link depends"))
        {
            continue;
        }

        let key = diagnostic_group_key(error);
        if let Some((_, values)) = groups.iter_mut().find(|(existing, _)| existing == &key) {
            values.push(error);
        } else {
            groups.push((key, vec![error]));
        }
    }

    groups
        .into_iter()
        .map(|(_, values)| {
            let source_context = values[0]
                .operation_index
                .and_then(|index| operation_sources.get(index));
            GoogleAdsOperationDiagnostic::from_failures(&values, source_context)
        })
        .collect()
}

fn diagnostic_group_key(error: &GoogleAdsFailureItem) -> String {
    format!(
        "{}|{}|{:?}",
        error.display_message(),
        error
            .location
            .as_deref()
            .map(normalized_google_ads_location)
            .unwrap_or_default(),
        error.length_limit.map(|limit| (limit.field, limit.max))
    )
}

fn normalized_google_ads_location(location: &str) -> String {
    let Some(start) = location.find("mutate_operations[") else {
        return location.to_string();
    };
    let index_start = start + "mutate_operations[".len();
    let Some(index_end) = location[index_start..].find(']') else {
        return location.to_string();
    };
    let index_end = index_start + index_end;

    format!(
        "{}mutate_operations[]{}",
        &location[..start],
        &location[index_end + 1..]
    )
}

fn field_length_limit(location: &str, is_too_long: bool) -> Option<FieldLengthLimit> {
    if !is_too_long {
        return None;
    }

    let limits = [
        (
            ".callout_asset.callout_text",
            FieldLengthLimit {
                field: "callout text",
                max: 25,
            },
        ),
        (
            ".sitelink_asset.link_text",
            FieldLengthLimit {
                field: "sitelink text",
                max: 25,
            },
        ),
        (
            ".sitelink_asset.description1",
            FieldLengthLimit {
                field: "sitelink line1",
                max: 35,
            },
        ),
        (
            ".sitelink_asset.description2",
            FieldLengthLimit {
                field: "sitelink line2",
                max: 35,
            },
        ),
        (
            ".responsive_search_ad.headlines",
            FieldLengthLimit {
                field: "responsive search ad headline",
                max: 30,
            },
        ),
        (
            ".responsive_search_ad.descriptions",
            FieldLengthLimit {
                field: "responsive search ad description",
                max: 90,
            },
        ),
        (
            ".responsive_search_ad.path1",
            FieldLengthLimit {
                field: "responsive search ad path1",
                max: 15,
            },
        ),
        (
            ".responsive_search_ad.path2",
            FieldLengthLimit {
                field: "responsive search ad path2",
                max: 15,
            },
        ),
    ];

    limits
        .iter()
        .find_map(|(suffix, limit)| location.contains(suffix).then_some(*limit))
}

impl fmt::Display for ErrorAggregator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for err in &self.errors {
            writeln!(f, " - {}", err)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn too_long_errors_include_limit_without_raw_path_in_title() {
        let item = GoogleAdsFailureItem {
            message: "Too long.".to_string(),
            location: Some(
                "mutate_operations[10].ad_group_ad_operation.create.ad.responsive_search_ad.descriptions[0].text"
                    .to_string(),
            ),
            operation_index: Some(10),
            length_limit: field_length_limit(
                "mutate_operations[10].ad_group_ad_operation.create.ad.responsive_search_ad.descriptions[0].text",
                true,
            ),
        };

        assert_eq!(
            item.to_string(),
            "Too long: maximum 90 characters for responsive search ad description"
        );
    }

    #[test]
    fn unknown_length_errors_keep_google_ads_path() {
        let item = GoogleAdsFailureItem {
            message: "Resource was not found.".to_string(),
            location: Some(
                "mutate_operations[2].campaign_asset_operation.create.asset".to_string(),
            ),
            operation_index: Some(2),
            length_limit: None,
        };

        assert_eq!(item.to_string(), "Resource was not found.");
    }

    #[test]
    fn grouped_diagnostics_collapse_repeated_operations() {
        let first = GoogleAdsFailureItem {
            message: "Too long.".to_string(),
            location: Some(
                "mutate_operations[9].ad_group_ad_operation.create.ad.responsive_search_ad.descriptions[0].text"
                    .to_string(),
            ),
            operation_index: Some(9),
            length_limit: field_length_limit(
                "mutate_operations[9].ad_group_ad_operation.create.ad.responsive_search_ad.descriptions[0].text",
                true,
            ),
        };
        let second = GoogleAdsFailureItem {
            operation_index: Some(10),
            location: Some(
                "mutate_operations[10].ad_group_ad_operation.create.ad.responsive_search_ad.descriptions[0].text"
                    .to_string(),
            ),
            ..first.clone()
        };

        let message = grouped_failure_message(&[&first, &second], None);

        assert_eq!(
            message,
            "Too long: maximum 90 characters for responsive search ad description"
        );
    }

    #[test]
    fn dependent_asset_link_failures_are_suppressed() {
        let source = OperationSourceContext {
            source_name: "campaign.yaml".to_string(),
            source: "callouts:\n  - text: Too long for a callout\n".to_string(),
            span: None,
            label: "this asset link depends on the asset value highlighted here".to_string(),
        };
        let error = GoogleAdsFailureItem {
            message: "Resource was not found.".to_string(),
            location: Some(
                "mutate_operations[2].campaign_asset_operation.create.asset".to_string(),
            ),
            operation_index: Some(0),
            length_limit: None,
        };

        let diagnostics = grouped_operation_diagnostics(&[error], &[source]);

        assert!(diagnostics.is_empty());
    }
}
