use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// A Google Ads campaign.
#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
pub struct Campaign {
    /// Internal Google Ads campaign ID. Set automatically on import; omit when creating.
    pub id: Option<i64>,
    /// Campaign name.
    pub name: String,
    /// Campaign status. One of: `ENABLED`, `PAUSED`, `REMOVED`.
    pub status: String,
    #[serde(skip)]
    #[schemars(skip)]
    #[allow(dead_code)]
    pub budget_id: Option<i64>,
    /// Daily budget in the account's currency (e.g. `10.50` for $10.50).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daily_budget: Option<f64>,
    /// Bidding strategy for the campaign.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bidding_strategy: Option<BiddingStrategy>,
    /// Campaign start date in `YYYY-MM-DD` format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<String>,
    /// Campaign end date in `YYYY-MM-DD` format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<String>,
    /// Geographic targeting locations.
    #[serde(default)]
    pub locations: Vec<Location>,
    /// Callout assets attached to the campaign.
    #[serde(default)]
    pub callouts: Vec<Callout>,
    /// Sitelink assets attached to the campaign.
    #[serde(default)]
    pub sitelinks: Vec<Sitelink>,
    /// Campaign-level negative keywords. Use `[exact]`, `"phrase"`, or `broad` match notation.
    #[serde(default)]
    pub negative_keywords: Vec<Keyword>,
    /// Ad groups belonging to this campaign.
    #[serde(default)]
    pub ad_groups: Vec<AdGroup>,
}

impl Campaign {
    pub fn normalize(&mut self) {
        self.locations
            .sort_by_key(|l| l.geo_target_constant.clone());
        self.callouts.sort_by_key(|c| c.text.clone());
        self.sitelinks.sort_by_key(|s| s.link_text.clone());
        for sitelink in &mut self.sitelinks {
            sitelink.normalize();
        }
        self.negative_keywords.sort_by_key(|k| k.to_string());
        for ad_group in &mut self.ad_groups {
            ad_group.normalize();
        }
        self.ad_groups.sort_by_key(|ag| ag.name.clone());
    }
}

/// An ad group within a campaign.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, JsonSchema)]
pub struct AdGroup {
    /// Internal Google Ads ad group ID. Set automatically on import; omit when creating.
    pub id: Option<i64>,
    /// Ad group name.
    pub name: String,
    /// Ad group status. One of: `ENABLED`, `PAUSED`, `REMOVED`.
    pub status: String,
    /// Demographic targeting settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub demographics: Option<Demographics>,
    /// Responsive search ads in this ad group.
    #[serde(default)]
    pub ads: Vec<TextAd>,
    /// Positive keywords. Use `[exact]`, `"phrase"`, or `broad` match notation.
    #[serde(default)]
    pub keywords: Vec<Keyword>,
    /// Negative keywords. Use `[exact]`, `"phrase"`, or `broad` match notation.
    #[serde(default)]
    pub negative_keywords: Vec<Keyword>,
    /// Callout assets attached to this ad group.
    #[serde(default)]
    pub callouts: Vec<Callout>,
    /// Sitelink assets attached to this ad group.
    #[serde(default)]
    pub sitelinks: Vec<Sitelink>,
}

impl AdGroup {
    pub fn normalize(&mut self) {
        self.ads
            .sort_by_key(|a| a.headlines.first().cloned().unwrap_or_default());
        for ad in &mut self.ads {
            ad.normalize();
        }
        self.keywords.sort_by_key(|k| k.to_string());
        self.negative_keywords.sort_by_key(|k| k.to_string());
        self.callouts.sort_by_key(|c| c.text.clone());
        self.sitelinks.sort_by_key(|s| s.link_text.clone());
        for sitelink in &mut self.sitelinks {
            sitelink.normalize();
        }
    }
}

/// A sitelink asset.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, JsonSchema)]
pub struct Sitelink {
    #[serde(skip)]
    #[schemars(skip)]
    pub asset_id: Option<i64>,
    /// Display text for the sitelink.
    pub link_text: String,
    /// Landing page URLs for the sitelink.
    pub final_urls: Vec<String>,
    /// First description line (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line1: Option<String>,
    /// Second description line (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line2: Option<String>,
}

impl Sitelink {
    pub fn normalize(&mut self) {
        self.final_urls.sort();
    }
}

/// A responsive search ad.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, JsonSchema)]
pub struct TextAd {
    /// Internal Google Ads ad ID. Set automatically on import; omit when creating.
    pub id: Option<i64>,
    /// Headlines for the ad (up to 15).
    pub headlines: Vec<String>,
    /// Descriptions for the ad (up to 4).
    pub descriptions: Vec<String>,
    /// Landing page URLs.
    pub final_urls: Vec<String>,
}

impl TextAd {
    pub fn normalize(&mut self) {
        self.headlines.sort();
        self.descriptions.sort();
        self.final_urls.sort();
    }
}

/// A keyword with match type encoded in the string.
///
/// Use bracket notation for match types:
/// - `[keyword]` — exact match
/// - `"keyword"` — phrase match
/// - `keyword` — broad match
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keyword {
    pub criterion_id: Option<i64>,
    pub text: String,
    pub match_type: String, // EXACT, BROAD, PHRASE
}

/// Bidding strategy configuration. The `type` field selects the variant.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, JsonSchema)]
#[serde(tag = "type")]
pub enum BiddingStrategy {
    /// Target cost-per-acquisition bidding.
    TargetCpa {
        /// Target CPA in the account's currency.
        target_cpa: f64,
    },
    /// Target return-on-ad-spend bidding.
    TargetRoas {
        /// Target ROAS as a ratio (e.g. `4.0` = 400%).
        target_roas: f64,
    },
    /// Maximize conversions, with an optional CPA target.
    MaximizeConversions {
        /// Optional target CPA in the account's currency.
        target_cpa: Option<f64>,
    },
    /// Maximize conversion value, with an optional ROAS target.
    MaximizeConversionValue {
        /// Optional target ROAS as a ratio.
        target_roas: Option<f64>,
    },
    /// Manual CPC bidding.
    ManualCpc {
        /// Whether enhanced CPC is enabled.
        enhanced_cpc_enabled: bool,
    },
}

/// A geographic targeting location.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, JsonSchema)]
pub struct Location {
    #[serde(skip)]
    #[schemars(skip)]
    pub criterion_id: Option<i64>,
    /// Google Ads geo target constant resource name (e.g. `geoTargetConstants/2840`).
    pub geo_target_constant: String,
}

/// A callout asset.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, JsonSchema)]
pub struct Callout {
    #[serde(skip)]
    #[schemars(skip)]
    pub asset_id: Option<i64>,
    /// Callout text (max 25 characters).
    pub text: String,
}

/// Demographic targeting settings for an ad group.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, JsonSchema)]
pub struct Demographics {
    /// Targeted genders. Values: `MALE`, `FEMALE`, `UNDETERMINED`.
    pub genders: Vec<String>,
    /// Targeted age ranges. Values: `AGE_RANGE_18_24`, `AGE_RANGE_25_34`, etc.
    pub age_ranges: Vec<String>,
}

impl schemars::JsonSchema for Keyword {
    fn schema_name() -> String {
        "Keyword".to_owned()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema = generator.subschema_for::<String>().into_object();
        schema.metadata().description = Some(
            "A keyword with encoded match type: `[exact]`, `\"phrase\"`, or `broad`.".to_owned(),
        );
        schema.metadata().examples = vec![
            serde_json::Value::String("[buy shoes]".to_owned()),
            serde_json::Value::String("\"running shoes\"".to_owned()),
            serde_json::Value::String("shoes".to_owned()),
        ];
        schemars::schema::Schema::Object(schema)
    }
}

impl fmt::Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.match_type.as_str() {
            "EXACT" => write!(f, "[{}]", self.text),
            "PHRASE" => write!(f, "\"{}\"", self.text),
            _ => write!(f, "{}", self.text),
        }
    }
}

impl FromStr for Keyword {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.trim();
        if s.starts_with('[') && s.ends_with(']') {
            Ok(Keyword {
                criterion_id: None,
                text: s[1..s.len() - 1].to_string(),
                match_type: "EXACT".to_string(),
            })
        } else if s.starts_with('"') && s.ends_with('"') {
            Ok(Keyword {
                criterion_id: None,
                text: s[1..s.len() - 1].to_string(),
                match_type: "PHRASE".to_string(),
            })
        } else {
            Ok(Keyword {
                criterion_id: None,
                text: s.to_string(),
                match_type: "BROAD".to_string(),
            })
        }
    }
}

impl Serialize for Keyword {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Keyword {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Keyword::from_str(&s).map_err(serde::de::Error::custom)
    }
}
