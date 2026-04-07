use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Campaign {
    pub id: Option<i64>,
    pub name: String,
    pub status: String,
    #[serde(skip)]
    #[allow(dead_code)]
    pub budget_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daily_budget: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bidding_strategy: Option<BiddingStrategy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<String>,
    #[serde(default)]
    pub locations: Vec<Location>,
    #[serde(default)]
    pub callouts: Vec<Callout>,
    #[serde(default)]
    pub sitelinks: Vec<Sitelink>,
    #[serde(default)]
    pub negative_keywords: Vec<Keyword>,
    #[serde(default)]
    pub ad_groups: Vec<AdGroup>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AdGroup {
    pub id: Option<i64>,
    pub name: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub demographics: Option<Demographics>,
    #[serde(default)]
    pub ads: Vec<TextAd>,
    #[serde(default)]
    pub keywords: Vec<Keyword>,
    #[serde(default)]
    pub negative_keywords: Vec<Keyword>,
    #[serde(default)]
    pub callouts: Vec<Callout>,
    #[serde(default)]
    pub sitelinks: Vec<Sitelink>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Sitelink {
    #[serde(skip)]
    pub asset_id: Option<i64>,
    pub link_text: String,
    pub final_urls: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line1: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line2: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TextAd {
    pub id: Option<i64>,
    pub headlines: Vec<String>,
    pub descriptions: Vec<String>,
    pub final_urls: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Keyword {
    pub criterion_id: Option<i64>,
    pub text: String,
    pub match_type: String, // EXACT, BROAD, PHRASE
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum BiddingStrategy {
    TargetCpa { target_cpa: f64 },
    TargetRoas { target_roas: f64 },
    MaximizeConversions { target_cpa: Option<f64> },
    MaximizeConversionValue { target_roas: Option<f64> },
    ManualCpc { enhanced_cpc_enabled: bool },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Location {
    #[serde(skip)]
    pub criterion_id: Option<i64>,
    pub geo_target_constant: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Callout {
    #[serde(skip)]
    pub asset_id: Option<i64>,
    pub text: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Demographics {
    pub genders: Vec<String>,
    pub age_ranges: Vec<String>,
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
