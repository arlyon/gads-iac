use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Campaign {
    pub id: Option<i64>,
    pub name: String,
    pub status: String, // PAUSED, ENABLED, REMOVED
    #[serde(default)]
    pub ad_groups: Vec<AdGroup>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AdGroup {
    pub id: Option<i64>,
    pub name: String,
    pub status: String,
    #[serde(default)]
    pub ads: Vec<TextAd>,
    #[serde(default)]
    pub keywords: Vec<Keyword>,
    #[serde(default)]
    pub negative_keywords: Vec<Keyword>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TextAd {
    pub id: Option<i64>,
    pub headlines: Vec<String>,
    pub descriptions: Vec<String>,
    pub final_urls: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Keyword {
    pub text: String,
    pub match_type: String, // EXACT, BROAD, PHRASE
}
