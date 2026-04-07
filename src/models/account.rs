use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AccountId(String);

impl AccountId {
    /// Creates a new AccountId, automatically stripping hyphens and validating it contains exactly 10 digits.
    pub fn new(id: &str) -> Result<Self, String> {
        let clean = id.replace("-", "");
        if clean.len() != 10 || !clean.chars().all(|c| c.is_ascii_digit()) {
            return Err(format!("Invalid Account ID '{}': must be 10 digits", id));
        }
        Ok(AccountId(clean))
    }

    /// Returns the raw unhyphenated string (e.g. "1234567890"), perfect for gRPC operations.
    pub fn unhyphenated(&self) -> String {
        self.0.clone()
    }

    /// Returns the formatted hyphenated string (e.g. "123-456-7890"), perfect for UI and logging.
    pub fn hyphenated(&self) -> String {
        format!("{}-{}-{}", &self.0[0..3], &self.0[3..6], &self.0[6..10])
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.hyphenated())
    }
}

impl FromStr for AccountId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        AccountId::new(s)
    }
}
