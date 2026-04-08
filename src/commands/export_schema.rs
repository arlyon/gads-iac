use anyhow::Result;
use schemars::schema_for;

use crate::models::schema::Campaign;

pub fn run() -> Result<()> {
    let schema = schema_for!(Campaign);
    let json = serde_json::to_string_pretty(&schema)?;
    std::fs::write("gads-schema.json", &json)?;
    println!("Wrote gads-schema.json");
    Ok(())
}
