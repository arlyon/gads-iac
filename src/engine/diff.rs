use crate::models::schema::Campaign;

/// Computes the difference between local YAML definition and remote Google Ads API state.
pub fn compute_diff(local: &Campaign, remote: &Campaign) -> Vec<String> {
    let mut differences = Vec::new();
    
    if local.name != remote.name {
        differences.push(format!(
            "Drift - Name: Local '{}' vs Remote '{}'",
            local.name, remote.name
        ));
    }
    
    if local.status != remote.status {
        differences.push(format!(
            "Drift - Status: Local '{}' vs Remote '{}'",
            local.status, remote.status
        ));
    }
    
    if local.ad_groups.len() != remote.ad_groups.len() {
        differences.push(format!(
            "Drift - AdGroups count: Local {} vs Remote {}",
            local.ad_groups.len(), remote.ad_groups.len()
        ));
    }
    
    differences
}
