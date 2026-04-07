use crate::models::schema::Campaign;

/// Computes the difference between local YAML definition and remote Google Ads API state.
pub fn compute_diff(local: &Campaign, remote: &Campaign) -> Vec<String> {
    let mut differences = Vec::new();
    
    let local_yml = serde_yaml::to_string(local).unwrap_or_default();
    let remote_yml = serde_yaml::to_string(remote).unwrap_or_default();
    
    if local_yml == remote_yml {
        return differences;
    }
    
    let l_lines: Vec<&str> = local_yml.lines().collect();
    let r_lines: Vec<&str> = remote_yml.lines().collect();
    
    let mut l_adds = 0;
    for line in &l_lines {
        if !r_lines.contains(line) {
            differences.push(format!("+ {}", line));
            l_adds += 1;
        }
    }
    
    for line in &r_lines {
        if !l_lines.contains(line) {
            differences.push(format!("- {}", line));
            l_adds += 1;
        }
    }
    
    if l_adds == 0 {
        differences.push("~ State difference detected (likely array sorting/sequence)".to_string());
    }
    
    differences
}
