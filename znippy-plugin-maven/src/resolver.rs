//! Transitive Maven dependency resolver.
//!
//! Fetches POMs from Maven Central, parses dependencies, and recursively
//! resolves the full dependency tree. Requires the `resolve` feature.

use crate::pom::{parse_pom_dependencies, parse_pom_project, MavenCoord};
use std::collections::{HashSet, VecDeque};

/// Resolve all transitive dependencies starting from a root POM.
/// Returns a deduplicated list of all coords (including the root's direct deps).
pub fn resolve_transitive(root_pom: &[u8], max_depth: usize) -> Vec<MavenCoord> {
    let direct = parse_pom_dependencies(root_pom);
    let mut resolved: Vec<MavenCoord> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(MavenCoord, usize)> = VecDeque::new();

    for coord in direct {
        let key = coord_key(&coord);
        if visited.insert(key) {
            queue.push_back((coord, 0));
        }
    }

    while let Some((coord, depth)) = queue.pop_front() {
        resolved.push(coord.clone());

        if depth >= max_depth {
            continue;
        }

        // Fetch this dep's POM to get its transitive deps
        let pom_url = pom_url(&coord);
        match fetch_pom(&pom_url) {
            Some(pom_bytes) => {
                let transitive = parse_pom_dependencies(&pom_bytes);
                for dep in transitive {
                    // Skip deps without version (managed by parent — we can't resolve those without parent POM)
                    if dep.version.is_empty() || dep.version.starts_with('$') {
                        continue;
                    }
                    let key = coord_key(&dep);
                    if visited.insert(key) {
                        queue.push_back((dep, depth + 1));
                    }
                }
            }
            None => {
                // POM not found — skip transitives for this one
            }
        }
    }

    resolved
}

/// Resolve direct dependencies only (no recursion), just parse + return coords.
pub fn resolve_direct(root_pom: &[u8]) -> Vec<MavenCoord> {
    parse_pom_dependencies(root_pom)
}

/// Download a JAR/artifact from Maven Central. Returns bytes or None.
pub fn download_artifact(coord: &MavenCoord) -> Option<Vec<u8>> {
    let url = coord.central_url();
    let resp = ureq::get(&url).call().ok()?;
    if resp.status() != 200 {
        return None;
    }
    let mut body = Vec::new();
    resp.into_reader().read_to_end(&mut body).ok()?;
    Some(body)
}

/// Download a JAR to a file path. Returns true on success.
pub fn download_artifact_to_file(coord: &MavenCoord, path: &std::path::Path) -> bool {
    let url = coord.central_url();
    match ureq::get(&url).call() {
        Ok(resp) if resp.status() == 200 => {
            let mut body = Vec::new();
            if resp.into_reader().read_to_end(&mut body).is_ok() {
                std::fs::write(path, &body).is_ok()
            } else {
                false
            }
        }
        _ => false,
    }
}

fn coord_key(c: &MavenCoord) -> String {
    format!("{}:{}:{}", c.group_id, c.artifact_id, c.version)
}

fn pom_url(coord: &MavenCoord) -> String {
    let group_path = coord.group_id.replace('.', "/");
    format!(
        "https://repo1.maven.org/maven2/{}/{}/{}/{}-{}.pom",
        group_path, coord.artifact_id, coord.version,
        coord.artifact_id, coord.version
    )
}

fn fetch_pom(url: &str) -> Option<Vec<u8>> {
    let resp = ureq::get(url).call().ok()?;
    if resp.status() != 200 {
        return None;
    }
    let mut body = Vec::new();
    resp.into_reader().read_to_end(&mut body).ok()?;
    Some(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // requires network
    fn test_resolve_guava_transitive() {
        let pom = br#"<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>test</groupId>
    <artifactId>test</artifactId>
    <version>1.0</version>
    <dependencies>
        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
            <version>33.2.0-jre</version>
        </dependency>
    </dependencies>
</project>"#;

        let all = resolve_transitive(pom, 2);
        assert!(!all.is_empty());
        // Guava pulls in failureaccess, checker-qual, etc.
        println!("Resolved {} deps from guava:", all.len());
        for c in &all {
            println!("  {}:{}:{}", c.group_id, c.artifact_id, c.version);
        }
        assert!(all.len() > 1);
    }
}
