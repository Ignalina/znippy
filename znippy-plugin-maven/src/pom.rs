//! Fast POM dependency parser using quick-xml.
//!
//! Parses `<dependencies>` from a Maven POM file and provides
//! Maven Central URL construction for each dependency.

use quick_xml::events::Event;
use quick_xml::Reader;

/// A Maven coordinate (GAV).
#[derive(Debug, Clone, PartialEq)]
pub struct MavenCoord {
    pub group_id: String,
    pub artifact_id: String,
    pub version: String,
    pub packaging: String,
    pub classifier: Option<String>,
}

impl MavenCoord {
    /// Construct the Maven Central download URL for this coordinate.
    /// e.g. https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.13/3.5.1/spark-core_2.13-3.5.1.jar
    pub fn central_url(&self) -> String {
        let group_path = self.group_id.replace('.', "/");
        let ext = if self.packaging.is_empty() { "jar" } else { &self.packaging };
        let classifier_suffix = match &self.classifier {
            Some(c) if !c.is_empty() => format!("-{}", c),
            _ => String::new(),
        };
        format!(
            "https://repo1.maven.org/maven2/{}/{}/{}/{}-{}{}.{}",
            group_path, self.artifact_id, self.version,
            self.artifact_id, self.version, classifier_suffix, ext
        )
    }

    /// Filename for this artifact (e.g. "spark-core_2.13-3.5.1.jar")
    pub fn filename(&self) -> String {
        let ext = if self.packaging.is_empty() { "jar" } else { &self.packaging };
        let classifier_suffix = match &self.classifier {
            Some(c) if !c.is_empty() => format!("-{}", c),
            _ => String::new(),
        };
        format!("{}-{}{}.{}", self.artifact_id, self.version, classifier_suffix, ext)
    }
}

/// Parse all `<dependency>` entries from a POM XML string.
/// Only returns dependencies with groupId, artifactId, and version present.
/// Skips test/provided scope dependencies.
pub fn parse_pom_dependencies(xml: &[u8]) -> Vec<MavenCoord> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);

    let mut deps = Vec::new();
    let mut in_dependencies = false;
    let mut in_dependency = false;
    let mut depth = 0;

    // Current dependency being parsed
    let mut group_id = String::new();
    let mut artifact_id = String::new();
    let mut version = String::new();
    let mut packaging = String::new();
    let mut classifier: Option<String> = None;
    let mut scope = String::new();

    let mut current_tag = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "dependencies" && depth == 1 {
                    in_dependencies = true;
                } else if name == "dependency" && in_dependencies {
                    in_dependency = true;
                    group_id.clear();
                    artifact_id.clear();
                    version.clear();
                    packaging.clear();
                    classifier = None;
                    scope.clear();
                }
                current_tag = name;
                depth += 1;
            }
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                depth -= 1;
                if name == "dependencies" {
                    in_dependencies = false;
                } else if name == "dependency" && in_dependency {
                    in_dependency = false;
                    // Skip test/provided/system scope
                    if !group_id.is_empty()
                        && !artifact_id.is_empty()
                        && !version.is_empty()
                        && scope != "test"
                        && scope != "provided"
                        && scope != "system"
                    {
                        if packaging.is_empty() {
                            packaging = "jar".to_string();
                        }
                        deps.push(MavenCoord {
                            group_id: group_id.clone(),
                            artifact_id: artifact_id.clone(),
                            version: version.clone(),
                            packaging: packaging.clone(),
                            classifier: classifier.clone(),
                        });
                    }
                }
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                if in_dependency {
                    let text = e.unescape().unwrap_or_default().to_string();
                    match current_tag.as_str() {
                        "groupId" => group_id = text,
                        "artifactId" => artifact_id = text,
                        "version" => version = text,
                        "type" | "packaging" => packaging = text,
                        "classifier" => classifier = Some(text),
                        "scope" => scope = text,
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    deps
}

/// Parse the project's own GAV from a POM (top-level groupId/artifactId/version).
pub fn parse_pom_project(xml: &[u8]) -> Option<MavenCoord> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);

    let mut group_id = String::new();
    let mut artifact_id = String::new();
    let mut version = String::new();
    let mut packaging = String::from("jar");

    let mut current_tag = String::new();
    let mut depth = 0;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                current_tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                depth += 1;
            }
            Ok(Event::End(_)) => {
                depth -= 1;
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                // Only capture top-level project fields (depth == 2: <project><X>)
                if depth == 2 {
                    let text = e.unescape().unwrap_or_default().to_string();
                    match current_tag.as_str() {
                        "groupId" => group_id = text,
                        "artifactId" => artifact_id = text,
                        "version" => version = text,
                        "packaging" => packaging = text,
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    if !group_id.is_empty() && !artifact_id.is_empty() && !version.is_empty() {
        Some(MavenCoord {
            group_id,
            artifact_id,
            version,
            packaging,
            classifier: None,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_pom() {
        let pom = br#"<?xml version="1.0"?>
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>my-app</artifactId>
    <version>1.0.0</version>
    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.13</artifactId>
            <version>3.5.1</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>"#;

        let deps = parse_pom_dependencies(pom);
        assert_eq!(deps.len(), 1); // junit skipped (test scope)
        assert_eq!(deps[0].group_id, "org.apache.spark");
        assert_eq!(deps[0].artifact_id, "spark-core_2.13");
        assert_eq!(deps[0].version, "3.5.1");
        assert_eq!(
            deps[0].central_url(),
            "https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.13/3.5.1/spark-core_2.13-3.5.1.jar"
        );
    }

    #[test]
    fn test_parse_project_gav() {
        let pom = br#"<project>
    <groupId>com.example</groupId>
    <artifactId>my-app</artifactId>
    <version>2.1.0</version>
    <packaging>war</packaging>
</project>"#;

        let proj = parse_pom_project(pom).unwrap();
        assert_eq!(proj.group_id, "com.example");
        assert_eq!(proj.packaging, "war");
    }

    #[test]
    fn test_coord_filename() {
        let c = MavenCoord {
            group_id: "io.netty".into(),
            artifact_id: "netty-all".into(),
            version: "4.1.109.Final".into(),
            packaging: "jar".into(),
            classifier: None,
        };
        assert_eq!(c.filename(), "netty-all-4.1.109.Final.jar");
    }
}
