use std::collections::HashMap;
use rayon::prelude::*;
use znippy_common::plugin::{ArchiveTypePlugin, BatchItem, ExtensionRow, ExtensionValue};
use crate::pom::parse_pom_project;

pub struct NativeMavenPlugin;

impl ArchiveTypePlugin for NativeMavenPlugin {
    fn name(&self) -> &str {
        "maven"
    }

    fn type_id(&self) -> i8 {
        1
    }

    fn matches_path(&self, path: &str) -> bool {
        path.ends_with(".jar")
            || path.ends_with(".war")
            || path.ends_with(".ear")
            || path.ends_with(".pom")
    }

    fn extract_metadata(&self, path: &str, data: &[u8]) -> Option<ExtensionRow> {
        let coord = if path.ends_with(".pom") {
            parse_pom_project(data)?
        } else {
            let entries = ljar::decompress_jar_filter(data, "pom.xml").ok()?;
            let entry = entries
                .iter()
                .find(|e| e.name.starts_with("META-INF/maven/") && e.name.ends_with("/pom.xml"))?;
            parse_pom_project(&entry.data)?
        };

        Some(make_row(coord))
    }

    fn supports_batch(&self) -> bool {
        true
    }

    fn batch_threshold(&self) -> usize {
        200 * 1024 * 1024
    }

    fn extract_batch(&self, items: &[BatchItem<'_>]) -> Vec<Option<ExtensionRow>> {
        ljar::thread_pool().install(|| {
            items.par_iter().map(|item| self.extract_metadata(item.path, item.data)).collect()
        })
    }
}

fn make_row(coord: crate::pom::MavenCoord) -> ExtensionRow {
    let mut fields = HashMap::new();
    fields.insert("group_id".into(), ExtensionValue::Str(coord.group_id));
    fields.insert("artifact_id".into(), ExtensionValue::Str(coord.artifact_id));
    fields.insert("version".into(), ExtensionValue::Str(coord.version));
    fields.insert("packaging".into(), ExtensionValue::Str(coord.packaging));
    ExtensionRow { fields }
}
