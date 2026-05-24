use std::collections::HashMap;
use rayon::prelude::*;
use znippy_common::plugin::{ArchiveTypePlugin, BatchItem, ExtensionRow, ExtensionValue};
use crate::wheel::parse_wheel_filename;

pub struct NativePythonPlugin;

impl ArchiveTypePlugin for NativePythonPlugin {
    fn name(&self) -> &str {
        "python"
    }

    fn type_id(&self) -> i8 {
        2
    }

    fn matches_path(&self, path: &str) -> bool {
        path.ends_with(".whl")
            || path.ends_with(".tar.gz")
            || path.ends_with(".zip")
    }

    fn extract_metadata(&self, path: &str, data: &[u8]) -> Option<ExtensionRow> {
        let filename = path.rsplit('/').next().unwrap_or(path);

        if filename.ends_with(".whl") {
            let info = parse_wheel_filename(filename)?;

            // Try to extract METADATA from inside the wheel for dependencies
            let metadata_content = extract_metadata_via_lzip(data);

            let mut fields = HashMap::new();
            fields.insert("name".into(), ExtensionValue::Str(info.name));
            fields.insert("version".into(), ExtensionValue::Str(info.version));
            fields.insert("python_tag".into(), ExtensionValue::Str(info.python_tag));
            fields.insert("abi_tag".into(), ExtensionValue::Str(info.abi_tag));
            fields.insert("platform_tag".into(), ExtensionValue::Str(info.platform_tag));
            if let Some(build) = info.build_tag {
                fields.insert("build_tag".into(), ExtensionValue::OptStr(Some(build)));
            }
            if let Some(meta) = metadata_content {
                // Extract Requires-Dist lines for dependency info
                let deps = parse_requires_dist(&meta);
                if !deps.is_empty() {
                    fields.insert("requires_dist".into(), ExtensionValue::StrList(deps));
                }
            }

            Some(ExtensionRow { fields })
        } else {
            // sdist (.tar.gz / .zip) — extract name/version from filename
            let stem = if filename.ends_with(".tar.gz") {
                filename.strip_suffix(".tar.gz")?
            } else {
                filename.strip_suffix(".zip")?
            };
            let (name, version) = stem.rsplit_once('-')?;

            let mut fields = HashMap::new();
            fields.insert("name".into(), ExtensionValue::Str(name.to_string()));
            fields.insert("version".into(), ExtensionValue::Str(version.to_string()));
            fields.insert("python_tag".into(), ExtensionValue::Str("source".to_string()));
            fields.insert("abi_tag".into(), ExtensionValue::Str("none".to_string()));
            fields.insert("platform_tag".into(), ExtensionValue::Str("any".to_string()));

            Some(ExtensionRow { fields })
        }
    }

    fn supports_batch(&self) -> bool {
        true
    }

    fn batch_threshold(&self) -> usize {
        200 * 1024 * 1024
    }

    fn extract_batch(&self, items: &[BatchItem<'_>]) -> Vec<Option<ExtensionRow>> {
        lzip_parallel::thread_pool().install(|| {
            items.par_iter().map(|item| self.extract_metadata(item.path, item.data)).collect()
        })
    }
}

/// Use lzip's filter_set to extract only the METADATA file from a wheel
fn extract_metadata_via_lzip(data: &[u8]) -> Option<String> {
    let entries = lzip_parallel::decompress_zip_filter(data, "METADATA").ok()?;
    let entry = entries.iter()
        .find(|e| e.name.contains(".dist-info/") && e.name.ends_with("/METADATA"))?;
    String::from_utf8(entry.data.clone()).ok()
}

/// Parse Requires-Dist lines from METADATA content
fn parse_requires_dist(metadata: &str) -> Vec<String> {
    metadata.lines()
        .filter(|line| line.starts_with("Requires-Dist:"))
        .map(|line| line.trim_start_matches("Requires-Dist:").trim().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_path() {
        let plugin = NativePythonPlugin;
        assert!(plugin.matches_path("packages/numpy/numpy-1.26.0-cp311-cp311-linux_x86_64.whl"));
        assert!(plugin.matches_path("packages/requests/requests-2.31.0.tar.gz"));
        assert!(!plugin.matches_path("packages/something.jar"));
    }

    #[test]
    fn test_extract_from_filename() {
        let plugin = NativePythonPlugin;
        let row = plugin.extract_metadata(
            "packages/numpy/numpy-1.26.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
            &[], // empty data — metadata comes from filename
        ).unwrap();
        assert_eq!(row.fields["name"], ExtensionValue::Str("numpy".into()));
        assert_eq!(row.fields["version"], ExtensionValue::Str("1.26.0".into()));
        assert_eq!(row.fields["python_tag"], ExtensionValue::Str("cp311".into()));
    }

    #[test]
    fn test_sdist_extraction() {
        let plugin = NativePythonPlugin;
        let row = plugin.extract_metadata("packages/requests/requests-2.31.0.tar.gz", &[]).unwrap();
        assert_eq!(row.fields["name"], ExtensionValue::Str("requests".into()));
        assert_eq!(row.fields["version"], ExtensionValue::Str("2.31.0".into()));
        assert_eq!(row.fields["python_tag"], ExtensionValue::Str("source".into()));
    }
}
