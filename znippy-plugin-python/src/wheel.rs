//! Wheel filename parser per PEP 427.
//!
//! Format: `{name}-{version}(-{build})?-{python}-{abi}-{platform}.whl`
//!
//! Handles all variants: cp39, pp310, py3, etc.

/// Parsed wheel filename metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct WheelInfo {
    pub name: String,
    pub version: String,
    pub build_tag: Option<String>,
    pub python_tag: String,
    pub abi_tag: String,
    pub platform_tag: String,
}

impl WheelInfo {
    /// Normalized package name per PEP 503 (lowercase, replace [-_.] with -)
    pub fn normalized_name(&self) -> String {
        normalize_name(&self.name)
    }

    /// The dist-info directory name inside the wheel
    pub fn dist_info_dir(&self) -> String {
        format!("{}-{}.dist-info", self.name, self.version)
    }
}

/// Normalize a package name per PEP 503: lowercase, replace [-_.] with -
pub fn normalize_name(s: &str) -> String {
    s.to_lowercase().replace(['_', '.', '-'], "-")
}

/// Parse a wheel filename into its components.
///
/// Returns None if the filename doesn't match the wheel naming convention.
pub fn parse_wheel_filename(filename: &str) -> Option<WheelInfo> {
    let stem = filename.strip_suffix(".whl")?;
    let parts: Vec<&str> = stem.split('-').collect();

    // Minimum: name-version-python-abi-platform (5 parts)
    // With build tag: name-version-build-python-abi-platform (6 parts)
    match parts.len() {
        5 => Some(WheelInfo {
            name: parts[0].to_string(),
            version: parts[1].to_string(),
            build_tag: None,
            python_tag: parts[2].to_string(),
            abi_tag: parts[3].to_string(),
            platform_tag: parts[4].to_string(),
        }),
        6 => Some(WheelInfo {
            name: parts[0].to_string(),
            version: parts[1].to_string(),
            build_tag: Some(parts[2].to_string()),
            python_tag: parts[3].to_string(),
            abi_tag: parts[4].to_string(),
            platform_tag: parts[5].to_string(),
        }),
        // Handle names with hyphens encoded as underscores + complex platform tags
        n if n >= 5 => {
            // Platform tag can contain dots (manylinux_2_17_x86_64.manylinux2014_x86_64)
            // which don't split on '-', so we should be fine. But version can be complex
            // like "1.0.0rc1" — still no dashes.
            // The tricky case: name might have been multi-word e.g. "my_cool_pkg"
            // PEP 427 says name uses _ for separator, so split on - is safe.
            // If we get > 6 parts, the extra parts are likely part of the platform tag
            // that somehow got a dash. Try from the end:
            let platform_tag = parts[n - 1].to_string();
            let abi_tag = parts[n - 2].to_string();
            let python_tag = parts[n - 3].to_string();

            // Check if there's a build tag (numeric prefix)
            let version_end = n - 3;
            if version_end >= 3 && parts[version_end - 1].chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
                // Might be a build tag
                let maybe_build = parts[version_end - 1];
                // Build tags start with a digit per PEP 427
                Some(WheelInfo {
                    name: parts[..version_end - 2].join("_"),
                    version: parts[version_end - 2].to_string(),
                    build_tag: Some(maybe_build.to_string()),
                    python_tag,
                    abi_tag,
                    platform_tag,
                })
            } else {
                Some(WheelInfo {
                    name: parts[..version_end - 1].join("_"),
                    version: parts[version_end - 1].to_string(),
                    build_tag: None,
                    python_tag,
                    abi_tag,
                    platform_tag,
                })
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_wheel() {
        let info = parse_wheel_filename("numpy-1.26.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl").unwrap();
        assert_eq!(info.name, "numpy");
        assert_eq!(info.version, "1.26.0");
        assert_eq!(info.build_tag, None);
        assert_eq!(info.python_tag, "cp311");
        assert_eq!(info.abi_tag, "cp311");
        assert_eq!(info.platform_tag, "manylinux_2_17_x86_64.manylinux2014_x86_64");
    }

    #[test]
    fn pure_python_wheel() {
        let info = parse_wheel_filename("requests-2.31.0-py3-none-any.whl").unwrap();
        assert_eq!(info.name, "requests");
        assert_eq!(info.version, "2.31.0");
        assert_eq!(info.python_tag, "py3");
        assert_eq!(info.abi_tag, "none");
        assert_eq!(info.platform_tag, "any");
    }

    #[test]
    fn with_build_tag() {
        let info = parse_wheel_filename("package-1.0.0-1-cp39-cp39-linux_x86_64.whl").unwrap();
        assert_eq!(info.name, "package");
        assert_eq!(info.version, "1.0.0");
        assert_eq!(info.build_tag, Some("1".to_string()));
        assert_eq!(info.python_tag, "cp39");
    }

    #[test]
    fn normalized_name() {
        let info = parse_wheel_filename("My_Cool.Package-1.0-py3-none-any.whl").unwrap();
        assert_eq!(info.normalized_name(), "my-cool-package");
    }

    #[test]
    fn not_a_wheel() {
        assert!(parse_wheel_filename("requests-2.31.0.tar.gz").is_none());
        assert!(parse_wheel_filename("something.zip").is_none());
    }

    #[test]
    fn dist_info_dir() {
        let info = parse_wheel_filename("numpy-1.26.0-cp311-cp311-linux_x86_64.whl").unwrap();
        assert_eq!(info.dist_info_dir(), "numpy-1.26.0.dist-info");
    }
}
