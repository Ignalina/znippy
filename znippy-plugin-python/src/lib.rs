pub mod wheel;

#[cfg(feature = "host-decompressors")]
mod native;
#[cfg(feature = "host-decompressors")]
pub use native::NativePythonPlugin;

use wheel::{WheelInfo, parse_wheel_filename};

/// Extract Python package metadata from a wheel filename.
/// No need to open the file — all info is encoded in the name per PEP 427.
pub fn extract_python_metadata(path: &str, _data: &[u8]) -> Option<WheelInfo> {
    let filename = path.rsplit('/').next().unwrap_or(path);
    parse_wheel_filename(filename)
}

/// Extract METADATA content from inside a wheel (zip) for PEP 658 dependency resolution.
/// Returns the raw bytes of the METADATA file if found.
pub fn extract_metadata_from_wheel(data: &[u8]) -> Option<Vec<u8>> {
    // METADATA lives at: {name}-{version}.dist-info/METADATA
    // Use substring filter since we don't know the exact dist-info prefix
    find_file_in_zip(data, "METADATA")
}

/// Extract RECORD from inside a wheel for file listing.
pub fn extract_record_from_wheel(data: &[u8]) -> Option<Vec<u8>> {
    find_file_in_zip(data, "RECORD")
}

fn find_file_in_zip(data: &[u8], needle: &str) -> Option<Vec<u8>> {
    // Minimal ZIP central directory scan — no lzip dependency needed for fallback
    let eocd = find_eocd(data)?;
    let entry_count = u16::from_le_bytes(data[eocd + 8..eocd + 10].try_into().ok()?) as usize;
    let cd_offset = u32::from_le_bytes(data[eocd + 16..eocd + 20].try_into().ok()?) as usize;

    let mut pos = cd_offset;
    for _ in 0..entry_count {
        if pos + 46 > data.len() {
            break;
        }
        if &data[pos..pos + 4] != b"PK\x01\x02" {
            break;
        }
        let method = u16::from_le_bytes(data[pos + 10..pos + 12].try_into().ok()?);
        let comp_size = u32::from_le_bytes(data[pos + 20..pos + 24].try_into().ok()?) as usize;
        let uncomp_size = u32::from_le_bytes(data[pos + 24..pos + 28].try_into().ok()?) as usize;
        let name_len = u16::from_le_bytes(data[pos + 28..pos + 30].try_into().ok()?) as usize;
        let extra_len = u16::from_le_bytes(data[pos + 30..pos + 32].try_into().ok()?) as usize;
        let comment_len = u16::from_le_bytes(data[pos + 32..pos + 34].try_into().ok()?) as usize;
        let local_off = u32::from_le_bytes(data[pos + 42..pos + 46].try_into().ok()?) as usize;

        let name = data
            .get(pos + 46..pos + 46 + name_len)
            .and_then(|b| std::str::from_utf8(b).ok())
            .unwrap_or("");

        // Match: ends with /METADATA or /RECORD (inside .dist-info/)
        if name.ends_with(&format!("/{}", needle)) && name.contains(".dist-info/") {
            if local_off + 30 > data.len() {
                return None;
            }
            if &data[local_off..local_off + 4] != b"PK\x03\x04" {
                return None;
            }
            let lname_len =
                u16::from_le_bytes(data[local_off + 26..local_off + 28].try_into().ok()?)
                    as usize;
            let lextra_len =
                u16::from_le_bytes(data[local_off + 28..local_off + 30].try_into().ok()?)
                    as usize;
            let data_start = local_off + 30 + lname_len + lextra_len;
            let raw = data.get(data_start..data_start + comp_size)?;
            return match method {
                0 => Some(raw.to_vec()),
                8 => {
                    let mut out = vec![0u8; uncomp_size];
                    let result = miniz_oxide::inflate::decompress_to_vec(raw).ok()?;
                    out = result;
                    Some(out)
                }
                _ => None,
            };
        }

        pos += 46 + name_len + extra_len + comment_len;
    }
    None
}

fn find_eocd(data: &[u8]) -> Option<usize> {
    const MIN_EOCD: usize = 22;
    if data.len() < MIN_EOCD {
        return None;
    }
    let earliest = data.len().saturating_sub(MIN_EOCD + 65535);
    for i in (earliest..=data.len() - MIN_EOCD).rev() {
        if data[i..].starts_with(b"PK\x05\x06") {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_metadata_from_path() {
        let info = extract_python_metadata(
            "packages/numpy/numpy-1.26.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
            &[],
        ).unwrap();
        assert_eq!(info.name, "numpy");
        assert_eq!(info.version, "1.26.0");
        assert_eq!(info.python_tag, "cp311");
        assert_eq!(info.abi_tag, "cp311");
        assert!(info.platform_tag.contains("manylinux"));
    }

    #[test]
    fn test_sdist_not_wheel() {
        let info = extract_python_metadata("packages/requests/requests-2.31.0.tar.gz", &[]);
        assert!(info.is_none());
    }
}
