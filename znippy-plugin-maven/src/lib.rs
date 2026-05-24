pub mod pom;
#[cfg(feature = "resolve")]
pub mod resolver;

#[cfg(feature = "host-decompressors")]
mod native;
#[cfg(feature = "host-decompressors")]
pub use native::NativeMavenPlugin;

use pom::{MavenCoord, parse_pom_project};

/// Minimal single-threaded JAR parser — no ljar dependency required.
/// Extracts Maven GAV from a JAR/WAR/EAR's embedded pom.xml, or parses a .pom directly.
pub fn extract_maven_metadata(path: &str, data: &[u8]) -> Option<MavenCoord> {
    if path.ends_with(".pom") {
        return parse_pom_project(data);
    }
    let pom_bytes = find_pom_in_jar(data)?;
    parse_pom_project(&pom_bytes)
}

fn find_pom_in_jar(data: &[u8]) -> Option<Vec<u8>> {
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
        let name_len = u16::from_le_bytes(data[pos + 28..pos + 30].try_into().ok()?) as usize;
        let extra_len = u16::from_le_bytes(data[pos + 30..pos + 32].try_into().ok()?) as usize;
        let comment_len = u16::from_le_bytes(data[pos + 32..pos + 34].try_into().ok()?) as usize;
        let local_off = u32::from_le_bytes(data[pos + 42..pos + 46].try_into().ok()?) as usize;

        let name = data
            .get(pos + 46..pos + 46 + name_len)
            .and_then(|b| std::str::from_utf8(b).ok())
            .unwrap_or("");

        if name.starts_with("META-INF/maven/") && name.ends_with("/pom.xml") {
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
                8 => miniz_oxide::inflate::decompress_to_vec(raw).ok(),
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
