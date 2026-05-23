//! Maven plugin for znippy — extracts GAV (groupId:artifactId:version) from JARs/POMs.
//!
//! Compiles to both native (rlib) and WASM (cdylib).
//! Uses minimal JAR central directory parsing + DEFLATE (no native deps, WASM-safe).

use std::cell::RefCell;
#[cfg(feature = "host-decompressors")]
use std::collections::HashMap;
#[cfg(feature = "host-decompressors")]
use znippy_common::plugin::{ArchiveTypePlugin, ExtensionRow, ExtensionValue};

pub mod pom;
#[cfg(feature = "resolve")]
pub mod resolver;

// ─── Maven Metadata ──────────────────────────────────────────────────

pub struct MavenMeta {
    pub group_id: String,
    pub artifact_id: String,
    pub version: String,
    pub packaging: String,
}

/// File extensions handled by the maven plugin.
/// Shared by the native `matches_path` and the WASM `plugin_extensions` export
/// so the two implementations never diverge.
pub const MAVEN_EXTENSIONS: &[&str] = &[".jar", ".war", ".ear", ".pom"];

/// True if `path` is a file the maven plugin can extract metadata from.
pub fn matches_maven_path(path: &str) -> bool {
    MAVEN_EXTENSIONS.iter().any(|ext| path.ends_with(*ext))
}

/// Index columns this module contributes, as `(column_name, arrow_type_tag)`.
/// Shared by the native `schema_fields()` and the WASM `plugin_schema` export so the two
/// can't diverge. Column names match the keys used in the extracted `ExtensionRow` / JSON.
/// Type tags understood by the host: `utf8`, `u32`, `i8`.
pub const MAVEN_SCHEMA: &[(&str, &str)] = &[
    ("group_id",    "utf8"),
    ("artifact_id", "utf8"),
    ("version",     "utf8"),
    ("packaging",   "utf8"),
];

fn parse_pom_xml(contents: &str) -> Option<MavenMeta> {
    let group_id = extract_xml_tag(contents, "groupId")?;
    let artifact_id = extract_xml_tag(contents, "artifactId")?;
    let version = extract_xml_tag(contents, "version")?;
    let packaging = extract_xml_tag(contents, "packaging").unwrap_or("jar".to_string());
    Some(MavenMeta { group_id, artifact_id, version, packaging })
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].trim().to_string())
}

// ─── Minimal JAR parser (Central Directory → pom.xml) ────────────────

const EOCD_SIG: u32 = 0x06054b50;
const CD_SIG: u32 = 0x02014b50;
const LOCAL_SIG: u32 = 0x04034b50;

fn find_pom_in_jar(data: &[u8]) -> Option<String> {
    let eocd_pos = find_eocd(data)?;
    let cd_offset = u32::from_le_bytes(data[eocd_pos + 16..eocd_pos + 20].try_into().ok()?) as usize;
    let cd_entries = u16::from_le_bytes(data[eocd_pos + 10..eocd_pos + 12].try_into().ok()?) as usize;

    let mut pos = cd_offset;
    for _ in 0..cd_entries {
        if pos + 46 > data.len() { break; }
        let sig = u32::from_le_bytes(data[pos..pos+4].try_into().ok()?);
        if sig != CD_SIG { break; }

        let compression = u16::from_le_bytes(data[pos+10..pos+12].try_into().ok()?);
        let compressed_size = u32::from_le_bytes(data[pos+20..pos+24].try_into().ok()?) as usize;
        let uncompressed_size = u32::from_le_bytes(data[pos+24..pos+28].try_into().ok()?) as usize;
        let name_len = u16::from_le_bytes(data[pos+28..pos+30].try_into().ok()?) as usize;
        let extra_len = u16::from_le_bytes(data[pos+30..pos+32].try_into().ok()?) as usize;
        let comment_len = u16::from_le_bytes(data[pos+32..pos+34].try_into().ok()?) as usize;
        let local_offset = u32::from_le_bytes(data[pos+42..pos+46].try_into().ok()?) as usize;

        let name = std::str::from_utf8(&data[pos+46..pos+46+name_len]).unwrap_or("");

        if name.contains("pom.xml") && !name.ends_with('/') {
            return decompress_entry(data, local_offset, compression, compressed_size, uncompressed_size);
        }

        pos += 46 + name_len + extra_len + comment_len;
    }
    None
}

fn find_eocd(data: &[u8]) -> Option<usize> {
    let start = data.len().saturating_sub(65557);
    for i in (start..data.len().saturating_sub(21)).rev() {
        if data.len() >= i + 4 {
            if u32::from_le_bytes(data[i..i+4].try_into().ok()?) == EOCD_SIG {
                return Some(i);
            }
        }
    }
    None
}

fn decompress_entry(data: &[u8], offset: usize, compression: u16, comp_size: usize, _uncomp_size: usize) -> Option<String> {
    if offset + 30 > data.len() { return None; }
    let sig = u32::from_le_bytes(data[offset..offset+4].try_into().ok()?);
    if sig != LOCAL_SIG { return None; }

    let name_len = u16::from_le_bytes(data[offset+26..offset+28].try_into().ok()?) as usize;
    let extra_len = u16::from_le_bytes(data[offset+28..offset+30].try_into().ok()?) as usize;
    let data_start = offset + 30 + name_len + extra_len;

    if data_start + comp_size > data.len() { return None; }
    let compressed = &data[data_start..data_start + comp_size];

    let bytes = match compression {
        0 => compressed.to_vec(),
        8 => miniz_oxide::inflate::decompress_to_vec(compressed).ok()?,
        _ => return None,
    };

    String::from_utf8(bytes).ok()
}

// ─── WASM Exports ────────────────────────────────────────────────────

thread_local! {
    static RESULT_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::new());
}

#[unsafe(no_mangle)]
pub extern "C" fn alloc(size: usize) -> *mut u8 {
    let mut buf = Vec::with_capacity(size);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

#[unsafe(no_mangle)]
pub extern "C" fn dealloc(ptr: *mut u8, size: usize) {
    unsafe { drop(Vec::from_raw_parts(ptr, 0, size)); }
}

#[unsafe(no_mangle)]
pub extern "C" fn extract(path_ptr: *const u8, path_len: usize, data_ptr: *const u8, data_len: usize) -> *const u8 {
    let path = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(path_ptr, path_len)) };
    let data = unsafe { std::slice::from_raw_parts(data_ptr, data_len) };
    let result = extract_inner(path, data);
    RESULT_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        *buf = result.into_bytes();
        buf.as_ptr()
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn result_len() -> usize {
    RESULT_BUF.with(|buf| buf.borrow().len())
}

/// Self-description: comma-separated list of file extensions this plugin handles.
/// The host calls this once at load time and caches it — matching is then a cheap
/// host-side string check, with no per-file WASM call.
#[unsafe(no_mangle)]
pub extern "C" fn plugin_extensions() -> *const u8 {
    let exts = MAVEN_EXTENSIONS.join(",");
    RESULT_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        *buf = exts.into_bytes();
        buf.as_ptr()
    })
}

/// Self-description: the index columns this plugin contributes, as `name:type` pairs
/// (e.g. `group_id:utf8,version:utf8`). The host parses this into Arrow fields once at load
/// and composes them into the on-disk schema.
#[unsafe(no_mangle)]
pub extern "C" fn plugin_schema() -> *const u8 {
    let s = MAVEN_SCHEMA
        .iter()
        .map(|(n, t)| format!("{}:{}", n, t))
        .collect::<Vec<_>>()
        .join(",");
    RESULT_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        *buf = s.into_bytes();
        buf.as_ptr()
    })
}

fn extract_inner(path: &str, data: &[u8]) -> String {
    let meta = if path.ends_with(".pom") {
        std::str::from_utf8(data).ok().and_then(parse_pom_xml)
    } else if path.ends_with(".jar") || path.ends_with(".war") || path.ends_with(".ear") {
        // Try host function first (multi-core ljar), fall back to built-in parser
        let pom_xml = find_pom_via_host(data).or_else(|| find_pom_in_jar(data));
        pom_xml.and_then(|xml| parse_pom_xml(&xml))
    } else {
        None
    };

    match meta {
        Some(m) => format!(
            r#"{{"group_id":"{}","artifact_id":"{}","version":"{}","packaging":"{}"}}"#,
            m.group_id, m.artifact_id, m.version, m.packaging
        ),
        None => "null".to_string(),
    }
}

/// Try to extract pom.xml via host_archive_* functions (available when host provides them)
fn find_pom_via_host(data: &[u8]) -> Option<String> {
    #[cfg(target_arch = "wasm32")]
    {
        unsafe {
            // Open archive with filter — host decompresses only pom.xml, skips everything else.
            let name = b"pom.xml";
            let handle = host_archive_open(
                data.as_ptr(), data.len(), 0,
                name.as_ptr(), name.len(),
            );
            if handle == 0 { return None; }

            let result = host_archive_entry(handle, name.as_ptr(), name.len());
            host_archive_close(handle);

            if result == 0 { return None; }

            let ptr = (result >> 32) as *const u8;
            let len = (result & 0xFFFFFFFF) as usize;
            let bytes = std::slice::from_raw_parts(ptr, len);
            String::from_utf8(bytes.to_vec()).ok()
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    { None }
}

// Host function imports (resolved by wasmtime linker at instantiation)
#[cfg(target_arch = "wasm32")]
unsafe extern "C" {
    fn host_archive_open(data_ptr: *const u8, data_len: usize, format: u32, filter_ptr: *const u8, filter_len: usize) -> u32;
    fn host_archive_entry(handle: u32, name_ptr: *const u8, name_len: usize) -> u64;
    fn host_archive_close(handle: u32);
}

// ─── Native API (for direct use without WASM) ────────────────────────

pub fn extract_maven_metadata(path: &str, data: &[u8]) -> Option<MavenMeta> {
    if path.ends_with(".pom") {
        std::str::from_utf8(data).ok().and_then(parse_pom_xml)
    } else if path.ends_with(".jar") || path.ends_with(".war") {
        find_pom_in_jar(data).and_then(|xml| parse_pom_xml(&xml))
    } else {
        None
    }
}

// ─── Native plugin (non-WASM, uses ljar for parallel JAR entry extraction) ──

#[cfg(feature = "host-decompressors")]
pub struct NativeMavenPlugin;

#[cfg(feature = "host-decompressors")]
impl ArchiveTypePlugin for NativeMavenPlugin {
    fn name(&self) -> &str { "maven" }
    fn type_id(&self) -> i8 { 1 }

    fn matches_path(&self, path: &str) -> bool {
        matches_maven_path(path)
    }

    fn schema_fields(&self) -> Vec<znippy_common::arrow::datatypes::Field> {
        use znippy_common::arrow::datatypes::{DataType, Field};
        MAVEN_SCHEMA.iter().map(|(name, ty)| {
            let dt = match *ty {
                "u32" => DataType::UInt32,
                "i8"  => DataType::Int8,
                _      => DataType::Utf8,
            };
            Field::new(*name, dt, true)
        }).collect()
    }

    fn extract_metadata(&self, path: &str, data: &[u8]) -> Option<ExtensionRow> {
        let meta = if path.ends_with(".pom") {
            std::str::from_utf8(data).ok().and_then(parse_pom_xml)
        } else {
            // Use ljar to filter at central-directory level — only pom.xml is decompressed.
            ljar::decompress_jar_filter(data, "pom.xml")
                .ok()
                .and_then(|entries| entries.into_iter().find(|e| e.name.contains("pom.xml")))
                .and_then(|e| std::str::from_utf8(&e.data).ok().and_then(parse_pom_xml))
        }?;

        let mut fields = HashMap::new();
        fields.insert("group_id".into(),    ExtensionValue::Str(meta.group_id));
        fields.insert("artifact_id".into(), ExtensionValue::Str(meta.artifact_id));
        fields.insert("version".into(),     ExtensionValue::Str(meta.version));
        fields.insert("packaging".into(),   ExtensionValue::Str(meta.packaging));
        Some(ExtensionRow { fields })
    }
}
