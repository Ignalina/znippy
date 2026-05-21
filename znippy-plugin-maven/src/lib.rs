//! Maven plugin for znippy — extracts GAV (groupId:artifactId:version) from JARs/POMs.
//!
//! This crate compiles to both native (rlib) and WASM (cdylib).
//! When loaded as WASM, znippy calls the exported functions via wasmtime.
//!
//! WASM ABI contract:
//!   - `extract(path_ptr, path_len, data_ptr, data_len) -> ptr` to JSON result
//!   - `alloc(size) -> ptr` for host to write input data
//!   - `dealloc(ptr, size)` for cleanup

use std::io::Read;

/// Maven coordinates
struct MavenMeta {
    group_id: String,
    artifact_id: String,
    version: String,
    packaging: String,
}

/// Parse pom.xml content (simple XML extraction, no full parser)
fn parse_pom_xml(contents: &str) -> Option<MavenMeta> {
    let group_id = extract_xml_tag(contents, "groupId")?;
    let artifact_id = extract_xml_tag(contents, "artifactId")?;
    let version = extract_xml_tag(contents, "version")?;
    let packaging = extract_xml_tag(contents, "packaging").unwrap_or("jar".to_string());
    Some(MavenMeta { group_id, artifact_id, version, packaging })
}

/// Extract text between <tag>...</tag> (first occurrence, top-level only)
fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].trim().to_string())
}

/// Try to read pom.xml from a JAR (zip) file
fn parse_jar(data: &[u8]) -> Option<MavenMeta> {
    let reader = std::io::Cursor::new(data);
    let mut archive = zip::ZipArchive::new(reader).ok()?;

    // Look for META-INF/maven/**/pom.xml
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).ok()?;
        if file.name().contains("pom.xml") {
            let mut contents = String::new();
            file.read_to_string(&mut contents).ok()?;
            return parse_pom_xml(&contents);
        }
    }
    None
}

// ─── WASM Exports ────────────────────────────────────────────────────

use std::cell::RefCell;

thread_local! {
    static RESULT_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::new());
}

/// Allocate memory for host to write input data
#[unsafe(no_mangle)]
pub extern "C" fn alloc(size: usize) -> *mut u8 {
    let mut buf = Vec::with_capacity(size);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

/// Free allocated memory
#[unsafe(no_mangle)]
pub extern "C" fn dealloc(ptr: *mut u8, size: usize) {
    unsafe { drop(Vec::from_raw_parts(ptr, 0, size)); }
}

/// Main entry: extract metadata from file at (path_ptr, path_len) with data at (data_ptr, data_len).
/// Returns pointer to JSON result.
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

/// Returns the length of the last result
#[unsafe(no_mangle)]
pub extern "C" fn result_len() -> usize {
    RESULT_BUF.with(|buf| buf.borrow().len())
}

fn extract_inner(path: &str, data: &[u8]) -> String {
    let meta = if path.ends_with(".pom") {
        let contents = std::str::from_utf8(data).ok();
        contents.and_then(|c| parse_pom_xml(c))
    } else if path.ends_with(".jar") {
        parse_jar(data)
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

// ─── Native Plugin Interface ─────────────────────────────────────────

/// For native usage (non-WASM): direct call
pub fn extract_maven_metadata(path: &str, data: &[u8]) -> Option<MavenMeta> {
    if path.ends_with(".pom") {
        let contents = std::str::from_utf8(data).ok()?;
        parse_pom_xml(contents)
    } else if path.ends_with(".jar") {
        parse_jar(data)
    } else {
        None
    }
}
