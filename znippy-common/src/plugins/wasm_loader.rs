//! WASM plugin loader with host-provided parallel decompression services.
//!
//! Host functions give WASM plugins access to native multi-core decompressors
//! (ljar-rs, lbzip2-rs, lgz-rs) without the plugins needing threading support.

use crate::plugin::{ArchiveTypePlugin, ExtensionRow, ExtensionValue};
use std::collections::HashMap;

#[cfg(feature = "wasm-plugins")]
use wasmtime::*;

// ─── Host State (shared decompression services) ──────────────────────

/// Codec identifiers for host_decompress
#[repr(u32)]
pub enum HostCodec {
    Deflate = 0,
    Gzip = 1,    // lgz-rs (parallel)
    Bzip2 = 2,   // lbzip2-rs (workers per core)
    Zstd = 3,
}

/// Archive format identifiers for host_archive_open
#[repr(u32)]
pub enum ArchiveFormat {
    Jar = 0,      // ljar-rs (parallel JAR/ZIP)
    TarGz = 1,   // tar + lgz
    TarBz2 = 2,  // tar + lbzip2
}

/// An opened archive handle — holds decompressed entries
struct OpenArchive {
    entries: Vec<ArchiveEntry>,
}

struct ArchiveEntry {
    name: String,
    data: Vec<u8>,
}

/// Host state accessible from WASM via host functions
struct HostState {
    /// Currently open archives (handle → entries)
    archives: HashMap<u32, OpenArchive>,
    next_handle: u32,
}

impl HostState {
    fn new() -> Self {
        Self { archives: HashMap::new(), next_handle: 1 }
    }
}

// ─── WASM Plugin ─────────────────────────────────────────────────────

#[cfg(feature = "wasm-plugins")]
pub struct WasmPlugin {
    name: String,
    type_id: i8,
    engine: Engine,
    module: Module,
}

#[cfg(feature = "wasm-plugins")]
impl WasmPlugin {
    /// Load a WASM plugin from file
    pub fn load(wasm_path: &str, name: &str, type_id: i8) -> anyhow::Result<Self> {
        let engine = Engine::default();
        let module = Module::from_file(&engine, wasm_path)?;
        Ok(Self { name: name.to_string(), type_id, engine, module })
    }

    /// Load a WASM plugin from bytes
    pub fn load_bytes(wasm_bytes: &[u8], name: &str, type_id: i8) -> anyhow::Result<Self> {
        let engine = Engine::default();
        let module = Module::new(&engine, wasm_bytes)?;
        Ok(Self { name: name.to_string(), type_id, engine, module })
    }

    fn call_extract(&self, path: &str, data: &[u8]) -> Option<String> {
        let host_state = HostState::new();
        let mut store = Store::new(&self.engine, host_state);

        // Build linker with host functions
        let mut linker = Linker::new(&self.engine);
        register_host_functions(&mut linker).ok()?;

        let instance = linker.instantiate(&mut store, &self.module).ok()?;
        let memory = instance.get_memory(&mut store, "memory")?;

        // Allocate + write path
        let alloc = instance.get_typed_func::<u32, u32>(&mut store, "alloc").ok()?;
        let path_ptr = alloc.call(&mut store, path.len() as u32).ok()?;
        memory.write(&mut store, path_ptr as usize, path.as_bytes()).ok()?;

        // Allocate + write data
        let data_ptr = alloc.call(&mut store, data.len() as u32).ok()?;
        memory.write(&mut store, data_ptr as usize, data).ok()?;

        // Call extract
        let extract = instance.get_typed_func::<(u32, u32, u32, u32), u32>(&mut store, "extract").ok()?;
        let result_ptr = extract.call(&mut store, (path_ptr, path.len() as u32, data_ptr, data.len() as u32)).ok()?;

        // Read result
        let result_len = instance.get_typed_func::<(), u32>(&mut store, "result_len").ok()?;
        let len = result_len.call(&mut store, ()).ok()? as usize;

        let mut buf = vec![0u8; len];
        memory.read(&store, result_ptr as usize, &mut buf).ok()?;
        String::from_utf8(buf).ok()
    }
}

#[cfg(feature = "wasm-plugins")]
impl ArchiveTypePlugin for WasmPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn type_id(&self) -> i8 {
        self.type_id
    }

    fn extract_metadata(&self, path: &str, data: &[u8]) -> Option<ExtensionRow> {
        let json = self.call_extract(path, data)?;
        if json == "null" {
            return None;
        }
        parse_json_to_row(&json)
    }
}

// ─── Host Functions (native decompression services) ──────────────────

#[cfg(feature = "wasm-plugins")]
fn register_host_functions(linker: &mut Linker<HostState>) -> anyhow::Result<()> {
    // host_decompress(data_ptr, data_len, codec) -> result_ptr
    // Writes decompressed bytes into WASM memory, returns (ptr << 32 | len)
    linker.func_wrap(
        "env",
        "host_decompress",
        |mut caller: Caller<'_, HostState>, data_ptr: u32, data_len: u32, codec: u32| -> u64 {
            let memory = match caller.get_export("memory") {
                Some(Extern::Memory(m)) => m,
                _ => return 0,
            };

            // Read input data from WASM memory
            let mut input = vec![0u8; data_len as usize];
            if memory.read(&caller, data_ptr as usize, &mut input).is_err() {
                return 0;
            }

            // Decompress using native multi-core implementations
            let decompressed = match codec {
                0 => { // Deflate
                    miniz_oxide_decompress(&input)
                }
                1 => { // Gzip (lgz-rs when available)
                    gzip_decompress(&input)
                }
                2 => { // Bzip2 (lbzip2-rs when available)
                    bzip2_decompress(&input)
                }
                3 => { // Zstd
                    zstd_decompress(&input)
                }
                _ => return 0,
            };

            let decompressed = match decompressed {
                Some(d) => d,
                None => return 0,
            };

            // Allocate in WASM memory and write result
            let alloc = match caller.get_export("alloc") {
                Some(Extern::Func(f)) => f,
                _ => return 0,
            };
            let mut results = [Val::I32(0)];
            if alloc.call(&mut caller, &[Val::I32(decompressed.len() as i32)], &mut results).is_err() {
                return 0;
            }
            let out_ptr = results[0].unwrap_i32() as u32;

            let memory = match caller.get_export("memory") {
                Some(Extern::Memory(m)) => m,
                _ => return 0,
            };
            if memory.write(&mut caller, out_ptr as usize, &decompressed).is_err() {
                return 0;
            }

            // Pack ptr and len into u64: high 32 = ptr, low 32 = len
            ((out_ptr as u64) << 32) | (decompressed.len() as u64)
        },
    )?;

    // host_archive_open(data_ptr, data_len, format) -> handle
    linker.func_wrap(
        "env",
        "host_archive_open",
        |mut caller: Caller<'_, HostState>, data_ptr: u32, data_len: u32, format: u32| -> u32 {
            let memory = match caller.get_export("memory") {
                Some(Extern::Memory(m)) => m,
                _ => return 0,
            };

            let mut input = vec![0u8; data_len as usize];
            if memory.read(&caller, data_ptr as usize, &mut input).is_err() {
                return 0;
            }

            let entries = match format {
                0 => jar_list_entries(&input),  // JAR/ZIP via ljar-rs
                1 => tar_gz_list_entries(&input),
                2 => tar_bz2_list_entries(&input),
                _ => return 0,
            };

            let entries = match entries {
                Some(e) => e,
                None => return 0,
            };

            let state = caller.data_mut();
            let handle = state.next_handle;
            state.next_handle += 1;
            state.archives.insert(handle, OpenArchive { entries });
            handle
        },
    )?;

    // host_archive_list(handle) -> result_ptr (JSON array of names written to WASM mem)
    linker.func_wrap(
        "env",
        "host_archive_list",
        |mut caller: Caller<'_, HostState>, handle: u32| -> u64 {
            let names: Vec<String> = {
                let state = caller.data();
                match state.archives.get(&handle) {
                    Some(archive) => archive.entries.iter().map(|e| e.name.clone()).collect(),
                    None => return 0,
                }
            };

            // Simple JSON array
            let json = format!("[{}]",
                names.iter().map(|n| format!("\"{}\"", n)).collect::<Vec<_>>().join(",")
            );

            write_to_wasm(&mut caller, json.as_bytes())
        },
    )?;

    // host_archive_entry(handle, name_ptr, name_len) -> result_ptr (raw bytes)
    linker.func_wrap(
        "env",
        "host_archive_entry",
        |mut caller: Caller<'_, HostState>, handle: u32, name_ptr: u32, name_len: u32| -> u64 {
            let memory = match caller.get_export("memory") {
                Some(Extern::Memory(m)) => m,
                _ => return 0,
            };

            let mut name_buf = vec![0u8; name_len as usize];
            if memory.read(&caller, name_ptr as usize, &mut name_buf).is_err() {
                return 0;
            }
            let name = match std::str::from_utf8(&name_buf) {
                Ok(s) => s.to_string(),
                Err(_) => return 0,
            };

            let data: Option<Vec<u8>> = {
                let state = caller.data();
                state.archives.get(&handle)
                    .and_then(|a| a.entries.iter().find(|e| e.name.contains(&name)))
                    .map(|e| e.data.clone())
            };

            match data {
                Some(d) => write_to_wasm(&mut caller, &d),
                None => 0,
            }
        },
    )?;

    // host_archive_close(handle)
    linker.func_wrap(
        "env",
        "host_archive_close",
        |mut caller: Caller<'_, HostState>, handle: u32| {
            caller.data_mut().archives.remove(&handle);
        },
    )?;

    Ok(())
}

// ─── Write helper ────────────────────────────────────────────────────

#[cfg(feature = "wasm-plugins")]
fn write_to_wasm(caller: &mut Caller<'_, HostState>, data: &[u8]) -> u64 {
    let alloc = match caller.get_export("alloc") {
        Some(Extern::Func(f)) => f,
        _ => return 0,
    };
    let mut results = [Val::I32(0)];
    if alloc.call(&mut *caller, &[Val::I32(data.len() as i32)], &mut results).is_err() {
        return 0;
    }
    let out_ptr = results[0].unwrap_i32() as u32;

    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(m)) => m,
        _ => return 0,
    };
    if memory.write(&mut *caller, out_ptr as usize, data).is_err() {
        return 0;
    }

    ((out_ptr as u64) << 32) | (data.len() as u64)
}

// ─── Native decompression backends ──────────────────────────────────
// Each one uses the parallel native library when available,
// falls back to single-threaded otherwise.

fn miniz_oxide_decompress(data: &[u8]) -> Option<Vec<u8>> {
    miniz_oxide::inflate::decompress_to_vec(data).ok()
}

fn gzip_decompress(data: &[u8]) -> Option<Vec<u8>> {
    // TODO: wire lgz-rs here for parallel gzip decompression
    // Fallback: single-threaded via miniz_oxide
    miniz_oxide::inflate::decompress_to_vec_zlib(data).ok()
}

fn bzip2_decompress(data: &[u8]) -> Option<Vec<u8>> {
    // TODO: wire lbzip2-rs here (workers-per-core, no rayon)
    // Fallback: not available without lbzip2 dep
    None
}

fn zstd_decompress(data: &[u8]) -> Option<Vec<u8>> {
    #[cfg(feature = "zstd")]
    {
        crate::codec::decompress_frame(data).ok()
    }
    #[cfg(not(feature = "zstd"))]
    { None }
}

/// List entries in a JAR/ZIP using ljar-rs central directory scanner
fn jar_list_entries(data: &[u8]) -> Option<Vec<ArchiveEntry>> {
    // TODO: wire ljar::decompress_jar(data) here for parallel multi-core extraction
    // For now: use minimal central directory scan + single-entry inflate
    // (same approach as znippy-plugin-maven's internal parser)
    //
    // When ljar-rs gets a `no_rayon` feature or we link it natively here,
    // this becomes: ljar::decompress_jar(data).map(|entries| entries.into_iter()...)
    minimal_jar_entries(data)
}

fn tar_gz_list_entries(data: &[u8]) -> Option<Vec<ArchiveEntry>> {
    // TODO: lgz decompress → tar parse
    None
}

fn tar_bz2_list_entries(data: &[u8]) -> Option<Vec<ArchiveEntry>> {
    // TODO: lbzip2 decompress → tar parse
    None
}

// ─── Minimal JAR parser (for bootstrap until ljar native link) ───────

fn minimal_jar_entries(data: &[u8]) -> Option<Vec<ArchiveEntry>> {
    const EOCD_SIG: u32 = 0x06054b50;
    const CD_SIG: u32 = 0x02014b50;
    const LOCAL_SIG: u32 = 0x04034b50;

    // Find EOCD
    let start = data.len().saturating_sub(65557);
    let mut eocd_pos = None;
    for i in (start..data.len().saturating_sub(21)).rev() {
        if u32::from_le_bytes(data[i..i+4].try_into().ok()?) == EOCD_SIG {
            eocd_pos = Some(i);
            break;
        }
    }
    let eocd_pos = eocd_pos?;
    let cd_offset = u32::from_le_bytes(data[eocd_pos+16..eocd_pos+20].try_into().ok()?) as usize;
    let cd_entries = u16::from_le_bytes(data[eocd_pos+10..eocd_pos+12].try_into().ok()?) as usize;

    let mut entries = Vec::with_capacity(cd_entries);
    let mut pos = cd_offset;

    for _ in 0..cd_entries {
        if pos + 46 > data.len() { break; }
        let sig = u32::from_le_bytes(data[pos..pos+4].try_into().ok()?);
        if sig != CD_SIG { break; }

        let compression = u16::from_le_bytes(data[pos+10..pos+12].try_into().ok()?);
        let comp_size = u32::from_le_bytes(data[pos+20..pos+24].try_into().ok()?) as usize;
        let uncomp_size = u32::from_le_bytes(data[pos+24..pos+28].try_into().ok()?) as usize;
        let name_len = u16::from_le_bytes(data[pos+28..pos+30].try_into().ok()?) as usize;
        let extra_len = u16::from_le_bytes(data[pos+30..pos+32].try_into().ok()?) as usize;
        let comment_len = u16::from_le_bytes(data[pos+32..pos+34].try_into().ok()?) as usize;
        let local_offset = u32::from_le_bytes(data[pos+42..pos+46].try_into().ok()?) as usize;

        let name = std::str::from_utf8(&data[pos+46..pos+46+name_len]).unwrap_or("").to_string();

        if !name.ends_with('/') {
            // Decompress from local file header
            if local_offset + 30 <= data.len() {
                let lsig = u32::from_le_bytes(data[local_offset..local_offset+4].try_into().ok()?);
                if lsig == LOCAL_SIG {
                    let ln = u16::from_le_bytes(data[local_offset+26..local_offset+28].try_into().ok()?) as usize;
                    let le = u16::from_le_bytes(data[local_offset+28..local_offset+30].try_into().ok()?) as usize;
                    let ds = local_offset + 30 + ln + le;

                    if ds + comp_size <= data.len() {
                        let raw = &data[ds..ds + comp_size];
                        let entry_data = match compression {
                            0 => raw.to_vec(),
                            8 => miniz_oxide::inflate::decompress_to_vec(raw).unwrap_or_default(),
                            _ => Vec::new(),
                        };
                        entries.push(ArchiveEntry { name, data: entry_data });
                    }
                }
            }
        }

        pos += 46 + name_len + extra_len + comment_len;
    }

    Some(entries)
}

// ─── JSON parser ─────────────────────────────────────────────────────

fn parse_json_to_row(json: &str) -> Option<ExtensionRow> {
    let json = json.trim().trim_start_matches('{').trim_end_matches('}');
    let mut fields = HashMap::new();
    for pair in json.split(',') {
        let mut kv = pair.splitn(2, ':');
        let key = kv.next()?.trim().trim_matches('"').to_string();
        let val = kv.next()?.trim().trim_matches('"').to_string();
        fields.insert(key, ExtensionValue::Str(val));
    }
    Some(ExtensionRow { fields })
}
