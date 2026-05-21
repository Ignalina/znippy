//! WASM plugin loader — loads .wasm plugins at runtime via wasmtime.
//! This is the bridge between znippy's ArchiveTypePlugin trait and WASM modules.

use crate::plugin::{ArchiveTypePlugin, ExtensionRow, ExtensionValue};
use std::collections::HashMap;

#[cfg(feature = "wasm-plugins")]
use wasmtime::*;

/// A plugin loaded from a .wasm file at runtime
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

    fn call_extract(&self, path: &str, data: &[u8]) -> Option<String> {
        let mut store = Store::new(&self.engine, ());
        let instance = Instance::new(&mut store, &self.module, &[]).ok()?;
        let memory = instance.get_memory(&mut store, "memory")?;

        // Allocate space for path
        let alloc = instance.get_typed_func::<u32, u32>(&mut store, "alloc").ok()?;
        let path_ptr = alloc.call(&mut store, path.len() as u32).ok()?;
        memory.write(&mut store, path_ptr as usize, path.as_bytes()).ok()?;

        // Allocate space for data
        let data_ptr = alloc.call(&mut store, data.len() as u32).ok()?;
        memory.write(&mut store, data_ptr as usize, data).ok()?;

        // Call extract
        let extract = instance.get_typed_func::<(u32, u32, u32, u32), u32>(&mut store, "extract").ok()?;
        let result_ptr = extract.call(&mut store, (path_ptr, path.len() as u32, data_ptr, data.len() as u32)).ok()?;

        // Read result length
        let result_len = instance.get_typed_func::<(), u32>(&mut store, "result_len").ok()?;
        let len = result_len.call(&mut store, ()).ok()? as usize;

        // Read result JSON
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
        // Parse simple JSON into ExtensionRow fields
        parse_json_to_row(&json)
    }
}

/// Simple JSON object → ExtensionRow (no serde dependency)
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
