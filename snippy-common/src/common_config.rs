use once_cell::sync::Lazy;
use sysinfo::{System, RefreshKind, MemoryRefreshKind};

pub struct StrategicConfig {
    pub max_core_in_flight: usize,
    pub max_core_in_compress: usize,
    pub max_mem_allowed: u64,
    pub min_free_memory_ratio: f32,
}
pub static CONFIG: Lazy<StrategicConfig> = Lazy::new(|| {
    let refresh = RefreshKind::everything().with_memory(MemoryRefreshKind::everything());
    let mut sys = System::new_with_specifics(refresh);
    sys.refresh_memory();

    let total_memory = sys.total_memory();
    let cores = sys.cpus().len(); // Vi använder logiska cores som fallback

    let max_core_in_flight = ((cores as f32 * 0.25).ceil()) as usize;
    let max_core_in_compress = cores.saturating_sub(max_core_in_flight);

    StrategicConfig {
        max_core_in_flight,
        max_core_in_compress,
        max_mem_allowed: total_memory,
        min_free_memory_ratio: 0.25, // ← här definieras det
    }
});
