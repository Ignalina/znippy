use once_cell::sync::Lazy;
use sysinfo::{System, RefreshKind, MemoryRefreshKind};

pub struct StrategicConfig {
    pub max_core_in_flight: usize,
    pub max_core_in_compress: usize,
    pub max_mem_allowed: u64,
    pub min_free_memory_ratio: f32,
}

pub static CONFIG: Lazy<StrategicConfig> = Lazy::new(strategic_config);

fn strategic_config() -> StrategicConfig {
    let refresh = RefreshKind::everything().with_memory(MemoryRefreshKind::everything());
    let mut sys = System::new_with_specifics(refresh);
    sys.refresh_memory();

    let total_memory = sys.total_memory();
    let cores = sysinfo::System::physical_core_count().unwrap_or_else(|| sys.cpus().len());

    let max_core_in_flight = ((cores as f32) * 0.10).ceil() as usize;
    let max_core_in_compress = cores.saturating_sub(max_core_in_flight);
    let min_free_memory_ratio = 0.25;

    eprintln!(
        "[strategic_config] Detekterade {} kärnor och {} MiB minne",
        cores,
        total_memory / 1024
    );
    eprintln!(
        "[strategic_config] max_core_in_flight: {} (10%)",
        max_core_in_flight
    );
    eprintln!(
        "[strategic_config] max_core_in_compress: {} (90%)",
        max_core_in_compress
    );
    eprintln!(
        "[strategic_config] min_free_memory_ratio: {:.0}%",
        min_free_memory_ratio * 100.0
    );

    StrategicConfig {
        max_core_in_flight,
        max_core_in_compress,
        max_mem_allowed: total_memory,
        min_free_memory_ratio,
    }
}
