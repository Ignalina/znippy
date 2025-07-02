use std::cmp::max;
use once_cell::sync::Lazy;
use sysinfo::{System, RefreshKind, MemoryRefreshKind};
impl StrategicConfig {
    pub fn file_split_block_size_usize(&self) -> usize {
        self.file_split_block_size.try_into().unwrap()
    }
}
pub struct StrategicConfig {
    pub max_core_in_flight: usize,
    pub max_core_in_compress: usize,
    pub max_mem_allowed: u64,
    pub min_free_memory_ratio: f32,
    pub file_split_block_size: u64,
    pub max_chunks: u32,
    pub compression_level: i32,
    pub zstd_output_buffer_size: usize,
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
    let compression_level = 19;
    let max_mem_allowed= ((total_memory as f32) * (1.0 - min_free_memory_ratio)) as u64;
    let file_split_block_size=10 * 1024 * 1024;
    let zstd_output_buffer_size = 1 * 1024 * 1024;
    
    let max_chunks:u32= (max_mem_allowed / file_split_block_size) as u32;
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
    eprintln!(
        "[strategic_config] compression_level: {}",
        compression_level
    );

    eprintln!(
        "[strategic_config] max_chunks: {}",
        max_chunks
    );

    eprintln!(
        "[strategic_config] zstd_output_buffer_size: {}",
        zstd_output_buffer_size
    );


    StrategicConfig {
        max_core_in_flight,
        max_core_in_compress,
        max_mem_allowed,
        min_free_memory_ratio,
        file_split_block_size,
        compression_level,
        max_chunks,
        zstd_output_buffer_size
    }
}
