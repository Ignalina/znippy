use std::cmp::{max, min};
use once_cell::sync::Lazy;
use sysinfo::{System, RefreshKind, MemoryRefreshKind};
use crate::{int_ring, ChunkQueue, RingBuffer};
use crate::int_ring::MINI_SIZE;

impl StrategicConfig {
    pub fn file_split_block_size_usize(&self) -> usize {
        self.file_split_block_size.try_into().unwrap()
    }
}
#[derive(Debug, Clone)] //
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

pub static CONFIG: Lazy<StrategicConfig> = Lazy::new(strategic_config_mini);

fn strategic_config_mini() -> StrategicConfig {
    let refresh = RefreshKind::everything().with_memory(MemoryRefreshKind::everything());
    let mut sys = System::new_with_specifics(refresh);
    sys.refresh_memory();

    let total_memory = sys.total_memory();
    let cores = sysinfo::System::physical_core_count().unwrap_or_else(|| sys.cpus().len());

    let max_core_in_flight = ((cores as f32) * 0.90).ceil() as usize;
    let max_core_in_compress = cores.saturating_sub(max_core_in_flight);
    let min_free_memory_ratio = 0.0f32;
    let compression_level = 19;
    let max_mem_allowed= ((total_memory as f32) * (1.0 - min_free_memory_ratio)) as u64;
    let file_split_block_size=10 * 1024 * 1024;
    let zstd_output_buffer_size = 1 * 1024 * 1024;
    
    let max_chunks:u32= min((max_mem_allowed / file_split_block_size), int_ring::MINI_SIZE as u64) as u32;

    
    log::info!(
        "[strategic_config] Detekterade {} kärnor och {} MiB minne",
        cores,
        total_memory / 1024
    );

    let sc=StrategicConfig {
        max_core_in_flight,
        max_core_in_compress,
        max_mem_allowed,
        min_free_memory_ratio,
        file_split_block_size,
        compression_level,
        max_chunks,
        zstd_output_buffer_size
    };
    log_strategic_conf(&sc);    
    sc 
}


fn log_strategic_conf (sc: &StrategicConfig) {

    log::info!(
        "[strategic_config] max_core_in_flight: {} (10%)",
        sc.max_core_in_flight
    );

    log::info!(
        "[strategic_config] max_core_in_compress: {} (90%)",
        sc.max_core_in_compress
    );
    log::info!(
        "[strategic_config] min_free_memory_ratio: {:.0}%",
        sc.min_free_memory_ratio * 100.0
    );
    log::info!(
        "[strategic_config] compression_level: {}",
        sc.compression_level
    );

    log::info!(
        "[strategic_config] max_chunks: {}",
        sc.max_chunks
    );

    log::info!(
        "[strategic_config] zstd_output_buffer_size: {}",
        sc.zstd_output_buffer_size
    );


}