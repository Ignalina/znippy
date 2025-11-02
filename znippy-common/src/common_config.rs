use crate::int_ring::MINI_SIZE;
use crate::{ChunkQueue, RingBuffer, int_ring};
use once_cell::sync::Lazy;
use std::cmp::{max, min};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};

impl StrategicConfig {
    pub fn file_split_block_size_usize(&self) -> usize {
        self.file_split_block_size.try_into().unwrap()
    }
}
#[derive(Debug, Clone)] //
pub struct StrategicConfig {
    pub max_core_allowed: usize,
    pub max_core_in_flight: usize,
    pub max_core_in_compress: usize,
    pub max_mem_allowed: u64,
    pub min_free_memory_ratio: f32,
    pub file_split_block_size: u64,
    pub max_chunks: u32,
    pub compression_level: i32,
    pub zstd_output_buffer_size: usize,
}

pub static CONFIG: Lazy<StrategicConfig> = Lazy::new(strategic_confi_large);

pub fn strategic_config(resource: f32) -> StrategicConfig {
    let refresh = RefreshKind::everything().with_memory(MemoryRefreshKind::everything());
    let mut sys = System::new_with_specifics(refresh);
    sys.refresh_memory();

    let total_memory = sys.total_memory();
    let max_core_allowed =
        sysinfo::System::physical_core_count().unwrap_or_else(|| sys.cpus().len());

    let max_core_in_flight = ((max_core_allowed as f32) * 0.90).ceil() as usize;
    let max_core_in_compress = max_core_allowed.saturating_sub(max_core_in_flight);
    let min_free_memory_ratio = 1.0 - resource;
    let compression_level = 19;
    let max_mem_allowed = ((total_memory as f32) * (1.0 - min_free_memory_ratio)) as u64;
    let file_split_block_size = 10 * 1024 * 1024;
    let zstd_output_buffer_size = 1 * 1024 * 1024;

    let mut max_chunks: u32 = (max_mem_allowed / file_split_block_size) as u32;

    log::info!(
        "[strategic_config] Detekterade {} kärnor och {} MiB minne",
        max_core_allowed,
        total_memory / 1024
    );

    let sc = StrategicConfig {
        max_core_allowed,
        max_core_in_flight,
        max_core_in_compress,
        max_mem_allowed,
        min_free_memory_ratio,
        file_split_block_size,
        compression_level,
        max_chunks,
        zstd_output_buffer_size,
    };
    log_strategic_conf(&sc);
    sc
}

fn strategic_config_mini() -> StrategicConfig {
    let mut sc = strategic_config(0.5);

    // Downscale to mini
    sc.max_chunks = min(sc.max_chunks as u64, int_ring::MINI_SIZE as u64) as u32;
    sc.max_core_in_flight = min(sc.max_chunks as usize, sc.max_core_in_flight);
    // TODO could add extra cores to zstd .. i guess.

    log::info!(
        "[strategic_config mini]  max_core_in_flight={}  max_core_in_compress for zstd {} max_chunks {} ",
        sc.max_core_in_flight,
        sc.max_core_in_compress,
        sc.max_chunks
    );
    log_strategic_conf(&sc);
    sc
}

fn strategic_confi_large() -> StrategicConfig {
    let mut sc = strategic_config(1.0);

    // scale to large
    sc.max_chunks = min(sc.max_chunks as u64, int_ring::LARGE_SIZE as u64) as u32;
    log::info!(
        "[strategic_config large]  max_core_in_flight={}  max_core_in_compress for zstd {} max_chunks {} ",
        sc.max_core_in_flight,
        sc.max_core_in_compress,
        sc.max_chunks
    );
    log_strategic_conf(&sc);
    sc
}

fn log_strategic_conf(sc: &StrategicConfig) {
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

    log::info!("[strategic_config] max_chunks: {}", sc.max_chunks);

    log::info!(
        "[strategic_config] zstd_output_buffer_size: {}",
        sc.zstd_output_buffer_size
    );
}
