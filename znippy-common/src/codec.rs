//! Codec layer: OpenZL compression/decompression.
//! OpenZL is znippy's codec — it wraps zstd+lz4 with improved framing.

use anyhow::{Result, anyhow};

// ─── Compression Context ────────────────────────────────────────────

pub struct CompressCtx {
    cctx: openzl_sys_rs::ZlCCtx,
    level: i32,
}

unsafe impl Send for CompressCtx {}

impl CompressCtx {
    pub fn new(compression_level: i32) -> Result<Self> {
        use openzl_sys_rs::*;
        let mut cctx = ZlCCtx::new().ok_or_else(|| anyhow!("ZL_CCtx_create failed"))?;
        let version = unsafe { ZL_getDefaultEncodingVersion() } as i32;
        // stickyParameters=1 keeps params across compress calls (reuse ctx)
        cctx.set_parameter(ZL_CParam_ZL_CParam_stickyParameters, 1)
            .map_err(|e| anyhow!(e))?;
        cctx.set_parameter(ZL_CParam_ZL_CParam_formatVersion, version)
            .map_err(|e| anyhow!(e))?;
        cctx.set_parameter(ZL_CParam_ZL_CParam_compressionLevel, compression_level)
            .map_err(|e| anyhow!(e))?;
        Ok(Self { cctx, level: compression_level })
    }

    pub fn compress(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        use openzl_sys_rs::zl_compress_bound;
        let bound = zl_compress_bound(input.len());
        let mut output = vec![0u8; bound];
        let compressed_size = self.cctx.compress(&mut output, input)
            .map_err(|e| anyhow!(e))?;
        output.truncate(compressed_size);
        Ok(output)
    }
}

pub fn decompress_frame(compressed: &[u8]) -> Result<Vec<u8>> {
    use openzl_sys_rs::*;
    let decompressed_size = zl_get_decompressed_size(compressed)
        .map_err(|e| anyhow!("OpenZL getDecompressedSize: {}", e))?;
    let mut output = vec![0u8; decompressed_size];
    let written = zl_decompress(&mut output, compressed)
        .map_err(|e| anyhow!("OpenZL decompress: {}", e))?;
    output.truncate(written);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let mut ctx = CompressCtx::new(3).unwrap();
        let input = b"Hello world! This is a test of compression roundtrip. Repeated data helps compression. Repeated data helps compression. Repeated data helps compression.";
        let compressed = ctx.compress(input).unwrap();
        println!("Compressed {} -> {} bytes", input.len(), compressed.len());
        let decompressed = decompress_frame(&compressed).unwrap();
        assert_eq!(&decompressed[..], &input[..]);
    }

    #[test]
    fn test_multi_compress_same_ctx() {
        let mut ctx = CompressCtx::new(3).unwrap();
        for i in 0..10 {
            let input: Vec<u8> = (0..4096).map(|x| ((x + i) % 251) as u8).collect();
            let compressed = ctx.compress(&input).unwrap();
            let decompressed = decompress_frame(&compressed).unwrap();
            assert_eq!(decompressed, input, "Failed at iteration {}", i);
        }
        println!("10 sequential compress calls OK");
    }

    #[test]
    fn test_parallel_contexts() {
        let handles: Vec<_> = (0..8).map(|t| {
            std::thread::spawn(move || {
                let mut ctx = CompressCtx::new(3).unwrap();
                for i in 0..5 {
                    let input: Vec<u8> = (0..8192).map(|x| ((x + i + t*100) % 251) as u8).collect();
                    let compressed = ctx.compress(&input).unwrap();
                    let decompressed = decompress_frame(&compressed).unwrap();
                    assert_eq!(decompressed, input);
                }
            })
        }).collect();
        for h in handles {
            h.join().unwrap();
        }
        println!("8 parallel contexts x 5 calls each OK");
    }
}
