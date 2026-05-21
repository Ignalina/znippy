//! Codec abstraction: unified compress/decompress API for zstd and openzl backends.
//! Features are mutually exclusive: enable either "zstd" (default) or "openzl".

use anyhow::{Result, anyhow};

#[cfg(all(feature = "zstd", feature = "openzl"))]
compile_error!("Features 'zstd' and 'openzl' are mutually exclusive. Enable only one.");

// ─── Compression Context ────────────────────────────────────────────

#[cfg(feature = "zstd")]
mod inner {
    use anyhow::{Result, anyhow};

    pub struct CompressCtx {
        cctx: *mut zstd_sys_rs::ZSTD_CCtx,
    }

    unsafe impl Send for CompressCtx {}

    impl CompressCtx {
        pub fn new(compression_level: i32) -> Result<Self> {
            use zstd_sys_rs::*;
            let cctx = unsafe { ZSTD_createCCtx() };
            if cctx.is_null() {
                return Err(anyhow!("ZSTD_createCCtx failed"));
            }
            unsafe {
                ZSTD_CCtx_setParameter(
                    cctx,
                    ZSTD_cParameter::ZSTD_c_compressionLevel,
                    compression_level,
                );
            }
            Ok(Self { cctx })
        }

        pub fn compress(&mut self, input: &[u8]) -> Result<Vec<u8>> {
            use zstd_sys_rs::*;
            unsafe {
                let bound = ZSTD_compressBound(input.len());
                let mut output = vec![0u8; bound];
                let compressed_size = ZSTD_compress2(
                    self.cctx,
                    output.as_mut_ptr() as *mut _,
                    output.len(),
                    input.as_ptr() as *const _,
                    input.len(),
                );
                if ZSTD_isError(compressed_size) != 0 {
                    let msg = std::ffi::CStr::from_ptr(ZSTD_getErrorName(compressed_size));
                    return Err(anyhow!("ZSTD_compress2 failed: {}", msg.to_string_lossy()));
                }
                output.truncate(compressed_size);
                Ok(output)
            }
        }
    }

    impl Drop for CompressCtx {
        fn drop(&mut self) {
            unsafe { zstd_sys_rs::ZSTD_freeCCtx(self.cctx) };
        }
    }

    pub fn decompress_frame(compressed: &[u8]) -> Result<Vec<u8>> {
        use zstd_sys_rs::*;
        unsafe {
            let size = ZSTD_getFrameContentSize(compressed.as_ptr() as *const _, compressed.len());
            if size == ZSTD_CONTENTSIZE_ERROR as u64 || size == ZSTD_CONTENTSIZE_UNKNOWN as u64 {
                return Err(anyhow!("Cannot determine decompressed size"));
            }
            let mut output = vec![0u8; size as usize];
            let written = ZSTD_decompress(
                output.as_mut_ptr() as *mut _,
                output.len(),
                compressed.as_ptr() as *const _,
                compressed.len(),
            );
            if ZSTD_isError(written) != 0 {
                let msg = std::ffi::CStr::from_ptr(ZSTD_getErrorName(written));
                return Err(anyhow!("ZSTD_decompress failed: {}", msg.to_string_lossy()));
            }
            output.truncate(written);
            Ok(output)
        }
    }
}

#[cfg(feature = "openzl")]
mod inner {
    use anyhow::{Result, anyhow};

    pub struct CompressCtx {
        cctx: openzl_sys_rs::ZlCCtx,
    }

    unsafe impl Send for CompressCtx {}

    impl CompressCtx {
        pub fn new(compression_level: i32) -> Result<Self> {
            use openzl_sys_rs::*;
            let mut cctx = ZlCCtx::new().ok_or_else(|| anyhow!("ZL_CCtx_create failed"))?;
            cctx.set_parameter(ZL_CParam_ZL_CParam_compressionLevel, compression_level)
                .map_err(|e| anyhow!(e))?;
            Ok(Self { cctx })
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
}

pub use inner::CompressCtx;
pub use inner::decompress_frame;
