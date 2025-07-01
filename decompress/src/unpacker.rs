use std::path::{Path, PathBuf};

use anyhow::{Result, Context};
use znippy_common::verify_archive_integrity;
use znippy_common::VerifyReport;
pub fn decompress_archive(archive_path: &Path, output_dir: &Path) -> Result<VerifyReport> {
    verify_archive_integrity(archive_path, true, Some(output_dir.to_path_buf()))
}