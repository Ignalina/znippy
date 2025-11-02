use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use znippy_common::VerifyReport;

pub fn decompress_archive(archive_path: &Path, output_dir: &Path) -> Result<VerifyReport> {
    znippy_common::decompress_archive(archive_path, true, output_dir)
}
