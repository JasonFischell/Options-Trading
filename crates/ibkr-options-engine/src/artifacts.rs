use std::path::{Path, PathBuf};

use chrono::Local;

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .map(|path| path.to_path_buf())
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

pub fn docs_dir() -> PathBuf {
    workspace_root().join("docs")
}

pub fn logs_dir() -> PathBuf {
    workspace_root().join("logs")
}

pub fn logs_subdir(name: &str) -> PathBuf {
    logs_dir().join(name)
}

pub fn timestamped_log_path(name: &str, file_stem: &str, extension: &str) -> PathBuf {
    timestamped_log_path_in(&logs_dir(), name, file_stem, extension)
}

pub fn timestamped_log_path_in(
    root: &Path,
    name: &str,
    file_stem: &str,
    extension: &str,
) -> PathBuf {
    root.join(name).join(format!(
        "{}_{}_Log.{}",
        Local::now().format("%Y%m%d-%H-%M-%S"),
        file_stem,
        extension
    ))
}
