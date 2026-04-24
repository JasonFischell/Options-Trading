use std::path::PathBuf;

use chrono::Local;

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .map(|path| path.to_path_buf())
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

pub fn logs_dir() -> PathBuf {
    workspace_root().join("logs")
}

pub fn logs_subdir(name: &str) -> PathBuf {
    logs_dir().join(name)
}

pub fn timestamped_log_path(name: &str, file_stem: &str, extension: &str) -> PathBuf {
    logs_subdir(name).join(format!(
        "{}{}_log.{}",
        Local::now().format("%Y%m%d%H%M%S"),
        file_stem,
        extension
    ))
}
