//! An RAII scratch path under `CARGO_TARGET_TMPDIR`: a real disk under
//! `target/`, not a tmpfs `/tmp`, so test indexes never consume RAM. Any
//! file or directory created at the path is removed on drop, panics included.

use std::path::{Path, PathBuf};

/// An owned scratch path under `CARGO_TARGET_TMPDIR`, removed on drop.
#[derive(Debug)]
pub struct ScratchPath(Option<PathBuf>);

/// `name` under this build's integration-test scratch directory, removed on drop.
pub fn scratch_path(name: &str) -> ScratchPath {
    ScratchPath(Some(PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name)))
}

impl std::ops::Deref for ScratchPath {
    type Target = Path;

    fn deref(&self) -> &Path {
        self.0.as_deref().expect("active scratch path")
    }
}

impl AsRef<Path> for ScratchPath {
    fn as_ref(&self) -> &Path {
        self
    }
}

impl From<ScratchPath> for PathBuf {
    fn from(mut guard: ScratchPath) -> Self {
        guard.0.take().expect("active scratch path")
    }
}

impl Drop for ScratchPath {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            if std::fs::remove_file(&path).is_err() {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }
}
