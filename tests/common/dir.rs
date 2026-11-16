//! A scratch directory under `CARGO_TARGET_TMPDIR` (a real disk under
//! `target/`, not a tmpfs `/tmp`), emptied on creation and removed when the
//! guard drops, panics included.

use std::path::{Path, PathBuf};

/// An owned scratch directory, removed on drop.
#[derive(Debug)]
pub struct TestDir(PathBuf);

/// A fresh, empty directory named for the test binary, `tag` and process.
pub fn tempdir(tag: &str) -> TestDir {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "{}_{tag}_{}",
        env!("CARGO_CRATE_NAME"),
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TestDir(dir)
}

impl std::ops::Deref for TestDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for TestDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
