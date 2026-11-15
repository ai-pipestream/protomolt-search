//! Descriptor-relative reads of the incoming bundle.
use super::*;

/// Each component is opened relative to a held directory, with symlink
/// following disabled. Nonblocking open prevents a replaced FIFO from hanging
/// before the regular-file check. The initial root is owner-selected.
pub(super) struct Directory(File);
#[cfg(unix)]
impl Directory {
    pub(super) fn open(path: &Path) -> Result<Self, Status> {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .map(Self)
            .map_err(|e| damaged(format!("open bundle directory: {e}")))
    }
    pub(super) fn file(&self, name: &str) -> Result<File, Status> {
        use std::os::fd::{AsRawFd, FromRawFd};
        relative(name)?;
        let mut held = self.0.try_clone().map_err(storage)?;
        let mut parts = name.split('/').peekable();
        while let Some(part) = parts.next() {
            let component =
                std::ffi::CString::new(part).map_err(|_| damaged("NUL in artifact path"))?;
            let directory = parts.peek().is_some();
            let flags = libc::O_RDONLY
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | libc::O_NONBLOCK
                | if directory { libc::O_DIRECTORY } else { 0 };
            // SAFETY: held remains open, component is NUL-terminated, and flags
            // do not request creation. A successful descriptor transfers once.
            let fd = unsafe { libc::openat(held.as_raw_fd(), component.as_ptr(), flags) };
            if fd < 0 {
                return Err(damaged(format!(
                    "open artifact {name:?}: {}",
                    std::io::Error::last_os_error()
                )));
            }
            // SAFETY: openat returned a new owned descriptor above.
            held = unsafe { File::from_raw_fd(fd) };
            let metadata = held.metadata().map_err(storage)?;
            if (directory && !metadata.is_dir()) || (!directory && !metadata.is_file()) {
                return Err(damaged(
                    "artifact must be a regular file below real directories",
                ));
            }
        }
        Ok(held)
    }
}
#[cfg(not(unix))]
impl Directory {
    pub(super) fn open(_: &Path) -> Result<Self, Status> {
        Err(Status::unimplemented(
            "local restore requires descriptor-relative file opens on this platform",
        ))
    }
    pub(super) fn file(&self, _: &str) -> Result<File, Status> {
        Err(Status::unimplemented(
            "local restore requires descriptor-relative file opens on this platform",
        ))
    }
}

pub(super) fn read_bounded(root: &Directory, name: &str, limit: u64) -> Result<Vec<u8>, Status> {
    let mut file = root.file(name)?;
    let length = file.metadata().map_err(storage)?.len();
    if length > limit {
        return Err(exhausted());
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(storage)?;
    if bytes.len() as u64 > limit {
        return Err(exhausted());
    }
    if bytes.len() as u64 != length {
        return Err(damaged("metadata changed during its read"));
    }
    Ok(bytes)
}
