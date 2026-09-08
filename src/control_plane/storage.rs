//! File ownership survives replacement of the JSON state inode.
use super::*;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};

fn error(action: &str, path: &Path, error: impl std::fmt::Display) -> String {
    format!("{action} {}: {error}", path.display())
}

fn existing_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

// Preserve bootstrap's directory creation, syncing each newly created entry
// rather than assuming a final sync of the deepest directory covers ancestors.
fn ensure_directory(path: &Path) -> Result<(), String> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => return Ok(()),
        Ok(_) => {
            return Err(format!(
                "control container {} is not a directory",
                path.display()
            ))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(error("inspect control container", path, e)),
    }
    let parent = existing_parent(path);
    ensure_directory(parent)?;
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if !std::fs::metadata(path)
                .map_err(|e| error("inspect control container", path, e))?
                .is_dir()
            {
                return Err(format!(
                    "control container {} is not a directory",
                    path.display()
                ));
            }
        }
        Err(e) => return Err(error("create control container", path, e)),
    }
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|e| error("sync control container parent", parent, e))
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    options
}

/// Take ownership before reading any state. Parent aliases resolve to the same
/// lock. A symlink at the state leaf is refused, since replacing it would split
/// one apparent authority into two independent paths.
pub(super) fn acquire(path: &Path, allow_create: bool) -> Result<(PathBuf, Arc<File>), String> {
    let name = path
        .file_name()
        .ok_or_else(|| "control state path requires a filename".to_string())?;
    let parent = existing_parent(path);
    if allow_create {
        ensure_directory(parent)?;
    }
    let parent =
        std::fs::canonicalize(parent).map_err(|e| error("read control container", parent, e))?;
    let path = parent.join(name);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(format!(
                "control state {} must be a regular file, not a symlink or special file",
                path.display()
            ));
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && allow_create => {}
        Err(e) => return Err(error("read existing control state", &path, e)),
    }
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    let lock_path = PathBuf::from(name);
    match std::fs::symlink_metadata(&lock_path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(format!("exclusive control ownership lock {} must be a regular file, not a symlink or special file", lock_path.display()));
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(error(
                "inspect exclusive control ownership lock",
                &lock_path,
                e,
            ))
        }
    }
    // This file is never truncated, renamed or removed by the adapter.
    // Locking the replaceable state inode instead would permit a second owner
    // immediately after the first successful atomic state replacement.
    let lock = private_options()
        .read(true)
        .write(true)
        .create(true)
        .open(&lock_path)
        .map_err(|e| error("open exclusive control ownership lock", &lock_path, e))?;
    if !lock
        .metadata()
        .map_err(|e| error("inspect control ownership lock", &lock_path, e))?
        .is_file()
    {
        return Err(format!(
            "exclusive control ownership lock {} must be a regular file",
            lock_path.display()
        ));
    }
    lock.try_lock().map_err(|e| {
        error(
            "exclusive control ownership lock unavailable",
            &lock_path,
            e,
        )
    })?;
    lock.sync_all()
        .map_err(|e| error("sync control ownership lock", &lock_path, e))?;
    File::open(&parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|e| error("sync control ownership directory", &parent, e))?;
    Ok((path, Arc::new(lock)))
}

pub(super) fn read(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    let mut file = private_options().read(true).open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "control state must be a regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

pub(super) struct Candidate {
    path: PathBuf,
}
impl Drop for Candidate {
    fn drop(&mut self) {
        // Only our create_new candidate is eligible for cleanup. After rename,
        // its former name no longer exists; the published state is retained.
        let _ = std::fs::remove_file(&self.path);
    }
}
impl Candidate {
    pub(super) fn write(path: &Path, bytes: &[u8]) -> Result<Self, String> {
        for _ in 0..8 {
            let mut nonce = [0u8; 16];
            getrandom::getrandom(&mut nonce)
                .map_err(|e| format!("control state candidate entropy: {e}"))?;
            let nonce = nonce
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let mut name = path.as_os_str().to_owned();
            name.push(format!(".tmp-{nonce}"));
            let candidate = PathBuf::from(name);
            let mut file = match private_options()
                .write(true)
                .create_new(true)
                .open(&candidate)
            {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    return Err(error(
                        "create private control state candidate",
                        &candidate,
                        e,
                    ))
                }
            };
            let candidate = Self { path: candidate };
            file.write_all(bytes)
                .and_then(|_| file.sync_all())
                .map_err(|e| error("write control state candidate", &candidate.path, e))?;
            return Ok(candidate);
        }
        Err("control state candidate names exhausted after collisions".into())
    }

    pub(super) fn publish(&self, destination: &Path) -> Result<(), std::io::Error> {
        std::fs::rename(&self.path, destination)
    }
}

#[cfg(feature = "net")]
pub(super) fn read_bounded(path: &Path, max: usize) -> Result<(Vec<u8>, File), std::io::Error> {
    let mut file = private_options().read(true).open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "control state must be a regular file",
        ));
    }
    let mut bytes = Vec::new();
    (&mut file).take(max as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "retired control record exceeds 17MiB",
        ));
    }
    Ok((bytes, file))
}
