//! Artifact copies, digests and atomic JSON replacement.

use super::*;

pub(super) fn digest_file(path: &Path) -> Result<(u64, String), String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| format!("open artifact {}: {error}", path.display()))?;
    let mut hash = crate::sha256::Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("read artifact {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(read as u64)
            .ok_or("artifact byte count overflow")?;
        hash.update(&buffer[..read]);
    }
    Ok((bytes, crate::sha256::to_hex(&hash.finalize())))
}

pub(super) fn copy_artifact(
    source: &Path,
    directory: &Path,
    name: &str,
) -> Result<SegmentArtifact, String> {
    let target = directory.join(name);
    std::fs::copy(source, &target).map_err(|error| {
        format!(
            "copy segment artifact {} -> {}: {error}",
            source.display(),
            target.display()
        )
    })?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .open(&target)
        .map_err(|error| format!("open copied artifact {}: {error}", target.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync copied artifact {}: {error}", target.display()))?;
    let (bytes, sha256) = digest_file(&target)?;
    Ok(SegmentArtifact {
        file: name.to_string(),
        bytes,
        sha256,
    })
}

pub(super) fn verify_artifact(directory: &Path, artifact: &SegmentArtifact) -> Result<(), String> {
    validate_relative_file(&artifact.file)?;
    let path = directory.join(&artifact.file);
    let (bytes, sha256) = digest_file(&path)?;
    if bytes != artifact.bytes || sha256 != artifact.sha256 {
        return Err(format!(
            "segment artifact {} integrity mismatch: manifest {}/{}, actual {}/{}",
            path.display(),
            artifact.bytes,
            artifact.sha256,
            bytes,
            sha256
        ));
    }
    Ok(())
}

#[cfg(test)]
thread_local! { pub(crate) static FAIL_SET_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

pub(super) fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("mkdir {}: {error}", parent.display()))?;
    let mut temp = path.as_os_str().to_owned();
    temp.push(format!(".tmp-{}", std::process::id()));
    let temp = PathBuf::from(temp);
    let bytes =
        serde_json::to_vec_pretty(value).map_err(|error| format!("encode JSON: {error}"))?;
    {
        let mut file = std::fs::File::create(&temp)
            .map_err(|error| format!("create {}: {error}", temp.display()))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| format!("write {}: {error}", temp.display()))?;
    }
    std::fs::rename(&temp, path).map_err(|error| format!("replace {}: {error}", path.display()))?;
    #[cfg(test)]
    if path.file_name().is_some_and(|name| name == SET_FILE)
        && FAIL_SET_SYNC.with(|fail| fail.replace(false))
    {
        return Err("injected directory sync failure after manifest rename".into());
    }
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync {}: {error}", parent.display()))
}
