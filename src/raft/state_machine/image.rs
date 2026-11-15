//! Published snapshot images: their ids, signatures, metadata, and the
//! pointer to the current generation.

use super::*;

pub(super) fn next_generation(snapshots: &Path) -> Result<u64, Status> {
    let mut highest = read_pointer(snapshots)?.unwrap_or(0);
    for entry in std::fs::read_dir(snapshots.join(GENERATIONS)).map_err(io)? {
        let entry = entry.map_err(io)?;
        if let Ok(n) = entry.file_name().to_string_lossy().parse::<u64>() {
            highest = highest.max(n);
        }
    }
    highest
        .checked_add(1)
        .ok_or_else(|| Status::resource_exhausted("snapshot generations exhausted"))
}

/// Publish one generation through the pointer, then drop the other
/// generations, under the pointer lock a serve holds from reading the
/// pointer to opening its image. The previous generation stays usable
/// until the pointer moved.
pub(super) fn publish(lock: &Mutex<()>, snapshots: &Path, generation: u64) -> Result<(), Status> {
    let _pointer = lock.lock().unwrap();
    write_pointer(snapshots, generation)?;
    for entry in std::fs::read_dir(snapshots.join(GENERATIONS)).map_err(io)? {
        let entry = entry.map_err(io)?;
        if entry.file_name().to_string_lossy().parse::<u64>().ok() != Some(generation) {
            std::fs::remove_dir_all(entry.path()).map_err(io)?;
        }
    }
    Ok(())
}

/// Streams the file through a bounded buffer; refuses past `max_bytes`.
pub(super) fn image_signature(path: &Path, max_bytes: u64) -> Result<ImageSignature, Status> {
    let mut file = std::fs::File::open(path).map_err(io)?;
    let mut hasher = crate::sha256::Sha256::new();
    let mut buffer = vec![0u8; STREAM_BUFFER];
    let mut length = 0u64;
    loop {
        let read = file.read(&mut buffer).map_err(io)?;
        if read == 0 {
            break;
        }
        length += read as u64;
        if length > max_bytes {
            return Err(Status::resource_exhausted(format!(
                "snapshot image exceeds the {max_bytes} byte bound"
            )));
        }
        hasher.update(&buffer[..read]);
    }
    Ok(ImageSignature {
        length,
        sha256: hasher.finalize(),
    })
}

pub(super) fn verify_image(
    path: &Path,
    expected: &ImageSignature,
    max_bytes: u64,
) -> Result<(), Status> {
    let length = std::fs::metadata(path).map_err(io)?.len();
    if length != expected.length {
        return Err(Status::data_loss(format!(
            "snapshot image length {length} differs from the announced {}",
            expected.length
        )));
    }
    let actual = image_signature(path, max_bytes)?;
    if actual != *expected {
        return Err(super::super::reasons::reason(
            Status::data_loss("snapshot image digest differs from the announced digest"),
            super::super::reasons::TRANSPORT_SNAPSHOT_DIGEST,
        ));
    }
    Ok(())
}

/// `"<index>-<length>-<sha256 hex>"`: the id openraft carries names the
/// exact bytes, so a receiver verifies before it installs.
pub fn format_snapshot_id(
    last_log_id: Option<&LogId<NodeId>>,
    signature: &ImageSignature,
) -> String {
    format!(
        "{}-{}-{}",
        last_log_id.map_or(0, |id| id.index),
        signature.length,
        crate::sha256::to_hex(&signature.sha256)
    )
}

pub fn parse_snapshot_id(id: &str) -> Result<ImageSignature, Status> {
    let mut parts = id.splitn(3, '-');
    let _index = parts.next();
    let length = parts
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| Status::data_loss("snapshot id carries no length"))?;
    let hex = parts
        .next()
        .ok_or_else(|| Status::data_loss("snapshot id carries no digest"))?;
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        return Err(Status::data_loss("snapshot id digest is not 64 hex digits"));
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        _ => Err(Status::data_loss("snapshot id digest is not lowercase hex")),
    };
    let mut sha256 = [0u8; 32];
    for (i, pair) in bytes.chunks(2).enumerate() {
        sha256[i] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Ok(ImageSignature { length, sha256 })
}

pub(super) fn sync_dir(dir: &Path) -> Result<(), Status> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(io)
}

/// The stored form of a snapshot meta: the group, the position, the
/// membership, the id and the image's length and digest.
pub(crate) fn meta_to_proto(
    group: &SourceAuthorityIdentity,
    meta: &SnapshotMeta<NodeId, BasicNode>,
    signature: &ImageSignature,
) -> RaftSnapshotMeta {
    RaftSnapshotMeta {
        format_version: 1,
        group: Some(group.clone()),
        last_log_id: meta.last_log_id.as_ref().map(log_id_to_proto),
        membership: Some(stored_membership_to_proto(&meta.last_membership)),
        snapshot_id: meta.snapshot_id.clone(),
        length: signature.length,
        sha256: signature.sha256.to_vec(),
    }
}

pub(super) fn write_meta(
    dir: &Path,
    group: &SourceAuthorityIdentity,
    meta: &SnapshotMeta<NodeId, BasicNode>,
    signature: &ImageSignature,
) -> Result<(), Status> {
    let value = meta_to_proto(group, meta, signature);
    let path = dir.join(META);
    std::fs::write(&path, value.encode_to_vec()).map_err(io)?;
    std::fs::File::open(&path)
        .and_then(|f| f.sync_all())
        .map_err(io)?;
    sync_dir(dir)
}

/// The published generation, read from the pointer file.
pub(super) fn read_pointer(snapshots: &Path) -> Result<Option<u64>, Status> {
    match std::fs::read_to_string(snapshots.join(POINTER)) {
        Ok(text) => text
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| Status::data_loss("snapshot pointer is not a generation number")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io(error)),
    }
}

/// Publish a generation: write the pointer to a temporary file, sync it,
/// rename it into place and sync the directory. Readers see the old or the
/// new pointer, never a partial one.
pub(super) fn write_pointer(snapshots: &Path, generation: u64) -> Result<(), Status> {
    let tmp = snapshots.join("current.tmp");
    std::fs::write(&tmp, generation.to_string()).map_err(io)?;
    std::fs::File::open(&tmp)
        .and_then(|f| f.sync_all())
        .map_err(io)?;
    std::fs::rename(&tmp, snapshots.join(POINTER)).map_err(io)?;
    sync_dir(snapshots)
}

/// Read one generation: the meta must name this group, its id must be the
/// image's length and digest, and the image must verify against them.
pub(super) fn read_generation(
    dir: &Path,
    group: &SourceAuthorityIdentity,
) -> Result<(SnapshotMeta<NodeId, BasicNode>, ImageSignature), Status> {
    read_generation_with(dir, group, |image, signature| {
        verify_image(image, signature, u64::MAX)
    })
}

/// [`read_generation`] with the image checked by length only: the serve
/// path, where the receiver verifies the digest.
pub(super) fn read_generation_by_length(
    dir: &Path,
    group: &SourceAuthorityIdentity,
) -> Result<(SnapshotMeta<NodeId, BasicNode>, ImageSignature), Status> {
    read_generation_with(dir, group, |image, signature| {
        let length = std::fs::metadata(image).map_err(io)?.len();
        if length != signature.length {
            return Err(Status::data_loss(format!(
                "snapshot image length {length} differs from the announced {}",
                signature.length
            )));
        }
        Ok(())
    })
}

pub(super) fn read_generation_with(
    dir: &Path,
    group: &SourceAuthorityIdentity,
    check: impl FnOnce(&Path, &ImageSignature) -> Result<(), Status>,
) -> Result<(SnapshotMeta<NodeId, BasicNode>, ImageSignature), Status> {
    let bytes = std::fs::read(dir.join(META)).map_err(io)?;
    let value = RaftSnapshotMeta::decode(bytes.as_slice())
        .map_err(|e| Status::data_loss(format!("snapshot meta: {e}")))?;
    if value.format_version != 1 || value.sha256.len() != 32 {
        return Err(Status::data_loss(
            "snapshot meta format or digest is invalid",
        ));
    }
    if value.group.as_ref() != Some(group) {
        return Err(Status::data_loss("snapshot meta names another group"));
    }
    let signature = ImageSignature {
        length: value.length,
        sha256: value.sha256.as_slice().try_into().expect("checked length"),
    };
    check(&dir.join(IMAGE), &signature)?;
    let last_log_id = value.last_log_id.as_ref().map(log_id_from_proto);
    if value.snapshot_id != format_snapshot_id(last_log_id.as_ref(), &signature) {
        return Err(Status::data_loss("snapshot meta id differs from its image"));
    }
    Ok((
        SnapshotMeta {
            last_log_id,
            last_membership: stored_membership_from_proto(
                value
                    .membership
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("snapshot meta has no membership"))?,
            )?,
            snapshot_id: value.snapshot_id,
        },
        signature,
    ))
}

impl PublishedImage {
    /// Verify the published image by digest, under the pointer lock (a
    /// publish waits for it). `Ok(None)`: no generation is published.
    /// `Err`: the image differs from what its meta announces, or cannot be
    /// read.
    pub(crate) fn verify(&self) -> Result<Option<u64>, Status> {
        let _pointer = self.pointer_lock.lock().unwrap();
        let Some(generation) = read_pointer(&self.snapshots)? else {
            return Ok(None);
        };
        let dir = self
            .snapshots
            .join(GENERATIONS)
            .join(generation.to_string());
        read_generation(&dir, &self.identity)?;
        Ok(Some(generation))
    }
}

/// The published generation's directory, for tests.
#[cfg(test)]
pub(crate) fn published_generation(snapshots: &Path) -> Result<Option<PathBuf>, Status> {
    Ok(read_pointer(snapshots)?.map(|g| snapshots.join(GENERATIONS).join(g.to_string())))
}
