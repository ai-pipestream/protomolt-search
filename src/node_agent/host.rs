//! What the agent reads off the host and its shards: row counts,
//! fingerprints, disk and memory, and a peer's health.

use std::path::Path;

use crate::pb::{HealthRequest, HealthResponse};

/// The row count a shard reports: the larger of its vector and document
/// tips (aligned shards have one number).
pub fn rows_of(health: &HealthResponse) -> u64 {
    health.num_vectors.max(health.document_slots)
}

/// Per-field analysis fingerprints as one string, field-table order.
pub fn fingerprint_string(fingerprints: &[u64]) -> String {
    fingerprints
        .iter()
        .map(|fp| format!("{fp:016x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// The health of the node listening at `addr`.
pub(super) async fn node_health(addr: &str) -> Result<HealthResponse, String> {
    let endpoint =
        tonic::transport::Endpoint::from_shared(crate::security::process_secure_url(addr))
            .map_err(|e| format!("node address {addr}: {e}"))?;
    let endpoint = crate::security::secure_endpoint(endpoint)?;
    Ok(
        crate::pb::node_service_client::NodeServiceClient::new(endpoint.connect_lazy())
            .health(HealthRequest {})
            .await
            .map_err(|e| format!("health of {addr}: {e}"))?
            .into_inner(),
    )
}

/// Bytes on disk of a shard's file family: the index path and every
/// sibling that starts with its name (`.bm25`, `.segments/`, `.snap/`,
/// `.wal/`, ...).
pub fn shard_bytes(index_path: &Path) -> u64 {
    fn size_of(path: &Path) -> u64 {
        match std::fs::metadata(path) {
            Ok(meta) if meta.is_dir() => std::fs::read_dir(path)
                .map(|entries| entries.flatten().map(|e| size_of(&e.path())).sum())
                .unwrap_or(0),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        }
    }
    let (Some(parent), Some(name)) = (index_path.parent(), index_path.file_name()) else {
        return 0;
    };
    let prefix = name.to_string_lossy().into_owned();
    std::fs::read_dir(parent)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().starts_with(&prefix))
                .map(|e| size_of(&e.path()))
                .sum()
        })
        .unwrap_or(0)
}

/// `(total, used)` bytes of the filesystem holding `path`; zeros when the
/// statistics are unavailable.
///
/// The path is the agent's data directory, and this is the first thing a
/// fresh node calls with it: registration reports capacity before any
/// shard has been placed, so the directory may not exist yet. It is
/// created here (`create_dir_all`, a no-op when present) because
/// `statvfs` needs an existing path, and the directory must exist
/// before any placement anyway. A creation failure is not an error: the
/// call then fails too and the capacity reads as unknown (zeros).
// statvfs field widths differ by platform; the conversions are the
// portable spelling even where they are identities.
#[allow(clippy::useless_conversion)]
pub fn filesystem_bytes(path: &Path) -> (u64, u64) {
    let _ = std::fs::create_dir_all(path);
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
            return (0, 0);
        };
        let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: `c_path` is a valid NUL-terminated string and `stats`
        // is a zeroed statvfs the call fills in.
        if unsafe { libc::statvfs(c_path.as_ptr(), &mut stats) } != 0 {
            return (0, 0);
        }
        let frag = u64::from(stats.f_frsize).max(1);
        let total = u64::from(stats.f_blocks).saturating_mul(frag);
        let available = u64::from(stats.f_bavail).saturating_mul(frag);
        (total, total.saturating_sub(available))
    }
    #[cfg(not(unix))]
    {
        (0, 0)
    }
}

/// Total memory in bytes from `/proc/meminfo`; 0 elsewhere.
pub fn memory_bytes() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("MemTotal:"))
                .and_then(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|kb| kb.parse::<u64>().ok())
                })
        })
        .map_or(0, |kb| kb.saturating_mul(1024))
}

#[cfg(test)]
mod tests {
    use super::super::PlacedShard;
    use super::*;

    #[test]
    fn placed_shard_round_trips_and_bytes_count_the_file_family() {
        let dir = std::env::temp_dir().join(format!("node-agent-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let placed = PlacedShard {
            shard_id: "s0".into(),
            slot_offset: 7,
            hash_hi: u64::MAX,
            port: 5000,
            installed: true,
            ..Default::default()
        };
        placed.write(&dir).unwrap();
        assert_eq!(PlacedShard::load(&dir).unwrap(), Some(placed));
        assert_eq!(PlacedShard::load(&dir.join("none")).unwrap(), None);
        std::fs::write(dir.join("shard"), b"abcd").unwrap();
        std::fs::create_dir_all(dir.join("shard.segments/segments")).unwrap();
        std::fs::write(dir.join("shard.segments/segments.json"), b"ab").unwrap();
        std::fs::write(dir.join("other"), b"zzzzzz").unwrap();
        assert_eq!(shard_bytes(&dir.join("shard")), 6);
        assert_eq!(
            fingerprint_string(&[1, 255]),
            "0000000000000001,00000000000000ff"
        );
        let (total, used) = filesystem_bytes(&dir);
        assert!(total > 0 && used <= total);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
