//! Publication of retained vector rows beside a single-image generation.
//!
//! The declaration is authoritative: a missing declared payload is corruption,
//! and an undeclared payload is an interrupted publication, never legacy data.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::multi_res::MultiResVectors;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
const MAX_DECLARATION_BYTES: u64 = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Declaration {
    version: u32,
    physical_rows: u64,
    retained_rows: u64,
    bytes: u64,
    sha256: String,
}

pub(crate) fn declaration_path(payload: &Path) -> PathBuf {
    let mut path = payload.as_os_str().to_owned();
    path.push(".json");
    PathBuf::from(path)
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(feature = "net")]
pub(crate) fn export_declaration(payload: &Path) -> io::Result<Option<Vec<u8>>> {
    let file = match File::open(declaration_path(payload)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if payload.try_exists()? {
                return Err(invalid(
                    "retained vector payload has no publication declaration",
                ));
            }
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::new();
    file.take(MAX_DECLARATION_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_DECLARATION_BYTES {
        return Err(invalid("retained vector declaration exceeds size bound"));
    }
    let declared: Declaration = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    open(payload, declared.physical_rows)?;
    Ok(Some(bytes))
}

pub(crate) fn open_generation(payload: &Path, physical_rows: u64, live: &crate::live_docs::LiveDocs) -> io::Result<MultiResVectors> {
    if live.requires_retained_vectors() && !declaration_path(payload).try_exists()? {
        return Err(invalid("live generation requires a retained vector declaration"));
    }
    let rows = open(payload, physical_rows)?;
    if !rows.is_empty() && !live.requires_retained_vectors() {
        return Err(invalid("retained vector generation requires its live-doc reader gate"));
    }
    Ok(rows)
}

pub(crate) fn open(payload: &Path, physical_rows: u64) -> io::Result<MultiResVectors> {
    let declaration = match File::open(declaration_path(payload)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return match std::fs::metadata(payload) {
                Ok(_) => Err(invalid(
                    "retained vector payload has no publication declaration",
                )),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    Ok(MultiResVectors::default())
                }
                Err(error) => Err(error),
            };
        }
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::new();
    declaration
        .take(MAX_DECLARATION_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_DECLARATION_BYTES {
        return Err(invalid("retained vector declaration exceeds size bound"));
    }
    let declared: Declaration = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("invalid retained vector declaration"))?;
    if declared.version != 1
        || declared.physical_rows != physical_rows
        || std::fs::metadata(payload)?.len() != declared.bytes
    {
        return Err(invalid(
            "retained vector declaration disagrees with generation",
        ));
    }
    let rows = MultiResVectors::open(payload, physical_rows, &declared.sha256)?;
    if rows.row_count() as u64 != declared.retained_rows {
        return Err(invalid("retained vector coverage differs from declaration"));
    }
    Ok(rows)
}

pub(crate) fn write(rows: &MultiResVectors, payload: &Path, physical_rows: u64, required: bool) -> io::Result<()> {
    // Ordinary legacy generations remain unchanged. Reusing a retained path,
    // however, must publish even an empty map to retire the prior rows.
    if rows.is_empty() && !required && !payload.try_exists()? && !declaration_path(payload).try_exists()? {
        return Ok(());
    }
    let (bytes, sha256) = rows.write(payload, physical_rows)?;
    let declaration = Declaration {
        version: 1,
        physical_rows,
        retained_rows: rows.row_count() as u64,
        bytes,
        sha256,
    };
    let bytes = serde_json::to_vec(&declaration).map_err(io::Error::other)?;
    let path = declaration_path(payload);
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let temp = parent.join(format!(
        ".multi-declaration-{}-{}.tmp",
        std::process::id(),
        NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
    ));
    // Exclusive creation precedes the cleanup scope: a collision belongs to
    // another writer and must survive this failed attempt.
    let directory = File::open(parent)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let mut renamed = false;
    let result = (|| {
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temp, &path)?;
        renamed = true;
        directory.sync_all().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("retained vector declaration publication uncertain after rename: {error}"),
            )
        })
    })();
    if result.is_err() && !renamed {
        let _ = std::fs::remove_file(&temp);
    }
    result
}
