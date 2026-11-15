//! The durable records the agent keeps under `--data-dir`: a placed
//! shard's `placed.toml`, a split's `split.toml`, and the marker a split
//! leaves for a configured source it retired.

use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The durable record of a dynamically placed shard
/// (`<data-dir>/<shard_id>/placed.toml`): what the plan said when it was
/// placed, the listener port it bound, and how far its bootstrap got —
/// so a restart resumes rather than repeats.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlacedShard {
    pub shard_id: String,
    pub collection: String,
    pub slot_offset: u64,
    pub hash_lo: u64,
    pub hash_hi: u64,
    pub port: u16,
    /// The shard generation the plan assigned this copy (the action's
    /// `target_generation`): what the copy reports. The WAL cursor below
    /// is a different clock, the source's log generation.
    pub source_generation: u64,
    /// The image is installed; a retry after a crash before this point
    /// installs again.
    pub installed: bool,
    /// The replication cursor after the last successful catch-up.
    pub cursor_generation: u64,
    pub cursor_clock: u64,
    /// Caught up within the lag bound at least once: reported ready.
    pub ready: bool,
    /// The action whose completion the plane acknowledged, when one did.
    pub completed_action: u64,
}

impl PlacedShard {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join("placed.toml")
    }

    pub fn load(dir: &Path) -> Result<Option<Self>, String> {
        load_toml(&Self::path(dir))
    }

    /// Write the record durably: the file is synced before it replaces
    /// the old one, and the directory after.
    pub fn write(&self, dir: &Path) -> Result<(), String> {
        let text = toml::to_string(self).map_err(|e| format!("encode placed shard: {e}"))?;
        publish_toml(dir, &Self::path(dir), &text, true)
    }
}

/// One child of a split, as chosen when the split started.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitChild {
    pub shard_id: String,
    pub hash_lo: u64,
    pub hash_hi: u64,
    pub slot_offset: u64,
}

/// Durable progress of one `SPLIT_SHARD` action
/// (`<data-dir>/<shard_id>.split/split.toml`): the children and their
/// ranges are fixed when the split starts, so a retry after a crash
/// resumes the same split rather than starting another.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitState {
    pub action_id: u64,
    pub shard_id: String,
    pub source_generation: u64,
    pub target_generation: u64,
    pub children: Vec<SplitChild>,
    /// The baseline images are built and the children placed.
    pub built: bool,
    /// The live tail cursor into the children.
    pub live: Option<crate::replication::LiveReshardState>,
    /// The source's ingest is fenced (re-applied after a restart).
    pub fenced: bool,
    pub completed: bool,
}

impl SplitState {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join("split.toml")
    }

    pub fn load(dir: &Path) -> Result<Option<Self>, String> {
        load_toml(&Self::path(dir))
    }

    pub fn write(&self, dir: &Path) -> Result<(), String> {
        let text = toml::to_string(self).map_err(|e| format!("encode split state: {e}"))?;
        publish_toml(dir, &Self::path(dir), &text, false)
    }

    /// The children's ids, joined for a message.
    pub(super) fn child_names(&self) -> String {
        self.children
            .iter()
            .map(|child| child.shard_id.as_str())
            .collect::<Vec<_>>()
            .join(" and ")
    }
}

/// A TOML record at `path`; `None` when there is none.
fn load_toml<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    toml::from_str(&text)
        .map(Some)
        .map_err(|e| format!("parse {}: {e}", path.display()))
}

/// Write `text` beside `path` and rename it into place, then sync the
/// directory; with `sync_file` the file's bytes are synced before the
/// rename.
fn publish_toml(dir: &Path, path: &Path, text: &str, sync_file: bool) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    if sync_file {
        use std::io::Write;
        let mut file =
            std::fs::File::create(&tmp).map_err(|e| format!("create {}: {e}", tmp.display()))?;
        file.write_all(text.as_bytes())
            .and_then(|()| file.sync_all())
            .map_err(|e| format!("write {}: {e}", tmp.display()))?;
    } else {
        std::fs::write(&tmp, text).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| format!("publish {}: {e}", path.display()))?;
    crate::postings::fsync_parent(path).map_err(|e| format!("fsync {}: {e}", path.display()))
}

/// The marker a split leaves for a configured source it retired.
pub(super) fn retired_marker(data_dir: &Path, shard_id: &str) -> PathBuf {
    data_dir.join("retired").join(shard_id)
}

/// Move one built image file into a placed shard's directory: a rename
/// on the same filesystem, a copy across.
pub(super) fn move_image_file(from: &Path, to: &Path) -> Result<(), String> {
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    if std::fs::rename(from, to).is_ok() {
        return Ok(());
    }
    std::fs::copy(from, to)
        .map_err(|e| format!("copy {} to {}: {e}", from.display(), to.display()))?;
    std::fs::remove_file(from).map_err(|e| format!("remove {}: {e}", from.display()))
}
