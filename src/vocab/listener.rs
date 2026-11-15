//! The vocabulary listener: the live window, its rollover, and drift
//! requests against persisted snapshots.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use prost::Message;

use super::drift::{compute_drift, ChannelDrift};
use super::snapshots::{load_snapshot_file, now_millis, scan_snapshots, SnapshotMeta};
use super::window::WindowState;
use crate::pb::storage::{VocabChannel, VocabChannelStats, VocabHeavyHitter, VocabSnapshot};

/// The reference that names the live window in a drift request.
pub const LIVE: &str = "live";

/// The vocabulary listener: streaming statistics over every term and token
/// the AnalyzeStream ingest path consumes. Cheap to share: behind one
/// `Arc`, all mutating and reading methods take the internal lock, so
/// concurrent sessions never observe a half-applied document.
///
/// Snapshot files are `snapshot-<seq>-<epochMillis>.pb` in the vocab
/// directory — write-then-move, so a crash mid-write never leaves a half
/// snapshot that startup would later fail to index.
pub struct VocabularyListener {
    vocab_dir: PathBuf,
    window_docs: u64,
    top_k: usize,
    inner: Mutex<Inner>,
}

struct Inner {
    terms: WindowState,
    tokens: WindowState,
    window_started_millis: i64,
    next_sequence: u64,
    snapshots: Vec<SnapshotMeta>,
    files_by_sequence: HashMap<u64, PathBuf>,
}

/// Documents per window before automatic rollover.
pub const DEFAULT_WINDOW_DOCS: u64 = 1_000_000;

impl VocabularyListener {
    /// Creates the listener and indexes the prior snapshots in
    /// `vocab_dir` (created when missing, must be writable).
    pub fn create(vocab_dir: &Path, window_docs: u64, top_k: usize) -> io::Result<Self> {
        assert!(window_docs > 0, "window_docs must be positive");
        assert!(top_k > 0, "top_k must be positive");
        std::fs::create_dir_all(vocab_dir)?;
        // A cheap writability probe: the caller degrades to disabled when
        // the directory rejects it.
        let probe = vocab_dir.join(".vocab-write-probe");
        std::fs::write(&probe, b"")?;
        std::fs::remove_file(&probe)?;
        let (snapshots, files_by_sequence, next_sequence) = scan_snapshots(vocab_dir)?;
        if !snapshots.is_empty() {
            eprintln!(
                "vocab: resumed {} prior snapshot(s) from {}",
                snapshots.len(),
                vocab_dir.display()
            );
        }
        Ok(Self {
            vocab_dir: vocab_dir.to_path_buf(),
            window_docs,
            top_k,
            inner: Mutex::new(Inner {
                terms: WindowState::new(VocabChannel::Terms, top_k),
                tokens: WindowState::new(VocabChannel::Tokens, top_k),
                window_started_millis: now_millis(),
                next_sequence,
                snapshots,
                files_by_sequence,
            }),
        })
    }

    /// Feeds one successfully analyzed document: its term-vector entries
    /// into the TERMS channel and its raw token texts into the TOKENS
    /// channel. Never fails — a listener problem must never reach ingest,
    /// so persistence errors are logged and the window carries on.
    pub fn feed<'a>(
        &self,
        terms: impl Iterator<Item = (&'a str, i64)>,
        tokens: impl Iterator<Item = &'a str>,
    ) {
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                eprintln!("vocab: listener lock poisoned; this document's counts are lost");
                poisoned.into_inner()
            }
        };
        for (term, frequency) in terms {
            if frequency > 0 {
                guard.terms.add_term(term, frequency as u64);
            }
        }
        for token in tokens {
            guard.tokens.add_term(token, 1);
        }
        guard.terms.add_document();
        guard.tokens.add_document();
        if guard.terms.documents() >= self.window_docs {
            self.rollover_locked(&mut guard, "window size reached");
        }
    }

    /// Seals and persists the live window and starts a fresh one. An empty
    /// window is not persisted.
    pub fn snapshot_now(&self) -> Option<SnapshotMeta> {
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.rollover_locked(&mut guard, "explicit snapshot request")
    }

    /// Documents accumulated in the live window.
    pub fn window_documents(&self) -> u64 {
        match self.inner.lock() {
            Ok(guard) => guard.terms.documents(),
            Err(poisoned) => poisoned.into_inner().terms.documents(),
        }
    }

    /// Live-window statistics, one entry per channel.
    pub fn live_stats(&self) -> Vec<VocabChannelStats> {
        let guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        [&guard.terms, &guard.tokens]
            .into_iter()
            .map(|state| VocabChannelStats {
                channel: state.channel() as i32,
                documents: state.documents(),
                term_occurrences: state.occurrences(),
                cardinality_estimate: state.cardinality_estimate(),
                heavy_hitters: state
                    .heavy_hitters()
                    .into_iter()
                    .map(|entry| VocabHeavyHitter {
                        term: entry.term,
                        count: entry.count,
                    })
                    .collect(),
            })
            .collect()
    }

    /// The persisted snapshots, ordered by sequence ascending.
    pub fn snapshots(&self) -> Vec<SnapshotMeta> {
        match self.inner.lock() {
            Ok(guard) => guard.snapshots.clone(),
            Err(poisoned) => poisoned.into_inner().snapshots.clone(),
        }
    }

    /// Seals the live window if it holds documents (graceful-shutdown
    /// counterpart of the Java shutdown hook; Rust has no hook mechanism,
    /// so the node calls this explicitly).
    pub fn persist_on_shutdown(&self) {
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.rollover_locked(&mut guard, "process shutdown");
    }

    /// Computes the drift between two windows, per channel. Either
    /// reference may be [`LIVE`] for the current window, a snapshot file
    /// name, or a bare sequence number; a named snapshot's sketches are
    /// loaded from disk for the comparison. `embedding_vocabulary` applies
    /// to the TOKENS channel only (surface forms against the model
    /// vocabulary; TERMS identity is folded/stemmed and not a model input).
    pub fn drift(
        &self,
        from_ref: &str,
        to_ref: &str,
        embedding_vocabulary: Option<&HashSet<String>>,
    ) -> Result<Vec<ChannelDrift>, String> {
        let from = self.resolve(from_ref)?;
        let to = self.resolve(to_ref)?;
        let mut out = Vec::with_capacity(2);
        for channel in [VocabChannel::Terms, VocabChannel::Tokens] {
            let from_state = from
                .get(&channel)
                .ok_or_else(|| format!("from snapshot {from_ref:?} has no {channel:?} channel"))?;
            let to_state = to
                .get(&channel)
                .ok_or_else(|| format!("to snapshot {to_ref:?} has no {channel:?} channel"))?;
            let vocabulary = if channel == VocabChannel::Tokens {
                embedding_vocabulary
            } else {
                None
            };
            out.push(ChannelDrift {
                channel,
                metrics: compute_drift(from_state, to_state, vocabulary),
            });
        }
        Ok(out)
    }

    /// Resolves a drift reference to per-channel window state. The live
    /// window resolves to clones taken under the lock (a drift comparison
    /// must not hold the lock while it computes, nor race a rollover).
    fn resolve(&self, reference: &str) -> Result<HashMap<VocabChannel, WindowState>, String> {
        let reference = reference.trim();
        if reference.is_empty() {
            return Err("snapshot reference must not be empty".to_string());
        }
        if reference.eq_ignore_ascii_case(LIVE) {
            let guard = match self.inner.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            return Ok(HashMap::from([
                (VocabChannel::Terms, clone_state(&guard.terms)),
                (VocabChannel::Tokens, clone_state(&guard.tokens)),
            ]));
        }
        let file = {
            let guard = match self.inner.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            locate_snapshot(&guard, reference)
        };
        let Some(file) = file else {
            return Err(format!("unknown snapshot '{reference}'"));
        };
        let snapshot = load_snapshot_file(&file)?;
        let mut states = HashMap::with_capacity(2);
        for channel_snapshot in &snapshot.channels {
            let state = WindowState::from_proto(channel_snapshot)?;
            states.insert(state.channel(), state);
        }
        Ok(states)
    }

    /// Seals the live window, persists it, and starts fresh. Returns
    /// `None` when the window holds no documents — an empty snapshot is
    /// noise, and every caller (rollover, explicit request, shutdown) is
    /// fine with none.
    fn rollover_locked(&self, guard: &mut Inner, reason: &str) -> Option<SnapshotMeta> {
        let documents = guard.terms.documents();
        if documents == 0 {
            return None;
        }
        let sealed_millis = now_millis();
        let snapshot = VocabSnapshot {
            sequence: guard.next_sequence,
            started_epoch_millis: guard.window_started_millis,
            sealed_epoch_millis: sealed_millis,
            channels: vec![guard.terms.to_proto(), guard.tokens.to_proto()],
        };
        let file_name = format!("snapshot-{}-{sealed_millis}.pb", guard.next_sequence);
        let target = self.vocab_dir.join(&file_name);
        let temp = self.vocab_dir.join(format!("{file_name}.tmp"));
        let persisted = std::fs::write(&temp, snapshot.encode_to_vec())
            .and_then(|()| std::fs::rename(&temp, &target));
        if let Err(e) = persisted {
            eprintln!(
                "vocab: could not persist snapshot {}; the window continues instead of \
                 rolling over: {e}",
                target.display()
            );
            return None;
        }
        let meta = SnapshotMeta {
            size_bytes: std::fs::metadata(&target).map(|m| m.len()).unwrap_or(0),
            name: file_name,
            sequence: snapshot.sequence,
            started_epoch_millis: snapshot.started_epoch_millis,
            sealed_epoch_millis: snapshot.sealed_epoch_millis,
            documents,
        };
        guard.files_by_sequence.insert(guard.next_sequence, target);
        eprintln!(
            "vocab: snapshot {} sealed ({} documents, {})",
            meta.name, documents, reason
        );
        guard.snapshots.push(meta.clone());
        guard.next_sequence += 1;
        guard.terms = WindowState::new(VocabChannel::Terms, self.top_k);
        guard.tokens = WindowState::new(VocabChannel::Tokens, self.top_k);
        guard.window_started_millis = sealed_millis;
        Some(meta)
    }
}

/// A detached copy of a window's state (sketches and counters), so a drift
/// comparison over the live window runs without holding the listener lock.
fn clone_state(state: &WindowState) -> WindowState {
    WindowState::from_proto(&state.to_proto()).expect("own serialization round-trips")
}

/// Finds a snapshot file by file name or bare sequence number.
fn locate_snapshot(guard: &Inner, reference: &str) -> Option<PathBuf> {
    if let Ok(sequence) = reference.parse::<u64>() {
        if let Some(file) = guard.files_by_sequence.get(&sequence) {
            return Some(file.clone());
        }
    }
    guard
        .files_by_sequence
        .values()
        .find(|file| {
            file.file_name()
                .map(|name| name == std::ffi::OsStr::new(reference))
                .unwrap_or(false)
        })
        .cloned()
}
