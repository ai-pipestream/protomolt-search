//! Reading the child's catalog and digesting every live row of a shard's
//! segments in parallel.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::pb::AddDocumentsRequest;
use crate::reshard::{
    open_segment_exact, reconstruct_document, segment_tables, SegmentSourceShard, SourceTables,
};

/// The catalog root of a child path: the `.segments` root itself, or
/// the index path a node serves.
fn child_root(path: &Path) -> PathBuf {
    let is_root = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".segments"));
    if is_root {
        path.to_path_buf()
    } else {
        crate::node::segments_root(path)
    }
}

/// The child as a source shard: its catalog, its live overlay, no log.
pub(super) fn open_child(path: &Path) -> Result<(SegmentSourceShard, SourceTables), String> {
    let root = child_root(path);
    if !root.exists() {
        return Err(format!("{}: no segment catalog", root.display()));
    }
    let set = Arc::new(crate::segments::OpenedSegmentSet::open(&root)?);
    if set.is_empty() {
        return Err(format!(
            "{}: the catalog has no sealed segment",
            root.display()
        ));
    }
    let tables = segment_tables(set.bm25(0));
    for i in 1..set.len() {
        if segment_tables(set.bm25(i)) != tables {
            return Err(format!(
                "{} segment {}: declares tables that differ from the first segment's",
                root.display(),
                set.metadata(i).segment_id
            ));
        }
    }
    let index_path = root.with_file_name(
        root.file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix(".segments"))
            .unwrap_or_default(),
    );
    let overlay_path = crate::node::live_docs_sidecar_path(&index_path);
    let overlay = if overlay_path.exists() {
        Some(
            crate::live_docs::LiveDocs::open(&overlay_path)
                .map_err(|e| format!("open {}: {e}", overlay_path.display()))?,
        )
    } else {
        None
    };
    let shard = SegmentSourceShard {
        gen: PathBuf::new(),
        root,
        set,
        overlay,
        deleted: BTreeSet::new(),
        slot_offset: 0,
    };
    Ok((shard, tables))
}

/// A row's digest and its id, as one thread files it.
pub(super) type Digested = ([u8; 32], u64);

/// What digesting one shard came to: each thread's list, and the live
/// and deleted row counts.
pub(super) struct DigestedRows {
    pub(super) lists: Vec<Vec<Digested>>,
    pub(super) live: u64,
    pub(super) deleted: u64,
}

/// The state the digest threads share.
struct DigestRun<'a, F> {
    shard: &'a SegmentSourceShard,
    tables: &'a SourceTables,
    each: F,
    next: AtomicUsize,
    failure: Mutex<Option<String>>,
    live: AtomicU64,
    deleted: AtomicU64,
}

/// Digest every live row of `shard`'s segments in parallel, `each`
/// turning a reconstructed row into the digest to file, or `None` to
/// skip it (a source row for another leaf).
pub(super) fn digest_rows<F>(
    shard: &SegmentSourceShard,
    tables: &SourceTables,
    threads: usize,
    each: F,
) -> Result<DigestedRows, String>
where
    F: Fn(u64, AddDocumentsRequest, Option<&[f32]>) -> Result<Option<[u8; 32]>, String> + Sync,
{
    let run = DigestRun {
        shard,
        tables,
        each,
        next: AtomicUsize::new(0),
        failure: Mutex::new(None),
        live: AtomicU64::new(0),
        deleted: AtomicU64::new(0),
    };
    let per_thread: Mutex<Vec<Vec<Digested>>> = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                let mine = run.work();
                per_thread.lock().expect("lock").push(mine);
            });
        }
    });
    if let Some(e) = run.failure.into_inner().expect("lock") {
        return Err(e);
    }
    Ok(DigestedRows {
        lists: per_thread.into_inner().expect("lock"),
        live: run.live.into_inner(),
        deleted: run.deleted.into_inner(),
    })
}

impl<F> DigestRun<'_, F>
where
    F: Fn(u64, AddDocumentsRequest, Option<&[f32]>) -> Result<Option<[u8; 32]>, String> + Sync,
{
    /// One thread: claim segments until none are left or one failed.
    fn work(&self) -> Vec<Digested> {
        let mut mine = Vec::new();
        loop {
            if self.failure.lock().expect("lock").is_some() {
                break;
            }
            let i = self.next.fetch_add(1, Ordering::SeqCst);
            if i >= self.shard.set.len() {
                break;
            }
            if let Err(e) = self.segment(i, &mut mine) {
                let mut slot = self.failure.lock().expect("lock");
                if slot.is_none() {
                    *slot = Some(e);
                }
                break;
            }
        }
        mine
    }

    fn segment(&self, i: usize, mine: &mut Vec<Digested>) -> Result<(), String> {
        let shard = self.shard;
        let meta = shard.set.metadata(i);
        let bm25 = shard.set.bm25(i);
        let rows = bm25.next_doc_id();
        let context = || format!("{} segment {}", shard.root.display(), meta.segment_id);
        if u64::from(rows) != meta.rows {
            return Err(format!(
                "{}: the store has {rows} rows, the metadata {}",
                context(),
                meta.rows
            ));
        }
        let exact = open_segment_exact(shard, i)?;
        for row in 0..rows {
            let label = meta.base_label + u64::from(row);
            if !shard.is_live(i, row, label) {
                self.deleted.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            self.live.fetch_add(1, Ordering::Relaxed);
            let id = shard.slot_offset + label;
            let doc = reconstruct_document(bm25, row, self.tables, true)
                .map_err(|e| format!("{}: {e}", context()))?;
            let vector = exact
                .as_ref()
                .map(|store| store.row_values(row as usize, row as usize + 1))
                .transpose()
                .map_err(|e| format!("read exact row {row}: {e}"))?;
            if let Some(digest) = (self.each)(id, doc, vector.as_deref())? {
                mine.push((digest, id));
            }
        }
        Ok(())
    }
}
