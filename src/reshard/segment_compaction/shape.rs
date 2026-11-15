//! What every segment of a catalog compacted from segments must share.

use crate::segments::OpenedSegmentSet;
use crate::vector::VectorBackendConfig;

use super::super::segment_sources::{segment_tables, SourceTables};

/// The one store and one provider state a catalog compacts under.
pub(super) struct CatalogShape {
    pub(super) tables: SourceTables,
    pub(super) derived: Option<crate::postings::StoredDerived>,
    pub(super) dim: usize,
    pub(super) backend_config: VectorBackendConfig,
}

impl CatalogShape {
    /// Every segment declares the same tables, fingerprints, and
    /// declaration over one contiguous id space, and carries the same
    /// vector provider state with exact rows; a difference is refused
    /// naming the segment.
    pub(super) fn check(set: &OpenedSegmentSet) -> Result<Self, String> {
        let tables = segment_tables(set.bm25(0));
        let derived = set.bm25(0).derived().cloned();
        check_stores(set, &tables, derived.as_ref())?;
        let (dim, backend_config) = check_provider(set)?;
        Ok(Self {
            tables,
            derived,
            dim,
            backend_config,
        })
    }
}

fn check_stores(
    set: &OpenedSegmentSet,
    tables: &SourceTables,
    derived: Option<&crate::postings::StoredDerived>,
) -> Result<(), String> {
    let root = set.root().display();
    let mut rows_total = 0u64;
    for i in 0..set.len() {
        let meta = set.metadata(i);
        let id = &meta.segment_id;
        if meta.base_label != rows_total {
            return Err(format!(
                "{root} segment {id} starts at label {} but the segments before it end at \
                 {rows_total}; a compaction from segments needs one contiguous id space",
                meta.base_label
            ));
        }
        rows_total = meta.end_label_exclusive()?;
        let bm25 = set.bm25(i);
        if u64::from(bm25.next_doc_id()) != meta.rows {
            return Err(format!(
                "{root} segment {id}: the store has {} rows, the metadata {}",
                bm25.next_doc_id(),
                meta.rows
            ));
        }
        if segment_tables(bm25) != *tables {
            return Err(format!(
                "{root} segment {id}: field or column tables differ from the first segment's; \
                 one catalog compacts under one table"
            ));
        }
        if bm25.derived() != derived {
            return Err(format!(
                "{root} segment {id}: derived-column declaration {:?} differs from the first \
                 segment's {:?}; a compaction cannot restamp a declaration",
                bm25.derived().map(|d| &d.fingerprint),
                derived.map(|d| &d.fingerprint)
            ));
        }
    }
    Ok(())
}

/// One provider state: the segments' own vector images carry it, and the
/// outputs must reproduce it exactly.
fn check_provider(set: &OpenedSegmentSet) -> Result<(usize, VectorBackendConfig), String> {
    let root = set.root().display();
    let mut provider: Option<(usize, VectorBackendConfig)> = None;
    for i in 0..set.len() {
        let id = &set.metadata(i).segment_id;
        let Some(image) = set.vector(i) else {
            return Err(format!(
                "{root} segment {id}: no vector image; a compaction from segments needs the \
                 provider state the sealed images carry"
            ));
        };
        let dim = image
            .dim_opt()
            .ok_or_else(|| format!("{root} segment {id}: vector image has no dimension"))?;
        let config = image
            .backend_config()
            .map_err(|e| format!("{root} segment {id}: {e}"))?;
        match &provider {
            Some((held_dim, held)) if *held_dim != dim || *held != config => {
                return Err(format!(
                    "{root} segment {id}: vector provider state differs from the first \
                     segment's; a compaction cannot mix scoring configurations"
                ));
            }
            Some(_) => {}
            None => provider = Some((dim, config)),
        }
        if set.exact_vectors(i).is_none() {
            return Err(format!(
                "{root} segment {id}: has a vector image but no exact-vector sidecar; a \
                 transplant copies FP32 rows, rebuild or backfill the source"
            ));
        }
    }
    Ok(provider.expect("checked above: the set is not empty"))
}
