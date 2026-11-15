//! What every segment source shares before its rows are transplanted.

use std::path::Path;

use crate::postings::StoredDerived;
use crate::segments::OpenedSegmentSet;
use crate::wal::WalManifest;

use super::child_build::manifest_derived;
use super::segment_sources::{segment_tables, SourceTables};

/// The tables and the derived-column declaration the sources admitted
/// so far pin on every child.
#[derive(Default)]
pub(super) struct PinnedSources {
    pub(super) tables: Option<SourceTables>,
    declaration: Option<Option<StoredDerived>>,
}

impl PinnedSources {
    /// Admit one shard. Its log's manifest, each of its segments, and the
    /// shards before it carry one derived-column declaration, or none;
    /// its segments carry the pinned tables.
    pub(super) fn admit(
        &mut self,
        set: &OpenedSegmentSet,
        root: &Path,
        manifest: &WalManifest,
    ) -> Result<(), String> {
        let declared =
            manifest_derived(manifest).map_err(|e| format!("{}: {e}", root.display()))?;
        match &self.declaration {
            None => self.declaration = Some(declared.clone()),
            Some(first) if *first != declared => {
                return Err(format!(
                    "{}: the log records derived-column declaration {} and an earlier source \
                     records {}; sources share one declaration, or none",
                    root.display(),
                    name(declared.as_ref()),
                    name(first.as_ref())
                ));
            }
            Some(_) => {}
        }
        for i in 0..set.len() {
            let bm25 = set.bm25(i);
            let what = format!("{} segment {}", root.display(), set.metadata(i).segment_id);
            if bm25.derived() != declared.as_ref() {
                return Err(format!(
                    "{what}: derived-column declaration {} differs from {} in the shard's log \
                     manifest; a transplant cannot restamp a declaration",
                    name(bm25.derived()),
                    name(declared.as_ref())
                ));
            }
            let this = segment_tables(bm25);
            match &self.tables {
                None => self.tables = Some(this),
                Some(first) => require_same_tables(first, &this, &what)?,
            }
        }
        Ok(())
    }
}

fn name(declaration: Option<&StoredDerived>) -> String {
    declaration.map_or_else(|| "none".to_string(), |d| format!("{:?}", d.fingerprint))
}

/// One store per child: a segment's field, analysis, and column tables
/// equal the first source's.
fn require_same_tables(
    first: &SourceTables,
    this: &SourceTables,
    what: &str,
) -> Result<(), String> {
    if first.fields != this.fields {
        return Err(format!(
            "{what}: field table {:?} differs from the first source's {:?}; a \
             child is one store and cannot mix tables",
            this.fields, first.fields
        ));
    }
    if first.fingerprints != this.fingerprints {
        return Err(format!(
            "{what}: analysis fingerprints {:?} differ from the first source's \
             {:?}; a child cannot mix analyzers, split from the logs to re-analyze",
            this.fingerprints, first.fingerprints
        ));
    }
    if first.position_fields != this.position_fields {
        return Err(format!(
            "{what}: positional fields {:?} differ from the first source's {:?}",
            this.position_fields, first.position_fields
        ));
    }
    if first.sentence_fields != this.sentence_fields {
        return Err(format!(
            "{what}: sentence fields {:?} differ from the first source's {:?}",
            this.sentence_fields, first.sentence_fields
        ));
    }
    if first.columns != this.columns {
        return Err(format!(
            "{what}: column tables differ from the first source's; a child is \
             one store and cannot mix column tables"
        ));
    }
    Ok(())
}
