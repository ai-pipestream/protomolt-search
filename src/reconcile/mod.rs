//! Reconcile a re-placement child against its sources, document for
//! document: every live source
//! row the tree sends to the child's leaf appears in the child exactly
//! once, carried whole (text, lineage, identity, original source, every
//! column, the FP32 vector), with the placement code rewritten and the
//! declared derived columns computed; nothing else appears; the child's
//! tables are the sources' with the derived names appended; every child
//! segment carries the declaration. Beside that, independent column
//! checks that do not go through the declaration's evaluator: two
//! columns equal, or a column equal to the civil year of a timestamp
//! column counted here from epoch days.
//!
//! The comparison is by content digest: the expected form of each
//! source row (the split's own transformation) and the reconstructed
//! form of each child row are hashed the same way, and the two
//! multisets must be equal. A row is unmatched when its digest has no
//! counterpart; the report names the first of them.

mod digest;
mod report;
mod rows;

use std::collections::HashMap;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::derived::Declaration;
use crate::pb::{self, AddDocumentsRequest};
use crate::placement::Placement;
use crate::reshard::{open_segment_sources, tree_children, SegmentSourceShard, SourceTables};

pub use self::digest::civil_year;
use self::digest::{digest, tally};
pub use self::report::{CheckTally, ColumnCheck, ReconcileOptions, ReconcileReport};
use self::rows::{digest_rows, open_child, Digested};

const NAMED: usize = 20;

/// A named problem list capped at [`NAMED`] entries per kind, the
/// count kept.
#[derive(Default)]
struct Problems {
    named: Vec<String>,
    counts: HashMap<&'static str, u64>,
}

impl Problems {
    fn push(&mut self, kind: &'static str, text: String) {
        let count = self.counts.entry(kind).or_insert(0);
        *count += 1;
        if *count <= NAMED as u64 {
            self.named.push(text);
        }
    }

    fn finish(mut self) -> Vec<String> {
        let mut kinds: Vec<(&str, u64)> = self
            .counts
            .iter()
            .filter(|(_, &count)| count > NAMED as u64)
            .map(|(&kind, &count)| (kind, count))
            .collect();
        kinds.sort_unstable();
        for (kind, count) in kinds {
            self.named.push(format!(
                "{kind}: {count} in all, the first {NAMED} named above"
            ));
        }
        self.named
    }
}

/// The child's tables are the sources' with the declared names
/// appended per kind, in declaration order.
fn expected_tables(sources: &SourceTables, declaration: Option<&Declaration>) -> SourceTables {
    let mut expected = sources.clone();
    if let Some(declaration) = declaration {
        for name in declaration.names_of(pb::MaterializeKind::F64) {
            if !expected.columns.numerics.contains(&name) {
                expected.columns.numerics.push(name);
            }
        }
        for name in declaration.names_of(pb::MaterializeKind::I64) {
            if !expected.columns.integers.contains(&name) {
                expected.columns.integers.push(name);
            }
        }
        for name in declaration.names_of(pb::MaterializeKind::U64) {
            if !expected.columns.unsigned_integers.contains(&name) {
                expected.columns.unsigned_integers.push(name);
            }
        }
    }
    expected
}

/// Run the reconciliation.
pub fn reconcile(options: &ReconcileOptions) -> Result<ReconcileReport, String> {
    let threads = match options.threads {
        0 => std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(8),
        threads => threads,
    };
    let (placement, leaf, code) = resolve_child(options)?;
    let mut report = ReconcileReport {
        leaf,
        code,
        ..Default::default()
    };
    let mut problems = Problems::default();
    let declaration = options.declaration.as_deref();
    let (sources, source_tables) = open_segment_sources(&options.sources)?;
    let (child_shard, child_tables) = open_child(&options.child)?;
    report.source_segments = sources.iter().map(|s| s.set.len()).sum();
    report.child_segments = child_shard.set.len();
    let expected = expected_tables(&source_tables, declaration);
    check_tables(&mut problems, &child_tables, &expected);
    check_declarations(&mut problems, &child_shard, declaration);
    let route = SourceRoute {
        placement: &placement,
        column: placement.column(),
        code,
        declaration,
        derive: &options.derive,
    };
    let mut expected_rows = ExpectedRows::default();
    for shard in &sources {
        expected_rows.add(shard, &source_tables, threads, &route)?;
    }
    report.source_rows_live = expected_rows.source_live;
    report.source_rows_to_child = expected_rows.routed;
    let (child_rows, tallies) =
        digest_child(&child_shard, &child_tables, threads, &options.checks)?;
    report.child_rows_live = child_rows.live;
    report.child_rows_deleted = child_rows.deleted;
    match_rows(&mut report, &mut problems, expected_rows, child_rows.lists);
    if report.source_rows_to_child != report.child_rows_live {
        problems.push(
            "counts",
            format!(
                "{} source rows route to the leaf, the child holds {} live rows",
                report.source_rows_to_child, report.child_rows_live
            ),
        );
    }
    record_checks(&mut report, &mut problems, &options.checks, tallies);
    report.problems = problems.finish();
    Ok(report)
}

/// The tree, and the leaf and code of the child being reconciled, which
/// must be a one-shard leaf.
fn resolve_child(options: &ReconcileOptions) -> Result<(Placement, String, i64), String> {
    let placement = Placement::validate(&options.tree)?;
    let children = tree_children(&placement)?;
    let child = children.get(options.child_index).ok_or_else(|| {
        format!(
            "child {} names no child; the tree has {} leaf shards",
            options.child_index,
            children.len()
        )
    })?;
    if children.iter().filter(|c| c.code == child.code).count() > 1 {
        return Err(format!(
            "leaf {:?} has more than one shard, tiled by stable routing keys; a reconciliation \
             from sealed segments covers one-shard leaves",
            child.leaf
        ));
    }
    let (leaf, code) = (child.leaf.clone(), child.code);
    Ok((placement, leaf, code))
}

/// Tables: fields as the sources', columns with the derived names.
fn check_tables(problems: &mut Problems, child: &SourceTables, expected: &SourceTables) {
    if child.fields != expected.fields {
        problems.push(
            "tables",
            format!(
                "child field table {:?} differs from the sources' {:?}",
                child.fields, expected.fields
            ),
        );
    }
    if child.fingerprints != expected.fingerprints {
        problems.push(
            "tables",
            format!(
                "child analysis fingerprints {:?} differ from the sources' {:?}",
                child.fingerprints, expected.fingerprints
            ),
        );
    }
    if child.columns != expected.columns {
        problems.push(
            "tables",
            format!(
                "child column tables {:?} differ from the expected {:?} (the sources' with the \
                 declared names appended)",
                child.columns, expected.columns
            ),
        );
    }
}

/// Every child segment carries the declaration, and only it.
fn check_declarations(
    problems: &mut Problems,
    child: &SegmentSourceShard,
    declaration: Option<&Declaration>,
) {
    for i in 0..child.set.len() {
        let stored = child.set.bm25(i).derived();
        let segment = &child.set.metadata(i).segment_id;
        let problem = match (declaration, stored) {
            (Some(declaration), Some(stored))
                if stored.fingerprint == declaration.fingerprint() =>
            {
                continue
            }
            (Some(declaration), Some(stored)) => format!(
                "child segment {segment} carries declaration {} where {} was expected",
                stored.fingerprint,
                declaration.fingerprint()
            ),
            (Some(declaration), None) => format!(
                "child segment {segment} carries no declaration where {} was expected",
                declaration.fingerprint()
            ),
            (None, Some(stored)) => format!(
                "child segment {segment} carries declaration {} where none was expected",
                stored.fingerprint
            ),
            (None, None) => continue,
        };
        problems.push("declaration", problem);
    }
}

/// The split's own transformation of a source row, and the leaf test.
struct SourceRoute<'a> {
    placement: &'a Placement,
    column: &'a str,
    code: i64,
    declaration: Option<&'a Declaration>,
    derive: &'a [String],
}

impl SourceRoute<'_> {
    /// The digest of the row the child should hold for source row `id`,
    /// or `None` when the tree sends it to another leaf.
    fn expected(
        &self,
        id: u64,
        doc: AddDocumentsRequest,
        vector: Option<&[f32]>,
    ) -> Result<Option<[u8; 32]>, String> {
        let doc = match self.declaration {
            Some(declaration) => declaration
                .rederive(doc, self.derive, None)
                .map_err(|e| format!("live source id {id}: {}", e.message()))?,
            None if !doc.derived_fingerprint.is_empty() => {
                return Err(format!(
                    "live source id {id} was derived under declaration {:?} and the child is \
                     expected to carry none",
                    doc.derived_fingerprint
                ))
            }
            None => doc,
        };
        let leaf = self
            .placement
            .evaluate(&doc)
            .map_err(|e| format!("live source id {id}: {e}"))?;
        if leaf.code != self.code {
            return Ok(None);
        }
        let mut rewritten = doc;
        rewritten
            .integers
            .retain(|value| value.field != self.column);
        rewritten.integers.push(pb::IntegerValue {
            field: self.column.to_string(),
            value: self.code,
        });
        Ok(Some(digest(&rewritten, vector)))
    }
}

/// The expected rows: each live source row through the split's own
/// transformation, kept when the tree sends it to this leaf.
#[derive(Default)]
struct ExpectedRows {
    counts: HashMap<[u8; 32], u32>,
    /// The first source id of each digest, for the report.
    ids: HashMap<[u8; 32], u64>,
    source_live: u64,
    routed: u64,
}

impl ExpectedRows {
    fn add(
        &mut self,
        shard: &SegmentSourceShard,
        tables: &SourceTables,
        threads: usize,
        route: &SourceRoute<'_>,
    ) -> Result<(), String> {
        let routed = AtomicU64::new(0);
        let rows = digest_rows(shard, tables, threads, |id, doc, vector| {
            let expected = route.expected(id, doc, vector)?;
            if expected.is_some() {
                routed.fetch_add(1, Ordering::Relaxed);
            }
            Ok(expected)
        })?;
        self.source_live += rows.live;
        self.routed += routed.into_inner();
        for (digest, id) in rows.lists.into_iter().flatten() {
            *self.counts.entry(digest).or_insert(0) += 1;
            self.ids.entry(digest).or_insert(id);
        }
        Ok(())
    }
}

/// The child's rows, each digested the same way and tallied by every
/// check.
fn digest_child(
    shard: &SegmentSourceShard,
    tables: &SourceTables,
    threads: usize,
    checks: &[ColumnCheck],
) -> Result<(rows::DigestedRows, Vec<CheckTally>), String> {
    let tallies: Mutex<Vec<CheckTally>> = Mutex::new(vec![CheckTally::default(); checks.len()]);
    let rows = digest_rows(shard, tables, threads, |_id, doc, vector| {
        if !checks.is_empty() {
            let mut mine: Vec<CheckTally> = vec![CheckTally::default(); checks.len()];
            for (check, tally_of) in checks.iter().zip(mine.iter_mut()) {
                tally(check, &doc, tally_of);
            }
            let mut all = tallies.lock().expect("lock");
            for (total, part) in all.iter_mut().zip(mine) {
                total.add(&part);
            }
        }
        Ok(Some(digest(&doc, vector)))
    })?;
    Ok((rows, tallies.into_inner().expect("lock")))
}

/// Match the child's digests against the expected multiset; whatever is
/// left on either side is unmatched.
fn match_rows(
    report: &mut ReconcileReport,
    problems: &mut Problems,
    mut expected: ExpectedRows,
    child: Vec<Vec<Digested>>,
) {
    let mut child_ids: Vec<(u64, [u8; 32])> = child
        .into_iter()
        .flatten()
        .map(|(digest, id)| (id, digest))
        .collect();
    child_ids.sort_unstable();
    for (id, digest) in child_ids {
        match expected.counts.get_mut(&digest) {
            Some(count) if *count > 0 => {
                *count -= 1;
                report.matched += 1;
            }
            _ => {
                report.unmatched_child += 1;
                problems.push(
                    "unmatched child row",
                    format!("child row {id} matches no source row sent to this leaf"),
                );
            }
        }
    }
    let mut leftover: Vec<(u64, u32)> = expected
        .counts
        .iter()
        .filter(|(_, &count)| count > 0)
        .map(|(digest, &count)| (expected.ids[digest], count))
        .collect();
    leftover.sort_unstable();
    for (id, count) in leftover {
        report.unmatched_source += u64::from(count);
        problems.push(
            "unmatched source row",
            format!(
                "source row {id} (and {} more with the same content) has no child row",
                count - 1
            ),
        );
    }
}

/// Every check's tally into the report, and its disagreements into the
/// problems.
fn record_checks(
    report: &mut ReconcileReport,
    problems: &mut Problems,
    checks: &[ColumnCheck],
    tallies: Vec<CheckTally>,
) {
    for (check, tally) in checks.iter().zip(tallies) {
        if tally.disagree > 0 {
            problems.push(
                "check",
                format!("{}: {} rows disagree", check.describe(), tally.disagree),
            );
        }
        let civil = matches!(check, ColumnCheck::CivilYear { .. });
        if civil && (tally.only_column > 0 || tally.only_other > 0) {
            problems.push(
                "check",
                format!(
                    "{}: {} rows carry the column without the timestamp, {} the timestamp \
                     without the column",
                    check.describe(),
                    tally.only_column,
                    tally.only_other
                ),
            );
        }
        report.checks.push((check.clone(), tally));
    }
}

#[cfg(test)]
mod tests;
