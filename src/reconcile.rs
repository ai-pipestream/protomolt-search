//! Reconcile a re-placement child against its sources, document for
//! document (`docs/derived-columns.md`, "Rebuild"): every live source
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

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::derived::Declaration;
use crate::pb::{self, AddDocumentsRequest};
use crate::placement::{Placement, PlacementTreeConfig};
use crate::reshard::{
    open_segment_exact, open_segment_sources, reconstruct_document, segment_tables, tree_children,
    SegmentSourceShard, SourceTables,
};

/// A check of one child column against another, independent of the
/// declaration's evaluator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnCheck {
    /// Two integer columns hold the same value on every row that
    /// carries both; the tally also counts rows carrying only one.
    Equal { column: String, other: String },
    /// `column` is the proleptic Gregorian UTC year of the epoch-micro
    /// timestamp in `timestamp`, counted here year by year from 1970;
    /// present exactly when the timestamp is.
    CivilYear { column: String, timestamp: String },
}

impl ColumnCheck {
    /// The `a:b` spelling of the command line.
    pub fn parse(kind: &str, spec: &str) -> Result<ColumnCheck, String> {
        let (left, right) = spec
            .split_once(':')
            .ok_or_else(|| format!("--{kind}={spec}: expected <column>:<other column>"))?;
        if left.is_empty() || right.is_empty() {
            return Err(format!("--{kind}={spec}: both column names are needed"));
        }
        match kind {
            "equal" => Ok(ColumnCheck::Equal {
                column: left.to_string(),
                other: right.to_string(),
            }),
            "civil-year" => Ok(ColumnCheck::CivilYear {
                column: left.to_string(),
                timestamp: right.to_string(),
            }),
            other => Err(format!("no column check named {other:?}")),
        }
    }

    fn describe(&self) -> String {
        match self {
            ColumnCheck::Equal { column, other } => format!("{column} == {other}"),
            ColumnCheck::CivilYear { column, timestamp } => {
                format!("{column} == civil year of {timestamp}")
            }
        }
    }
}

/// How one check came out over the child's live rows.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CheckTally {
    /// Rows carrying both columns.
    pub both: u64,
    /// Of those, rows where the check holds.
    pub agree: u64,
    /// Of those, rows where it does not.
    pub disagree: u64,
    /// Rows carrying the checked column and not the other.
    pub only_column: u64,
    /// Rows carrying the other column and not the checked one.
    pub only_other: u64,
    /// Rows carrying neither.
    pub neither: u64,
}

pub struct ReconcileOptions {
    /// The sources' log generations (`reshard::resolve_gen`); the
    /// sealed catalog beside each is read.
    pub sources: Vec<PathBuf>,
    /// The child: `<out>/shard-<i>.tv`, or its `.segments` root.
    pub child: PathBuf,
    pub tree: PlacementTreeConfig,
    /// The child's index in leaf order (`--only-child`, the shard map).
    pub child_index: usize,
    /// The declaration the child was written under; `None` for a child
    /// carrying none.
    pub declaration: Option<Arc<Declaration>>,
    /// The columns the split computed (`--derive`).
    pub derive: Vec<String>,
    pub checks: Vec<ColumnCheck>,
    /// Worker threads over segments; 0 takes one per available core, at
    /// most eight.
    pub threads: usize,
}

#[derive(Debug, Default)]
pub struct ReconcileReport {
    pub leaf: String,
    pub code: i64,
    pub source_segments: usize,
    pub source_rows_live: u64,
    /// Live source rows the tree sends to the child's leaf.
    pub source_rows_to_child: u64,
    pub child_segments: usize,
    pub child_rows_live: u64,
    pub child_rows_deleted: u64,
    pub matched: u64,
    /// Expected rows with no child counterpart.
    pub unmatched_source: u64,
    /// Child rows with no expected counterpart.
    pub unmatched_child: u64,
    pub checks: Vec<(ColumnCheck, CheckTally)>,
    /// Everything that disagrees, the first twenty of each kind named.
    pub problems: Vec<String>,
}

impl ReconcileReport {
    pub fn is_clean(&self) -> bool {
        self.problems.is_empty()
    }

    /// The report as lines.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "leaf {:?} code {}: {} source segments, {} live source rows, {} routed to the child\n",
            self.leaf,
            self.code,
            self.source_segments,
            self.source_rows_live,
            self.source_rows_to_child
        ));
        out.push_str(&format!(
            "child: {} segments, {} live rows, {} deleted rows\n",
            self.child_segments, self.child_rows_live, self.child_rows_deleted
        ));
        out.push_str(&format!(
            "matched {}; unmatched source {}; unmatched child {}\n",
            self.matched, self.unmatched_source, self.unmatched_child
        ));
        for (check, tally) in &self.checks {
            out.push_str(&format!(
                "check {}: both {} (agree {}, disagree {}); only the column {}; only the other \
                 {}; neither {}\n",
                check.describe(),
                tally.both,
                tally.agree,
                tally.disagree,
                tally.only_column,
                tally.only_other,
                tally.neither
            ));
        }
        if self.problems.is_empty() {
            out.push_str("clean\n");
        } else {
            out.push_str(&format!("{} problems\n", self.problems.len()));
            for problem in &self.problems {
                out.push_str("  ");
                out.push_str(problem);
                out.push('\n');
            }
        }
        out
    }
}

const NAMED: usize = 20;

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
fn open_child(path: &Path) -> Result<(SegmentSourceShard, SourceTables), String> {
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
    let mut tables: Option<SourceTables> = None;
    for i in 0..set.len() {
        let this = segment_tables(set.bm25(i));
        match &tables {
            None => tables = Some(this),
            Some(first) if *first != this => {
                return Err(format!(
                    "{} segment {}: declares tables that differ from the first segment's",
                    root.display(),
                    set.metadata(i).segment_id
                ))
            }
            Some(_) => {}
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
    Ok((
        SegmentSourceShard {
            gen: PathBuf::new(),
            root,
            set,
            overlay,
            deleted: BTreeSet::new(),
            slot_offset: 0,
        },
        tables.expect("a non-empty catalog has tables"),
    ))
}

/// The digest of a document's content: every value list sorted, so the
/// order a store lists columns in does not matter, plus the vector.
fn digest(doc: &AddDocumentsRequest, vector: Option<&[f32]>) -> [u8; 32] {
    let mut h = crate::sha256::Sha256::new();
    let mut bytes = |tag: u8, data: &[u8]| {
        h.update(&[tag]);
        h.update(&(data.len() as u64).to_le_bytes());
        h.update(data);
    };
    bytes(1, doc.text.as_bytes());
    match &doc.lineage {
        Some(l) => {
            let mut v = Vec::with_capacity(32);
            v.extend_from_slice(&l.parent_id.to_le_bytes());
            v.extend_from_slice(&l.group_id.to_le_bytes());
            v.extend_from_slice(&l.span_start.to_le_bytes());
            v.extend_from_slice(&l.span_end.to_le_bytes());
            bytes(2, &v);
        }
        None => bytes(3, &[]),
    }
    match &doc.identity {
        Some(identity) => {
            bytes(4, &identity.document_key);
            bytes(5, &identity.version.to_le_bytes());
            match identity.chunk_ordinal {
                Some(ordinal) => bytes(6, &ordinal.to_le_bytes()),
                None => bytes(7, &[]),
            }
        }
        None => bytes(8, &[]),
    }
    match &doc.original_source {
        Some(source) => {
            bytes(9, &source.descriptor_set);
            bytes(10, source.message_type.as_bytes());
            bytes(11, &source.payload);
        }
        None => bytes(12, &[]),
    }
    match doc.source_chunk_ordinal {
        Some(ordinal) => bytes(13, &ordinal.to_le_bytes()),
        None => bytes(14, &[]),
    }
    let mut facets: Vec<(&str, &str)> = doc
        .facets
        .iter()
        .map(|v| (v.field.as_str(), v.value.as_str()))
        .collect();
    facets.sort_unstable();
    for (field, value) in facets {
        bytes(20, field.as_bytes());
        bytes(21, value.as_bytes());
    }
    let mut numerics: Vec<(&str, u64)> = doc
        .numerics
        .iter()
        .map(|v| (v.field.as_str(), v.value.to_bits()))
        .collect();
    numerics.sort_unstable();
    for (field, value) in numerics {
        bytes(22, field.as_bytes());
        bytes(23, &value.to_le_bytes());
    }
    let mut integers: Vec<(&str, i64)> = doc
        .integers
        .iter()
        .map(|v| (v.field.as_str(), v.value))
        .collect();
    integers.sort_unstable();
    for (field, value) in integers {
        bytes(24, field.as_bytes());
        bytes(25, &value.to_le_bytes());
    }
    let mut unsigned: Vec<(&str, u64)> = doc
        .unsigned_integers
        .iter()
        .map(|v| (v.field.as_str(), v.value))
        .collect();
    unsigned.sort_unstable();
    for (field, value) in unsigned {
        bytes(26, field.as_bytes());
        bytes(27, &value.to_le_bytes());
    }
    let mut geo: Vec<(&str, u64, u64)> = doc
        .geo_points
        .iter()
        .map(|v| (v.field.as_str(), v.lat.to_bits(), v.lon.to_bits()))
        .collect();
    geo.sort_unstable();
    for (field, lat, lon) in geo {
        bytes(28, field.as_bytes());
        bytes(29, &lat.to_le_bytes());
        bytes(30, &lon.to_le_bytes());
    }
    let mut map_facets: Vec<(&str, &str, &str)> = doc
        .map_facets
        .iter()
        .map(|v| (v.field.as_str(), v.key.as_str(), v.value.as_str()))
        .collect();
    map_facets.sort_unstable();
    for (field, key, value) in map_facets {
        bytes(31, field.as_bytes());
        bytes(32, key.as_bytes());
        bytes(33, value.as_bytes());
    }
    let mut map_numerics: Vec<(&str, &str, u64)> = doc
        .map_numerics
        .iter()
        .map(|v| (v.field.as_str(), v.key.as_str(), v.value.to_bits()))
        .collect();
    map_numerics.sort_unstable();
    for (field, key, value) in map_numerics {
        bytes(34, field.as_bytes());
        bytes(35, key.as_bytes());
        bytes(36, &value.to_le_bytes());
    }
    bytes(40, doc.derived_fingerprint.as_bytes());
    match vector {
        Some(vector) => {
            let mut v = Vec::with_capacity(vector.len() * 4);
            for x in vector {
                v.extend_from_slice(&x.to_bits().to_le_bytes());
            }
            bytes(50, &v);
        }
        None => bytes(51, &[]),
    }
    h.finalize()
}

fn integer_of(doc: &AddDocumentsRequest, name: &str) -> Option<i64> {
    doc.integers
        .iter()
        .find(|v| v.field == name)
        .map(|v| v.value)
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// The UTC civil year of an epoch-micro timestamp, counted year by year
/// from 1970 over whole days: the independent form of `calendar.year`.
pub fn civil_year(micros: i64) -> i64 {
    let mut days = micros.div_euclid(86_400_000_000);
    let mut year = 1970i64;
    if days >= 0 {
        loop {
            let length = if is_leap(year) { 366 } else { 365 };
            if days < length {
                return year;
            }
            days -= length;
            year += 1;
        }
    }
    loop {
        year -= 1;
        days += if is_leap(year) { 366 } else { 365 };
        if days >= 0 {
            return year;
        }
    }
}

fn tally(check: &ColumnCheck, doc: &AddDocumentsRequest, tally: &mut CheckTally) {
    let (column, other) = match check {
        ColumnCheck::Equal { column, other } => (integer_of(doc, column), integer_of(doc, other)),
        ColumnCheck::CivilYear { column, timestamp } => {
            (integer_of(doc, column), integer_of(doc, timestamp))
        }
    };
    let holds = |a: i64, b: i64| match check {
        ColumnCheck::Equal { .. } => a == b,
        ColumnCheck::CivilYear { .. } => a == civil_year(b),
    };
    match (column, other) {
        (Some(a), Some(b)) => {
            tally.both += 1;
            if holds(a, b) {
                tally.agree += 1;
            } else {
                tally.disagree += 1;
            }
        }
        (Some(_), None) => tally.only_column += 1,
        (None, Some(_)) => tally.only_other += 1,
        (None, None) => tally.neither += 1,
    }
}

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

/// Digest every live row of `shard`'s segments in parallel, `each`
/// turning a reconstructed row into the digest to file, or `None` to
/// skip it (a source row for another leaf).
#[allow(clippy::type_complexity)]
fn digest_rows<F>(
    shard: &SegmentSourceShard,
    tables: &SourceTables,
    threads: usize,
    each: F,
) -> Result<(Vec<Vec<([u8; 32], u64)>>, u64, u64), String>
where
    F: Fn(u64, AddDocumentsRequest, Option<&[f32]>) -> Result<Option<[u8; 32]>, String> + Sync,
{
    let next = AtomicUsize::new(0);
    let failure: Mutex<Option<String>> = Mutex::new(None);
    let live = AtomicU64::new(0);
    let deleted = AtomicU64::new(0);
    let per_thread: Mutex<Vec<Vec<([u8; 32], u64)>>> = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                let mut mine: Vec<([u8; 32], u64)> = Vec::new();
                loop {
                    if failure.lock().expect("lock").is_some() {
                        break;
                    }
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    if i >= shard.set.len() {
                        break;
                    }
                    let meta = shard.set.metadata(i);
                    let bm25 = shard.set.bm25(i);
                    let rows = bm25.next_doc_id();
                    let outcome = (|| -> Result<(), String> {
                        if u64::from(rows) != meta.rows {
                            return Err(format!(
                                "{} segment {}: the store has {rows} rows, the metadata {}",
                                shard.root.display(),
                                meta.segment_id,
                                meta.rows
                            ));
                        }
                        let exact = open_segment_exact(shard, i)?;
                        for row in 0..rows {
                            let label = meta.base_label + u64::from(row);
                            if !shard.is_live(i, row, label) {
                                deleted.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            live.fetch_add(1, Ordering::Relaxed);
                            let id = shard.slot_offset + label;
                            let doc =
                                reconstruct_document(bm25, row, tables, true).map_err(|e| {
                                    format!(
                                        "{} segment {}: {e}",
                                        shard.root.display(),
                                        meta.segment_id
                                    )
                                })?;
                            let vector = exact
                                .as_ref()
                                .map(|store| store.row_values(row as usize, row as usize + 1));
                            if let Some(digest) = each(id, doc, vector.as_deref())? {
                                mine.push((digest, id));
                            }
                        }
                        Ok(())
                    })();
                    if let Err(e) = outcome {
                        let mut slot = failure.lock().expect("lock");
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                        break;
                    }
                }
                per_thread.lock().expect("lock").push(mine);
            });
        }
    });
    if let Some(e) = failure.into_inner().expect("lock") {
        return Err(e);
    }
    Ok((
        per_thread.into_inner().expect("lock"),
        live.into_inner(),
        deleted.into_inner(),
    ))
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
    let threads = if options.threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(8)
    } else {
        options.threads
    };
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
    let code = child.code;
    let column = placement.column().to_string();
    let mut report = ReconcileReport {
        leaf: child.leaf.clone(),
        code,
        ..Default::default()
    };
    let mut problems = Problems::default();
    let declaration = options.declaration.as_deref();

    let (sources, source_tables) = open_segment_sources(&options.sources)?;
    let (child_shard, child_tables) = open_child(&options.child)?;
    report.source_segments = sources.iter().map(|s| s.set.len()).sum();
    report.child_segments = child_shard.set.len();

    // Tables: fields as the sources', columns with the derived names.
    let expected = expected_tables(&source_tables, declaration);
    if child_tables.fields != expected.fields {
        problems.push(
            "tables",
            format!(
                "child field table {:?} differs from the sources' {:?}",
                child_tables.fields, expected.fields
            ),
        );
    }
    if child_tables.fingerprints != expected.fingerprints {
        problems.push(
            "tables",
            format!(
                "child analysis fingerprints {:?} differ from the sources' {:?}",
                child_tables.fingerprints, expected.fingerprints
            ),
        );
    }
    if child_tables.columns != expected.columns {
        problems.push(
            "tables",
            format!(
                "child column tables {:?} differ from the expected {:?} (the sources' with the \
                 declared names appended)",
                child_tables.columns, expected.columns
            ),
        );
    }
    // Every child segment carries the declaration, and only it.
    for i in 0..child_shard.set.len() {
        let stored = child_shard.set.bm25(i).derived();
        let segment = &child_shard.set.metadata(i).segment_id;
        match (declaration, stored) {
            (Some(declaration), Some(stored))
                if stored.fingerprint == declaration.fingerprint() => {}
            (Some(declaration), Some(stored)) => problems.push(
                "declaration",
                format!(
                    "child segment {segment} carries declaration {} where {} was expected",
                    stored.fingerprint,
                    declaration.fingerprint()
                ),
            ),
            (Some(declaration), None) => problems.push(
                "declaration",
                format!(
                    "child segment {segment} carries no declaration where {} was expected",
                    declaration.fingerprint()
                ),
            ),
            (None, Some(stored)) => problems.push(
                "declaration",
                format!(
                    "child segment {segment} carries declaration {} where none was expected",
                    stored.fingerprint
                ),
            ),
            (None, None) => {}
        }
    }

    // The expected rows: each live source row through the split's own
    // transformation, kept when the tree sends it to this leaf.
    let derive = &options.derive;
    let routed = AtomicU64::new(0);
    let mut expected_rows: HashMap<[u8; 32], u32> = HashMap::new();
    let mut expected_ids: HashMap<[u8; 32], u64> = HashMap::new();
    let mut source_live = 0u64;
    for shard in &sources {
        let (lists, live, _) = digest_rows(shard, &source_tables, threads, |id, doc, vector| {
            let doc = match declaration {
                Some(declaration) => declaration
                    .rederive(doc, derive, None)
                    .map_err(|e| format!("live source id {id}: {}", e.message()))?,
                None if !doc.derived_fingerprint.is_empty() => {
                    return Err(format!(
                        "live source id {id} was derived under declaration {:?} and the child \
                         is expected to carry none",
                        doc.derived_fingerprint
                    ))
                }
                None => doc,
            };
            let leaf = placement
                .evaluate(&doc)
                .map_err(|e| format!("live source id {id}: {e}"))?;
            if leaf.code != code {
                return Ok(None);
            }
            routed.fetch_add(1, Ordering::Relaxed);
            let mut rewritten = doc;
            rewritten.integers.retain(|value| value.field != column);
            rewritten.integers.push(pb::IntegerValue {
                field: column.clone(),
                value: code,
            });
            Ok(Some(digest(&rewritten, vector)))
        })?;
        source_live += live;
        for list in lists {
            for (digest, id) in list {
                *expected_rows.entry(digest).or_insert(0) += 1;
                expected_ids.entry(digest).or_insert(id);
            }
        }
    }
    report.source_rows_live = source_live;
    report.source_rows_to_child = routed.into_inner();

    // The child's rows, each digested the same way and checked.
    let checks = &options.checks;
    let tallies: Mutex<Vec<CheckTally>> = Mutex::new(vec![CheckTally::default(); checks.len()]);
    let (lists, live, deleted) =
        digest_rows(&child_shard, &child_tables, threads, |_id, doc, vector| {
            if !checks.is_empty() {
                let mut mine: Vec<CheckTally> = vec![CheckTally::default(); checks.len()];
                for (check, tally_of) in checks.iter().zip(mine.iter_mut()) {
                    tally(check, &doc, tally_of);
                }
                let mut all = tallies.lock().expect("lock");
                for (total, part) in all.iter_mut().zip(mine) {
                    total.both += part.both;
                    total.agree += part.agree;
                    total.disagree += part.disagree;
                    total.only_column += part.only_column;
                    total.only_other += part.only_other;
                    total.neither += part.neither;
                }
            }
            Ok(Some(digest(&doc, vector)))
        })?;
    report.child_rows_live = live;
    report.child_rows_deleted = deleted;
    let mut child_ids: Vec<(u64, [u8; 32])> = Vec::new();
    for list in lists {
        for (digest, id) in list {
            child_ids.push((id, digest));
        }
    }
    child_ids.sort_unstable();
    for (id, digest) in child_ids {
        match expected_rows.get_mut(&digest) {
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
    let mut leftover: Vec<(u64, u32)> = expected_rows
        .iter()
        .filter(|(_, &count)| count > 0)
        .map(|(digest, &count)| (expected_ids[digest], count))
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
    if report.source_rows_to_child != report.child_rows_live {
        problems.push(
            "counts",
            format!(
                "{} source rows route to the leaf, the child holds {} live rows",
                report.source_rows_to_child, report.child_rows_live
            ),
        );
    }
    let tallies = tallies.into_inner().expect("lock");
    for (check, tally) in checks.iter().zip(tallies) {
        if tally.disagree > 0 {
            problems.push(
                "check",
                format!("{}: {} rows disagree", check.describe(), tally.disagree),
            );
        }
        if let ColumnCheck::CivilYear { .. } = check {
            if tally.only_column > 0 || tally.only_other > 0 {
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
        }
        report.checks.push((check.clone(), tally));
    }
    report.problems = problems.finish();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_civil_year_counts_days_from_the_epoch() {
        let day = 86_400_000_000i64;
        assert_eq!(civil_year(0), 1970);
        assert_eq!(civil_year(-1), 1969);
        assert_eq!(civil_year(365 * day - 1), 1970);
        assert_eq!(civil_year(365 * day), 1971);
        // 1972 is a leap year: 1970 and 1971 are 730 days, 1972 366.
        assert_eq!(civil_year((730 + 365) * day), 1972);
        assert_eq!(civil_year((730 + 366) * day), 1973);
        // 2000-01-01T00:00:00Z is 946684800 s after the epoch.
        assert_eq!(civil_year(946_684_800_000_000), 2000);
        assert_eq!(civil_year(946_684_800_000_000 - 1), 1999);
        // 1900-01-01T00:00:00Z is -2208988800 s; 1900 is not a leap year.
        assert_eq!(civil_year(-2_208_988_800_000_000), 1900);
        assert_eq!(civil_year(-2_208_988_800_000_000 - 1), 1899);
        for (micros, year) in [
            (crate::calendar::days_from_civil(1600, 2, 29) * day, 1600),
            (crate::calendar::days_from_civil(202, 1, 1) * day, 202),
            (crate::calendar::days_from_civil(202, 12, 31) * day, 202),
            (crate::calendar::days_from_civil(2100, 3, 1) * day, 2100),
        ] {
            assert_eq!(civil_year(micros), year, "{micros}");
        }
    }

    #[test]
    fn a_digest_ignores_the_order_of_a_value_list() {
        let mut a = AddDocumentsRequest {
            text: "t".into(),
            ..Default::default()
        };
        a.integers.push(pb::IntegerValue {
            field: "year".into(),
            value: 1,
        });
        a.integers.push(pb::IntegerValue {
            field: "decided".into(),
            value: 2,
        });
        let mut b = a.clone();
        b.integers.reverse();
        assert_eq!(digest(&a, None), digest(&b, None));
        b.integers[0].value += 1;
        assert_ne!(digest(&a, None), digest(&b, None));
        assert_ne!(digest(&a, None), digest(&a, Some(&[0.0])));
    }

    #[test]
    fn a_check_spec_names_two_columns() {
        assert_eq!(
            ColumnCheck::parse("equal", "a:b").unwrap(),
            ColumnCheck::Equal {
                column: "a".into(),
                other: "b".into()
            }
        );
        assert!(ColumnCheck::parse("equal", "a").is_err());
        assert!(ColumnCheck::parse("civil-year", ":b").is_err());
        assert!(ColumnCheck::parse("other", "a:b").is_err());
    }
}
