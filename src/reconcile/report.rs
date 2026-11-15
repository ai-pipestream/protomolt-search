//! What a reconciliation is asked to do and what it reports.

use std::path::PathBuf;
use std::sync::Arc;

use crate::derived::Declaration;
use crate::placement::PlacementTreeConfig;

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

    pub(super) fn describe(&self) -> String {
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

impl CheckTally {
    pub(super) fn add(&mut self, part: &CheckTally) {
        self.both += part.both;
        self.agree += part.agree;
        self.disagree += part.disagree;
        self.only_column += part.only_column;
        self.only_other += part.only_other;
        self.neither += part.neither;
    }
}

/// What to reconcile: the sources, the child, the tree it was split
/// under, and the checks to run.
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

/// What the reconciliation found.
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
