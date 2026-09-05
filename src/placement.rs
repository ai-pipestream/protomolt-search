//! Placement trees (`docs/placement.md`): CEL as the partition function.
//!
//! An ordered chain of predicates at each level assigns every document
//! to a leaf, and a leaf is a shard group with its own shard count and
//! node set. The choice is stored on the row as an `i64` column holding
//! the root-to-leaf path as a prefix code, so a subtree is one integer
//! range and the segment pruner's range rules cover the tree with no new
//! operator in the filter dialect.
//!
//! This module is the shared core: the tree as configured (TOML in the
//! shard map) and as published (proto), validation, and the code
//! arithmetic. Ingest evaluation, fan-out pruning, the dry run, and the
//! leaf reshard build on it.
//!
//! Rules the validation enforces:
//!
//! - Every chain is ordered and first match wins. A node with an empty
//!   `cel` is its level's default and must be last; the root chain and
//!   every node with children end with one. A document on which every
//!   predicate of a level is false or UNKNOWN takes the default.
//! - Predicates are in the FILTER dialect, so the pruner can reason
//!   about them; a rule needing arithmetic goes through a materialized
//!   column first (`docs/cel-values.md`).
//! - The code stores the chosen index per level in a fixed field of
//!   `level_bits` bits, parent above child, sign bit unused. A leaf's
//!   own predicates along its path (the non-default ones) are a sound
//!   superset of its rows, which is what fan-out pruning uses; the
//!   "AND NOT earlier siblings" part is realized by first-match at
//!   ingest and is never needed for pruning.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::filter::{cmp_f64_i64, edge_of, int_range, Edge, NumBound, Tri};
use crate::pb;

/// The field width (bits per level) when the tree does not say.
pub const DEFAULT_LEVEL_BITS: u32 = 9;
/// Usable bits: `i64` without its sign, so placement codes stay nonnegative.
pub const CODE_BITS: u32 = 63;

/// One node as written in a shard map's `[placement]` table.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlacementNodeConfig {
    pub name: String,
    /// Filter-dialect CEL; absent or empty marks the level's default.
    #[serde(default)]
    pub cel: Option<String>,
    /// Shards in this leaf (leaves only; 0 selects 1).
    #[serde(default)]
    pub shards: u32,
    /// Node addresses this leaf's shards may live on (leaves only).
    #[serde(default)]
    pub nodes: Vec<String>,
    #[serde(default)]
    pub children: Vec<PlacementNodeConfig>,
}

/// The tree as written in a shard map's `[placement]` table.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlacementTreeConfig {
    /// The `i64` column every row carries with its path code.
    pub column: String,
    /// Bits per level; 0 selects [`DEFAULT_LEVEL_BITS`].
    #[serde(default)]
    pub level_bits: u32,
    #[serde(default)]
    pub nodes: Vec<PlacementNodeConfig>,
}

impl PlacementTreeConfig {
    pub fn to_proto(&self) -> pb::PlacementTree {
        fn node(n: &PlacementNodeConfig) -> pb::PlacementNode {
            pb::PlacementNode {
                name: n.name.clone(),
                cel: n.cel.clone().unwrap_or_default(),
                shards: n.shards,
                nodes: n.nodes.clone(),
                children: n.children.iter().map(node).collect(),
            }
        }
        pb::PlacementTree {
            column: self.column.clone(),
            level_bits: self.level_bits,
            nodes: self.nodes.iter().map(node).collect(),
        }
    }

    pub fn from_proto(tree: &pb::PlacementTree) -> Self {
        fn node(n: &pb::PlacementNode) -> PlacementNodeConfig {
            PlacementNodeConfig {
                name: n.name.clone(),
                cel: (!n.cel.is_empty()).then(|| n.cel.clone()),
                shards: n.shards,
                nodes: n.nodes.clone(),
                children: n.children.iter().map(node).collect(),
            }
        }
        PlacementTreeConfig {
            column: tree.column.clone(),
            level_bits: tree.level_bits,
            nodes: tree.nodes.iter().map(node).collect(),
        }
    }
}

/// One leaf of a validated tree.
#[derive(Debug, Clone)]
pub struct Leaf {
    /// Dotted path of node names from the root.
    pub name: String,
    /// The path code every row of this leaf carries.
    pub code: i64,
    /// Index per level from the root.
    pub path: Vec<u32>,
    pub shards: u32,
    pub nodes: Vec<String>,
    /// The compiled predicates of every non-default node on the path.
    /// A row in this leaf satisfies all of them; a filter that cannot
    /// hold together with them cannot match a row here.
    pub own: Vec<pb::FilterExpr>,
    /// Whether the last node on the path is a level default.
    pub is_default: bool,
    /// What `own` says per column, plus the placement column pinned to
    /// this leaf's code: the ranges and value sets a filter is tested
    /// against before fan-out.
    pub bounds: ColumnBounds,
}

/// One node of the validated tree with its predicate compiled once.
#[derive(Debug, Clone)]
struct CompiledNode {
    /// `None` marks the level's default.
    predicate: Option<pb::FilterExpr>,
    children: Vec<CompiledNode>,
}

/// A validated placement tree.
#[derive(Debug, Clone)]
pub struct Placement {
    column: String,
    level_bits: u32,
    depth: u32,
    leaves: Vec<Leaf>,
    config: PlacementTreeConfig,
    compiled: Vec<CompiledNode>,
}

impl Placement {
    /// Validate a configured tree. Every refusal names the node.
    pub fn validate(config: &PlacementTreeConfig) -> Result<Placement, String> {
        let column = config.column.trim();
        if column.is_empty() {
            return Err("placement: column is empty".to_string());
        }
        if column.chars().any(char::is_whitespace) {
            return Err(format!("placement: column {column:?} holds whitespace"));
        }
        let level_bits = if config.level_bits == 0 {
            DEFAULT_LEVEL_BITS
        } else {
            config.level_bits
        };
        if !(1..=32).contains(&level_bits) {
            return Err(format!(
                "placement: level_bits {level_bits} is outside 1..=32"
            ));
        }
        if config.nodes.is_empty() {
            return Err("placement: the root has no nodes".to_string());
        }
        let mut leaves = Vec::new();
        let mut depth = 0u32;
        let compiled = Self::walk(
            column,
            &config.nodes,
            "",
            &[],
            &[],
            level_bits,
            &mut leaves,
            &mut depth,
        )?;
        Ok(Placement {
            column: column.to_string(),
            level_bits,
            depth,
            leaves,
            config: config.clone(),
            compiled,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn walk(
        column: &str,
        chain: &[PlacementNodeConfig],
        prefix: &str,
        path: &[u32],
        own: &[pb::FilterExpr],
        level_bits: u32,
        leaves: &mut Vec<Leaf>,
        depth: &mut u32,
    ) -> Result<Vec<CompiledNode>, String> {
        let mut compiled = Vec::with_capacity(chain.len());
        let level = path.len() as u32 + 1;
        if level * level_bits > CODE_BITS {
            return Err(format!(
                "placement: level {level} at {level_bits} bits per level exceeds {CODE_BITS} bits \
                 under {prefix:?}"
            ));
        }
        if chain.len() as u64 > 1u64 << level_bits {
            return Err(format!(
                "placement: {} nodes under {prefix:?} exceed {} at {level_bits} bits per level",
                chain.len(),
                1u64 << level_bits
            ));
        }
        *depth = (*depth).max(level);
        let mut names = std::collections::HashSet::new();
        let last = chain.len() - 1;
        for (index, node) in chain.iter().enumerate() {
            let name = node.name.trim();
            if name.is_empty() || name.contains('.') || name.chars().any(char::is_whitespace) {
                return Err(format!(
                    "placement: node {index} under {prefix:?} has an invalid name {:?} (non-empty, \
                     no dots, no whitespace)",
                    node.name
                ));
            }
            if !names.insert(name) {
                return Err(format!(
                    "placement: node name {name:?} repeats under {prefix:?}"
                ));
            }
            let full = if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{prefix}.{name}")
            };
            let cel = node.cel.as_deref().map(str::trim).unwrap_or("");
            let is_default = cel.is_empty();
            if is_default && index != last {
                return Err(format!(
                    "placement: default node {full:?} must be last in its chain"
                ));
            }
            let mut own_here = own.to_vec();
            let mut predicate = None;
            if !is_default {
                let expr = crate::cel::compile_filter(cel)
                    .map_err(|status| format!("placement: node {full:?}: {}", status.message()))?
                    .ok_or_else(|| format!("placement: node {full:?} compiles to no filter"))?;
                predicate = Some(expr.clone());
                own_here.push(expr);
            }
            let mut path_here = path.to_vec();
            path_here.push(index as u32);
            let mut children = Vec::new();
            if node.children.is_empty() {
                let code = encode(&path_here, level_bits);
                let mut bounds = ColumnBounds::of_conjunction(&own_here);
                bounds.pin_int(column, code);
                leaves.push(Leaf {
                    name: full,
                    code,
                    path: path_here,
                    shards: node.shards.max(1),
                    nodes: node.nodes.clone(),
                    own: own_here,
                    is_default,
                    bounds,
                });
            } else {
                if node.shards != 0 || !node.nodes.is_empty() {
                    return Err(format!(
                        "placement: node {full:?} has children and also shards or nodes; size \
                         the leaves"
                    ));
                }
                children = Self::walk(
                    column,
                    &node.children,
                    &full,
                    &path_here,
                    &own_here,
                    level_bits,
                    leaves,
                    depth,
                )?;
            }
            compiled.push(CompiledNode {
                predicate,
                children,
            });
        }
        let tail = chain[last].cel.as_deref().map(str::trim).unwrap_or("");
        if !tail.is_empty() {
            return Err(format!(
                "placement: the chain under {prefix:?} has no default (a last node with no cel)"
            ));
        }
        Ok(compiled)
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn level_bits(&self) -> u32 {
        self.level_bits
    }

    /// Levels in the deepest path.
    pub fn depth(&self) -> u32 {
        self.depth
    }

    /// Every leaf in chain order (which is code order).
    pub fn leaves(&self) -> &[Leaf] {
        &self.leaves
    }

    pub fn config(&self) -> &PlacementTreeConfig {
        &self.config
    }

    pub fn leaf_by_code(&self, code: i64) -> Option<&Leaf> {
        self.leaves.iter().find(|leaf| leaf.code == code)
    }

    pub fn leaf_by_name(&self, name: &str) -> Option<&Leaf> {
        self.leaves.iter().find(|leaf| leaf.name == name)
    }

    /// The inclusive code range of the subtree under `path` (the empty
    /// path is the whole tree).
    pub fn subtree_range(&self, path: &[u32]) -> (i64, i64) {
        subtree_range(path, self.level_bits)
    }
}

impl Placement {
    /// The leaf a document goes to: first match per level, UNKNOWN
    /// falls through, the level's default last. Evaluated over the
    /// document's own request values (`DocColumns`), the same
    /// three-valued rules a shard applies to its stored rows.
    pub fn evaluate(&self, doc: &pb::AddDocumentsRequest) -> Result<&Leaf, String> {
        let columns = DocColumns::of(doc)?;
        Ok(self.evaluate_columns(&columns))
    }

    /// [`Self::evaluate`] over columns already indexed.
    pub fn evaluate_columns(&self, columns: &DocColumns<'_>) -> &Leaf {
        let mut chain = &self.compiled;
        let mut path: Vec<u32> = Vec::new();
        loop {
            let last = chain.len() - 1;
            let mut chosen = last;
            for (index, node) in chain.iter().enumerate() {
                match node.predicate.as_ref() {
                    None => {
                        chosen = index;
                        break;
                    }
                    Some(expr) if eval_document(expr, columns) == Tri::True => {
                        chosen = index;
                        break;
                    }
                    Some(_) => {}
                }
            }
            path.push(chosen as u32);
            let node = &chain[chosen];
            if node.children.is_empty() {
                let code = encode(&path, self.level_bits);
                return self
                    .leaf_by_code(code)
                    .expect("validation listed a leaf for every path");
            }
            chain = &node.children;
        }
    }
}

/// One document's request values, indexed the way the filter dialect
/// reads a stored row: facets, integers (timestamps among them, as
/// epoch microseconds), numerics, map entries, and geo points, each by
/// column name.
pub struct DocColumns<'a> {
    doc: &'a pb::AddDocumentsRequest,
    timestamps: Vec<(&'a str, i64)>,
}

impl<'a> DocColumns<'a> {
    pub fn of(doc: &'a pb::AddDocumentsRequest) -> Result<Self, String> {
        let mut timestamps = Vec::with_capacity(doc.timestamps.len());
        for tv in &doc.timestamps {
            let Some(ts) = tv.value.as_ref() else {
                return Err(format!("timestamp field {:?} carries no instant", tv.field));
            };
            let micros = crate::node::timestamp_to_epoch_micros(&tv.field, ts)
                .map_err(|status| status.message().to_string())?;
            timestamps.push((tv.field.as_str(), micros));
        }
        Ok(DocColumns { doc, timestamps })
    }

    fn facet(&self, column: &str) -> Option<&str> {
        self.doc
            .facets
            .iter()
            .find(|f| f.field == column)
            .map(|f| f.value.as_str())
    }

    fn integer(&self, column: &str) -> Option<i64> {
        self.doc
            .integers
            .iter()
            .find(|v| v.field == column)
            .map(|v| v.value)
            .or_else(|| {
                self.timestamps
                    .iter()
                    .find(|(name, _)| *name == column)
                    .map(|(_, micros)| *micros)
            })
    }

    fn numeric(&self, column: &str) -> Option<f64> {
        self.doc
            .numerics
            .iter()
            .find(|v| v.field == column)
            .map(|v| v.value)
    }

    fn unsigned_integer(&self, column: &str) -> Option<u64> {
        self.doc
            .unsigned_integers
            .iter()
            .find(|v| v.field == column)
            .map(|v| v.value)
    }

    fn map_facet(&self, column: &str, key: &str) -> Option<&str> {
        self.doc
            .map_facets
            .iter()
            .find(|e| e.field == column && e.key == key)
            .map(|e| e.value.as_str())
    }

    fn map_numeric(&self, column: &str, key: &str) -> Option<f64> {
        self.doc
            .map_numerics
            .iter()
            .find(|e| e.field == column && e.key == key)
            .map(|e| e.value)
    }

    fn geo(&self, column: &str) -> Option<(f64, f64)> {
        self.doc
            .geo_points
            .iter()
            .find(|g| g.field == column)
            .map(|g| (g.lat, g.lon))
    }

    fn has(&self, column: &str) -> bool {
        self.facet(column).is_some()
            || self.integer(column).is_some()
            || self.unsigned_integer(column).is_some()
            || self.numeric(column).is_some()
            || self.geo(column).is_some()
    }
}

fn in_number_range(value: NumberValue, min: Option<Edge>, max: Option<Edge>) -> bool {
    match value {
        NumberValue::Int(v) => {
            let (lo, hi) = int_range(&min, &max);
            v >= lo && v <= hi
        }
        NumberValue::Uint(v) => {
            let (lo, hi) = crate::filter::uint_range(&min, &max);
            v >= lo && v <= hi
        }
        NumberValue::Float(v) => {
            min.as_ref().is_none_or(|e| e.admits_from_below(v))
                && max.as_ref().is_none_or(|e| e.admits_from_above(v))
        }
    }
}

#[derive(Clone, Copy)]
enum NumberValue {
    Int(i64),
    Uint(u64),
    Float(f64),
}

fn string_in_range(
    value: &str,
    min: Option<&pb::StringBound>,
    max: Option<&pb::StringBound>,
) -> bool {
    let above = min.is_none_or(|b| match value.as_bytes().cmp(b.value.as_bytes()) {
        Ordering::Greater => true,
        Ordering::Equal => !b.exclusive,
        Ordering::Less => false,
    });
    let below = max.is_none_or(|b| match value.as_bytes().cmp(b.value.as_bytes()) {
        Ordering::Less => true,
        Ordering::Equal => !b.exclusive,
        Ordering::Greater => false,
    });
    above && below
}

/// A validated filter tree evaluated over one document's request
/// values with the shard's three-valued rules (`docs/cel-filters.md`):
/// comparisons are UNKNOWN on absence, the presence tests are total,
/// `!` keeps UNKNOWN, and a document is in the set only on TRUE.
pub fn eval_document(expr: &pb::FilterExpr, doc: &DocColumns<'_>) -> Tri {
    use pb::filter_expr::Expr;
    match &expr.expr {
        Some(Expr::And(list)) => list
            .exprs
            .iter()
            .fold(Tri::True, |acc, child| acc.and(eval_document(child, doc))),
        Some(Expr::Or(list)) => list
            .exprs
            .iter()
            .fold(Tri::False, |acc, child| acc.or(eval_document(child, doc))),
        Some(Expr::Not(child)) => !eval_document(child, doc),
        Some(Expr::Facet(p)) => match doc.facet(&p.column) {
            None => Tri::Unknown,
            Some(value) => Tri::from(p.values.iter().any(|v| v == value)),
        },
        Some(Expr::Number(p)) => {
            let min = p.min.as_ref().and_then(edge_of);
            let max = p.max.as_ref().and_then(edge_of);
            if let Some(v) = doc.integer(&p.column) {
                Tri::from(in_number_range(NumberValue::Int(v), min, max))
            } else if let Some(v) = doc.unsigned_integer(&p.column) {
                Tri::from(in_number_range(NumberValue::Uint(v), min, max))
            } else if let Some(v) = doc.numeric(&p.column) {
                Tri::from(in_number_range(NumberValue::Float(v), min, max))
            } else {
                Tri::Unknown
            }
        }
        Some(Expr::MapFacet(p)) => match doc.map_facet(&p.column, &p.key) {
            None => Tri::Unknown,
            Some(value) => Tri::from(p.values.iter().any(|v| v == value)),
        },
        Some(Expr::MapNumber(p)) => match doc.map_numeric(&p.column, &p.key) {
            None => Tri::Unknown,
            Some(v) => Tri::from(in_number_range(
                NumberValue::Float(v),
                p.min.as_ref().and_then(edge_of),
                p.max.as_ref().and_then(edge_of),
            )),
        },
        Some(Expr::MapHasKey(p)) => Tri::from(
            doc.map_facet(&p.column, &p.key).is_some()
                || doc.map_numeric(&p.column, &p.key).is_some(),
        ),
        Some(Expr::Has(p)) => Tri::from(doc.has(&p.column)),
        Some(Expr::Geo(g)) => match doc.geo(&g.column) {
            None => Tri::Unknown,
            Some((lat, lon)) => match crate::node::validate_geo_filter(g) {
                Ok(region) => Tri::from(region.contains(lat, lon)),
                Err(_) => Tri::Unknown,
            },
        },
        Some(Expr::StringRange(p)) => {
            let value = if p.key.is_empty() {
                doc.facet(&p.column)
            } else {
                doc.map_facet(&p.column, &p.key)
            };
            match value {
                None => Tri::Unknown,
                Some(v) => Tri::from(string_in_range(v, p.min.as_ref(), p.max.as_ref())),
            }
        }
        Some(Expr::StringPrefix(p)) => {
            let value = if p.key.is_empty() {
                doc.facet(&p.column)
            } else {
                doc.map_facet(&p.column, &p.key)
            };
            match value {
                None => Tri::Unknown,
                Some(v) => Tri::from(v.as_bytes().starts_with(p.prefix.as_bytes())),
            }
        }
        None => Tri::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Bounds a leaf's predicates put on its rows, and the fan-out pruner.
// ---------------------------------------------------------------------------

/// Exact order of two bounds across the integer and float domains.
fn cmp_bound(a: &NumBound, b: &NumBound) -> Ordering {
    match (a, b) {
        (NumBound::I(x), NumBound::I(y)) => x.cmp(y),
        (NumBound::U(x), NumBound::U(y)) => x.cmp(y),
        (NumBound::U(x), NumBound::I(y)) => i128::from(*x).cmp(&i128::from(*y)),
        (NumBound::I(x), NumBound::U(y)) => i128::from(*x).cmp(&i128::from(*y)),
        (NumBound::F(x), NumBound::U(y)) => crate::filter::cmp_f64_u64(*x, *y),
        (NumBound::U(x), NumBound::F(y)) => crate::filter::cmp_f64_u64(*y, *x).reverse(),
        (NumBound::F(x), NumBound::F(y)) => x.total_cmp(y),
        (NumBound::F(x), NumBound::I(y)) => cmp_f64_i64(*x, *y),
        (NumBound::I(x), NumBound::F(y)) => cmp_f64_i64(*y, *x).reverse(),
    }
}

/// Whether the interval ending at `hi` lies entirely below the one
/// starting at `lo`: the two share no real number.
fn separated(hi: &Edge, lo: &Edge) -> bool {
    match cmp_bound(&hi.value, &lo.value) {
        Ordering::Less => true,
        Ordering::Equal => hi.exclusive || lo.exclusive,
        Ordering::Greater => false,
    }
}

/// The tighter of two lower edges.
fn tighter_lower(a: Edge, b: Edge) -> Edge {
    match cmp_bound(&a.value, &b.value) {
        Ordering::Greater => a,
        Ordering::Less => b,
        Ordering::Equal => {
            if a.exclusive {
                a
            } else {
                b
            }
        }
    }
}

/// The tighter of two upper edges.
fn tighter_upper(a: Edge, b: Edge) -> Edge {
    match cmp_bound(&a.value, &b.value) {
        Ordering::Less => a,
        Ordering::Greater => b,
        Ordering::Equal => {
            if a.exclusive {
                a
            } else {
                b
            }
        }
    }
}

fn merge_lower(current: &mut Option<Edge>, edge: Option<Edge>) {
    if let Some(edge) = edge {
        *current = Some(match current.take() {
            Some(have) => tighter_lower(have, edge),
            None => edge,
        });
    }
}

fn merge_upper(current: &mut Option<Edge>, edge: Option<Edge>) {
    if let Some(edge) = edge {
        *current = Some(match current.take() {
            Some(have) => tighter_upper(have, edge),
            None => edge,
        });
    }
}

/// What a conjunction of filter-dialect predicates guarantees about the
/// rows that satisfy it, per column: a numeric interval and a set of
/// admitted facet values. Only the sound part is kept: predicates under
/// `!` or `||`, map, geo, string, and presence predicates contribute
/// nothing, so the bounds describe a superset of the rows.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnBounds {
    numbers: HashMap<String, (Option<Edge>, Option<Edge>)>,
    facets: HashMap<String, BTreeSet<String>>,
}

impl ColumnBounds {
    /// The bounds of `exprs` taken together.
    pub fn of_conjunction(exprs: &[pb::FilterExpr]) -> Self {
        let mut bounds = ColumnBounds::default();
        for expr in exprs {
            bounds.absorb(expr);
        }
        bounds
    }

    fn absorb(&mut self, expr: &pb::FilterExpr) {
        use pb::filter_expr::Expr;
        match &expr.expr {
            Some(Expr::And(list)) => {
                for child in &list.exprs {
                    self.absorb(child);
                }
            }
            Some(Expr::Number(p)) => {
                let entry = self.numbers.entry(p.column.clone()).or_default();
                merge_lower(&mut entry.0, p.min.as_ref().and_then(edge_of));
                merge_upper(&mut entry.1, p.max.as_ref().and_then(edge_of));
            }
            Some(Expr::Facet(p)) => {
                let values: BTreeSet<String> = p.values.iter().cloned().collect();
                match self.facets.get_mut(&p.column) {
                    Some(have) => have.retain(|v| values.contains(v)),
                    None => {
                        self.facets.insert(p.column.clone(), values);
                    }
                }
            }
            _ => {}
        }
    }

    /// Pin an integer column to one value, the placement column to the
    /// leaf's code.
    pub fn pin_int(&mut self, column: &str, value: i64) {
        let edge = Edge {
            value: NumBound::I(value),
            exclusive: false,
        };
        let entry = self.numbers.entry(column.to_string()).or_default();
        merge_lower(&mut entry.0, Some(edge));
        merge_upper(&mut entry.1, Some(edge));
    }

    /// The numeric interval the bounds put on `column`, if any.
    pub fn number(&self, column: &str) -> Option<(Option<Edge>, Option<Edge>)> {
        self.numbers.get(column).copied()
    }

    /// The facet values the bounds admit on `column`, if any.
    pub fn facet(&self, column: &str) -> Option<&BTreeSet<String>> {
        self.facets.get(column)
    }
}

/// `Some(leaves)` when NO row under `bounds` can satisfy `filter`, with
/// the indices (in [`crate::filter::walk_leaves`] order) of the filter
/// leaves that established it; `None` means the shard must be asked.
/// The rules are the segment pruner's: an AND is impossible when a
/// child is, an OR when every child is, a NOT never, and only number
/// and facet-shaped leaves say anything.
pub fn impossible_under(filter: &pb::FilterExpr, bounds: &ColumnBounds) -> Option<Vec<usize>> {
    let mut next = 0usize;
    impossible_walk(filter, bounds, &mut next)
}

fn impossible_walk(
    expr: &pb::FilterExpr,
    bounds: &ColumnBounds,
    next: &mut usize,
) -> Option<Vec<usize>> {
    use pb::filter_expr::Expr;
    match &expr.expr {
        Some(Expr::And(list)) => {
            let mut found = None;
            for child in &list.exprs {
                let verdict = impossible_walk(child, bounds, next);
                if found.is_none() {
                    found = verdict;
                }
            }
            found
        }
        Some(Expr::Or(list)) => {
            let mut all = Vec::new();
            let mut every = !list.exprs.is_empty();
            for child in &list.exprs {
                match impossible_walk(child, bounds, next) {
                    Some(leaves) => all.extend(leaves),
                    None => every = false,
                }
            }
            every.then_some(all)
        }
        Some(Expr::Not(child)) => {
            let _ = impossible_walk(child, bounds, next);
            None
        }
        Some(Expr::Number(p)) => {
            let index = *next;
            *next += 1;
            let min = p.min.as_ref().and_then(edge_of);
            let max = p.max.as_ref().and_then(edge_of);
            if let (Some(lo), Some(hi)) = (min.as_ref(), max.as_ref()) {
                if separated(hi, lo) {
                    return Some(vec![index]);
                }
            }
            let (blo, bhi) = bounds.number(&p.column)?;
            let below = match (max.as_ref(), blo.as_ref()) {
                (Some(fmax), Some(blo)) => separated(fmax, blo),
                _ => false,
            };
            let above = match (bhi.as_ref(), min.as_ref()) {
                (Some(bhi), Some(fmin)) => separated(bhi, fmin),
                _ => false,
            };
            (below || above).then_some(vec![index])
        }
        Some(Expr::Facet(p)) => {
            let index = *next;
            *next += 1;
            let admitted = bounds.facet(&p.column)?;
            (!p.values.iter().any(|v| admitted.contains(v))).then_some(vec![index])
        }
        Some(Expr::StringRange(p)) => {
            let index = *next;
            *next += 1;
            if !p.key.is_empty() {
                return None;
            }
            let admitted = bounds.facet(&p.column)?;
            (!admitted
                .iter()
                .any(|v| string_in_range(v, p.min.as_ref(), p.max.as_ref())))
            .then_some(vec![index])
        }
        Some(Expr::StringPrefix(p)) => {
            let index = *next;
            *next += 1;
            if !p.key.is_empty() {
                return None;
            }
            let admitted = bounds.facet(&p.column)?;
            (!admitted
                .iter()
                .any(|v| v.as_bytes().starts_with(p.prefix.as_bytes())))
            .then_some(vec![index])
        }
        Some(Expr::MapFacet(_))
        | Some(Expr::MapNumber(_))
        | Some(Expr::MapHasKey(_))
        | Some(Expr::Has(_))
        | Some(Expr::Geo(_)) => {
            *next += 1;
            None
        }
        None => None,
    }
}

/// One request's verdict over a topology: which shards the filter
/// cannot match, and which filter leaves proved it (they count as
/// resolved for the typo handshake, since the leaf predicate that
/// excluded the shard named the same column).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardMask {
    pub skipped: Vec<bool>,
    /// Filter-leaf indices that took part in at least one exclusion.
    pub known: Vec<usize>,
}

impl ShardMask {
    /// The mask for `filter` over `codes` (one code per shard, `None`
    /// for a shard without one). A shard whose code the tree does not
    /// know is never skipped. One shard is always consulted: when the
    /// filter would exclude every shard, the first stays, so the known
    /// handshake and the answer's shape are the ones a consulted fleet
    /// produces.
    pub fn compute(placement: &Placement, codes: &[Option<i64>], filter: &pb::FilterExpr) -> Self {
        let mut skipped = vec![false; codes.len()];
        let mut known = BTreeSet::new();
        let mut verdicts: HashMap<i64, Option<Vec<usize>>> = HashMap::new();
        for (shard, code) in codes.iter().enumerate() {
            let Some(code) = code else { continue };
            let verdict = verdicts.entry(*code).or_insert_with(|| {
                placement
                    .leaf_by_code(*code)
                    .and_then(|leaf| impossible_under(filter, &leaf.bounds))
            });
            if let Some(leaves) = verdict {
                skipped[shard] = true;
                known.extend(leaves.iter().copied());
            }
        }
        if !skipped.is_empty() && skipped.iter().all(|s| *s) {
            skipped[0] = false;
        }
        ShardMask {
            skipped,
            known: known.into_iter().collect(),
        }
    }

    pub fn skipped_count(&self) -> u32 {
        self.skipped.iter().filter(|s| **s).count() as u32
    }
}

/// The path code: index per level in a fixed field, root highest.
pub fn encode(path: &[u32], level_bits: u32) -> i64 {
    let mut code: i64 = 0;
    for (level, index) in path.iter().enumerate() {
        let shift = CODE_BITS - level_bits * (level as u32 + 1);
        code |= (i64::from(*index)) << shift;
    }
    code
}

/// The inclusive code range of every path that extends `path`.
pub fn subtree_range(path: &[u32], level_bits: u32) -> (i64, i64) {
    let lo = encode(path, level_bits);
    let used = level_bits * path.len() as u32;
    let width = CODE_BITS - used;
    let span = if width >= 63 {
        i64::MAX
    } else {
        (1i64 << width) - 1
    };
    (lo, lo | span)
}

/// The index at `level` (0 = root) of `code`.
pub fn index_at(code: i64, level: u32, level_bits: u32) -> u32 {
    let shift = CODE_BITS - level_bits * (level + 1);
    ((code >> shift) & ((1i64 << level_bits) - 1)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(name: &str, cel: Option<&str>) -> PlacementNodeConfig {
        PlacementNodeConfig {
            name: name.into(),
            cel: cel.map(str::to_string),
            ..Default::default()
        }
    }

    fn tree(nodes: Vec<PlacementNodeConfig>) -> PlacementTreeConfig {
        PlacementTreeConfig {
            column: "placement".into(),
            level_bits: 0,
            nodes,
        }
    }

    #[test]
    fn codes_are_prefix_ranges() {
        let bits = 9;
        assert_eq!(encode(&[], bits), 0);
        assert_eq!(encode(&[0], bits), 0);
        assert_eq!(encode(&[1], bits), 1 << 54);
        assert_eq!(encode(&[1, 2], bits), (1 << 54) | (2 << 45));
        let (lo, hi) = subtree_range(&[1], bits);
        assert_eq!(lo, 1 << 54);
        assert_eq!(hi, (2 << 54) - 1);
        let (lo2, hi2) = subtree_range(&[1, 2], bits);
        assert!(lo <= lo2 && hi2 <= hi);
        assert_eq!(hi2 - lo2 + 1, 1 << 45);
        assert_eq!(index_at(encode(&[3, 7, 1], bits), 1, bits), 7);
        assert_eq!(index_at(encode(&[3, 7, 1], bits), 2, bits), 1);
        assert!(encode(&[511, 511, 511, 511, 511, 511, 511], bits) >= 0);
        let (_, whole_hi) = subtree_range(&[], bits);
        assert_eq!(whole_hi, i64::MAX);
    }

    #[test]
    fn a_valid_tree_lists_its_leaves_in_code_order() {
        let mut recent = leaf("recent", Some("year >= 2020"));
        recent.children = vec![
            leaf("scotus", Some("court == \"scotus\"")),
            leaf("rest", None),
        ];
        let config = tree(vec![
            leaf("large", Some("body_bytes >= 65536")),
            recent,
            leaf("other", None),
        ]);
        let placement = Placement::validate(&config).unwrap();
        let names: Vec<&str> = placement.leaves().iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, ["large", "recent.scotus", "recent.rest", "other"]);
        let codes: Vec<i64> = placement.leaves().iter().map(|l| l.code).collect();
        assert!(codes.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(placement.depth(), 2);
        let scotus = placement.leaf_by_name("recent.scotus").unwrap();
        assert_eq!(scotus.own.len(), 2, "both predicates on the path");
        assert!(!scotus.is_default);
        let rest = placement.leaf_by_name("recent.rest").unwrap();
        assert_eq!(rest.own.len(), 1, "only the parent's predicate");
        assert!(rest.is_default);
        let (lo, hi) = placement.subtree_range(&[1]);
        assert!(lo <= scotus.code && rest.code <= hi);
        assert!(placement.leaf_by_name("large").unwrap().code < lo);
        assert_eq!(
            placement.leaf_by_code(rest.code).unwrap().name,
            "recent.rest"
        );
        let round = PlacementTreeConfig::from_proto(&config.to_proto());
        assert_eq!(round, config);
    }

    #[test]
    fn refusals_name_the_node() {
        let no_default = tree(vec![leaf("a", Some("year >= 1"))]);
        let err = Placement::validate(&no_default).unwrap_err();
        assert!(err.contains("no default"), "{err}");

        let default_first = tree(vec![leaf("d", None), leaf("a", Some("year >= 1"))]);
        let err = Placement::validate(&default_first).unwrap_err();
        assert!(err.contains("\"d\"") && err.contains("last"), "{err}");

        let value_dialect = tree(vec![leaf("a", Some("year + 1 >= 2")), leaf("d", None)]);
        let err = Placement::validate(&value_dialect).unwrap_err();
        assert!(err.contains("\"a\""), "{err}");

        let repeated = tree(vec![
            leaf("a", Some("year >= 1")),
            leaf("a", Some("year >= 2")),
            leaf("d", None),
        ]);
        let err = Placement::validate(&repeated).unwrap_err();
        assert!(err.contains("repeats"), "{err}");

        let mut sized_parent = leaf("p", Some("year >= 1"));
        sized_parent.shards = 2;
        sized_parent.children = vec![leaf("d", None)];
        let err = Placement::validate(&tree(vec![sized_parent, leaf("d", None)])).unwrap_err();
        assert!(err.contains("children and also shards"), "{err}");

        let mut deep = tree(vec![leaf("a", Some("year >= 1")), leaf("d", None)]);
        deep.level_bits = 32;
        let mut inner = leaf("b", Some("year >= 2"));
        inner.children = vec![leaf("d", None)];
        deep.nodes[0].children = vec![inner, leaf("d", None)];
        let err = Placement::validate(&deep).unwrap_err();
        assert!(err.contains("exceeds"), "{err}");

        let mut wide = tree(
            (0..4)
                .map(|i| leaf(&format!("n{i}"), Some("year >= 1")))
                .collect(),
        );
        wide.nodes.push(leaf("d", None));
        wide.level_bits = 2;
        let err = Placement::validate(&wide).unwrap_err();
        assert!(err.contains("exceed"), "{err}");

        let empty = tree(vec![]);
        assert!(Placement::validate(&empty)
            .unwrap_err()
            .contains("no nodes"));
    }

    fn doc(court: Option<&str>, year: Option<i64>, score: Option<f64>) -> pb::AddDocumentsRequest {
        pb::AddDocumentsRequest {
            text: "t".into(),
            facets: court
                .map(|c| {
                    vec![pb::FacetValue {
                        field: "court".into(),
                        value: c.into(),
                    }]
                })
                .unwrap_or_default(),
            integers: year
                .map(|y| {
                    vec![pb::IntegerValue {
                        field: "year".into(),
                        value: y,
                    }]
                })
                .unwrap_or_default(),
            numerics: score
                .map(|v| {
                    vec![pb::NumericValue {
                        field: "score".into(),
                        value: v,
                    }]
                })
                .unwrap_or_default(),
            ..Default::default()
        }
    }

    fn two_level() -> Placement {
        let mut recent = leaf("recent", Some("year >= 2020"));
        recent.children = vec![
            leaf("scotus", Some("court == \"scotus\"")),
            leaf("rest", None),
        ];
        Placement::validate(&tree(vec![
            leaf("old", Some("year < 2000")),
            recent,
            leaf("other", None),
        ]))
        .unwrap()
    }

    #[test]
    fn a_document_takes_the_first_match_and_unknown_falls_through() {
        let placement = two_level();
        let name = |d: &pb::AddDocumentsRequest| placement.evaluate(d).unwrap().name.clone();
        assert_eq!(name(&doc(Some("ca9"), Some(1990), None)), "old");
        assert_eq!(
            name(&doc(Some("scotus"), Some(2021), None)),
            "recent.scotus"
        );
        assert_eq!(name(&doc(Some("ca9"), Some(2021), None)), "recent.rest");
        assert_eq!(
            name(&doc(None, Some(2021), None)),
            "recent.rest",
            "unknown court"
        );
        assert_eq!(name(&doc(Some("scotus"), Some(2010), None)), "other");
        assert_eq!(
            name(&doc(Some("scotus"), None, None)),
            "other",
            "unknown year"
        );
        // A timestamp is an integer column at evaluation time.
        let mut ts = doc(None, None, None);
        ts.timestamps.push(pb::TimestampValue {
            field: "year".into(),
            value: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
        });
        assert_eq!(name(&ts), "recent.rest");
    }

    #[test]
    fn document_evaluation_matches_the_shard_rules() {
        let columns_doc = doc(Some("ca9"), Some(2001), Some(0.5));
        let columns = DocColumns::of(&columns_doc).unwrap();
        let tri = |cel: &str| {
            let expr = crate::cel::compile_filter(cel).unwrap().unwrap();
            eval_document(&expr, &columns)
        };
        assert_eq!(tri("year >= 2001"), Tri::True);
        assert_eq!(tri("year > 2001"), Tri::False);
        assert_eq!(tri("year > 2000.5"), Tri::True);
        assert_eq!(tri("score < 0.5"), Tri::False);
        assert_eq!(tri("score <= 0.5"), Tri::True);
        assert_eq!(tri("court == \"ca9\""), Tri::True);
        assert_eq!(tri("court in [\"ca5\", \"scotus\"]"), Tri::False);
        assert_eq!(tri("!(court == \"ca9\")"), Tri::False);
        assert_eq!(tri("pages >= 1"), Tri::Unknown);
        assert_eq!(tri("!(pages >= 1)"), Tri::Unknown);
        assert_eq!(tri("has(pages)"), Tri::False);
        assert_eq!(tri("has(court)"), Tri::True);
        assert_eq!(tri("\"k\" in tags"), Tri::False);
        assert_eq!(tri("court.startsWith(\"ca\")"), Tri::True);
        assert_eq!(tri("court < \"cb\" && court >= \"ca9\""), Tri::True);
        assert_eq!(tri("court > \"ca9\""), Tri::False);
        assert_eq!(tri("pages >= 1 || year == 2001"), Tri::True);
        assert_eq!(tri("pages >= 1 && year == 2001"), Tri::Unknown);
    }

    fn compile(cel: &str) -> pb::FilterExpr {
        crate::cel::compile_filter(cel).unwrap().unwrap()
    }

    #[test]
    fn a_filter_the_path_cannot_hold_with_excludes_the_leaf() {
        let placement = two_level();
        let scotus = placement.leaf_by_name("recent.scotus").unwrap();
        let rest = placement.leaf_by_name("recent.rest").unwrap();
        let old = placement.leaf_by_name("old").unwrap();
        let other = placement.leaf_by_name("other").unwrap();
        let verdict = |leaf: &Leaf, cel: &str| impossible_under(&compile(cel), &leaf.bounds);
        // Ranges on the path's column.
        assert_eq!(verdict(scotus, "year < 2000"), Some(vec![0]));
        assert_eq!(verdict(scotus, "year < 2020"), Some(vec![0]));
        assert_eq!(verdict(scotus, "year <= 2020"), None);
        assert_eq!(verdict(scotus, "year < 2020.5"), None);
        assert_eq!(verdict(old, "year >= 2000"), Some(vec![0]));
        assert_eq!(verdict(old, "year > 1999.5"), None);
        assert_eq!(
            verdict(other, "year < 1900"),
            None,
            "the default carries no bound"
        );
        // Facet equality on the path.
        assert_eq!(verdict(scotus, "court == \"ca9\""), Some(vec![0]));
        assert_eq!(verdict(scotus, "court in [\"ca9\", \"scotus\"]"), None);
        assert_eq!(verdict(rest, "court == \"ca9\""), None);
        assert_eq!(verdict(scotus, "court.startsWith(\"ca\")"), Some(vec![0]));
        assert_eq!(verdict(scotus, "court < \"s\""), Some(vec![0]));
        // Connectives: AND takes one impossible child, OR needs all,
        // NOT never prunes, and indices follow the walk order.
        assert_eq!(verdict(scotus, "pages >= 1 && year < 2000"), Some(vec![1]));
        assert_eq!(
            verdict(scotus, "year < 2000 || court == \"ca9\""),
            Some(vec![0, 1])
        );
        assert_eq!(verdict(scotus, "year < 2000 || has(pages)"), None);
        assert_eq!(verdict(scotus, "!(year >= 2020)"), None);
        assert_eq!(verdict(scotus, "has(year)"), None);
        // The placement column itself is pinned per leaf.
        assert_eq!(
            verdict(scotus, &format!("placement > {}", scotus.code)),
            Some(vec![0])
        );
        let (lo, hi) = placement.subtree_range(&[1]);
        assert_eq!(
            verdict(scotus, &format!("placement >= {lo} && placement <= {hi}")),
            None
        );
        assert_eq!(verdict(old, &format!("placement >= {lo}")), Some(vec![0]));
        // An empty filter range excludes on its own.
        assert_eq!(verdict(other, "year > 5 && year < 5"), None);
        assert_eq!(verdict(other, "year == 5 && year == 6"), None);
    }

    #[test]
    fn the_mask_keeps_one_shard_and_names_the_leaves_that_excluded() {
        let placement = two_level();
        let code = |name: &str| Some(placement.leaf_by_name(name).unwrap().code);
        let codes = vec![
            code("old"),
            code("recent.scotus"),
            code("recent.rest"),
            code("other"),
            None,
        ];
        let mask = ShardMask::compute(&placement, &codes, &compile("year >= 2020"));
        assert_eq!(mask.skipped, vec![true, false, false, false, false]);
        assert_eq!(mask.known, vec![0]);
        assert_eq!(mask.skipped_count(), 1);
        let mask = ShardMask::compute(
            &placement,
            &codes,
            &compile("year >= 2020 && court == \"ca9\""),
        );
        assert_eq!(mask.skipped, vec![true, true, false, false, false]);
        assert_eq!(mask.known, vec![0, 1]);
        // The unkeyed shard is never skipped, and one shard always stays.
        let two = vec![code("old"), code("recent.scotus")];
        let mask = ShardMask::compute(&placement, &two, &compile("year >= 2005 && year < 2010"));
        assert_eq!(mask.skipped, vec![false, true]);
    }
}
