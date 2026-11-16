//! Typed query binding, filtered ANN budgets, surface completion, and spellcheck.

use std::collections::BTreeMap;

use tonic::Status;

use super::{NlpColumnSpec, MICROS_PER_SECOND};
use crate::pb::{
    self, filter_bound::Value as V, filter_expr::Expr as E, FilterBound, FilterExpr, Suggestion,
    SynonymExpansion, TermCandidate, TermSuggestion,
};
use turbo_client::{QueryComparisonOp as Op, StructuredFilterPayload as P, StructuredFilterPlan};

/// HNSW filtered search execution strategy.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FilteredAnnStrategy {
    /// Graph traversal with a selectivity-adjusted budget.
    Graph { ef_search: u32 },
    /// Exact distance scan over a small filtered survivor set.
    PrefilteredExact,
}

/// Select graph or prefiltered scan budgets from estimated filter selectivity.
pub fn select_filtered_ann_strategy(
    base_ef: u32,
    k: u32,
    shard_docs: u64,
    estimated_selectivity: f64,
) -> FilteredAnnStrategy {
    let eff_base = base_ef.max(k).max(16);
    let sel = if !estimated_selectivity.is_finite() {
        1.0
    } else {
        estimated_selectivity.clamp(0.0, 1.0)
    };
    if sel >= 0.999 {
        return FilteredAnnStrategy::Graph {
            ef_search: eff_base,
        };
    }
    let expected_survivors = (shard_docs as f64 * sel).ceil() as u64;
    if expected_survivors <= u64::from(k.saturating_mul(8).max(128)) || sel <= 0.005 {
        return FilteredAnnStrategy::PrefilteredExact;
    }
    let scaled = ((f64::from(eff_base) / sel.max(0.02)).ceil() as u32).clamp(eff_base, 2048);
    FilteredAnnStrategy::Graph { ef_search: scaled }
}

/// Bound query execution plan produced from [`turbo_client::OnePassQueryPlan`].
#[derive(Clone, Debug)]
pub struct BoundOnePassQuery {
    /// Residual non-filter query text suitable for BM25 / neural encoding.
    pub residual_text: String,
    /// Ordinal-preserving residual query terms `(term, ordinal)`.
    pub residual_terms: Vec<(String, u32)>,
    /// Multi-word and single-word synonym expansions matched by `turbo-matcher`.
    pub synonym_expansions: Vec<SynonymExpansion>,
    /// Synthesized CEL predicate string for extracted structured filters (`""` when none).
    pub synthesized_cel: String,
    /// Validated caller and structured predicates.
    pub filter_expr: Option<FilterExpr>,
    /// Heuristic selectivity estimate in `(0.0, 1.0]` for HNSW budget escalation.
    pub estimated_selectivity: f64,
}

fn escape_cel_string(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn format_f64_cel(val: f64) -> String {
    let mut s = val.to_string();
    if !s.contains(['.', 'e', 'E']) {
        s.push_str(".0");
    }
    s
}

fn conjunction(mut exprs: Vec<FilterExpr>) -> Option<FilterExpr> {
    match exprs.len() {
        0 => None,
        1 => exprs.pop(),
        _ => Some(FilterExpr {
            expr: Some(E::And(pb::FilterList { exprs })),
        }),
    }
}

fn number(column: &str, key: Option<&str>, op: Op, value: V) -> FilterExpr {
    let value = Some(value);
    let bound = |exclusive| Some(pb::FilterBound { value, exclusive });
    let (min, max) = match op {
        Op::Eq | Op::Range => (bound(false), bound(false)),
        Op::Gt => (bound(true), None),
        Op::Gte => (bound(false), None),
        Op::Lt => (None, bound(true)),
        Op::Lte => (None, bound(false)),
    };
    let expr = match key {
        Some(key) => E::TypedMapNumber(pb::MapNumberPredicate {
            column: column.to_owned(),
            key: key.to_owned(),
            min,
            max,
        }),
        None => E::Number(pb::NumberPredicate {
            column: column.to_owned(),
            min,
            max,
        }),
    };
    FilterExpr { expr: Some(expr) }
}

fn temporal(column: &str, op: Op, start: Option<i64>, end: Option<i64>) -> Option<FilterExpr> {
    let leaf = |op, seconds: i64| {
        let value = V::Int(seconds.saturating_mul(MICROS_PER_SECOND));
        number(column, None, op, value)
    };
    match (op, start, end) {
        (Op::Gt, Some(_), Some(e)) => Some(leaf(Op::Gte, e)),
        (Op::Gte, Some(s), _) => Some(leaf(Op::Gte, s)),
        (Op::Lt, Some(s), _) => Some(leaf(Op::Lt, s)),
        (Op::Lte, _, Some(e)) => Some(leaf(Op::Lt, e)),
        (_, Some(s), Some(e)) if s < e => conjunction(vec![leaf(Op::Gte, s), leaf(Op::Lt, e)]),
        (_, Some(s), None) => Some(leaf(Op::Eq, s)),
        _ => None,
    }
}

fn numeric_filter(f: &StructuredFilterPlan, c: &NlpColumnSpec) -> Option<FilterExpr> {
    let (column, key, value) = match &f.payload {
        P::Money {
            raw,
            amount_f64,
            ecb_converted,
        } => {
            let (key, value) = match ecb_converted {
                Some(conv) if conv.amount_f64.is_finite() => {
                    (conv.currency.as_str(), conv.amount_f64)
                }
                _ => (raw.currency.as_str(), *amount_f64),
            };
            (&c.money_column, key, value)
        }
        P::Quantity {
            si_base_unit,
            si_value_f64,
            ..
        } => (&c.quantity_column, si_base_unit.as_str(), *si_value_f64),
        _ => return None,
    };
    (!column.is_empty() && !key.is_empty() && value.is_finite())
        .then(|| number(column, Some(key), f.comparison, V::Num(value)))
}

fn key_filter(f: &StructuredFilterPlan, c: &NlpColumnSpec) -> (Option<FilterExpr>, f64) {
    let (column, key, selectivity) = match &f.payload {
        P::Place {
            raw, spine_path, ..
        } => {
            let key = if spine_path.is_empty() {
                &raw.canonical_name
            } else {
                spine_path
            };
            (&c.geo_spine_column, key.clone(), 0.15)
        }
        P::Entity {
            kb_id,
            canonical_name,
            ..
        } => {
            let canonical = kb_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .or_else(|| canonical_name.as_deref().filter(|s| !s.is_empty()))
                .unwrap_or(&f.surface);
            if canonical.is_empty() {
                return (None, 1.0);
            }
            (&c.affirmed_entity_column, format!("id:{canonical}"), 0.10)
        }
        _ => return (None, 1.0),
    };
    if column.is_empty() || key.is_empty() {
        return (None, 1.0);
    }
    let expr = E::TypedMapHasKey(pb::MapKeyPredicate {
        column: column.clone(),
        key,
    });
    (Some(FilterExpr { expr: Some(expr) }), selectivity)
}

fn compile_structured_filters(
    plan: &turbo_client::OnePassQueryPlan,
    columns: &NlpColumnSpec,
) -> (Vec<FilterExpr>, f64) {
    let mut exprs = Vec::new();
    let mut selectivity = 1.0;
    for filter in &plan.filters {
        let (expr, factor) = match &filter.payload {
            P::Place { .. } | P::Entity { .. } => key_filter(filter, columns),
            P::Temporal(t) if !columns.date_column.is_empty() => {
                let expr = temporal(
                    &columns.date_column,
                    filter.comparison,
                    t.start_epoch_seconds,
                    t.end_epoch_seconds,
                );
                (expr, 0.20)
            }
            _ => {
                let expr = numeric_filter(filter, columns);
                let factor = if expr.is_some() { 0.25 } else { 1.0 };
                (expr, factor)
            }
        };
        selectivity *= factor;
        exprs.extend(expr);
    }
    (exprs, selectivity)
}

fn diagnostic_number(lhs: &str, min: &Option<FilterBound>, max: &Option<FilterBound>) -> String {
    let value = |b: &FilterBound| match b.value.expect("constructed bound") {
        V::Num(v) => format_f64_cel(v),
        V::Int(v) => v.to_string(),
        V::Uint(v) => v.to_string(),
    };
    let (op, bound) = match (min, max) {
        (Some(lo), Some(hi)) if lo == hi => ("==", lo),
        (Some(lo), None) => (if lo.exclusive { ">" } else { ">=" }, lo),
        (None, Some(hi)) => (if hi.exclusive { "<" } else { "<=" }, hi),
        _ => unreachable!("constructed numeric predicate"),
    };
    format!("{lhs} {op} {}", value(bound))
}

fn diagnostic_cel(expr: &FilterExpr) -> String {
    match expr.expr.as_ref().expect("constructed filter") {
        E::And(list) => {
            let children: Vec<_> = list.exprs.iter().map(diagnostic_cel).collect();
            format!("({})", children.join(" && "))
        }
        E::Number(n) => diagnostic_number(&n.column, &n.min, &n.max),
        E::TypedMapNumber(n) => diagnostic_number(
            &format!("{}[{}]", n.column, escape_cel_string(&n.key)),
            &n.min,
            &n.max,
        ),
        E::TypedMapHasKey(k) => format!("{} in {}", escape_cel_string(&k.key), k.column),
        _ => unreachable!("constructed structured filter"),
    }
}

/// Bind structured predicates directly; parse only the caller's CEL filter.
pub fn bind_one_pass_query(
    plan: &turbo_client::OnePassQueryPlan,
    columns: &NlpColumnSpec,
    caller_cel: Option<&str>,
) -> Result<BoundOnePassQuery, Status> {
    columns.validate()?;
    let (exprs, estimated_selectivity) = compile_structured_filters(plan, columns);
    let clauses: Vec<_> = exprs.iter().map(diagnostic_cel).collect();
    let synthesized_cel = clauses.join(" && ");
    let caller = crate::cel::compile_filter(caller_cel.unwrap_or_default())?;
    let filter_expr = conjunction(caller.into_iter().chain(conjunction(exprs)).collect());
    if let Some(expr) = &filter_expr {
        crate::filter::validate_filter(expr)?;
    }
    let mut synonym_expansions = Vec::new();
    for fp in &plan.fields {
        for syn in fp.synonyms.iter().filter(|syn| !syn.terms.is_empty()) {
            synonym_expansions.push(SynonymExpansion {
                field: fp.field.clone(),
                term: syn.surface.clone(),
                terms: syn.terms.clone(),
            });
        }
    }
    Ok(BoundOnePassQuery {
        residual_text: if plan.free_text.trim().is_empty() {
            plan.query.clone()
        } else {
            plan.free_text.clone()
        },
        residual_terms: plan
            .residual_terms
            .iter()
            .map(|t| (t.term.clone(), t.ordinal))
            .collect(),
        synonym_expansions,
        synthesized_cel,
        filter_expr,
        estimated_selectivity,
    })
}

/// Aligned `TSUGLEX1` storage for zero-copy [`turbo_client::MappedSurfaceLexicon`] views.
#[derive(Debug)]
pub enum SharedSurfaceLexicon {
    /// Zero-copy OS file mapping (`memmap2::Mmap` is page-aligned).
    Mmap(memmap2::Mmap),
    /// 8-byte-aligned heap buffer for in-memory builders and tests.
    Aligned { words: Vec<u64>, len: usize },
}

impl SharedSurfaceLexicon {
    /// Memory-maps a `TSUGLEX1` file from disk and validates its header/bounds.
    pub fn open_mmap(path: impl AsRef<std::path::Path>) -> Result<Self, Status> {
        let path = path.as_ref();
        let file = std::fs::File::open(path).map_err(|e| {
            Status::invalid_argument(format!("open surface lexicon {}: {e}", path.display()))
        })?;
        // SAFETY: read-only mapping of an immutable `TSUGLEX1` lexicon artifact.
        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            Status::invalid_argument(format!("mmap surface lexicon {}: {e}", path.display()))
        })?;
        turbo_client::MappedSurfaceLexicon::from_bytes(&mmap).map_err(|e| {
            Status::invalid_argument(format!("invalid TSUGLEX1 surface lexicon: {e}"))
        })?;
        Ok(Self::Mmap(mmap))
    }

    /// Copies `bytes` into an 8-byte-aligned buffer and validates its `TSUGLEX1` layout.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Status> {
        let mut words = vec![0u64; bytes.len().div_ceil(8)];
        for (w, chunk) in words.iter_mut().zip(bytes.chunks(8)) {
            let mut buf = [0u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            *w = u64::from_ne_bytes(buf);
        }
        let this = Self::Aligned {
            words,
            len: bytes.len(),
        };
        turbo_client::MappedSurfaceLexicon::from_bytes(this.as_bytes()).map_err(|e| {
            Status::invalid_argument(format!("invalid TSUGLEX1 surface lexicon: {e}"))
        })?;
        Ok(this)
    }

    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Mmap(map) => map,
            Self::Aligned { words, len } => {
                // SAFETY: `words` holds at least `len` initialized bytes and outlives `&self`.
                unsafe { std::slice::from_raw_parts(words.as_ptr().cast::<u8>(), *len) }
            }
        }
    }

    /// Returns a zero-copy [`turbo_client::MappedSurfaceLexicon`] view over this image.
    pub fn view(&self) -> turbo_client::MappedSurfaceLexicon<'_> {
        turbo_client::MappedSurfaceLexicon::from_bytes(self.as_bytes())
            .expect("validated at SharedSurfaceLexicon construction")
    }
}

/// Resolve stemmed matches and lexicon prefixes into surface completions, preserving shard metadata.
pub fn resolve_stemmed_suggestions(
    lexicon: &turbo_client::MappedSurfaceLexicon,
    normalized_prefix: &str,
    shard_union: &BTreeMap<String, (u64, u32)>,
    limit: usize,
) -> Vec<Suggestion> {
    let mut merged: BTreeMap<String, (u64, u32)> = BTreeMap::new();

    for comp in lexicon.suggest_prefix(normalized_prefix, limit.saturating_mul(4).max(32)) {
        let shards = shard_union.get(comp.stem).map_or(1, |&(_, s)| s.max(1));
        let df = shard_union
            .get(comp.stem)
            .map_or(comp.count, |&(d, _)| d.max(comp.count));
        merged.insert(comp.surface.to_string(), (df, shards));
    }

    for (stem, &(df, shards)) in shard_union {
        let surfaces = lexicon.stem_surfaces(stem, 16);
        let mut matched_surface = false;
        for s in surfaces {
            if s.surface.starts_with(normalized_prefix) {
                matched_surface = true;
                merged
                    .entry(s.surface.to_string())
                    .and_modify(|e| {
                        e.0 = e.0.max(s.count.min(df).max(1));
                        e.1 = e.1.max(shards);
                    })
                    .or_insert((s.count.max(1), shards));
            }
        }
        if !matched_surface && stem.starts_with(normalized_prefix) {
            merged.entry(stem.clone()).or_insert((df, shards));
        }
    }

    let mut ranked: Vec<(String, (u64, u32))> = merged.into_iter().collect();
    ranked.sort_by(|a, b| b.1 .0.cmp(&a.1 .0).then_with(|| a.0.cmp(&b.0)));
    ranked.truncate(limit);
    ranked
        .into_iter()
        .map(|(term, (df, shards))| Suggestion { term, df, shards })
        .collect()
}

/// Return contextual bigram corrections as [`TermSuggestion`] entries in query order.
pub fn spellcheck_query_terms(
    checker: &turbo_client::QuerySpellchecker,
    query_text: &str,
    limit: usize,
) -> Option<(String, Vec<TermSuggestion>)> {
    let res = checker.check_query(query_text, &[]);
    if !res.did_change {
        return None;
    }
    let mut out = Vec::with_capacity(res.edits.len());
    for edit in res.edits {
        let mut candidates = vec![TermCandidate {
            term: edit.replacement.clone(),
            df: 1,
            distance: edit.edit_distance,
            shards: 1,
        }];
        candidates.truncate(limit.max(1));
        out.push(TermSuggestion {
            term: edit.original,
            df: 0,
            candidates,
            dictionary_terms_scanned: 1,
        });
    }
    Some((res.corrected_query, out))
}
