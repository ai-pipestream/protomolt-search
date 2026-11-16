//! Paging a candidate pool, plainly or collapsed into groups.

use std::collections::HashMap;

use tonic::Status;

use super::cursor::{page, Cursor};
use super::refuse;
use super::response::{fill_projected, ms};
use super::run::Base;
use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::QueryHit;

/// What a page step produced: the page, or the depth a single-leaf
/// collapse must re-run at to find its groups. The leaf's order is
/// depth-independent, so a deeper pool only adds groups after the ones
/// already found.
pub(super) enum Paged {
    Done(Vec<QueryHit>, Vec<crate::pb::QueryGroup>, String, String),
    Deepen(u32),
}

/// A collapse group's identity: an integer or a facet term.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum GroupKey {
    Integer(i64),
    UnsignedInteger(u64),
    Text(String),
}

impl GroupKey {
    fn to_value(&self) -> crate::pb::SortValue {
        match self {
            GroupKey::Integer(i) => crate::sortkeys::Value::Integer(*i).to_pb(),
            GroupKey::UnsignedInteger(i) => crate::sortkeys::Value::UnsignedInteger(*i).to_pb(),
            GroupKey::Text(t) => crate::sortkeys::Value::Text(t.clone()).to_pb(),
        }
    }
}

/// The collapse key of every candidate: the lineage keys through the
/// parent resolver, a column through the value seam (an i64 or facet
/// column; a double or bool is not a group identity). A candidate
/// without a value is absent.
async fn collapse_keys(
    coordinator: &CoordinatorServiceImpl,
    selector: &crate::pb::CollapseSpec,
    ids: &[u64],
) -> Result<HashMap<u64, GroupKey>, Status> {
    let column = crate::sortkeys::target_name(&selector.column, selector.map.as_ref());
    let mut keys = HashMap::with_capacity(ids.len());
    if selector.map.is_none() && (column == "parent_id" || column == "group_id") {
        for (id, v) in coordinator.lineage_key(ids, &column).await? {
            keys.insert(id, GroupKey::UnsignedInteger(v));
        }
        return Ok(keys);
    }
    let compiled = vec![crate::pb::CompiledProjection {
        name: column.clone(),
        expr: Some(crate::pb::ValueExpr {
            expr: Some(match &selector.map {
                Some(map) => crate::pb::value_expr::Expr::TypedMap(map.clone()),
                None => crate::pb::value_expr::Expr::Column(column.clone()),
            }),
        }),
    }];
    let fetched = coordinator.fetch_values(ids, &compiled, &[]).await?;
    if fetched.projection_types.iter().any(|ty| {
        matches!(
            ty,
            crate::pb::ScalarValueType::Number | crate::pb::ScalarValueType::Boolean
        )
    }) {
        return Err(refuse(format!(
            "collapse by {column:?}: a double or bool column is not a group identity"
        )));
    }
    for (id, values) in fetched.rows {
        let Some(value) = values.first().and_then(|v| v.value.as_ref()) else {
            continue;
        };
        keys.insert(id, group_key(value, &column)?);
    }
    Ok(keys)
}

fn group_key(value: &crate::pb::projected_value::Value, column: &str) -> Result<GroupKey, Status> {
    match value {
        crate::pb::projected_value::Value::UintValue(v) => Ok(GroupKey::UnsignedInteger(*v)),
        crate::pb::projected_value::Value::IntValue(i) => Ok(GroupKey::Integer(*i)),
        crate::pb::projected_value::Value::StringValue(t) => Ok(GroupKey::Text(t.clone())),
        crate::pb::projected_value::Value::DoubleValue(_) => Err(refuse(format!(
            "collapse by {column:?}: a double column is not a group identity; \
             collapse by an i64 or facet column, or by parent_id / group_id"
        ))),
        crate::pb::projected_value::Value::BoolValue(_) => Err(refuse(format!(
            "collapse by {column:?}: a bool is not a group identity; collapse by an \
             i64 or facet column, or by parent_id / group_id"
        ))),
    }
}

/// One page step over a fetched pool: `pool_depth` is how deep the pool
/// was fetched, and `may_deepen` whether a single-leaf collapse may
/// re-run deeper when the pool holds too few groups.
pub(super) struct PageStep<'a> {
    pub(super) base: &'a Base<'a>,
    pub(super) cursor: Option<&'a Cursor>,
    pub(super) pool_depth: u32,
    pub(super) may_deepen: bool,
}

/// The candidate pool as collapse groups, in first-appearance order;
/// the pool's order is the selection's order, so the first hit of a
/// group is its best.
struct Grouped {
    index: HashMap<GroupKey, usize>,
    groups: Vec<(GroupKey, Vec<QueryHit>)>,
}

fn group_hits(hits: Vec<QueryHit>, keys: &HashMap<u64, GroupKey>) -> Grouped {
    let mut index: HashMap<GroupKey, usize> = HashMap::new();
    let mut groups: Vec<(GroupKey, Vec<QueryHit>)> = Vec::new();
    for hit in hits {
        let Some(key) = keys.get(&hit.doc_id) else {
            continue;
        };
        match index.get(key) {
            Some(&g) => groups[g].1.push(hit),
            None => {
                index.insert(key.clone(), groups.len());
                groups.push((key.clone(), vec![hit]));
            }
        }
    }
    Grouped { index, groups }
}

/// Cut the page, or collapse the candidate pool into groups and cut
/// the page of groups. A full pool that holds too few groups either
/// deepens (a single leaf) or refuses naming selection_k (a fixed
/// pool).
pub(super) async fn page_or_collapse(
    step: &PageStep<'_>,
    hits: Vec<QueryHit>,
    executed: String,
    prof: &mut Option<crate::pb::QueryProfile>,
) -> Result<Paged, Status> {
    let req = step.base.req;
    let Some(collapse) = req.collapse.as_ref() else {
        let (hits, next) = page(hits, req.k, step.cursor)?;
        return Ok(Paged::Done(hits, Vec::new(), next, executed));
    };
    let t0 = std::time::Instant::now();
    let pool_full = hits.len() as u64 >= u64::from(step.pool_depth);
    let ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
    let keys = collapse_keys(step.base.coordinator, collapse, &ids).await?;
    let grouped = group_hits(hits, &keys);
    if pool_full {
        if let Some(depth) = short_pool(step, collapse, grouped.groups.len())? {
            return Ok(Paged::Deepen(depth));
        }
    }
    let reps: Vec<QueryHit> = grouped
        .groups
        .iter()
        .map(|(_, hits)| hits[0].clone())
        .collect();
    let (page_reps, next) = page(reps, req.k, step.cursor)?;
    let listed = collapse.inner_hits.max(1) as usize;
    let mut out_groups = Vec::with_capacity(page_reps.len());
    for rep in &page_reps {
        let g = grouped.index[keys.get(&rep.doc_id).expect("a representative has a key")];
        let (key, members) = &grouped.groups[g];
        let pool_hits = members.len();
        let mut listed_hits: Vec<QueryHit> = members.iter().take(listed).cloned().collect();
        for (i, hit) in listed_hits.iter_mut().enumerate() {
            hit.rank = (i + 1) as u32;
        }
        out_groups.push(crate::pb::QueryGroup {
            key: Some(key.to_value()),
            hits: listed_hits,
            complete: pool_hits >= listed || !pool_full,
            pool_hits: pool_hits as u32,
        });
    }
    if let Some(p) = prof.as_mut() {
        p.collapse_ms = ms(t0);
    }
    Ok(Paged::Done(
        page_reps,
        out_groups,
        next,
        format!("{executed}+collapse"),
    ))
}

/// A full pool's verdict on its group count: the depth to re-run at
/// when it holds fewer groups than the page needs and may deepen, a
/// refusal when it may not, and `None` when the pool serves the page —
/// which includes a pool already at max_k, whose short page says
/// nothing follows at the served depth.
fn short_pool(
    step: &PageStep<'_>,
    collapse: &crate::pb::CollapseSpec,
    group_count: usize,
) -> Result<Option<u32>, Status> {
    let pool_depth = step.pool_depth;
    let needed = u64::from(step.cursor.map_or(0, |c| c.rank)) + u64::from(step.base.req.k);
    if group_count as u64 >= needed {
        return Ok(None);
    }
    let max_k = step.base.coordinator.max_k();
    if step.may_deepen && pool_depth < max_k {
        return Ok(Some(pool_depth.saturating_mul(2).min(max_k)));
    }
    if !step.may_deepen {
        return Err(Status::failed_precondition(format!(
            "the selection_k = {pool_depth} candidate pool holds {group_count} groups by {:?}, \
             fewer than the {needed} the page needs; deepening it would change the \
             ranking under the cursor; re-run from the first page with a larger \
             selection_k",
            crate::sortkeys::target_name(&collapse.column, collapse.map.as_ref())
        )));
    }
    Ok(None)
}

/// Projections on the listed inner hits, the same seam the page's hits
/// use.
pub(super) async fn fill_projected_groups(
    coordinator: &CoordinatorServiceImpl,
    compiled: &[crate::pb::CompiledProjection],
    groups: &mut [crate::pb::QueryGroup],
    prof: &mut Option<crate::pb::QueryProfile>,
) -> Result<(), Status> {
    if compiled.is_empty() || groups.is_empty() {
        return Ok(());
    }
    let mut all: Vec<QueryHit> = groups.iter().flat_map(|g| g.hits.iter().cloned()).collect();
    fill_projected(coordinator, compiled, &mut all, prof).await?;
    let mut filled = all.into_iter();
    for group in groups.iter_mut() {
        for hit in &mut group.hits {
            if let Some(f) = filled.next() {
                hit.projected = f.projected;
            }
        }
    }
    Ok(())
}
