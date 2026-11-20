//! Placement planning and shard assignment.

use super::*;
use crate::domain_error::DomainError;

/// One routed ingest stream's placement evaluator: the bind's plan as
/// an extractor, its materialize spec compiled, and the tree. A source
/// document's rows (one per chunk on a chunked plan) must agree on the
/// leaf, since the rows of one source go to one shard.
pub(super) struct PlacementRouter {
    pub(super) placement: Arc<crate::placement::Placement>,
    pub(super) extractor: crate::mapping::Extractor,
    pub(super) materialize: Vec<(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)>,
    /// Index derived-column declaration evaluated locally for routing only;
    /// each shard computes and persists its own derived values.
    pub(super) derived: Option<Arc<crate::derived::Declaration>>,
}

impl PlacementRouter {
    pub(super) fn new(
        placement: Arc<crate::placement::Placement>,
        bind: &crate::pb::MappedBind,
        derived: Option<Arc<crate::derived::Declaration>>,
    ) -> Result<Self, Status> {
        let extractor = crate::mapping::Extractor::new(
            &bind.descriptor_set,
            &bind.message_type,
            &bind.body_path,
        )?;
        let materialize = match bind.materialize.as_ref() {
            Some(spec) => crate::node::compile_materialize_spec(spec)?,
            None => Vec::new(),
        };
        Ok(PlacementRouter {
            placement,
            extractor,
            materialize,
            derived,
        })
    }

    /// The placement code of one source document at `position` in the
    /// stream. Quality and geography columns are derived on the node
    /// after analysis and are absent here, so a predicate on one is
    /// UNKNOWN at routing time and falls through to the default.
    pub(super) fn leaf_of(
        &self,
        bytes: &[u8],
        position: u64,
        stable_key: &[u8],
    ) -> Result<i64, Status> {
        let rows = self
            .extractor
            .extract(bytes)
            .map_err(|error| Status::from(error.prefixed(&format!("document {position}: "))))?;
        if rows.is_empty() {
            let empty = crate::pb::AddDocumentsRequest::default();
            let leaf = self
                .placement
                .evaluate(&empty)
                .map_err(Status::invalid_argument)?;
            return Ok(leaf.code);
        }
        let mut chosen: Option<(i64, String)> = None;
        for extracted in rows {
            let doc = crate::node::apply_materialize(extracted.request, &self.materialize)
                .map_err(|status| {
                    Status::new(
                        status.code(),
                        format!("document {position}: {}", status.message()),
                    )
                })?;
            let doc = match self.derived.as_deref() {
                Some(declaration) => declaration.apply(doc, Some(stable_key)).map_err(|error| {
                    Status::from(error.prefixed(&format!("document {position}: ")))
                })?,
                None => doc,
            };
            let leaf = self
                .placement
                .evaluate(&doc)
                .map_err(|e| Status::invalid_argument(format!("document {position}: {e}")))?;
            match chosen.as_ref() {
                None => chosen = Some((leaf.code, leaf.name.clone())),
                Some((code, name)) if *code != leaf.code => {
                    return Err(Status::invalid_argument(format!(
                        "document {position}: its rows evaluate to placement leaves {name:?} and \
                         {:?}; the rows of one source document go to one shard, so a placement \
                         predicate reads parent-scope columns, not chunk-scope ones",
                        leaf.name
                    )));
                }
                Some(_) => {}
            }
        }
        Ok(chosen.expect("at least one row").0)
    }
}

/// The dry run's counting state: filtered counts per shard, memoized by
/// node and restriction, so a default node's subtraction reuses its
/// siblings' counts (`src/placement_plan.rs` for the arithmetic).
pub(super) struct PlanCounter<'a> {
    pub(super) coordinator: &'a CoordinatorServiceImpl,
    pub(super) base: Option<crate::pb::FilterExpr>,
    pub(super) column: String,
    pub(super) memo: std::collections::HashMap<(String, Option<i64>), Vec<u64>>,
}

impl PlanCounter<'_> {
    /// Rows per shard passing `tree` (all live rows for `None`). With
    /// `refuse_unknown`, a leaf no shard can resolve refuses by name.
    pub(super) async fn count(
        &self,
        tree: Option<crate::pb::FilterExpr>,
        refuse_unknown: bool,
        node_name: &str,
    ) -> Result<Vec<u64>, Status> {
        if let Some(expr) = tree.as_ref() {
            crate::filter::validate_filter(expr).map_err(|status| {
                Status::invalid_argument(format!(
                    "plan_placement: node {node_name:?}: the combined predicate {}",
                    status.message()
                ))
            })?;
        }
        let filters = RequestFilters {
            geo: Vec::new(),
            tree,
            enumerate_max_rows: None,
            patience_saturation_ppm: None,
            patience_hops: None,
            finish_settle_us: None,
            finish_deadline_us: None,
            floor_prune: None,
            floor_k: None,
            parent_floor_share_ppm: None,
            floor_greediness_ppm: None,
            floor_clamp_min: None,
            include_rerank_candidate: false,
        };
        let count = crate::pb::CompiledAggregation {
            expr: Some(crate::cel::compile_value("1")?),
            op: crate::pb::AggregateOp::Count as i32,
            name: "rows".to_string(),
            max_distinct: 0,
        };
        let mut tasks = Vec::with_capacity(self.coordinator.node_addrs.len());
        for node in &self.coordinator.node_addrs {
            let request = crate::pb::AggregateShardRequest {
                visibility: None,
                expected_stats_epoch: 0,
                expected_stats_incarnation: Vec::new(),
                filter: filters.tree.clone(),
                geo_filters: Vec::new(),
                aggregations: vec![count.clone()],
                group_by: String::new(),
                max_groups: 0,
                histograms: Vec::new(),
                percentiles: Vec::new(),
                doc_ids: Vec::new(),
                restrict_doc_ids: false,
            };
            let client = self.coordinator.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?
                    .aggregate_shard(request)
                    .await
                    .map(|r| r.into_inner())
            }));
        }
        let mut known = FilterKnown::new(&filters);
        let mut counts = Vec::with_capacity(tasks.len());
        for task in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("plan_placement task failed: {e}")))??;
            known.merge(&response.geo_columns_known, &response.filter_columns_known)?;
            counts.push(response.matched);
        }
        if refuse_unknown {
            known.refuse_unknown(&filters).map_err(|status| {
                Status::new(
                    status.code(),
                    format!("plan_placement: node {node_name:?}: {}", status.message()),
                )
            })?;
        }
        Ok(counts)
    }

    /// Rows per shard that land on the node at `path` (indices from the
    /// root chain), restricted to `extra` when given.
    pub(super) fn first<'s>(
        &'s mut self,
        plan: &'s [crate::placement_plan::PlanNode],
        path: &'s [usize],
        extra: Option<crate::pb::FilterExpr>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u64>, Status>> + Send + 's>>
    {
        Box::pin(async move {
            let node = node_at(plan, path);
            let extra_code = extra.as_ref().map(|_| self.code_of(extra.as_ref()));
            let key = (node.name.clone(), extra_code);
            if let Some(counts) = self.memo.get(&key) {
                return Ok(counts.clone());
            }
            let refuse_unknown = extra.is_none();
            let counts = if !node.is_default {
                let (a, ab) = crate::placement_plan::first_match_trees(
                    node,
                    self.base.as_ref(),
                    extra.as_ref(),
                );
                let all = self.count(a, refuse_unknown, &node.name).await?;
                match ab {
                    Some(ab) => {
                        let taken = self.count(Some(ab), refuse_unknown, &node.name).await?;
                        all.iter()
                            .zip(&taken)
                            .map(|(a, b)| a.saturating_sub(*b))
                            .collect()
                    }
                    None => all,
                }
            } else {
                // The default: the parent's rows minus the siblings'.
                let parent_path = &path[..path.len() - 1];
                let mut counts = if parent_path.is_empty() {
                    let (a, _) = crate::placement_plan::first_match_trees(
                        node,
                        self.base.as_ref(),
                        extra.as_ref(),
                    );
                    self.count(a, refuse_unknown, &node.name).await?
                } else {
                    self.first(plan, parent_path, extra.clone()).await?
                };
                let siblings = if parent_path.is_empty() {
                    plan
                } else {
                    &node_at(plan, parent_path).children
                };
                for (i, sibling) in siblings.iter().enumerate() {
                    if sibling.is_default {
                        continue;
                    }
                    let mut sibling_path = parent_path.to_vec();
                    sibling_path.push(i);
                    let taken = self.first(plan, &sibling_path, extra.clone()).await?;
                    for (c, t) in counts.iter_mut().zip(&taken) {
                        *c = c.saturating_sub(*t);
                    }
                }
                counts
            };
            self.memo.insert(key, counts.clone());
            Ok(counts)
        })
    }

    /// The code an `extra` restriction names (the memo key); the
    /// restriction is always `column == code` here.
    pub(super) fn code_of(&self, extra: Option<&crate::pb::FilterExpr>) -> i64 {
        match extra.and_then(|e| e.expr.as_ref()) {
            Some(crate::pb::filter_expr::Expr::Number(n)) => {
                match n.min.as_ref().and_then(|b| b.value.as_ref()) {
                    Some(crate::pb::filter_bound::Value::Int(code)) => *code,
                    _ => 0,
                }
            }
            _ => 0,
        }
    }
}

pub(super) fn node_at<'p>(
    plan: &'p [crate::placement_plan::PlanNode],
    path: &[usize],
) -> &'p crate::placement_plan::PlanNode {
    let mut node = &plan[path[0]];
    for &i in &path[1..] {
        node = &node.children[i];
    }
    node
}
