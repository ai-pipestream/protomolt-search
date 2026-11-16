//! Placement dry-run handler.

use super::*;

/// Row movement accumulated over a proposed placement tree's leaves.
#[derive(Default)]
struct PlacementTally {
    cells: Vec<crate::pb::PlacementCell>,
    rows: u64,
    moving_rows: u64,
    defaulted_rows: u64,
}

impl PlacementTally {
    /// Record one leaf's per-shard counts against the rows already staying.
    fn record(
        &mut self,
        leaf: &crate::placement_plan::PlanNode,
        per_shard: &[u64],
        staying: &[u64],
    ) {
        for (shard, (count, stay)) in per_shard.iter().zip(staying).enumerate() {
            if *count == 0 {
                continue;
            }
            let moving = count.saturating_sub(*stay);
            self.rows += count;
            self.moving_rows += moving;
            if leaf.is_default {
                self.defaulted_rows += count;
            }
            self.cells.push(crate::pb::PlacementCell {
                shard: shard as u32,
                code: leaf.code as u64,
                leaf: leaf.name.clone(),
                rows: *count,
                moving_rows: moving,
            });
        }
    }
}

/// The plan's leaves in code order: depth-first, chain order.
fn plan_leaves(
    plan: &[crate::placement_plan::PlanNode],
) -> Vec<(Vec<usize>, &crate::placement_plan::PlanNode)> {
    let mut stack: Vec<(Vec<usize>, &crate::placement_plan::PlanNode)> =
        plan.iter().enumerate().map(|(i, n)| (vec![i], n)).collect();
    let mut leaves: Vec<(Vec<usize>, &crate::placement_plan::PlanNode)> = Vec::new();
    stack.reverse();
    while let Some((path, node)) = stack.pop() {
        if node.is_leaf() {
            leaves.push((path, node));
        } else {
            for (i, child) in node.children.iter().enumerate().rev() {
                let mut p = path.clone();
                p.push(i);
                stack.push((p, child));
            }
        }
    }
    leaves
}

impl CoordinatorServiceImpl {
    /// Read-only placement dry run computing per-shard row movement under a
    /// proposed tree via filtered counts (`src/placement_plan.rs`), refusing
    /// predicates on columns unknown to every shard.
    pub(in crate::coordinator) async fn rpc_plan_placement(
        &self,
        request: Request<crate::pb::PlanPlacementRequest>,
    ) -> Result<Response<crate::pb::PlanPlacementResponse>, Status> {
        crate::metrics::timed(Route::PlanPlacement, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::plan_placement(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            self.plan_placement_movement(req).await.map(Response::new)
        })
        .await
    }

    async fn plan_placement_movement(
        &self,
        req: crate::pb::PlanPlacementRequest,
    ) -> Result<crate::pb::PlanPlacementResponse, Status> {
        let proposed = req
            .proposed
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("plan_placement: proposed tree is absent"))?;
        let config = crate::placement::PlacementTreeConfig::from_proto(proposed);
        let placement =
            crate::placement::Placement::validate(&config).map_err(Status::invalid_argument)?;
        let plan = crate::placement_plan::plan(&placement).map_err(Status::invalid_argument)?;
        let base = RequestFilters::compile(&[], &req.filter)?;
        let mut counter = PlanCounter {
            coordinator: self,
            base: base.tree,
            column: placement.column().to_string(),
            memo: std::collections::HashMap::new(),
        };
        let mut tally = PlacementTally::default();
        for (path, leaf) in plan_leaves(&plan) {
            let per_shard = counter.first(&plan, &path, None).await?;
            let staying = counter
                .first(
                    &plan,
                    &path,
                    Some(crate::placement_plan::code_equals(
                        &counter.column.clone(),
                        leaf.code,
                    )),
                )
                .await?;
            tally.record(leaf, &per_shard, &staying);
        }
        Ok(crate::pb::PlanPlacementResponse {
            topology_generation: self.current_topology_generation(),
            cells: tally.cells,
            rows: tally.rows,
            moving_rows: tally.moving_rows,
            defaulted_rows: tally.defaulted_rows,
        })
    }
}
