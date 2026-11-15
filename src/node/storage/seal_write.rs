//! Write frozen segment artifacts after checking their catalog generation.

use super::SealPlan;
use crate::node::*;

impl NodeServiceImpl {
    pub(in crate::node) fn write_segment(
        &self,
        plan: &SealPlan,
    ) -> Result<Arc<crate::segments::OpenedSegmentSet>, Status> {
        plan.catalog
            .check_current_generation()
            .map_err(Status::failed_precondition)?;
        let _ = std::fs::remove_dir_all(&plan.stage);
        std::fs::create_dir_all(&plan.stage)
            .map_err(|e| Status::internal(format!("stage {}: {e}", plan.stage.display())))?;
        let io = |what: &str, e: std::io::Error| Status::internal(format!("seal {what}: {e}"));
        let bm25_path = plan.stage.join("documents.bm25");
        plan.documents
            .save(&bm25_path)
            .map_err(|e| io("documents", e))?;
        let live_path = plan.stage.join("live-docs.bin");
        plan.live
            .write(&live_path, plan.rows as u64)
            .map_err(|e| io("live docs", e))?;
        let (vector_path, exact_path) = match plan.image.as_deref() {
            Some(image) => {
                let vector_path = plan.stage.join("vector.index");
                image
                    .write(&vector_path)
                    .map_err(|e| Status::internal(format!("seal vectors: {e}")))?;
                let (dim, values) = self.sealed_exact_rows(plan)?;
                let exact_path = plan.stage.join("vectors.f32");
                ExactVectorStore::from_values(dim, values)
                    .and_then(|store| store.write(&exact_path))
                    .map_err(|e| io("exact rows", e))?;
                (Some(vector_path), Some(exact_path))
            }
            None => (None, None),
        };
        let multi_res_path = if plan.retained.is_empty() {
            None
        } else {
            let path = plan.stage.join(crate::multi_res::FILE_NAME);
            plan.retained
                .write(&path, plan.rows as u64)
                .map_err(|error| io("retained vectors", error))?;
            Some(path)
        };
        let source = crate::segments::SegmentSource {
            multi_res_path: multi_res_path.as_deref(),
            segment_id: &plan.segment_id,
            generation: plan.generation,
            base_label: plan.base as u64,
            backend_kind: &plan.backend_kind,
            vector_path: vector_path.as_deref(),
            exact_vector_path: exact_path.as_deref(),
            bm25_path: &bm25_path,
            live_docs_path: &live_path,
            partition_column: None,
        };
        plan.catalog
            .append(source)
            .map_err(|e| Status::internal(format!("seal segment: {e}")))
    }

    fn sealed_exact_rows(&self, plan: &SealPlan) -> Result<(usize, Vec<f32>), Status> {
        let guard = read_shard(&self.state);
        let exact = guard.exact_vectors.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "sealing a segment with vectors needs the exact-vector sidecar for \
                             its FP32 rows; the node has none",
            )
        })?;
        if exact.len() < plan.base + plan.rows {
            return Err(Status::failed_precondition(format!(
                "the exact-vector sidecar has {} rows, fewer than the {} the seal \
                             covers",
                exact.len(),
                plan.base + plan.rows
            )));
        }
        let dim = exact
            .dim()
            .ok_or_else(|| Status::failed_precondition("exact-vector sidecar has no dim"))?;
        Ok((
            dim,
            exact
                .row_values(plan.base, plan.base + plan.rows)
                .map_err(|e| Status::internal(format!("seal exact rows: {e}")))?,
        ))
    }
}
