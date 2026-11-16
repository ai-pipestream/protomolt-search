//! Clustered vector routing and product-local result assembly.

#[cfg(feature = "net")]
mod candidates;
#[cfg(feature = "net")]
mod parent_collapse;

use super::*;

#[cfg(feature = "net")]
#[derive(Clone, Copy, Debug)]
pub(super) struct ProductLabelRange {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) shard: u32,
}

#[cfg(feature = "net")]
impl ProductLabelRange {
    pub(super) fn contains(self, label: u64) -> bool {
        label >= self.start && label < self.end
    }
}

#[cfg(feature = "net")]
pub(super) struct ClusteredVectorResult {
    pub(super) hits: Vec<(u64, f32)>,
}

impl CoordinatorServiceImpl {
    /// Resolve a product filter into one packed stable-id bitmap per product
    /// shard for a vector provider that does not own document columns. No
    /// filter remains `None` and therefore costs no shard pass. An explicitly
    /// present empty bitmap set is an intentional match-none set and stays
    /// distinguishable at the provider boundary.
    #[cfg(feature = "net")]
    pub(super) async fn clustered_allowed_labels(
        &self,
        filters: &RequestFilters,
    ) -> Result<Option<ClusteredLabelFilter>, Status> {
        if filters.geo.is_empty() && filters.tree.is_none() && self.document_visibility.is_none() {
            return Ok(None);
        }

        if let Some(fields) = &self.field_permissions {
            fields.filter(&filters.geo, filters.tree.as_ref())?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let tasks = self.spawn_filter_bitmaps(filters, mask.as_ref());

        let mut known = Self::filter_known(filters, mask.as_ref());
        let mut bitmaps = Vec::with_capacity(tasks.len());
        for (shard, task) in tasks {
            let response = task.await.map_err(|error| {
                Status::internal(format!("filter bitmap task failed: {error}"))
            })??;
            known.merge_shard(
                shard,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            let response = Self::membership_from_filter(response);
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            if let Some(bitmap) = checked_label_bitmap(response)? {
                bitmaps.push(bitmap);
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        known.refuse_unknown(filters)?;
        refuse_overlapping_bitmaps(&mut bitmaps)?;
        Ok(Some(ClusteredLabelFilter::Bitmaps(bitmaps)))
    }

    #[cfg(feature = "net")]
    /// Product ownership ranges, independent of vector shard cuts. Hybrid
    /// provenance, BM25 rescoring, and lineage lookup all route by this map;
    /// the clustered provider remains unaware of product shard meaning.
    pub(super) async fn product_label_ranges(&self) -> Result<Vec<ProductLabelRange>, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let client = self.node_client(node);
            tasks.push(tokio::spawn(async move {
                client?
                    .health(HealthRequest {})
                    .await
                    .map(|response| (shard as u32, response.into_inner()))
            }));
        }
        let mut ranges = Vec::with_capacity(tasks.len());
        for task in tasks {
            let (shard, health) = task
                .await
                .map_err(|error| Status::internal(format!("health task failed: {error}")))??;
            let count = health.num_vectors.max(health.bm25_docs);
            if count == 0 {
                continue;
            }
            let end = health.slot_offset.checked_add(count).ok_or_else(|| {
                Status::failed_precondition("product shard label range overflows u64")
            })?;
            ranges.push(ProductLabelRange {
                start: health.slot_offset,
                end,
                shard,
            });
        }
        ranges.sort_unstable_by_key(|range| range.start);
        for pair in ranges.windows(2) {
            if pair[1].start < pair[0].end {
                return Err(Status::failed_precondition(format!(
                    "product shard label ranges overlap: [{}, {}) and [{}, {})",
                    pair[0].start, pair[0].end, pair[1].start, pair[1].end
                )));
            }
        }
        Ok(ranges)
    }

    #[cfg(feature = "net")]
    pub(super) fn product_owner(ranges: &[ProductLabelRange], label: u64) -> Result<u32, Status> {
        ranges
            .iter()
            .copied()
            .find(|range| range.contains(label))
            .map(|range| range.shard)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "clustered TurboVec label {label} has no product shard owner"
                ))
            })
    }

    #[cfg(feature = "net")]
    /// Resolve lineage in compact batches on the owning product shards. A
    /// document without stored lineage parents itself under the same tagged
    /// domain as the embedded path; raw document text never crosses this seam.
    pub(super) async fn product_parent_ids(
        &self,
        ranges: &[ProductLabelRange],
        labels: &[u64],
    ) -> Result<HashMap<u64, u64>, Status> {
        let mut by_shard: HashMap<usize, Vec<u64>> = HashMap::new();
        for &label in labels {
            by_shard
                .entry(Self::product_owner(ranges, label)? as usize)
                .or_default()
                .push(label);
        }
        let selection =
            crate::lineage::LineageSelection::new(&[crate::pb::LineageField::ParentId as i32])?;
        let parents: HashMap<u64, u64> = self
            .read_lineage(labels, &selection, Some(&by_shard))
            .await?
            .into_iter()
            .map(|(id, (parent, _))| (id, parent))
            .collect();
        for &label in labels {
            if !parents.contains_key(&label) {
                return Err(Status::failed_precondition(format!(
                    "product shard did not resolve parent identity for clustered label {label}"
                )));
            }
        }
        Ok(parents)
    }

    #[cfg(feature = "net")]
    fn spawn_filter_bitmaps(
        &self,
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
    ) -> Vec<(usize, FilterBitmapTask)> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = crate::pb::FilterBitmapRequest {
                visibility: self.document_visibility.clone(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask, shard),
            };
            let client = self.node_client(node);
            tasks.push((
                shard,
                tokio::spawn(async move {
                    client?
                        .resolve_filter_bitmap(request)
                        .await
                        .map(|response| response.into_inner())
                }),
            ));
        }
        tasks
    }
}

/// Validate one shard's packed bitmap; `None` when the shard holds no labels.
#[cfg(feature = "net")]
fn checked_label_bitmap(
    response: crate::pb::MembershipBitmapResponse,
) -> Result<Option<turbovec_grpc::proto::LabelBitmap>, Status> {
    if response.label_count == 0 {
        if !response.bits.is_empty() {
            return Err(Status::internal(
                "shard answered an empty filter bitmap with payload bytes",
            ));
        }
        return Ok(None);
    }
    let expected_bytes = usize::try_from(response.label_count.div_ceil(8))
        .map_err(|_| Status::internal("shard filter bitmap length does not fit this process"))?;
    if response.bits.len() != expected_bytes {
        return Err(Status::internal(format!(
            "shard answered {} filter bitmap bytes for {} labels; expected {expected_bytes}",
            response.bits.len(),
            response.label_count
        )));
    }
    response
        .base_label
        .checked_add(response.label_count)
        .ok_or_else(|| Status::internal("shard filter bitmap label range overflows u64"))?;
    Ok(Some(turbovec_grpc::proto::LabelBitmap {
        base_label: response.base_label,
        label_count: response.label_count,
        bits: response.bits,
    }))
}

/// Order the bitmaps by base label and refuse overlapping label ranges.
#[cfg(feature = "net")]
fn refuse_overlapping_bitmaps(
    bitmaps: &mut [turbovec_grpc::proto::LabelBitmap],
) -> Result<(), Status> {
    bitmaps.sort_unstable_by_key(|bitmap| bitmap.base_label);
    for pair in bitmaps.windows(2) {
        let previous_end = pair[0]
            .base_label
            .checked_add(pair[0].label_count)
            .expect("bitmap range was validated above");
        if pair[1].base_label < previous_end {
            return Err(Status::internal(
                "product shard filter bitmap label ranges overlap",
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "net")]
type FilterBitmapTask = tokio::task::JoinHandle<Result<crate::pb::FilterBitmapResponse, Status>>;
