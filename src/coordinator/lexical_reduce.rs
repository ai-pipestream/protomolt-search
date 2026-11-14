//! BM25 reply admission, schema agreement, and aggregate reduction.

use super::lexical_round::Bm25Reply;
use super::*;

type Summaries = (
    Vec<crate::pb::FacetFieldCounts>,
    Vec<crate::pb::RangeFacetCounts>,
    Vec<crate::pb::ColumnStats>,
    Vec<crate::pb::FacetCardinality>,
);

pub(super) struct RoundMerge<'a> {
    request: &'a Bm25QueryRequest,
    flat: bool,
    all: Vec<(u32, Bm25Hit)>,
    prune: crate::segment_prune::PruneStats,
    facets: Vec<Vec<crate::pb::FacetFieldCounts>>,
    ranges: Vec<Vec<crate::pb::RangeFacetCounts>>,
    stats: Vec<Vec<crate::pb::ColumnStats>>,
    distinct: Vec<Vec<crate::pb::FacetDistinct>>,
    stage_known: Vec<bool>,
    geo_known: Vec<bool>,
    filter_known: Vec<bool>,
    projection_leaves: Vec<crate::values::ValueLeaf>,
    projection_known: Vec<bool>,
    projection_types: Vec<crate::pb::ScalarValueType>,
    fingerprint: Option<String>,
    shard_millis: Vec<(u32, f64)>,
}

impl<'a> RoundMerge<'a> {
    pub(super) fn new(request: &'a Bm25QueryRequest, flat: bool) -> Self {
        let mut leaves = Vec::new();
        for projection in &request.projections {
            if let Some(expr) = &projection.expr {
                crate::values::column_leaves(expr, &mut leaves);
            }
        }
        Self {
            request,
            flat,
            all: Vec::new(),
            prune: Default::default(),
            facets: Vec::new(),
            ranges: Vec::new(),
            stats: Vec::new(),
            distinct: Vec::new(),
            stage_known: vec![false; request.score_stages.len()],
            geo_known: vec![false; request.geo_filters.len()],
            filter_known: vec![false; request.filter.as_ref().map_or(0, crate::filter::leaf_count)],
            projection_known: vec![false; leaves.len()],
            projection_leaves: leaves,
            projection_types: vec![
                crate::pb::ScalarValueType::Unspecified;
                request.projections.len()
            ],
            fingerprint: None,
            shard_millis: Vec::new(),
        }
    }

    pub(super) fn accept(&mut self, reply: Bm25Reply) -> Result<(), Status> {
        let Bm25Reply {
            shard,
            response,
            fingerprint,
            millis,
        } = reply;
        self.shard_millis.push((shard, millis));
        self.accept_fingerprint(shard, fingerprint)?;
        if self.flat {
            self.accept_projections(&response)?;
        }
        self.all
            .extend(response.hits.into_iter().map(|hit| (shard, hit)));
        self.facets.push(response.facets);
        self.ranges.push(response.range_facets);
        self.stats.push(response.stats);
        self.distinct.push(response.distinct);
        self.prune.add(crate::segment_prune::PruneStats {
            segments_total: response.segments_total,
            segments_skipped: response.segments_skipped,
        });
        merge_known(
            &mut self.geo_known,
            &response.geo_columns_known,
            "geo-column",
            "filters",
        )?;
        merge_known(
            &mut self.filter_known,
            &response.filter_columns_known,
            "filter-leaf",
            "leaves",
        )?;
        if self.flat {
            merge_known(
                &mut self.stage_known,
                &response.stage_columns_known,
                "stage-column",
                "stages",
            )?;
        }
        Ok(())
    }

    fn accept_fingerprint(
        &mut self,
        shard: u32,
        fingerprint: Option<String>,
    ) -> Result<(), Status> {
        if let Some(fingerprint) = fingerprint {
            match self.fingerprint.as_ref() {
                Some(expected) if expected != &fingerprint => {
                    return Err(Status::failed_precondition(
                        format!(
                    "BM25 shard {shard} scoring fingerprint {fingerprint} differs from {expected}"),
                    ))
                }
                None => self.fingerprint = Some(fingerprint),
                _ => {}
            }
        }
        Ok(())
    }

    fn accept_projections(&mut self, response: &Bm25QueryResponse) -> Result<(), Status> {
        merge_known(
            &mut self.projection_known,
            &response.projection_leaves_known,
            "projection-leaf",
            "leaves",
        )?;
        crate::values::merge_projection_types(
            &self.request.projections,
            &mut self.projection_types,
            &response.projection_types,
        )?;
        for hit in &response.hits {
            crate::values::validate_projection_row(&hit.projected, &response.projection_types)?;
        }
        Ok(())
    }

    pub(super) fn validate_columns(
        &mut self,
        mask: Option<&crate::placement::ShardMask>,
    ) -> Result<(), Status> {
        self.validate_stages()?;
        refuse_unknown_geo_columns(&self.request.geo_filters, &self.geo_known)?;
        if let Some(mask) = mask {
            mark_known(&mut self.filter_known, &mask.known);
        }
        refuse_unknown_filter_leaves(self.request.filter.as_ref(), &self.filter_known)?;
        let unknown: Vec<String> = self
            .projection_leaves
            .iter()
            .zip(&self.projection_known)
            .filter(|(_, known)| !**known)
            .map(|(leaf, _)| leaf.describe())
            .collect();
        if !unknown.is_empty() {
            return Err(Status::invalid_argument(format!(
                "projection: no shard has column {}: every value would be absent. \
                 Check the spelling, or the nodes' --numeric-fields / --integer-fields \
                 / --facet-fields / --map-numeric-fields / --map-facet-fields.",
                unknown.join(", ")
            )));
        }
        Ok(())
    }

    fn validate_stages(&self) -> Result<(), Status> {
        let stages = &self.request.score_stages;
        let unknown: Vec<String> = stages
            .iter()
            .zip(&self.stage_known)
            .filter(|(_, known)| !**known)
            .map(|(stage, _)| match stage.map_key() {
                None => format!("{:?}", stage.column),
                Some(key) => format!("{:?}[{:?}]", stage.column, key),
            })
            .collect();
        if unknown.is_empty() {
            return Ok(());
        }
        let any_geo = stages.iter().zip(&self.stage_known).any(|(stage, known)| {
            !known
                && matches!(
                    crate::pb::ScoreOp::try_from(stage.operation_code()),
                    Ok(crate::pb::ScoreOp::MultGeoDecayHaversine
                        | crate::pb::ScoreOp::MultGeoDecayManhattan)
                )
        });
        let knobs = if any_geo {
            "--numeric-fields / --integer-fields / --map-numeric-fields / --geo-fields"
        } else {
            "--numeric-fields / --map-numeric-fields"
        };
        Err(Status::invalid_argument(format!(
            "no shard has numeric column {}: the chain would be a silent no-op. \
             Check the spelling, or the nodes' {knobs}.",
            unknown.join(", ")
        )))
    }

    pub(super) fn summaries(&self) -> Result<Summaries, Status> {
        let facets = merge_facet_counts(
            &self.request.facet_fields,
            &self.request.map_facet_fields,
            &self.facets,
            true,
        )?;
        let ranges = merge_range_counts(&self.request.range_facet_fields, &self.ranges)?;
        let stats = if self.flat {
            merge_column_stats(&self.request.stats_fields, &self.stats)?
        } else {
            Vec::new()
        };
        let cardinality = if self.flat {
            merge_cardinality(&self.request.cardinality_fields, &self.distinct)?
        } else {
            Vec::new()
        };
        Ok((facets, ranges, stats, cardinality))
    }

    pub(super) fn fingerprint(&self) -> Option<&String> {
        self.fingerprint.as_ref()
    }
    pub(super) fn prune(&self) -> crate::segment_prune::PruneStats {
        self.prune
    }
    pub(super) fn into_hits(self) -> Vec<(u32, Bm25Hit)> {
        self.all
    }
    pub(super) fn trace_shards(&self) {
        eprintln!(
            "bm25-fused per-shard ms: {}",
            self.shard_millis
                .iter()
                .map(|(shard, ms)| format!("{shard}:{ms:.0}"))
                .collect::<Vec<_>>()
                .join(" ")
        );
    }
}

fn merge_known(
    known: &mut [bool],
    flags: &[bool],
    label: &str,
    count_label: &str,
) -> Result<(), Status> {
    if flags.len() != known.len() {
        return Err(Status::internal(format!(
            "shard answered {} {label} flags for {} {count_label}",
            flags.len(),
            known.len()
        )));
    }
    for (acc, known) in known.iter_mut().zip(flags) {
        *acc |= *known;
    }
    Ok(())
}
