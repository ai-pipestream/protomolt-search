//! What a public request contributes to the coordinator's ring: the `k` it
//! asked for and the figures its response reports.

use super::*;

/// The `k` a public request asks for, when it has one: the field a
/// principal's `max_k` quota is checked against.
pub(super) trait RequestK {
    fn k(&self) -> Option<u32> {
        None
    }
}

macro_rules! request_k {
    ($($ty:ty),* $(,)?) => {
        $(
            impl RequestK for $ty {
                fn k(&self) -> Option<u32> {
                    Some(self.k)
                }
            }
        )*
    };
}

request_k!(
    SearchRequest,
    Bm25SearchRequest,
    HybridSearchRequest,
    VariantSearchRequest,
    QueryRequest
);

impl RequestK for QueryStreamRequest {
    fn k(&self) -> Option<u32> {
        self.query.as_ref().map(|q| q.k)
    }
}

/// The figures a public response contributes to its ring entry, read
/// without cloning the response.
pub(super) trait RecentFields {
    fn figures(&self) -> crate::diagnostics::RecentFigures {
        Default::default()
    }
}

impl RecentFields for SearchResponse {
    fn figures(&self) -> crate::diagnostics::RecentFigures {
        crate::diagnostics::RecentFigures {
            hits: self.hits.len() as u32,
            segments_total: self.segments_total,
            segments_skipped: self.segments_skipped,
            ..Default::default()
        }
    }
}

impl RecentFields for Bm25SearchResponse {
    fn figures(&self) -> crate::diagnostics::RecentFigures {
        crate::diagnostics::RecentFigures {
            hits: self.hits.len() as u32,
            segments_total: self.segments_total,
            segments_skipped: self.segments_skipped,
            ..Default::default()
        }
    }
}

impl RecentFields for HybridSearchResponse {
    fn figures(&self) -> crate::diagnostics::RecentFigures {
        crate::diagnostics::RecentFigures {
            hits: (self.hits.len() + self.cascade_hits.len()) as u32,
            candidates_collected: self
                .debug
                .as_ref()
                .map(|d| {
                    d.shards
                        .iter()
                        .filter_map(|s| s.scan.as_ref())
                        .map(|s| s.candidates_collected)
                        .sum()
                })
                .unwrap_or(0),
            ..Default::default()
        }
    }
}

impl RecentFields for QueryResponse {
    fn figures(&self) -> crate::diagnostics::RecentFigures {
        let (segments_total, segments_skipped) = self
            .profile
            .as_ref()
            .map_or((0, 0), |p| (p.segments_total, p.segments_skipped));
        crate::diagnostics::RecentFigures {
            hits: (self.hits.len() + self.groups.iter().map(|g| g.hits.len()).sum::<usize>())
                as u32,
            segments_total,
            segments_skipped,
            executed: self.executed.clone(),
            ..Default::default()
        }
    }
}

impl RecentFields for VariantSearchResponse {
    fn figures(&self) -> crate::diagnostics::RecentFigures {
        crate::diagnostics::RecentFigures {
            hits: self.results.iter().map(|r| r.hits.len()).sum::<usize>() as u32,
            ..Default::default()
        }
    }
}

impl RecentFields for AggregateResponse {
    fn figures(&self) -> crate::diagnostics::RecentFigures {
        crate::diagnostics::RecentFigures {
            hits: u32::try_from(self.matched).unwrap_or(u32::MAX),
            ..Default::default()
        }
    }
}

impl RecentFields for PlanPlacementResponse {
    fn figures(&self) -> crate::diagnostics::RecentFigures {
        crate::diagnostics::RecentFigures {
            hits: u32::try_from(self.rows).unwrap_or(u32::MAX),
            ..Default::default()
        }
    }
}

impl RecentFields for SuggestResponse {
    fn figures(&self) -> crate::diagnostics::RecentFigures {
        crate::diagnostics::RecentFigures {
            hits: self.suggestions.len() as u32,
            ..Default::default()
        }
    }
}

impl RecentFields for TermSuggestResponse {
    fn figures(&self) -> crate::diagnostics::RecentFigures {
        crate::diagnostics::RecentFigures {
            hits: self.terms.len() as u32,
            ..Default::default()
        }
    }
}

impl RecentFields for PlanIndexResponse {}
impl RecentFields for DescribeSchemaResponse {}
impl RecentFields for BroadcastVectorBackendResponse {}
impl RecentFields for FreezeTopologyWritesResponse {}
impl RecentFields for PublishTopologyResponse {}
impl RecentFields for AbortTopologyCutoverResponse {}
impl RecentFields for OptimizeDensePolicyResponse {}

impl RequestK for PhraseSearchRequest {}
impl RequestK for AggregateRequest {}
impl RequestK for PlanPlacementRequest {}
impl RequestK for PlanIndexRequest {}
impl RequestK for DescribeSchemaRequest {}
impl RequestK for BroadcastVectorBackendRequest {}
impl RequestK for FreezeTopologyWritesRequest {}
impl RequestK for PublishTopologyRequest {}
impl RequestK for AbortTopologyCutoverRequest {}
impl RequestK for OptimizeDensePolicyRequest {}
impl RequestK for ClusterHealthRequest {}
// `limit` is not a `k`: it is capped absolutely at MAX_SUGGEST_LIMIT,
// so only the concurrency quota applies.
impl RequestK for SuggestRequest {}
// `limit` is per term and capped absolutely too.
impl RequestK for TermSuggestRequest {}
impl RequestK for Streaming<RoutedIngestMappedRequest> {}
