//! Stored document fetch and parent/group lineage resolution.

use crate::node::*;

const SELF_PARENT_TAG: u64 = 1 << 63;

/// Resolves requested ids to their lineage against a held shard state.
struct ParentWalk<'a> {
    guard: &'a ShardState,
    doc_filter: crate::filter::DocFilter<'a>,
    selection: &'a crate::lineage::LineageSelection,
    offset: u64,
    /// Physical rows across the vector index and the document store.
    rows: u64,
    restricted: bool,
}

impl ParentWalk<'_> {
    /// `None` for an id that is out of range, deleted, or hidden from the caller.
    fn resolve(&self, doc_id: u64) -> Option<ResolvedParent> {
        let guard = self.guard;
        let local = doc_id.checked_sub(self.offset)?;
        if local >= self.rows {
            return None;
        }
        if guard.live_docs.is_deleted(local as usize) {
            return None;
        }
        if self.restricted {
            let metadata = guard.bm25.as_ref()?;
            if local >= u64::from(metadata.next_doc_id())
                || !self
                    .doc_filter
                    .passes(local as u32, &ShardNumericRead(metadata))
            {
                return None;
            }
        }
        let store = guard.bm25.as_ref().map(Bm25Shard::as_index);
        let lineage = u32::try_from(local)
            .ok()
            .and_then(|local| store.and_then(|store| store.lineage(local)));
        let (parent_id, group_id) =
            lineage.map_or((SELF_PARENT_TAG | doc_id, 0), |l| (l.parent_id, l.group_id));
        Some(ResolvedParent {
            doc_id,
            parent_id: if self.selection.contains(crate::pb::LineageField::ParentId) {
                parent_id
            } else {
                0
            },
            group_id: if self.selection.contains(crate::pb::LineageField::GroupId) {
                group_id
            } else {
                0
            },
        })
    }
}

fn physical_span(guard: &ShardState) -> u64 {
    guard
        .index
        .as_ref()
        .map_or(0, |index| index.len() as u64)
        .max(
            guard
                .bm25
                .as_ref()
                .map_or(0, |store| u64::from(store.next_doc_id())),
        )
}

impl NodeServiceImpl {
    fn resolve_parent_rows(
        &self,
        req: ResolveParentsRequest,
    ) -> Result<Response<ResolveParentsResponse>, Status> {
        let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let selection = crate::lineage::LineageSelection::new(&req.fields)?;
        let guard = read_shard(&self.state);
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        let visibility = req
            .visibility
            .as_ref()
            .and_then(|view| view.filter.as_ref());
        let (_, visibility_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &[], visibility);
        let walk = ParentWalk {
            guard: &guard,
            doc_filter: crate::filter::DocFilter {
                deleted: None,
                geo: Default::default(),
                pred: guard
                    .bm25
                    .as_ref()
                    .and_then(|store| visibility.map(|filter| store.resolve_filter(filter)))
                    .transpose()?,
                phrase: Vec::new(),
            },
            selection: &selection,
            offset: self.config.slot_offset,
            rows: physical_span(&guard),
            restricted: req.visibility.is_some(),
        };
        let mut parents = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for doc_id in req.doc_ids {
            if !seen.insert(doc_id) {
                continue;
            }
            parents.extend(walk.resolve(doc_id));
        }
        Ok(Response::new(ResolveParentsResponse {
            parents,
            fields: selection.wire(),
            stats_epoch: guard.stats_epoch,
            stats_incarnation: guard.stats_incarnation.bytes()?,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known,
        }))
    }

    pub(in crate::node::rpc) async fn handle_get_documents(
        &self,
        request: Request<GetDocumentsRequest>,
    ) -> Result<Response<GetDocumentsResponse>, Status> {
        crate::metrics::timed(Route::GetDocuments, request, |request| async move {
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let guard = read_shard(&self.state);
            let mut documents = Vec::new();
            if let Some(store) = guard.bm25.as_ref() {
                let source_metadata = store;
                let store = store.as_index();
                for id in req.doc_ids {
                    if id < offset {
                        continue;
                    }
                    let Ok(local) = u32::try_from(id - offset) else {
                        continue;
                    };
                    if guard.live_docs.is_deleted(local as usize) {
                        continue;
                    }
                    if let Some(text) = store.text(local) {
                        documents.push(StoredDocument {
                            doc_id: id,
                            identity: source_metadata.document_identity(local),
                            sentence_spans: store
                                .doc_sentences(local)
                                .unwrap_or_default()
                                .into_iter()
                                .map(|(start, end)| crate::pb::OffsetSpan { start, end })
                                .collect(),
                            text,
                            lineage: store.lineage(local).map(|l| crate::pb::DocLineage {
                                parent_id: l.parent_id,
                                group_id: l.group_id,
                                span_start: l.span_start,
                                span_end: l.span_end,
                            }),
                        });
                    }
                }
            }
            Ok(Response::new(GetDocumentsResponse { documents }))
        })
        .await
    }

    pub(in crate::node::rpc) async fn handle_resolve_parents(
        &self,
        request: Request<ResolveParentsRequest>,
    ) -> Result<Response<ResolveParentsResponse>, Status> {
        crate::metrics::timed(Route::ResolveParents, request, |request| async move {
            self.resolve_parent_rows(request.into_inner())
        })
        .await
    }
}
