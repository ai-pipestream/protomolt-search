use super::map::{add_count32, child_error, child_ranges, route_ids};
use super::*;

pub fn merge_term_stats(
    request: &TermStatsRequest,
    children: &[TermStatsResponse],
) -> Result<TermStatsResponse, Status> {
    crate::visibility::validate_stats_request(request)?;
    let scope = crate::visibility::VisibilityScope::new(request.visibility.as_ref())?;
    let mut visibility_columns_known = vec![false; scope.column_count()];
    if children.is_empty() {
        return Err(Status::failed_precondition(
            "relay: TermStats over no children",
        ));
    }
    let n_terms = request.terms.len();
    let mut doc_count: u64 = 0;
    let mut total_doc_length: u64 = 0;
    let mut doc_frequencies: Vec<u32> = vec![0; n_terms];
    let mut field_stats: Vec<crate::pb::FieldStats> = request
        .fields
        .iter()
        .map(|ft| crate::pb::FieldStats {
            total_doc_length: 0,
            doc_frequencies: vec![0; ft.terms.len()],
            known: false,
            positions: false,
            sentences: false,
        })
        .collect();
    for (shard, child) in children.iter().enumerate() {
        scope.validate_response(child)?;
        crate::visibility::validate_stats_mode(request.version_only, child)?;
        for (known, child_known) in visibility_columns_known
            .iter_mut()
            .zip(&child.visibility_columns_known)
        {
            *known |= child_known;
        }
        doc_count = doc_count.checked_add(child.doc_count).ok_or_else(|| {
            Status::failed_precondition(format!(
                "relay: document count sums past u64 at child {shard}"
            ))
        })?;
        total_doc_length = total_doc_length
            .checked_add(child.total_doc_length)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "relay: total document length sums past u64 at child {shard}"
                ))
            })?;
        if child.doc_frequencies.len() != n_terms {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} body document frequencies for {n_terms} terms",
                child.doc_frequencies.len()
            )));
        }
        for (ti, (acc, df)) in doc_frequencies
            .iter_mut()
            .zip(&child.doc_frequencies)
            .enumerate()
        {
            *acc = acc.checked_add(*df).ok_or_else(|| {
                Status::failed_precondition(format!(
                    "relay: document frequency of body term {:?} sums past the u32 the \
                     statistics contract carries, at child {shard}; the collection is \
                     larger than the contract admits",
                    request.terms[ti]
                ))
            })?;
        }
        if child.field_stats.len() != field_stats.len() {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} field shares for {} fields",
                child.field_stats.len(),
                field_stats.len()
            )));
        }
        for (fi, (acc, share)) in field_stats.iter_mut().zip(&child.field_stats).enumerate() {
            let field = &request.fields[fi].field;
            if shard == 0 {
                acc.known = share.known;
                acc.positions = share.positions;
                acc.sentences = share.sentences;
            } else {
                for (name, mine, theirs) in [
                    ("known", acc.known, share.known),
                    ("positions", acc.positions, share.positions),
                    ("sentences", acc.sentences, share.sentences),
                ] {
                    if mine != theirs {
                        return Err(Status::failed_precondition(format!(
                            "relay: field {field:?} is {name}={mine} on child 0 and \
                             {name}={theirs} on child {shard}; a relay requires homogeneous \
                             field capabilities across its children"
                        )));
                    }
                }
            }
            acc.total_doc_length = acc
                .total_doc_length
                .checked_add(share.total_doc_length)
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "relay: total length of field {field:?} sums past u64 at child {shard}"
                    ))
                })?;
            if share.doc_frequencies.len() != acc.doc_frequencies.len() {
                return Err(Status::internal(format!(
                    "relay: child {shard} answered {} document frequencies for {} terms of \
                     field {field:?}",
                    share.doc_frequencies.len(),
                    acc.doc_frequencies.len()
                )));
            }
            for (ti, (a, df)) in acc
                .doc_frequencies
                .iter_mut()
                .zip(&share.doc_frequencies)
                .enumerate()
            {
                *a = a.checked_add(*df).ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "relay: document frequency of term {:?} in field {field:?} sums past \
                         the u32 the statistics contract carries, at child {shard}",
                        request.fields[fi].terms[ti]
                    ))
                })?;
            }
        }
    }
    Ok(TermStatsResponse {
        version_only: request.version_only,
        doc_count,
        total_doc_length,
        doc_frequencies,
        field_stats,
        stats_epoch: 0,
        stats_incarnation: Vec::new(),
        visibility_fingerprint: scope.fingerprint().to_vec(),
        visibility_columns_known,
    })
}

/// cardinality is a union of values, not a sum. Both are refused by name
/// rather than answered with a different reduction.
pub(super) fn refuse_bm25_aggregates(
    route: &str,
    stats_fields: &[String],
    cardinality_fields: &[String],
) -> Result<(), Status> {
    if !stats_fields.is_empty() {
        return Err(Status::unimplemented(format!(
            "relay: {route} with stats_fields {stats_fields:?} is not served through a relay; \
             a column statistic folds in the root's shard order and this level would change \
             it"
        )));
    }
    if !cardinality_fields.is_empty() {
        return Err(Status::unimplemented(format!(
            "relay: {route} with cardinality_fields {cardinality_fields:?} is not served \
             through a relay; an exact cardinality is a union of values, not a sum of counts"
        )));
    }
    Ok(())
}

/// OR one child's per-column flags into the accumulator, which every
/// child must size the same way.
pub(super) fn merge_known(
    what: &str,
    shard: usize,
    acc: &mut Option<Vec<bool>>,
    share: &[bool],
) -> Result<(), Status> {
    match acc {
        None => *acc = Some(share.to_vec()),
        Some(flags) => {
            if flags.len() != share.len() {
                return Err(Status::internal(format!(
                    "relay: child {shard} answered {} {what} flags while an earlier child \
                     answered {}",
                    share.len(),
                    flags.len()
                )));
            }
            for (a, b) in flags.iter_mut().zip(share) {
                *a |= *b;
            }
        }
    }
    Ok(())
}

/// The children's terminal responses merged into the one the parent
/// reads as a shard's: every child's local top-k concatenated (the
/// parent's global merge picks from the union, and nothing this level
/// could drop is provably outside the parent's top-k), facet counts
/// summed by value, range buckets summed by position, column-known
/// flags ORed, segment counts added with a check. A facet no child knows
/// stays `known = false` for the root's typo rule; refusing it here
/// would answer for shards this relay does not see.
/// The ranked union of the children's Boolean answers cut to `depth`,
/// score descending then doc id ascending (the root's own order), with
/// the match counts and segment counts summed and each leaf's known
/// flags joined by OR. A child answering a flag list of another shape
/// than the request's leaves is a protocol break.
pub fn merge_boolean_responses(
    req: &crate::pb::BooleanShardRequest,
    shares: &[crate::pb::BooleanShardResponse],
) -> Result<crate::pb::BooleanShardResponse, Status> {
    let leaves = req.leaves.len();
    let mut candidates = Vec::new();
    let mut matched = 0u64;
    let mut segments_total = 0u32;
    let mut segments_skipped = 0u32;
    let mut filters_known: Vec<Option<crate::pb::BooleanFilterKnown>> = vec![None; leaves];
    let mut stages_known: Vec<Option<crate::pb::BooleanStagesKnown>> = vec![None; leaves];
    for (shard, share) in shares.iter().enumerate() {
        if share.filters_known.len() != leaves || share.stages_known.len() != leaves {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} filter and {} stage flag lists for {leaves} leaves",
                share.filters_known.len(),
                share.stages_known.len()
            )));
        }
        candidates.extend(share.candidates.iter().cloned());
        matched = matched.checked_add(share.matched).ok_or_else(|| {
            Status::internal("relay: Boolean match count overflows u64 across children")
        })?;
        segments_total = segments_total.saturating_add(share.segments_total);
        segments_skipped = segments_skipped.saturating_add(share.segments_skipped);
        for (index, known) in share.filters_known.iter().enumerate() {
            join_flags(&mut filters_known[index], known, |acc, k| {
                or_flags(
                    &mut acc.geo_columns_known,
                    &k.geo_columns_known,
                    shard,
                    "geo",
                )?;
                or_flags(
                    &mut acc.filter_columns_known,
                    &k.filter_columns_known,
                    shard,
                    "filter-leaf",
                )
            })?;
        }
        for (index, known) in share.stages_known.iter().enumerate() {
            join_flags(&mut stages_known[index], known, |acc, k| {
                or_flags(
                    &mut acc.stage_columns_known,
                    &k.stage_columns_known,
                    shard,
                    "stage",
                )
            })?;
        }
    }
    candidates.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    candidates.truncate(req.depth as usize);
    Ok(crate::pb::BooleanShardResponse {
        read_receipt: None,
        candidates,
        matched,
        segments_total,
        segments_skipped,
        stats_epoch: 0,
        filters_known: filters_known
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect(),
        stages_known: stages_known
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect(),
        aggregate: None,
    })
}

pub(super) fn join_flags<T: Clone>(
    acc: &mut Option<T>,
    share: &T,
    join: impl FnOnce(&mut T, &T) -> Result<(), Status>,
) -> Result<(), Status> {
    match acc {
        None => {
            *acc = Some(share.clone());
            Ok(())
        }
        Some(acc) => join(acc, share),
    }
}

pub(super) fn or_flags(
    acc: &mut [bool],
    share: &[bool],
    shard: usize,
    what: &str,
) -> Result<(), Status> {
    if acc.len() != share.len() {
        return Err(Status::internal(format!(
            "relay: child {shard} answered {} {what} flags while an earlier child answered {}",
            share.len(),
            acc.len()
        )));
    }
    for (a, k) in acc.iter_mut().zip(share) {
        *a |= *k;
    }
    Ok(())
}

pub fn merge_bm25_responses(
    req: &Bm25QueryRequest,
    shares: Vec<Bm25QueryResponse>,
) -> Result<Bm25QueryResponse, Status> {
    crate::node::validate_range_facet_fields(&req.range_facet_fields)?;
    let mut range_shares = Vec::new();
    let mut hits = Vec::new();
    let mut facet_shares = Vec::new();
    let mut stage_known = None;
    let mut geo_known = None;
    let mut filter_known = None;
    let mut projection_known = None;
    let mut projection_types = vec![crate::pb::ScalarValueType::Unspecified; req.projections.len()];
    let mut segments_total: u32 = 0;
    let mut segments_skipped: u32 = 0;
    for (shard, share) in shares.into_iter().enumerate() {
        facet_shares.push(share.facets);
        range_shares.push(share.range_facets);
        merge_known(
            "stage-column",
            shard,
            &mut stage_known,
            &share.stage_columns_known,
        )?;
        merge_known(
            "geo-column",
            shard,
            &mut geo_known,
            &share.geo_columns_known,
        )?;
        merge_known(
            "filter-leaf",
            shard,
            &mut filter_known,
            &share.filter_columns_known,
        )?;
        merge_known(
            "projection-leaf",
            shard,
            &mut projection_known,
            &share.projection_leaves_known,
        )?;
        segments_total = segments_total
            .checked_add(share.segments_total)
            .ok_or_else(|| Status::internal("relay: segments_total overflows u32"))?;
        segments_skipped = segments_skipped
            .checked_add(share.segments_skipped)
            .ok_or_else(|| Status::internal("relay: segments_skipped overflows u32"))?;
        crate::values::merge_projection_types(
            &req.projections,
            &mut projection_types,
            &share.projection_types,
        )?;
        for hit in &share.hits {
            crate::values::validate_projection_row(&hit.projected, &share.projection_types)?;
        }
        hits.extend(share.hits);
    }
    // The monolith's order: score, then id. The parent re-merges under
    // its own total order; this order only decides the k-th seed below.
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    let k = req.k as usize;
    let kth_best = if k > 0 && hits.len() >= k {
        crate::bm25::floor_seed(hits[k - 1].score)
    } else {
        0.0
    };
    let facets = crate::coordinator::merge_facet_counts(
        &req.facet_fields,
        &req.map_facet_fields,
        &facet_shares,
        false,
    )?;
    let range_facets = crate::rangefacet::merge(&req.range_facet_fields, &range_shares, false)?;
    Ok(Bm25QueryResponse {
        hits,
        kth_best,
        facets,
        stage_columns_known: stage_known.unwrap_or_default(),
        range_facets,
        geo_columns_known: geo_known.unwrap_or_default(),
        filter_columns_known: filter_known.unwrap_or_default(),
        stats: Vec::new(),
        distinct: Vec::new(),
        projection_leaves_known: projection_known.unwrap_or_default(),
        projection_types: projection_types.into_iter().map(|ty| ty as i32).collect(),
        segments_total,
        segments_skipped,
    })
}
/// One child's packed bitmap as it came back, or nothing for a child the
/// placement mask skipped (it contributes no member).
pub struct ChildBitmap {
    pub base_label: u64,
    pub label_count: u64,
    pub bits: Vec<u8>,
}

/// The children's bitmaps laid over the relay's one slot range: child
/// `i`'s bits go at `offset_i - base` (its slot offset, which its
/// `base_label` must equal), every range checked for its declared
/// length and for zero padding, the relay's `label_count` running to
/// the end of the last labelled child. Between a child's last label and
/// the next child's first slot the bits are zero, which is what those
/// unfilled slots hold on a node too. Returns `(base_label, label_count,
/// bits)`.
pub fn concat_bitmaps(
    children: &[String],
    ranges: &[(u64, u64)],
    shares: &[Option<ChildBitmap>],
) -> Result<(u64, u64, Vec<u8>), Status> {
    let base = ranges
        .iter()
        .map(|&(offset, _)| offset)
        .min()
        .ok_or_else(|| Status::failed_precondition("relay: a relay coordinator has no children"))?;
    let name = |child: usize| children.get(child).map_or("", String::as_str);
    let mut total: u64 = 0;
    for (child, share) in shares.iter().enumerate() {
        let Some(share) = share else {
            continue;
        };
        let (offset, span) = ranges[child];
        if share.base_label != offset {
            return Err(Status::failed_precondition(format!(
                "relay: child {child} ({}) answered a bitmap based at label {} while its slot \
                 range starts at {offset}",
                name(child),
                share.base_label
            )));
        }
        if share.label_count > span {
            return Err(Status::failed_precondition(format!(
                "relay: child {child} ({}) answered {} labels over a slot range of {span}",
                name(child),
                share.label_count
            )));
        }
        let expected = usize::try_from(share.label_count.div_ceil(8)).map_err(|_| {
            Status::resource_exhausted("relay: membership bitmap does not fit usize")
        })?;
        if share.bits.len() != expected {
            return Err(Status::internal(format!(
                "relay: child {child} ({}) answered {} bitmap bytes for {} labels; expected \
                 {expected}",
                name(child),
                share.bits.len(),
                share.label_count
            )));
        }
        if !share.label_count.is_multiple_of(8)
            && share.bits.last().is_some_and(|last| {
                let used = (share.label_count % 8) as u8;
                *last & !((1u8 << used) - 1) != 0
            })
        {
            return Err(Status::internal(format!(
                "relay: child {child} ({}) sets padding bits beyond its label count",
                name(child)
            )));
        }
        let end = (offset - base)
            .checked_add(share.label_count)
            .ok_or_else(|| Status::internal("relay: membership label range overflows u64"))?;
        total = total.max(end);
    }
    let bytes = usize::try_from(total.div_ceil(8)).map_err(|_| {
        Status::resource_exhausted("relay: merged membership bitmap does not fit usize")
    })?;
    let mut bits = vec![0u8; bytes];
    for (child, share) in shares.iter().enumerate() {
        let Some(share) = share else {
            continue;
        };
        let at = usize::try_from(ranges[child].0 - base)
            .map_err(|_| Status::resource_exhausted("relay: bitmap offset does not fit usize"))?;
        let count = usize::try_from(share.label_count).map_err(|_| {
            Status::resource_exhausted("relay: bitmap label count does not fit usize")
        })?;
        or_bits(&mut bits, at, &share.bits, count);
    }
    Ok((base, total, bits))
}

/// OR `count` bits of `src` (LSB-first per byte, the wire packing) into
/// `dst` starting at bit `at`. Byte-aligned placements copy whole bytes;
/// the rest walk the set bits.
pub(super) fn or_bits(dst: &mut [u8], at: usize, src: &[u8], count: usize) {
    if at.is_multiple_of(8) {
        let first = at / 8;
        for (i, byte) in src.iter().enumerate() {
            dst[first + i] |= byte;
        }
        return;
    }
    for (byte_index, byte) in src.iter().copied().enumerate() {
        let mut held = byte;
        while held != 0 {
            let bit = held.trailing_zeros() as usize;
            let position = byte_index * 8 + bit;
            if position < count {
                let target = at + position;
                dst[target / 8] |= 1 << (target % 8);
            }
            held &= held - 1;
        }
    }
}
impl RelayService {
    pub(super) async fn term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        crate::metrics::timed(Route::TermStats, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            crate::visibility::validate_stats_request(&req)?;
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let req = req.clone();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.term_stats(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| {
                            Status::new(
                                status.code(),
                                format!(
                                    "relay child {shard} ({addr}) term stats: {}",
                                    status.message()
                                ),
                            )
                        })
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay term stats task: {e}")))??,
                );
            }
            let mut merged = merge_term_stats(&req, &shares)?;
            merged.stats_incarnation = self.stats_incarnation.bytes()?;
            self.still_current(&pinned, "TermStats")?;
            let tuple = TokenTuple {
                collection: self.collection.clone(),
                control_revision: pinned.control_revision,
                topology_generation: pinned.topology_generation,
                children,
                epochs: shares
                    .iter()
                    .map(|s| StatsClaim::required(s.stats_epoch, &s.stats_incarnation))
                    .collect::<Result<_, _>>()?,
            };
            merged.stats_epoch = self
                .tokens
                .lock()
                .expect("relay token registry poisoned")
                .allocate(tuple)?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn bm25_query_stream(
        &self,
        request: Request<Streaming<Bm25QueryStreamRequest>>,
    ) -> Result<Response<<Self as NodeService>::Bm25QueryStreamStream>, Status> {
        crate::metrics::timed_stream(Route::Bm25QueryStream, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let deadline = timeout.map(|d| tokio::time::Instant::now() + d);
            let mut inbound = request.into_inner();
            let req = match inbound.message().await? {
                Some(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::Start(req)),
                }) => req,
                _ => {
                    return Err(Status::invalid_argument(
                        "first Bm25QueryStreamRequest must be a Bm25QueryRequest start",
                    ))
                }
            };
            refuse_bm25_aggregates(
                "Bm25QueryStream",
                &req.stats_fields,
                &req.cardinality_fields,
            )?;
            let (pinned, frozen) = self.pin();
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                frozen.node_addresses().len(),
            )?;
            if frozen.node_addresses().is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let map_changes = self.inner.map.changes();
            let (tx, rx) = mpsc::channel::<Result<Bm25QueryStreamResponse, Status>>(64);
            let relay = self.clone();
            tokio::spawn(async move {
                let work = relay.relay_bm25_stream(
                    frozen,
                    pinned,
                    map_changes,
                    req,
                    claims,
                    inbound,
                    tx.clone(),
                    deadline,
                    timeout,
                );
                let result = tokio::select! {
                    _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
                    result = work => result,
                };
                if let Err(status) = result {
                    let _ = tokio::time::timeout(Duration::from_millis(250), tx.send(Err(status)))
                        .await;
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    pub(super) async fn expand_term_prefix(
        &self,
        request: Request<crate::pb::ExpandTermPrefixRequest>,
    ) -> Result<Response<crate::pb::ExpandTermPrefixResponse>, Status> {
        crate::metrics::timed(Route::ExpandTermPrefix, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let child_req = req.clone();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.expand_term_prefix(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "prefix expansion", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay prefix task: {e}")))??,
                );
            }
            for share in &shares {
                scope.validate_echo(
                    &share.visibility_fingerprint,
                    &share.visibility_columns_known,
                )?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&share.visibility_columns_known)
                {
                    *known |= present;
                }
            }
            let mut merged = merge_prefix_expansions(req.cap, &shares);
            merged.visibility_fingerprint = scope.fingerprint().to_vec();
            merged.visibility_columns_known = visibility_known;
            self.still_current(&pinned, "ExpandTermPrefix")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn suggest_terms(
        &self,
        request: Request<crate::pb::SuggestTermsRequest>,
    ) -> Result<Response<crate::pb::SuggestTermsResponse>, Status> {
        crate::metrics::timed(Route::SuggestTerms, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let child_req = req.clone();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.suggest_terms(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "dictionary scan", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay suggest task: {e}")))??,
                );
            }
            for share in &shares {
                scope.validate_echo(
                    &share.visibility_fingerprint,
                    &share.visibility_columns_known,
                )?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&share.visibility_columns_known)
                {
                    *known |= present;
                }
            }
            let mut merged = merge_suggest_terms(req.max_scan, &shares)?;
            merged.visibility_fingerprint = scope.fingerprint().to_vec();
            merged.visibility_columns_known = visibility_known;
            self.still_current(&pinned, "SuggestTerms")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25Query, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            refuse_bm25_aggregates("Bm25Query", &req.stats_fields, &req.cardinality_fields)?;
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.bm25_query(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "bm25 query", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay bm25 query task: {e}")))??,
                );
            }
            let merged = merge_bm25_responses(&req, shares)?;
            self.still_current(&pinned, "Bm25Query")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn bm25_phrase_query(
        &self,
        request: Request<Bm25PhraseQueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25PhraseQuery, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let query = req
                .query
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("Bm25PhraseQuery: query is absent"))?;
            refuse_bm25_aggregates(
                "Bm25PhraseQuery",
                &query.stats_fields,
                &query.cardinality_fields,
            )?;
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                query.expected_stats_epoch,
                &query.expected_stats_incarnation,
                children.len(),
            )?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                if let Some(q) = child_req.query.as_mut() {
                    q.expected_stats_epoch = claims[shard].epoch;
                    q.expected_stats_incarnation = claims[shard].incarnation();
                }
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.bm25_phrase_query(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "bm25 phrase query", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay bm25 phrase task: {e}")))??,
                );
            }
            let merged = merge_bm25_responses(query, shares)?;
            self.still_current(&pinned, "Bm25PhraseQuery")?;
            Ok(Response::new(merged))
        })
        .await
    }

    pub(super) async fn bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        crate::metrics::timed(Route::Bm25Rescore, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            // Each candidate goes to the child whose slot range holds it;
            // the ranges come from the children's health reports. An id
            // in no child's range is another shard's, and is ignored as
            // a node ignores an id outside its own range: the boolean
            // planner sends every shard every candidate.
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let by_child = route_ids(&ranges, &req.candidate_ids)?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, ids) in by_child.into_iter().enumerate() {
                let requested: std::collections::HashSet<_> = ids.iter().copied().collect();
                let mut link = frozen.node_client(&children[shard])?;
                let mut child_req = req.clone();
                child_req.candidate_ids = ids;
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                let addr = children[shard].clone();
                tasks.push((requested, tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.bm25_rescore(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "bm25 rescore", status))
                })));
            }
            let mut hits = Vec::new();
            let mut stage_known: Option<Vec<bool>> = None;
            let mut receipts = Vec::with_capacity(children.len());
            for (i, (requested, task)) in tasks.into_iter().enumerate() {
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay bm25 rescore task: {e}")))??;
                merge_known(
                    "stage-column",
                    i,
                    &mut stage_known,
                    &share.stage_columns_known,
                )?;
                let mut seen = std::collections::HashSet::new();
                for hit in &share.hits {
                    if !requested.contains(&hit.doc_id) || !hit.score.is_finite() || !seen.insert(hit.doc_id) {
                        return Err(Status::failed_precondition("relay: BM25 child returned an unrequested, duplicate or nonfinite score"));
                    }
                }
                receipts.push(read_metadata(&share));
                hits.extend(share.hits);
            }
            self.still_current(&pinned, "Bm25Rescore")?;
            let receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            hits.sort_by_key(|hit| hit.doc_id);
            Ok(Response::new(Bm25RescoreResponse {
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
                hits,
                stage_columns_known: stage_known
                    .unwrap_or_else(|| vec![false; req.score_stages.len()]),
            }))
        })
        .await
    }

    pub(super) async fn resolve_lexical_bitmap(
        &self,
        request: Request<crate::pb::LexicalBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let req = request.into_inner();
        crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        if children.is_empty() {
            return Err(Status::failed_precondition(
                "relay: a relay coordinator has no children",
            ));
        }
        let reports = self.children_health(&frozen, timeout).await?;
        let ranges = child_ranges(&self.collection, &children, &reports)?;
        let mut tasks = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let child_req = req.clone();
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new(child_req);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                link.resolve_lexical_bitmap(request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| child_error(shard, &addr, "lexical bitmap", status))
            }));
        }
        let mut shares = Vec::with_capacity(children.len());
        let mut receipts = Vec::with_capacity(children.len());
        let mut binding = None;
        let mut segments_total: u32 = 0;
        let mut segments_skipped: u32 = 0;
        for (shard, task) in tasks.into_iter().enumerate() {
            let share = task
                .await
                .map_err(|e| Status::internal(format!("relay lexical bitmap task: {e}")))??;
            add_count32(
                &mut segments_total,
                share.segments_total,
                shard,
                "segment count",
            )?;
            add_count32(
                &mut segments_skipped,
                share.segments_skipped,
                shard,
                "skipped segment count",
            )?;
            crate::vector_read::check_binding("", None, &mut binding)?;
            receipts.push(read_metadata(&share));
            shares.push(Some(ChildBitmap {
                base_label: share.base_label,
                label_count: share.label_count,
                bits: share.bits,
            }));
        }
        let (base_label, label_count, bits) = concat_bitmaps(&children, &ranges, &shares)?;
        self.still_current(&pinned, "ResolveLexicalBitmap")?;
        let receipt =
            self.read_receipt(&pinned, children, req.visibility.as_ref(), None, &receipts)?;
        Ok(Response::new(crate::pb::MembershipBitmapResponse {
            vector_binding: binding.flatten(),
            stats_incarnation: receipt.stats_incarnation,
            visibility_fingerprint: receipt.visibility_fingerprint,
            visibility_columns_known: receipt.visibility_columns_known,
            base_label,
            label_count,
            bits,
            stats_epoch: receipt.stats_epoch,
            segments_total,
            segments_skipped,
        }))
    }

    pub(super) async fn evaluate_boolean(
        &self,
        request: Request<crate::pb::BooleanShardRequest>,
    ) -> Result<Response<crate::pb::BooleanShardResponse>, Status> {
        crate::metrics::timed(Route::EvaluateBoolean, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            if req.depth == 0 {
                return Err(Status::invalid_argument(
                    "EvaluateBoolean: depth 0 asks for no candidates",
                ));
            }
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.evaluate_boolean(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "boolean evaluation", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay boolean task: {e}")))??,
                );
            }
            let mut merged = merge_boolean_responses(&req, &shares)?;
            self.still_current(&pinned, "EvaluateBoolean")?;
            let receipts = shares
                .iter()
                .map(|share| {
                    let receipt = share.read_receipt.clone().ok_or_else(|| {
                        Status::failed_precondition("relay child omitted Boolean read receipt")
                    })?;
                    if receipt.stats_epoch != share.stats_epoch {
                        return Err(Status::failed_precondition(
                            "Boolean receipt epoch mismatch",
                        ));
                    }
                    Ok(receipt)
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let mut receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            let mut binding = None;
            for leaf in &req.leaves {
                if let Some(crate::pb::boolean_plan_leaf::Leaf::Dense(dense)) = leaf.leaf.as_ref() {
                    for child in &receipts {
                        crate::vector_read::check_binding(
                            &dense.field,
                            child.vector_binding.as_ref(),
                            &mut binding,
                        )?;
                    }
                }
            }
            receipt.vector_binding = binding.flatten();
            merged.stats_epoch = receipt.stats_epoch;
            merged.read_receipt = Some(receipt);
            // The root aggregate each child folded over its match set,
            // folded in child order, with a receipt of its own the root
            // checks as it checks a shard's.
            if let Some(spec) = req.aggregate.as_ref() {
                let folds = shares
                    .iter()
                    .enumerate()
                    .map(|(shard, share)| {
                        share.aggregate.clone().ok_or_else(|| {
                            Status::failed_precondition(format!(
                                "relay: child {shard} answered no aggregate fold for a Boolean \
                                 plan that asked for one"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, Status>>()?;
                let mut fold = merge_aggregate_shares(
                    &spec.aggregations,
                    &spec.group_by,
                    spec.max_groups,
                    &spec.histograms,
                    &spec.percentiles,
                    &folds,
                )?;
                let fold_receipt = self.read_receipt(
                    &pinned,
                    self.children(),
                    req.visibility.as_ref(),
                    Some(&claims),
                    &folds,
                )?;
                fold.stats_epoch = fold_receipt.stats_epoch;
                fold.stats_incarnation = fold_receipt.stats_incarnation;
                fold.visibility_fingerprint = fold_receipt.visibility_fingerprint;
                fold.visibility_columns_known = fold_receipt.visibility_columns_known;
                merged.aggregate = Some(fold);
            }
            Ok(Response::new(merged))
        })
        .await
    }
}
