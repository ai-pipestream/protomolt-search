//! Locked per-document row, posting, and vector commit path.

use crate::node::*;

impl NodeServiceImpl {
    /// The durable-form document of [`Self::materialize_document`],
    /// applied against a state the caller holds (see
    /// [`Self::apply_batch_locked`]): id assignment, store insert, WAL
    /// append, and the mapped vector in lockstep.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_document_locked(
        &self,
        guard: &mut ShardState,
        doc: AddDocumentsRequest,
        analyzed: crate::postings::AnalyzedDoc,
        vector: Option<Vec<f32>>,
        stable_routing_key: Option<Vec<u8>>,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        guard.check_legacy_mutation()?;
        match &doc.original_source {
            Some(source) if source.descriptor_set.is_empty() || source.message_type.is_empty() => {
                return Err(Status::invalid_argument(
                    "original source requires descriptor bytes and message type",
                ));
            }
            None if doc.source_chunk_ordinal.is_some() => {
                return Err(Status::invalid_argument(
                    "source chunk ordinal requires an original source",
                ));
            }
            _ => {}
        }
        if let Some(identity) = &doc.identity {
            if doc.original_source.is_none() {
                return Err(Status::invalid_argument(
                    "document identity requires an original source",
                ));
            }
            crate::source_archive::validate_identity(identity, doc.source_chunk_ordinal)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        }
        // Invalidate before any fallible mutation, including builder/provider
        // creation; a refused append must not leave old files marked current.
        guard.advance_stats_epoch();
        // A disk-resident shard that receives more documents is first
        // reloaded into the heap builder (the append path is
        // bulk-load: build in memory, flush back to v3).
        if matches!(guard.bm25, Some(Bm25Shard::Resident(_))) {
            let bm25_path = self
                .config
                .index_path
                .as_ref()
                .map(|p| storage_paths(p, guard.generation.as_ref()).2)
                .ok_or_else(|| {
                    Status::failed_precondition("resident shard has no index path to reload from")
                })?;
            let store = Bm25Store::load(&bm25_path)
                .map_err(|e| Status::internal(format!("reload {}: {e}", bm25_path.display())))?;
            guard.bm25 = Some(Bm25Shard::Building(store));
        }
        // Shared positional id space with the vector side: the next id
        // is past both indexes' tips.
        let vector_tip = guard.index.as_ref().map_or(0, |i| i.len() as u32);
        // Mapped ingest carries the document's vector, applied in
        // LOCKSTEP below. Validate it BEFORE anything mutates, the same
        // rule every column list follows: dimension against the index,
        // coordinates finite, and — after doc_id is known — the tips in
        // agreement, so a document that fails never half-enters either
        // leg.
        let matryoshka_enabled = !self.config.matryoshka_prefix_dims.is_empty()
            || self.config.matryoshka_active_dim.is_some();
        let (projected_vector, vector_dim, mapped_multi_rows) = match vector {
            Some(v) => {
                let raw_dim = v.len();
                if let Some((_, ci, value)) = first_invalid_coordinate(&v, raw_dim) {
                    return Err(Status::invalid_argument(format!(
                        "mapped vector coordinate {ci} is {value}; vectors must be finite"
                    )));
                }
                let (eff_vec, dim, mrows) = if matryoshka_enabled {
                    let active_target = self
                        .config
                        .matryoshka_active_dim
                        .filter(|&ad| (ad as usize) <= raw_dim);
                    let (active, dim, rows) =
                        crate::turbo_bridge::MultiResVectorOutput::project_batch(
                            &v,
                            raw_dim,
                            &self.config.matryoshka_prefix_dims,
                            active_target,
                            vector_tip as usize,
                        )?;
                    let retained = rows
                        .into_iter()
                        .map(|(slot, output)| (slot, crate::multi_res::RetainedVector { output }))
                        .collect();
                    (active, dim, retained)
                } else {
                    (v, raw_dim, Vec::new())
                };
                if let Some(known) = guard.index.as_ref().and_then(|i| i.dim_opt()) {
                    if known != dim {
                        return Err(Status::invalid_argument(format!(
                            "mapped vector has {dim} floats but the shard's index is dim {known}"
                        )));
                    }
                }
                if guard.index.is_none() {
                    // From-scratch: same single-shard convenience the
                    // AddVectors path allows.
                    let created = self.fresh_index(guard.bm25.as_ref(), dim)?;
                    guard.index = Some(created);
                }
                if guard.exact_vectors.is_none() {
                    if vector_tip != 0 {
                        return Err(Status::failed_precondition(format!(
                            "the shard has {vector_tip} provider vectors but no exact-vector \
                             sidecar; rebuild or backfill the generation before mapped ingest"
                        )));
                    }
                    let store = self.fresh_exact_store(guard.generation.as_ref(), dim)?;
                    guard.exact_vectors = Some(store);
                }
                let exact = guard.exact_vectors.as_ref().expect("ensured above");
                if exact.len() != vector_tip as usize || exact.dim() != Some(dim) {
                    return Err(Status::failed_precondition(format!(
                        "exact-vector sidecar shape {:?}x{} does not match provider shape \
                         {dim}x{vector_tip}",
                        exact.dim(),
                        exact.len()
                    )));
                }
                (Some(eff_vec), Some(dim), mrows)
            }
            None => (None, None, Vec::new()),
        };
        for (_, row) in &mapped_multi_rows {
            crate::multi_res_wal::require_tiers(
                row,
                &self.config.matryoshka_prefix_dims,
                self.config.matryoshka_active_dim,
            )
            .map_err(Status::invalid_argument)?;
        }
        let mapped_multi_log = mapped_multi_rows
            .iter()
            .map(|(_, row)| crate::multi_res::encode(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                Status::invalid_argument(format!("retain multi-resolution vectors: {error}"))
            })?;

        if guard.bm25.is_none() {
            let builder = self.new_builder(guard.generation.as_ref())?;
            guard.bm25 = Some(builder);
            // Adopt any pre-existing plain vector index as the segmented
            // provider's tail so its rows seal together with documents.
            let snapshot = match guard.bm25.as_ref() {
                Some(Bm25Shard::Segmented(g)) => Some(g.snapshot().clone()),
                _ => None,
            };
            if let Some(set) = snapshot {
                if guard
                    .index
                    .as_ref()
                    .is_some_and(|index| index.as_segmented().is_none())
                {
                    let plain = guard.index.take().expect("checked above");
                    let provider = SegmentedProvider::adopt(set, plain)
                        .map_err(|e| Status::failed_precondition(format!("{e}")))?;
                    guard.index = Some(VectorIndex::from_provider(provider));
                }
            }
        }
        let doc_id = vector_tip.max(
            guard
                .bm25
                .as_ref()
                .expect("builder just ensured")
                .next_doc_id(),
        );
        // The lockstep rule: a mapped document's vector lands at the
        // SAME id, which is only true when the vector leg's tip is the
        // id being assigned. A shard whose document leg ran ahead (per-
        // leg ingest history) cannot take mapped documents — the vector
        // would land below its document and silently corrupt every
        // hybrid result, so it refuses by name instead.
        if projected_vector.is_some() && u64::from(doc_id) != u64::from(vector_tip) {
            return Err(Status::failed_precondition(format!(
                "the shard's document leg is ahead of its vector leg ({} documents, \
                 {vector_tip} vectors); mapped ingest appends both legs in lockstep — \
                 rebuild the shard or backfill vectors with AddVectors first",
                doc_id
            )));
        }
        // Multi-field documents were positioned against the CONFIGURED
        // table; the ACTIVE table (possibly loaded from a file) must be
        // at least as wide and agree on names, or the document would
        // index under the wrong field — refuse as a Status instead of
        // tripping the store's positional assert. Body-only documents
        // skip this entirely (any table serves them).
        if analyzed.fields.len() > 1 {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            if analyzed.fields.len() > shard.field_count() {
                return Err(Status::failed_precondition(format!(
                    "document carries {} fields but the shard's table has {}; the shard \
                     predates the configured field table — rebuild or reshard it",
                    analyzed.fields.len(),
                    shard.field_count()
                )));
            }
            for fi in 1..analyzed.fields.len() {
                let want = self.config.bm25_fields.get(fi).map(String::as_str);
                if want != Some(shard.field_name(fi)) {
                    return Err(Status::failed_precondition(format!(
                        "shard field {fi} is {:?} but the configured table names {:?}; \
                         field tables must agree",
                        shard.field_name(fi),
                        want.unwrap_or("<missing>")
                    )));
                }
            }
        }
        // Verify positional-field declarations match active storage before mutating.
        {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            for name in &doc.position_fields {
                let Some(fi) = self.config.bm25_fields.iter().position(|n| n == name) else {
                    continue; // refused upstream
                };
                if fi < shard.field_count() && !shard.field_has_positions(fi) {
                    return Err(Status::failed_precondition(format!(
                        "shard field {name:?} predates token positions (its storage declares \
                         none); rebuild or reshard the generation before positional ingest"
                    )));
                }
            }
            for name in &doc.sentence_fields {
                let Some(fi) = self.config.bm25_fields.iter().position(|n| n == name) else {
                    continue; // refused upstream
                };
                if fi < shard.field_count() && !shard.field_has_sentences(fi) {
                    return Err(Status::failed_precondition(format!(
                        "shard field {name:?} predates sentence spans (its storage declares \
                         none); rebuild or reshard the generation before sentence ingest"
                    )));
                }
            }
            for (fi, field) in analyzed.fields.iter().enumerate() {
                if fi < shard.field_count()
                    && shard.field_has_positions(fi)
                    && !field.terms.is_empty()
                    && field.positions.is_none()
                {
                    return Err(Status::failed_precondition(format!(
                        "shard field {:?} keeps token positions but this document's analysis \
                         carried none; positions are never guessed from spans",
                        shard.field_name(fi)
                    )));
                }
            }
        }
        // Record which analyzer produced each column, and refuse a
        // document that contradicts one already recorded. Field NAME
        // agreement (just above) does not catch this: two documents can
        // agree on the name `body_norm` and disagree on what a term IS,
        // and nothing downstream would notice. Half a column folded and
        // half not scores both halves against one idf.
        {
            let shard = guard.bm25.as_mut().expect("builder just ensured");
            if let Some(binding) = &guard.mapped_binding {
                let contract = crate::mapped_analysis::decode_contract(
                    &binding.analysis_sha,
                    &binding.analysis_contract,
                    &binding.body_path,
                )
                .map_err(Status::failed_precondition)?;
                for field in &contract.fields {
                    let offered = if field.name == "body" {
                        Some(doc.analysis.as_ref())
                    } else {
                        doc.fields
                            .iter()
                            .find(|value| value.field == field.name)
                            .map(|value| value.analysis.as_ref())
                    };
                    if offered.is_some_and(|spec| spec != field.analysis.as_ref()) {
                        return Err(Status::failed_precondition(format!(
                            "field {:?} analysis does not match the explicit mapped binding",
                            field.name
                        )));
                    }
                }
                for field in contract.fields {
                    let fi = (0..shard.field_count())
                        .find(|&fi| shard.field_name(fi) == field.name)
                        .ok_or_else(|| {
                            Status::failed_precondition(format!(
                                "bound analysis field {:?} is absent from the shard",
                                field.name
                            ))
                        })?;
                    shard
                        .set_analysis_fingerprint(
                            fi,
                            crate::analyzer::analysis_fingerprint(field.analysis.as_ref()),
                        )
                        .map_err(Status::failed_precondition)?;
                }
            }
            let body = crate::analyzer::analysis_fingerprint(doc.analysis.as_ref());
            shard
                .set_analysis_fingerprint(0, body)
                .map_err(Status::failed_precondition)?;
            // The cased field is fingerprinted as the twin of the body's
            // spec: what the same pass computed for it.
            if let Some(ci) = cased_field_index(&self.config, self.phrase_index.as_deref(), &doc)? {
                let spec = doc
                    .analysis
                    .as_ref()
                    .expect("cased_field_index requires an explicit body spec");
                let twin = crate::analyzer::cased_twin_spec(spec);
                shard
                    .set_analysis_fingerprint(
                        ci,
                        crate::analyzer::analysis_fingerprint(Some(&twin)),
                    )
                    .map_err(Status::failed_precondition)?;
            }
            for field in &doc.fields {
                let Some(fi) = self
                    .config
                    .bm25_fields
                    .iter()
                    .position(|n| *n == field.field)
                else {
                    continue; // already refused upstream
                };
                let fingerprint = crate::analyzer::analysis_fingerprint(field.analysis.as_ref());
                shard
                    .set_analysis_fingerprint(fi, fingerprint)
                    .map_err(Status::failed_precondition)?;
            }
            if let Some(phrases) = &self.phrase_index {
                let fi = self
                    .config
                    .bm25_fields
                    .iter()
                    .position(|field| field == phrases.phrase_field())
                    .expect("phrase field validated at configuration");
                shard
                    .set_analysis_fingerprint(fi, phrases.fingerprint())
                    .map_err(Status::failed_precondition)?;
            }
            // A bigram column's identity is its source's analyzer plus
            // the derivation: two columns built from differently
            // analyzed sources hold different pairs under one name.
            for bigram in &doc.bigram_fields {
                let (Some(source), Some(derived)) = (
                    self.config
                        .bm25_fields
                        .iter()
                        .position(|n| *n == bigram.source),
                    self.config
                        .bm25_fields
                        .iter()
                        .position(|n| *n == bigram.field),
                ) else {
                    continue; // refused upstream
                };
                let source_fingerprint = shard.analysis_fingerprint(source);
                if source_fingerprint != 0 {
                    shard
                        .set_analysis_fingerprint(
                            derived,
                            crate::proximity::bigram_fingerprint(source_fingerprint),
                        )
                        .map_err(Status::failed_precondition)?;
                }
            }
        }
        // Facet values: refuse unknown fields and repeats BEFORE anything
        // mutates. Empty strings are values; omission represents absence.
        // A document that fails validation must never half-enter the store or reach the log.
        let facet_slots: Vec<(usize, String)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<&str> = Vec::new();
            let mut slots = Vec::with_capacity(doc.facets.len());
            for fv in &doc.facets {
                if seen.contains(&fv.field.as_str()) {
                    return Err(Status::invalid_argument(format!(
                        "facet field {:?} repeats in one document",
                        fv.field
                    )));
                }
                seen.push(&fv.field);
                let Some(fi) = shard.facet_index(&fv.field) else {
                    return Err(Status::invalid_argument(format!(
                        "unknown facet field {:?}; this shard's facet table \
                         (--facet-fields) does not have it",
                        fv.field
                    )));
                };
                slots.push((fi, fv.value.clone()));
            }
            slots
        };
        // Numeric values: same shape as facets — unknown fields,
        // repeats, and non-finite values refused before anything
        // mutates (NaN is the absence sentinel, infinities break the
        // score-function bound algebra).
        let numeric_slots: Vec<(usize, f64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<&str> = Vec::new();
            let mut slots = Vec::with_capacity(doc.numerics.len());
            for nv in &doc.numerics {
                if seen.contains(&nv.field.as_str()) {
                    return Err(Status::invalid_argument(format!(
                        "numeric field {:?} repeats in one document",
                        nv.field
                    )));
                }
                seen.push(&nv.field);
                let Some(ni) = shard.numeric_index(&nv.field) else {
                    return Err(Status::invalid_argument(format!(
                        "unknown numeric field {:?}; this shard's numeric table \
                         (--numeric-fields) does not have it",
                        nv.field
                    )));
                };
                if !nv.value.is_finite() {
                    return Err(Status::invalid_argument(format!(
                        "numeric field {:?} has a non-finite value; omit absent values instead",
                        nv.field
                    )));
                }
                slots.push((ni, nv.value));
            }
            slots
        };
        // Validate map entries (unknown columns, duplicate keys, non-finite
        // values) before mutating; empty strings are present keys/values.
        let map_facet_slots: Vec<(usize, &str, &str)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<(&str, &str)> = Vec::new();
            let mut slots = Vec::with_capacity(doc.map_facets.len());
            for e in &doc.map_facets {
                if seen.contains(&(e.field.as_str(), e.key.as_str())) {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?} key {:?} repeats in one document (a map holds one \
                         value per key)",
                        e.field, e.key
                    )));
                }
                seen.push((&e.field, &e.key));
                let Some(ci) = shard.map_facet_index(&e.field) else {
                    return Err(Status::invalid_argument(format!(
                        "unknown map column {:?}; this shard's map-facet table \
                         (--map-facet-fields) does not have it",
                        e.field
                    )));
                };
                slots.push((ci, e.key.as_str(), e.value.as_str()));
            }
            slots
        };
        let map_numeric_slots: Vec<(usize, &str, f64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<(&str, &str)> = Vec::new();
            let mut slots = Vec::with_capacity(doc.map_numerics.len());
            for e in &doc.map_numerics {
                if seen.contains(&(e.field.as_str(), e.key.as_str())) {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?} key {:?} repeats in one document (a map holds one \
                         value per key)",
                        e.field, e.key
                    )));
                }
                seen.push((&e.field, &e.key));
                let Some(ci) = shard.map_numeric_index(&e.field) else {
                    return Err(Status::invalid_argument(format!(
                        "unknown map column {:?}; this shard's map-numeric table \
                         (--map-numeric-fields) does not have it",
                        e.field
                    )));
                };
                if !e.value.is_finite() {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?} key {:?} has a non-finite value; omit absent entries",
                        e.field, e.key
                    )));
                }
                slots.push((ci, e.key.as_str(), e.value));
            }
            slots
        };
        let map_integer_slots: Vec<(usize, &str, i64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<(&str, &str)> = Vec::new();
            let mut slots = Vec::with_capacity(doc.map_integers.len());
            for e in &doc.map_integers {
                if seen.contains(&(e.field.as_str(), e.key.as_str())) {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?} key {:?} repeats in one document (a map holds one \
                         value per key)",
                        e.field, e.key
                    )));
                }
                seen.push((&e.field, &e.key));
                let Some(ci) = shard.map_integer_index(&e.field) else {
                    return Err(Status::invalid_argument(format!(
                        "unknown map column {:?}; this shard's map-integer table \
                         (--map-integer-fields) does not have it",
                        e.field
                    )));
                };
                slots.push((ci, e.key.as_str(), e.value));
            }
            slots
        };
        let map_unsigned_integer_slots: Vec<(usize, &str, u64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<(&str, &str)> = Vec::new();
            let mut slots = Vec::with_capacity(doc.map_unsigned_integers.len());
            for e in &doc.map_unsigned_integers {
                if seen.contains(&(e.field.as_str(), e.key.as_str())) {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?} key {:?} repeats in one document (a map holds one \
                         value per key)",
                        e.field, e.key
                    )));
                }
                seen.push((&e.field, &e.key));
                let Some(ci) = shard.map_unsigned_integer_index(&e.field) else {
                    return Err(Status::invalid_argument(format!(
                        "unknown map column {:?}; this shard's map-unsigned-integer table \
                         (--map-unsigned-integer-fields) does not have it",
                        e.field
                    )));
                };
                slots.push((ci, e.key.as_str(), e.value));
            }
            slots
        };
        // Integer values, and timestamps as sugar over them: both land
        // in the SAME i64 table, so "repeats in one document" spans the
        // two lists. Every i64 value is valid; omission represents absence.
        let integer_slots: Vec<(usize, i64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<&str> = Vec::new();
            let mut slots = Vec::with_capacity(doc.integers.len() + doc.timestamps.len());
            let resolve = |field: &str, seen: &[&str]| -> Result<usize, Status> {
                if seen.contains(&field) {
                    return Err(Status::invalid_argument(format!(
                        "integer field {field:?} repeats in one document (integers and \
                         timestamps name the same columns)"
                    )));
                }
                shard.integer_index(field).ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "unknown integer field {field:?}; this shard's integer table \
                         (--integer-fields) does not have it"
                    ))
                })
            };
            for iv in &doc.integers {
                let ii = resolve(&iv.field, &seen)?;
                seen.push(&iv.field);
                slots.push((ii, iv.value));
            }
            for tv in &doc.timestamps {
                let ii = resolve(&tv.field, &seen)?;
                seen.push(&tv.field);
                let Some(ts) = tv.value.as_ref() else {
                    return Err(Status::invalid_argument(format!(
                        "timestamp field {:?} carries no instant; omit the entry instead",
                        tv.field
                    )));
                };
                slots.push((ii, timestamp_to_epoch_micros(&tv.field, ts)?));
            }
            slots
        };
        let unsigned_integer_slots: Vec<(usize, u64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen = std::collections::HashSet::new();
            let mut slots = Vec::with_capacity(doc.unsigned_integers.len());
            for value in &doc.unsigned_integers {
                if !seen.insert(&value.field) {
                    return Err(Status::invalid_argument(format!(
                        "unsigned integer field {:?} repeats in one document",
                        value.field
                    )));
                }
                let column = shard.unsigned_integer_index(&value.field).ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "unknown unsigned integer field {:?}; declare it in --unsigned-integer-fields",
                        value.field
                    ))
                })?;
                slots.push((column, value.value));
            }
            slots
        };
        // Validate all geo points before mutating so invalid coordinates leave
        // no partial writes.
        let geo_slots: Vec<(usize, f64, f64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<&str> = Vec::new();
            let mut slots = Vec::with_capacity(doc.geo_points.len());
            for gp in &doc.geo_points {
                if seen.contains(&gp.field.as_str()) {
                    return Err(Status::invalid_argument(format!(
                        "geo field {:?} repeats in one document (a document holds one point \
                         per column)",
                        gp.field
                    )));
                }
                seen.push(&gp.field);
                let gi = shard.geo_index(&gp.field).ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "unknown geo field {:?}; this shard's geo table (--geo-fields) does \
                         not have it",
                        gp.field
                    ))
                })?;
                validate_lat_lon(&format!("geo field {:?}", gp.field), gp.lat, gp.lon)?;
                slots.push((gi, gp.lat, gp.lon));
            }
            slots
        };
        let global_id = self.config.slot_offset + u64::from(doc_id);
        if *added == 0 {
            *first_id = global_id;
        }
        // Apply first, log after, as for vectors: a document that
        // fails to enter the store must never reach the log, or its
        // id would be reassigned and poison the replay.
        let lineage = doc.lineage.map(|l| crate::postings::DocLineage {
            parent_id: l.parent_id,
            group_id: l.group_id,
            span_start: l.span_start,
            span_end: l.span_end,
        });
        match guard.bm25.as_mut().expect("builder just ensured") {
            Bm25Shard::Segmented(g) => {
                let local = doc_id - g.tail_base();
                let store = g.tail_mut();
                if let Some(source) = &doc.original_source {
                    store
                        .source_archive_mut()
                        .attach_source_with_identity(
                            local,
                            source,
                            doc.source_chunk_ordinal,
                            doc.identity.as_ref(),
                        )
                        .map_err(|e| Status::internal(format!("retain source: {e}")))?;
                }
                store.add_document_with_lineage(local, doc.text.clone(), analyzed, lineage);
                for (fi, value) in &facet_slots {
                    store.set_facet(*fi, local, value);
                }
                for &(ni, value) in &numeric_slots {
                    store.set_numeric(ni, local, value);
                }
                for &(ci, key, value) in &map_facet_slots {
                    store.set_map_facet(ci, local, key, value);
                }
                for &(ci, key, value) in &map_numeric_slots {
                    store.set_map_numeric(ci, local, key, value);
                }
                for &(ci, key, value) in &map_integer_slots {
                    store
                        .set_map_integer(ci, local, key, value)
                        .map_err(|e| Status::internal(format!("integer map apply: {e}")))?;
                }
                for &(ci, key, value) in &map_unsigned_integer_slots {
                    store
                        .set_map_unsigned_integer(ci, local, key, value)
                        .map_err(|e| Status::internal(format!("integer map apply: {e}")))?;
                }
                for &(ii, value) in &integer_slots {
                    store.set_integer(ii, local, value);
                }
                for &(ii, value) in &unsigned_integer_slots {
                    store.set_unsigned_integer(ii, local, value);
                }
                for &(gi, lat, lon) in &geo_slots {
                    store.set_geo(gi, local, lat, lon);
                }
                g.sync_tail();
            }
            Bm25Shard::Building(store) => {
                if let Some(source) = &doc.original_source {
                    store
                        .source_archive_mut()
                        .attach_source_with_identity(
                            doc_id,
                            source,
                            doc.source_chunk_ordinal,
                            doc.identity.as_ref(),
                        )
                        .map_err(|e| Status::internal(format!("retain source: {e}")))?;
                }
                store.add_document_with_lineage(doc_id, doc.text.clone(), analyzed, lineage);
                for (fi, value) in &facet_slots {
                    store.set_facet(*fi, doc_id, value);
                }
                for &(ni, value) in &numeric_slots {
                    store.set_numeric(ni, doc_id, value);
                }
                for &(ci, key, value) in &map_facet_slots {
                    store.set_map_facet(ci, doc_id, key, value);
                }
                for &(ci, key, value) in &map_numeric_slots {
                    store.set_map_numeric(ci, doc_id, key, value);
                }
                for &(ci, key, value) in &map_integer_slots {
                    store
                        .set_map_integer(ci, doc_id, key, value)
                        .map_err(|e| Status::internal(format!("integer map apply: {e}")))?;
                }
                for &(ci, key, value) in &map_unsigned_integer_slots {
                    store
                        .set_map_unsigned_integer(ci, doc_id, key, value)
                        .map_err(|e| Status::internal(format!("integer map apply: {e}")))?;
                }
                for &(ii, value) in &integer_slots {
                    store.set_integer(ii, doc_id, value);
                }
                for &(ii, value) in &unsigned_integer_slots {
                    store.set_unsigned_integer(ii, doc_id, value);
                }
                for &(gi, lat, lon) in &geo_slots {
                    store.set_geo(gi, doc_id, lat, lon);
                }
            }
            Bm25Shard::Spilling(builder) => {
                if let Some(source) = &doc.original_source {
                    builder
                        .source_archive_mut()
                        .attach_source_with_identity(
                            doc_id,
                            source,
                            doc.source_chunk_ordinal,
                            doc.identity.as_ref(),
                        )
                        .map_err(|e| Status::internal(format!("retain source: {e}")))?;
                }
                builder
                    .add_document_with_lineage(doc_id, doc.text.clone(), analyzed, lineage)
                    .map_err(|e| Status::internal(format!("spill write: {e}")))?;
                for (fi, value) in &facet_slots {
                    builder.set_facet(*fi, doc_id, value);
                }
                for &(ni, value) in &numeric_slots {
                    builder.set_numeric(ni, doc_id, value);
                }
                for &(ci, key, value) in &map_facet_slots {
                    builder.set_map_facet(ci, doc_id, key, value);
                }
                for &(ci, key, value) in &map_numeric_slots {
                    builder.set_map_numeric(ci, doc_id, key, value);
                }
                for &(ci, key, value) in &map_integer_slots {
                    builder
                        .set_map_integer(ci, doc_id, key, value)
                        .map_err(|e| Status::internal(format!("integer map apply: {e}")))?;
                }
                for &(ci, key, value) in &map_unsigned_integer_slots {
                    builder
                        .set_map_unsigned_integer(ci, doc_id, key, value)
                        .map_err(|e| Status::internal(format!("integer map apply: {e}")))?;
                }
                for &(ii, value) in &integer_slots {
                    builder.set_integer(ii, doc_id, value);
                }
                for &(ii, value) in &unsigned_integer_slots {
                    builder.set_unsigned_integer(ii, doc_id, value);
                }
                for &(gi, lat, lon) in &geo_slots {
                    builder.set_geo(gi, doc_id, lat, lon);
                }
            }
            Bm25Shard::Resident(_) => {
                return Err(Status::internal("shard builder unavailable"));
            }
        }
        wal_append_or_degrade(
            &mut guard.wal,
            wal_record::Op::AddDocuments(LoggedAddDocuments {
                first_id: global_id,
                documents: vec![doc],
                stable_routing_keys: stable_routing_key.clone().into_iter().collect(),
                source_references: Vec::new(),
            }),
        );
        // The mapped document's vector, at the same id, under the same
        // lock, with the same WAL record AddVectors writes — replay
        // rebuilds both legs from their own records. Failure here is
        // ruled out by the validation above; if it happens anyway the
        // legs have diverged by one and the next mapped document
        // refuses on the lockstep check, loudly.
        if let Some(v) = projected_vector {
            let dim = vector_dim.expect("validated alongside the vector");
            let index = guard.index.as_mut().expect("ensured during validation");
            index.add(&v, dim).map_err(|e| {
                Status::internal(format!(
                    "vector apply failed after validation: {e}; the shard's legs may have \
                     diverged — the next mapped document will refuse if so"
                ))
            })?;
            guard
                .exact_vectors
                .as_mut()
                .expect("validated alongside provider index")
                .append(&v, dim)
                .map_err(|e| {
                    Status::internal(format!(
                        "exact-vector append failed after provider commit: {e}; refuse further \
                         ingest and rebuild this generation"
                    ))
                })?;
            if !mapped_multi_rows.is_empty() {
                for (slot, out) in mapped_multi_rows {
                    guard.multi_res_vectors.insert(slot, out);
                }
            }
            let index = guard
                .index
                .as_ref()
                .expect("provider index remains present");
            let committed_config = index.backend_config().map_err(|e| {
                Status::internal(format!(
                    "read vector backend config after mapped ingest: {e}"
                ))
            })?;
            if let Some(wal) = guard.wal.as_mut() {
                wal.update_manifest(|m| {
                    if m.dim == 0 {
                        m.dim = dim as u32;
                    }
                    m.set_backend_config(committed_config.clone());
                });
            }
            wal_append_or_degrade(
                &mut guard.wal,
                wal_record::Op::AddVectors(LoggedAddVectors {
                    first_id: global_id,
                    batch: Some(AddVectorsRequest {
                        materialized_rows: mapped_multi_log,
                        vectors: v,
                        dim: dim as u32,
                    }),
                    stable_routing_keys: stable_routing_key.into_iter().collect(),
                }),
            );
        }
        *added += 1;
        Ok(())
    }
}
