//! Relayed document fetches by ID and roaring bitmap union/gap enforcement.

use super::*;

/// The fetches by id a relay routes to the child whose slot range holds
/// each id: answers in the caller's order, an id repeated answered once
/// per occurrence, a foreign id left out, and the read receipt carried
/// as a relay token the relay translates back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetches_by_id_through_relays_equal_the_children() {
    use pipestream_search::pb::{
        CompiledProjection, FetchValuesRequest, GetDocumentsRequest, LineageField,
        ResolveParentsRequest,
    };
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let a = relay_over(&[&l[0], &l[1]]).await;
    let b = relay_over(&[&l[2], &l[3]]).await;
    let top = relay_over(&[&a, &b]).await;
    let rows = LEX_ROWS as u64;
    // Across the children, out of order, one repeated, one foreign.
    let ids: Vec<u64> = vec![rows + 3, 1, 1, 3 * rows + 2, 5 * rows, 2 * rows + 1, 0, 2];
    let child_of = |id: u64| (id / rows) as usize;
    let visibility = Some(pipestream_search::pb::DocumentVisibility {
        filter: pipestream_search::cel::compile_filter("court == 'scotus'").unwrap(),
    });
    let projections = vec![
        CompiledProjection {
            name: "year".into(),
            expr: Some(pipestream_search::cel::compile_value("year").unwrap()),
        },
        CompiledProjection {
            name: "pages".into(),
            expr: Some(pipestream_search::cel::compile_value("pages").unwrap()),
        },
    ];
    let fields = vec![LineageField::ParentId as i32, LineageField::GroupId as i32];
    for (name, relay_addr, children) in [
        ("one level", a.clone(), 0..2),
        ("two levels", top.clone(), 0..4),
    ] {
        let mut relay = NodeServiceClient::connect(relay_addr).await.unwrap();
        // The answer a node gives each id, in the caller's order, from
        // the child that serves it.
        let mut documents = Vec::new();
        let mut parents = Vec::new();
        let mut values = Vec::new();
        let mut identities = std::collections::BTreeMap::new();
        let mut direct_receipt = None;
        for &id in &ids {
            let child = child_of(id);
            if !children.contains(&child) {
                continue;
            }
            let mut node = NodeServiceClient::connect(l[child].clone()).await.unwrap();
            documents.extend(
                node.get_documents(GetDocumentsRequest { doc_ids: vec![id] })
                    .await
                    .unwrap()
                    .into_inner()
                    .documents,
            );
            parents.extend(
                node.resolve_parents(ResolveParentsRequest {
                    doc_ids: vec![id],
                    visibility: visibility.clone(),
                    fields: fields.clone(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner()
                .parents,
            );
            let fetched = node
                .fetch_values(FetchValuesRequest {
                    include_identities: true,
                    candidate_ids: vec![id],
                    projections: projections.clone(),
                    visibility: visibility.clone(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            assert!(fetched.identities_included);
            identities.extend(
                fetched
                    .identities
                    .into_iter()
                    .map(|row| (row.doc_id, row.identity)),
            );
            values.extend(fetched.rows);
            direct_receipt = Some((
                fetched.visibility_fingerprint,
                fetched.visibility_columns_known,
                fetched.projection_types,
                fetched.projection_leaves_known,
            ));
        }
        let served = ids
            .iter()
            .filter(|&&id| children.contains(&child_of(id)))
            .count();
        assert!(served < ids.len(), "{name}: an id is foreign");
        assert_eq!(documents.len(), served, "{name}: the foreign id is absent");
        let through = relay
            .get_documents(GetDocumentsRequest {
                doc_ids: ids.clone(),
            })
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .into_inner();
        assert_eq!(through.documents, documents, "{name}: GetDocuments");
        let through = relay
            .resolve_parents(ResolveParentsRequest {
                doc_ids: ids.clone(),
                visibility: visibility.clone(),
                fields: fields.clone(),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .into_inner();
        assert_eq!(through.parents, parents, "{name}: ResolveParents");
        assert_eq!(through.fields, fields, "{name}: the lineage fields echo");
        assert_ne!(through.stats_epoch, 0, "{name}: a relay token");
        assert_eq!(
            through.stats_incarnation.len(),
            32,
            "{name}: the relay's incarnation"
        );
        let (fingerprint, known, types, leaves_known) = direct_receipt.expect("a child answered");
        assert_eq!(
            through.visibility_fingerprint, fingerprint,
            "{name}: the visibility echo"
        );
        assert_eq!(through.visibility_columns_known, known);
        // The token translates back into each child's claim; a claim
        // the relay did not issue refuses by name.
        let claimed = relay
            .resolve_parents(ResolveParentsRequest {
                doc_ids: ids.clone(),
                visibility: visibility.clone(),
                fields: fields.clone(),
                expected_stats_epoch: through.stats_epoch,
                expected_stats_incarnation: through.stats_incarnation.clone(),
            })
            .await
            .unwrap_or_else(|e| panic!("{name}: the relay's own claim: {e}"))
            .into_inner();
        assert_eq!(claimed.parents, parents, "{name}: under the claim");
        let mut foreign = through.stats_incarnation.clone();
        foreign[0] ^= 0xff;
        let err = relay
            .resolve_parents(ResolveParentsRequest {
                doc_ids: ids.clone(),
                visibility: visibility.clone(),
                fields: fields.clone(),
                expected_stats_epoch: through.stats_epoch,
                expected_stats_incarnation: foreign,
            })
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::FailedPrecondition,
            "{name}: {}",
            err.message()
        );
        let through = relay
            .fetch_values(FetchValuesRequest {
                include_identities: true,
                candidate_ids: ids.clone(),
                projections: projections.clone(),
                visibility: visibility.clone(),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .into_inner();
        assert_eq!(through.rows, values, "{name}: FetchValues");
        assert!(through.identities_included);
        assert!(identities.values().any(Option::is_some));
        assert!(identities.values().any(Option::is_none));
        assert_eq!(through.identities.len(), identities.len());
        assert_eq!(
            through
                .identities
                .into_iter()
                .map(|row| (row.doc_id, row.identity))
                .collect::<std::collections::BTreeMap<_, _>>(),
            identities
        );
        assert_eq!(
            through.projection_types, types,
            "{name}: the projection types"
        );
        assert_eq!(through.projection_leaves_known, leaves_known);
        assert_eq!(through.visibility_fingerprint, fingerprint);
        assert_ne!(through.stats_epoch, 0);
        assert_eq!(through.stats_incarnation.len(), 32);
        // A stage on a column no child knows: false, for the root's
        // typo rule, not a refusal from this level.
        let unknown = relay
            .fetch_values(FetchValuesRequest {
                candidate_ids: vec![1],
                stages: vec![pipestream_search::pb::ScoreStage {
                    op: pipestream_search::pb::ScoreOp::MultLog as i32,
                    column: "nowhere".into(),
                    weight: 1.0,
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .into_inner();
        assert_eq!(unknown.stage_columns_known, vec![false], "{name}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitmaps_through_a_relay_lie_over_the_children_and_refuse_a_gap() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let relay_addr = relay_over(&[&l[1], &l[0]]).await;
    let mut relay = NodeServiceClient::connect(relay_addr).await.unwrap();
    let request = pipestream_search::pb::LexicalBitmapRequest {
        terms: vec!["court".into()],

        ..Default::default()
    };
    let through = relay
        .resolve_lexical_bitmap(request.clone())
        .await
        .expect("relayed lexical bitmap")
        .into_inner();
    assert_eq!(through.base_label, 0);
    assert_eq!(through.label_count, 2 * LEX_ROWS as u64);
    assert_ne!(through.stats_epoch, 0, "the epoch is a relay token");
    let mut want = vec![false; 2 * LEX_ROWS];
    for (child, addr) in l.iter().take(2).enumerate() {
        let direct = NodeServiceClient::connect(addr.clone())
            .await
            .unwrap()
            .resolve_lexical_bitmap(request.clone())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(direct.base_label, (child * LEX_ROWS) as u64);
        for position in 0..direct.label_count as usize {
            want[child * LEX_ROWS + position] =
                direct.bits[position / 8] & (1 << (position % 8)) != 0;
        }
    }
    let got: Vec<bool> = (0..2 * LEX_ROWS)
        .map(|position| through.bits[position / 8] & (1 << (position % 8)) != 0)
        .collect();
    assert_eq!(
        got, want,
        "the relayed bitmap is the children's laid side by side"
    );
    assert!(got.iter().any(|&b| b), "the term matched");
    // The token translates on a rescore that echoes it.
    let stats = relay
        .term_stats(TermStatsRequest {
            terms: vec!["court".into()],
            fields: Vec::new(),

            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let members: Vec<u64> = got
        .iter()
        .enumerate()
        .filter_map(|(id, &member)| member.then_some(id as u64))
        .collect();
    let rescored = relay
        .bm25_rescore(pipestream_search::pb::Bm25RescoreRequest {
            terms: vec!["court".into()],
            global_doc_count: stats.doc_count,
            global_total_doc_length: stats.total_doc_length,
            global_doc_frequencies: stats.doc_frequencies.clone(),
            candidate_ids: members.clone(),
            k1: 1.2,
            b: 0.75,
            expected_stats_epoch: through.stats_epoch,
            expected_stats_incarnation: through.stats_incarnation.clone(),
            score_stages: Vec::new(),

            ..Default::default()
        })
        .await
        .expect("the bitmap's token is a claim the relay translates")
        .into_inner();
    assert_eq!(rescored.hits.len(), members.len());
    // The filter and vector bitmaps share the layout.
    let filtered = relay
        .resolve_filter_bitmap(pipestream_search::pb::FilterBitmapRequest {
            geo_filters: Vec::new(),
            filter: pipestream_search::cel::compile_filter(r#"court == "scotus""#)
                .expect("compiles"),

            ..Default::default()
        })
        .await
        .expect("relayed filter bitmap")
        .into_inner();
    assert_eq!(filtered.base_label, 0);
    assert_eq!(filtered.label_count, 2 * LEX_ROWS as u64);
    assert_eq!(filtered.filter_columns_known, vec![true]);
    let scotus = (0..2 * LEX_ROWS)
        .filter(|position| filtered.bits[position / 8] & (1 << (position % 8)) != 0)
        .count();
    assert_eq!(scotus, LEX_ROWS, "every other document is scotus");
    let vectors = relay
        .resolve_vector_bitmap(pipestream_search::pb::VectorBitmapRequest {
            ..Default::default()
        })
        .await
        .expect("relayed vector bitmap")
        .into_inner();
    assert_eq!(vectors.label_count, 2 * LEX_ROWS as u64);
    assert_ne!(
        vectors.stats_epoch, 0,
        "vector membership carries a physical relay token"
    );
    assert_eq!(vectors.stats_incarnation.len(), 32);

    // A child past a gap: the layout the parent would derive is a lie,
    // so the routes refuse by name.
    let (gapped, _handle) = start_empty_node(NodeConfig {
        slot_offset: (LEX_ROWS + 3) as u64,
        analysis_addr: Some(leaves.analysis.clone()),
        ..Default::default()
    })
    .await;
    let relay_addr = relay_over(&[&l[0], &gapped]).await;
    let mut relay = NodeServiceClient::connect(relay_addr).await.unwrap();
    let err = relay
        .resolve_vector_bitmap(pipestream_search::pb::VectorBitmapRequest {
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("gap"), "{}", err.message());
    let err = relay
        .vector_rescore(pipestream_search::pb::VectorRescoreRequest {
            vector: leaves.corpus[..DIM].to_vec(),
            candidate_ids: vec![0],

            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("gap"), "{}", err.message());
}
