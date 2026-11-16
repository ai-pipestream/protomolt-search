//! Distributed range facets and block-max integer score-chain parity.

use super::*;

/// Distributed range facets: exact over the full match set, additive
/// across a fleet where one shard lacks the column, unchanged by k, and
/// answering the boundary rules the edge list promises.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_range_facets_are_exact_and_boundary_correct() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_integer_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    // "rust" matches d0 (0), d1 (10), d2 (5), d3 (20), d5 (-1), d6
    // (absent). Hand-computed: [0, 10) holds d0 and d2; [10, 20) holds
    // d1, which sits exactly on the interior edge and belongs to the
    // UPPER bucket; [20, 30) holds d3. d5 is below the first edge and
    // d6 has no value, so neither is counted anywhere.
    let want = vec![range_field("citations", &EDGES)];
    let (hits, _, ranges) =
        common::fanout::bm25_range_facets(&coordinator, "rust", 10, &want, None)
            .await
            .unwrap();
    assert_eq!(hits.len(), 6);
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].column, "citations");
    assert!(ranges[0].known, "two of three shards have the column");
    assert_eq!(
        buckets_of(&ranges[0]),
        vec![(0.0, 10.0, 2), (10.0, 20.0, 1), (20.0, 30.0, 1)]
    );
    // Additivity: shard 0 contributed [2, 1, 0] and shard 1 [0, 0, 1];
    // the total is 4, and the two uncounted matches are accounted for
    // by the rules, not lost.
    let counted: u64 = ranges[0].buckets.iter().map(|b| b.count).sum();
    assert_eq!(counted, 4);

    // Counts cover the whole match set at k = 1 too: the floor bounds
    // what is surfaced, never what matched.
    let (hits_k1, _, ranges_k1) =
        common::fanout::bm25_range_facets(&coordinator, "rust", 1, &want, None)
            .await
            .unwrap();
    assert_eq!(hits_k1.len(), 1);
    assert_eq!(buckets_of(&ranges_k1[0]), buckets_of(&ranges[0]));

    // A value sitting exactly on the LAST edge lands in no bucket:
    // "vector" matches d1 (10) and d4 (30).
    let (_, _, vector_ranges) =
        common::fanout::bm25_range_facets(&coordinator, "vector", 10, &want, None)
            .await
            .unwrap();
    assert_eq!(
        buckets_of(&vector_ranges[0]),
        vec![(0.0, 10.0, 0), (10.0, 20.0, 1), (20.0, 30.0, 0)],
        "the last edge is exclusive; there is no implicit overflow bucket"
    );

    // The public RPC carries range facets end to end, flat route.
    let resp = SearchService::bm25_search(
        &coordinator,
        Request::new(Bm25SearchRequest {
            explain: false,
            collection: String::new(),
            highlight: None,
            projections: Vec::new(),
            filter: String::new(),
            text: "rust".to_string(),
            k: 10,
            analysis: None,
            min_score: 0.0,
            fields: Vec::new(),
            facet_fields: Vec::new(),
            score_stages: Vec::new(),
            map_facet_fields: Vec::new(),
            range_facet_fields: want.clone(),
            geo_filters: Vec::new(),
            stats_fields: Vec::new(),
            cardinality_fields: Vec::new(),
            phrase: None,
            prefixes: Vec::new(),
            synonyms: Vec::new(),
            synonyms_off: false,
            pre_analyzed_terms: Vec::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(buckets_of(&resp.range_facets[0]), buckets_of(&ranges[0]));

    // The fused route counts over the union of every leg's terms.
    let fields = [pipestream_search::pb::QueryField {
        field: "body".to_string(),
        analysis: None,
        weight: 1.0,
        k1: 0.0,
        b: 0.0,
        phrase: None,
        prefixes: Vec::new(),
        synonyms: Vec::new(),
        synonyms_off: false,
    }];
    let (_, _, fused_ranges) = common::fanout::bm25_fused_req(
        &coordinator,
        pipestream_search::pb::Bm25SearchRequest {
            text: "rust".into(),
            fields: fields.to_vec(),
            range_facet_fields: want.to_vec(),
            ..Default::default()
        },
        10,
        None,
    )
    .await
    .unwrap();
    assert_eq!(buckets_of(&fused_ranges[0]), buckets_of(&ranges[0]));

    // A column NO shard knows is a typo, not an empty histogram.
    let err = common::fanout::bm25_range_facets(
        &coordinator,
        "rust",
        10,
        &[range_field("citation", &EDGES)],
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("citation") && err.message().contains("--integer-fields"),
        "refusal names the column and the knob: {}",
        err.message()
    );

    // Edge lists that do not describe intervals refuse, naming the
    // column — a silently repaired list answers a question nobody asked.
    for (edges, needle) in [
        (vec![10.0, 0.0, 20.0], "strictly ascending"),
        (vec![0.0, 0.0], "strictly ascending"),
        (vec![5.0], "at least 2"),
        (vec![], "at least 2"),
        (vec![0.0, f64::INFINITY], "not finite"),
        (vec![f64::NAN, 1.0], "not finite"),
    ] {
        let err = common::fanout::bm25_range_facets(
            &coordinator,
            "rust",
            10,
            &[range_field("citations", &edges)],
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle) && err.message().contains("citations"),
            "expected {needle:?} and the column in: {}",
            err.message()
        );
    }

    // A malformed edge list refuses even when the query analyzes to no
    // terms or asks for k = 0: edge validation needs no shard, so the
    // coordinator's early return must not swallow it into an empty Ok.
    for (text, k) in [("", 10u32), ("rust", 0)] {
        let err = common::fanout::bm25_range_facets(
            &coordinator,
            text,
            k,
            &[range_field("citations", &[5.0])],
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "text {text:?}, k {k}: the early return must not hide the refusal"
        );
    }
    let err = common::fanout::bm25_fused_req(
        &coordinator,
        pipestream_search::pb::Bm25SearchRequest {
            fields: fields.to_vec(),
            range_facet_fields: vec![range_field("citations", &[5.0])],
            ..Default::default()
        },
        10,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "the fused route honors the same rule before its own early return"
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// [`NumericRead`] over an open reader, as the node's shard wrapper
/// provides it in production.
struct ReaderNumerics<'a>(&'a Bm25Reader);
impl NumericRead for ReaderNumerics<'_> {
    fn value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.0.numeric_value(ni, doc_id)
    }
    fn map_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        self.0.map_numeric_value(column, key_ord, doc_id)
    }
    fn map_int_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<i64> {
        self.0.map_integer_value(column, key_ord, doc_id)
    }
    fn map_uint_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<u64> {
        self.0.map_unsigned_integer_value(column, key_ord, doc_id)
    }
    fn int_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        self.0.integer_value(ii, doc_id)
    }
    fn uint_value(&self, ii: usize, doc_id: u32) -> Option<u64> {
        self.0.unsigned_integer_value(ii, doc_id)
    }
    fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        self.0.geo_value(gi, doc_id)
    }
    fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        self.0.facet_ord(fi, doc_id)
    }
    fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        self.0.map_facet_value_ord(ci, key_ord, doc_id)
    }
}

/// The exactness gate for i64-backed chains: on a file-backed shard
/// (impacts present, so the block-max path really prunes) the chained
/// pruned scorer is bitwise identical to the chained exhaustive oracle,
/// with the bound lifted from i64 min/max through the same monotone
/// cast the values take.
#[test]
fn integer_chain_pruned_matches_exhaustive_bitwise() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("integer_chain_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("chain.bm25");
    let stats = build_integer_chain_store(&path);
    let reader = Bm25Reader::open(&path).unwrap();
    let body = reader.field(0);
    let cols = ReaderNumerics(&reader);
    let citations = reader.integer_index("citations").unwrap();
    let filed = reader.integer_index("filed").unwrap();
    let as_f64 = |(min, max): (i64, i64)| (min as f64, max as f64);
    let chain = ScoreChain {
        stages: vec![
            Stage {
                op: StageOp::AddLinear { weight: -0.002 },
                column: Some(ColumnRef::Integer(citations)),
                min_max: as_f64(reader.integer_min_max(citations)),
            },
            Stage {
                op: StageOp::MultExpDecay {
                    origin: 9.007e15,
                    scale: 2000.0,
                },
                column: Some(ColumnRef::Integer(filed)),
                min_max: as_f64(reader.integer_min_max(filed)),
            },
            Stage {
                op: StageOp::MultLog { weight: 0.4 },
                column: Some(ColumnRef::Integer(citations)),
                min_max: as_f64(reader.integer_min_max(citations)),
            },
        ],
    };
    let ctx = Some((&chain, &cols as &dyn NumericRead));
    let terms: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
    let params = Bm25Params::default();
    let sig = |docs: &[bm25::ScoredDoc]| -> Vec<(u32, u64)> {
        docs.iter().map(|d| (d.doc_id, d.score.to_bits())).collect()
    };
    for k in [1usize, 5, 50] {
        let exhaustive =
            bm25::top_k_exhaustive(&body, &terms, &stats, params, bm25::TopK::new(k).chain(ctx));
        let pruned = bm25::top_k(&body, &terms, &stats, params, bm25::TopK::new(k).chain(ctx));
        assert_eq!(
            sig(&exhaustive),
            sig(&pruned),
            "k={k}: pruned != exhaustive"
        );
        if let Some(kth) = exhaustive.last() {
            let q = bm25::TopK::new(k).floor(kth.score).chain(ctx);
            let seeded = bm25::top_k(&body, &terms, &stats, params, q);
            assert_eq!(sig(&exhaustive), sig(&seeded), "k={k}: seeded");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn build_integer_chain_store(path: &std::path::Path) -> CorpusStats {
    let n = 3000u32;
    let mut total = 0u64;
    let mut store =
        Bm25Store::with_text_fields(&["body"]).with_columns(Integer, &["citations", "filed"]);
    for doc in 0..n {
        let tf_a = 1 + (u64::from(doc) * 2654435761 % 7) as u32;
        let mut terms = vec![("a".to_string(), tf_a, Vec::new())];
        if doc % 3 == 0 {
            terms.push(("b".to_string(), 1 + doc % 3, Vec::new()));
        }
        if doc % 61 == 0 {
            terms.push(("c".to_string(), 1, Vec::new()));
        }
        let len: u32 = terms.iter().map(|(_, tf, _)| tf).sum();
        total += u64::from(len);
        store.add_document(doc, ".".to_string(), AnalyzedDoc::body(terms, len));
        if doc % 7 != 0 {
            store.set_integer(0, doc, i64::from(doc) * 48271 % 1000);
        }
        if doc % 4 != 0 {
            store.set_integer(1, doc, (1i64 << 53) + i64::from(doc));
        }
    }
    store.save(path).unwrap();
    CorpusStats {
        doc_count: u64::from(n),
        total_doc_length: total,
        dfs: vec![n, n.div_ceil(3), n.div_ceil(61)],
    }
}

/// A chain over an i64 column, distributed: bitwise equal to the
/// monolith, and a document on the column-less shard passes through
/// unchanged (absence is identity, which is exact).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_integer_chain_matches_monolith() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_integer_shards(&analysis).await;
    let distributed = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    let (mono_addr, mono) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        integer_fields: vec!["citations".to_string()],
        ..Default::default()
    })
    .await;
    let all: Vec<(&str, Integers)> = SHARD_DOCS.concat();
    add_documents_integer(&mono_addr, &all).await.unwrap();
    let monolithic = CoordinatorServiceImpl::new(vec![mono_addr])
        .with_bm25(Some(analysis.clone()), Default::default());

    let stages = [ScoreStage {
        op: ScoreOp::MultLog as i32,
        column: "citations".to_string(),
        weight: 1.0,
        ..Default::default()
    }];
    for text in ["rust", "search rust", "vector"] {
        let (got, _, _) = common::fanout::bm25_stages(&distributed, text, 10, &stages, None)
            .await
            .unwrap();
        let (want, _, _) = common::fanout::bm25_stages(&monolithic, text, 10, &stages, None)
            .await
            .unwrap();
        assert_eq!(
            hit_signature(&got),
            hit_signature(&want),
            "query {text:?}: distributed i64 chain != monolithic"
        );
    }

    // The chain does something, and d6 (on the column-less shard) keeps
    // its base score bit for bit.
    let unchained = distributed.fanout_bm25("rust", 10, None).await.unwrap();
    let (chained, _, _) = common::fanout::bm25_stages(&distributed, "rust", 10, &stages, None)
        .await
        .unwrap();
    assert_eq!(unchained.len(), chained.len(), "same match set");
    assert_ne!(
        hit_signature(&unchained),
        hit_signature(&chained),
        "the chain changed nothing"
    );
    let score = |hits: &[Bm25Hit], id: u64| hits.iter().find(|h| h.doc_id == id).unwrap().score;
    assert_eq!(
        score(&unchained, 6).to_bits(),
        score(&chained, 6).to_bits(),
        "a shard without the column must answer identity"
    );

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}
