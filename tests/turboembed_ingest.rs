//! End-to-end `#103` ingest integration tests:
//! 1. `(pos, pos_len)` token lattices (`COLUMN_KIND_POS_LENGTHS = 18`) across
//!    `Bm25Store`, `SpillBuilder` + `Bm25Reader`, `SegmentedShard` + compaction,
//!    derived bigrams (`proximity::derive_bigrams`), and gRPC `Bm25Search` /
//!    `Query` phrase matching.
//! 2. Pre-WAL `NlpColumnSpec` materialization (`money`, `quantity`, `date`,
//!    `geo_spine`, `geo_point`, `sentiment`, `affirmed` vs `negated` entities,
//!    `simhash`) and deterministic WAL replay on restart without re-running NLP.
//! 3. `IndexGenerationHandshake` mismatch refusal (`FAILED_PRECONDITION`) for
//!    both lexical `AnalysisSpec` and neural `BundleScoringFingerprint`.
//! 4. Single-pass `MultiResVectorOutput` Matryoshka tiers + sentence vectors +
//!    SHA-256 row digest.

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use pipestream_search::analyzer::{
    body_spec, turbo_handshake, CHAR_FILTER_ACCENT_FOLD, CHAR_FILTER_DEHYPHENATE,
    CHAR_FILTER_FULL_CASE_FOLD, CHAR_FILTER_STRIP_INVISIBLE, CHAR_FILTER_WHITESPACE,
    NATIVE_ANALYSIS_BACKEND, SOURCE_NORMALIZED_STEMS, STEMMER_PORTER, TERM_VECTOR_MODE_FULL,
    TOKENIZER_UAX29, TURBO_ANALYSIS_BACKEND,
};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::link::NodeLink;
use pipestream_search::node::{Layout, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::wal::wal_record;
use pipestream_search::pb::{
    filter_query, search_query, selection_query, AddDocumentsRequest, AnalysisSpec,
    Bm25SearchRequest, BooleanQuery, FilterQuery, FlushRequest, LexicalQuery, PhraseMatch,
    QueryRequest, SearchQuery, SelectionQuery,
};
use pipestream_search::postings::{
    AnalyzedDoc, AnalyzedField, Bm25Index, Bm25Reader, Bm25Store, SpillBuilder,
};
use pipestream_search::proximity;
use pipestream_search::reshard;
use pipestream_search::segmented::SegmentedShard;
use pipestream_search::turbo_bridge::{MultiResVectorOutput, NlpColumnSpec};
use pipestream_search::wal;
use tonic::{Code, Request};
use turbo_client::{BundleScoringFingerprint, BundleScoringSpec, IndexGenerationHandshake};

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "turboembed_ingest_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn lattice_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: TOKENIZER_UAX29,
        stemmer: STEMMER_PORTER,
        term_vector_mode: TERM_VECTOR_MODE_FULL,
        term_vector_source: SOURCE_NORMALIZED_STEMS,
        char_filters: vec![
            CHAR_FILTER_STRIP_INVISIBLE,
            CHAR_FILTER_ACCENT_FOLD,
            CHAR_FILTER_FULL_CASE_FOLD,
            CHAR_FILTER_DEHYPHENATE,
            CHAR_FILTER_WHITESPACE,
        ],
    }
}

/// Constructs a synthetic lattice document for `"NYC court"` where `"nyc"`
/// (`pos = 0, pos_len = 3`) runs parallel to `"new"` (`pos = 0, pos_len = 1`),
/// `"york"` (`pos = 1, pos_len = 1`), `"citi"` (`pos = 2, pos_len = 1`),
/// followed by `"court"` (`pos = 3, pos_len = 1`).
fn nyc_lattice_doc() -> AnalyzedDoc {
    let text = "NYC court";
    AnalyzedDoc {
        fields: vec![AnalyzedField {
            terms: vec![
                ("citi".into(), 1, vec![(0, 3)]),
                ("court".into(), 1, vec![(4, 9)]),
                ("new".into(), 1, vec![(0, 3)]),
                ("nyc".into(), 1, vec![(0, 3)]),
                ("york".into(), 1, vec![(0, 3)]),
            ],
            length: 2,
            positions: Some(vec![vec![2], vec![3], vec![0], vec![0], vec![1]]),
            pos_lengths: Some(vec![vec![1], vec![1], vec![1], vec![3], vec![1]]),
            sentences: Some(vec![(0, text.len() as u32)]),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn assert_lattice_phrases(index: &dyn Bm25Index) {
    assert!(index.has_positions());
    assert!(index.has_pos_lengths());

    // 1. Joined abbreviation arc: `"nyc"` (0, len=3) -> `"court"` (3, len=1) matches with slop = 0!
    assert!(proximity::phrase_matches(
        index,
        &["nyc".into(), "court".into()],
        &[0, 1],
        0,
        0,
    ));

    // 2. Parallel expanded path: `"new"` (0,1) -> `"york"` (1,1) -> `"citi"` (2,1) -> `"court"` (3,1) ALSO matches!
    assert!(proximity::phrase_matches(
        index,
        &["new".into(), "york".into(), "citi".into(), "court".into()],
        &[0, 1, 2, 3],
        0,
        0,
    ));

    // 3. Non-adjacent `"nyc"` (ends at 3) -> `"york"` (starts at 1) MUST NOT match!
    assert!(!proximity::phrase_matches(
        index,
        &["nyc".into(), "york".into()],
        &[0, 1],
        0,
        0,
    ));
}

#[test]
fn pos_len_lattice_matches_both_joined_and_split_phrases_across_store_spill_and_segments() {
    let dir = tempdir("lattice_formats");
    let doc0 = nyc_lattice_doc();
    let mut doc1 = AnalyzedDoc::body_positioned(
        vec![
            ("appeal".into(), 1, vec![(4, 10)]),
            ("nyc".into(), 1, vec![(0, 3)]),
        ],
        vec![vec![1], vec![0]],
        2,
    );
    doc1.fields[0].sentences = Some(vec![(0, 10)]);

    // Verify derived bigrams emit BOTH `"nyc court"` AND `"new york"` / `"york citi"` / `"citi court"`!
    let bigrams = proximity::derive_bigrams(&doc0.fields[0]).unwrap();
    let bigram_terms: Vec<&str> = bigrams.terms.iter().map(|(t, _, _)| t.as_str()).collect();
    assert!(bigram_terms.contains(&"nyc court"));
    assert!(bigram_terms.contains(&"new york"));
    assert!(bigram_terms.contains(&"york citi"));
    assert!(bigram_terms.contains(&"citi court"));

    // 1. Heap Bm25Store
    let mut store = Bm25Store::new()
        .with_positions(&["body"])
        .with_sentences(&["body"]);
    store.add_document(0, "NYC court".into(), doc0.clone());
    store.add_document(1, "NYC appeal".into(), doc1.clone());
    assert_lattice_phrases(&store.field(0));

    // 2. Persist via Bm25Store::save (`COLUMN_KIND_POS_LENGTHS = 18`) -> Bm25Reader mmap
    let store_path = dir.join("store.bm25");
    store.save(&store_path).unwrap();
    let reader = Bm25Reader::open(&store_path).unwrap();
    assert_lattice_phrases(&reader.field(0));

    // Verify transpose round-trip preserves `pos_lengths` (used by segment compaction!)
    let transposed = reader.field(0).transpose().unwrap().field(0).unwrap();
    assert_eq!(transposed.pos_lengths, doc0.fields[0].pos_lengths);

    // 3. Persist via SpillBuilder (forcing multi-run external merge with tiny buffer!) -> Bm25Reader mmap
    let spill_dir = dir.join("spill_work");
    let spill_path = dir.join("spill.bm25");
    let mut spill = SpillBuilder::create(&spill_dir)
        .unwrap()
        .with_position_fields(&["body"])
        .with_sentence_fields(&["body"])
        .with_buffer_bytes(1);
    spill
        .add_document_with_lineage(0, "NYC court".into(), doc0.clone(), None)
        .unwrap();
    spill
        .add_document_with_lineage(1, "NYC appeal".into(), doc1.clone(), None)
        .unwrap();
    spill.finish(&spill_path).unwrap();
    let spill_reader = Bm25Reader::open(&spill_path).unwrap();
    assert_lattice_phrases(&spill_reader.field(0));

    // 4. SegmentedShard
    let seg_root = dir.join("segments");
    let mut seg = SegmentedShard::open(
        &seg_root,
        Bm25Store::new()
            .with_positions(&["body"])
            .with_sentences(&["body"]),
    )
    .unwrap();
    seg.add_document(0, "NYC court".into(), doc0, None).unwrap();
    seg.add_document(1, "NYC appeal".into(), doc1, None)
        .unwrap();
    assert_lattice_phrases(&seg.field(0));
}

#[tokio::test]
async fn dehyphenated_lattice_ingest_and_flush_preserve_both_joined_and_split_phrases() {
    let dir = tempdir("dehyphen_node");
    let index_path = dir.join("shard.idx");
    let spec = lattice_spec();

    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            index_path: Some(index_path),
            layout: Layout::Segments,
            analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
            bm25_fields: vec!["body".to_string(), "body.bigrams".to_string()],
            position_fields: vec!["body".to_string()],
            bigram_fields: vec!["body".to_string()],
            sentence_fields: vec!["body".to_string()],
            ..Default::default()
        },
    ));

    let docs = [
        "The inter-\nnational court issued a landmark judgment.",
        "Domestic inter agency tribunal report.",
    ];
    let mut link = NodeLink::local(Arc::clone(&node));
    let reqs: Vec<AddDocumentsRequest> = docs
        .into_iter()
        .map(|text| AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(spec.clone()),
            ..Default::default()
        })
        .collect();
    link.add_documents(tokio_stream::iter(reqs)).await.unwrap();
    link.flush(FlushRequest::default()).await.unwrap();

    let coord = CoordinatorServiceImpl::with_local_nodes(vec![Arc::clone(&node)])
        .with_bm25(Some(TURBO_ANALYSIS_BACKEND.to_string()), Default::default());

    // 1. Two-term exact phrase `"international court"` rides derived `body.bigrams` (`intern court`)!
    let joined_resp = SearchService::bm25_search(
        &coord,
        Request::new(Bm25SearchRequest {
            text: "international court".into(),
            k: 10,
            analysis: Some(spec.clone()),
            phrase: Some(PhraseMatch { slop: 0 }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(joined_resp.hits.len(), 1);
    assert_eq!(joined_resp.hits[0].doc_id, 0);

    // 2. Three-term split phrase `"inter national court"` rides `body` token-lattice positions!
    let split_resp = SearchService::bm25_search(
        &coord,
        Request::new(Bm25SearchRequest {
            text: "inter national court".into(),
            k: 10,
            analysis: Some(spec.clone()),
            phrase: Some(PhraseMatch { slop: 0 }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(split_resp.hits.len(), 1);
    assert_eq!(split_resp.hits[0].doc_id, 0);
    for (text, slop, expected) in [
        ("inter-\nnational court", 0, 1),
        ("inter-\nnational missing court", 0, 0),
        ("inter-\nnational judgment", 0, 0),
        ("inter-\nnational judgment", 3, 0),
        ("inter-\nnational judgment", 4, 1),
    ] {
        let response = SearchService::bm25_search(
            &coord,
            Request::new(Bm25SearchRequest {
                text: text.into(),
                k: 10,
                analysis: Some(spec.clone()),
                phrase: Some(PhraseMatch { slop }),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(response.hits.len(), expected, "{text:?}, slop={slop}");
    }
}

#[tokio::test]
async fn pre_wal_nlp_columns_materialize_and_replay_deterministically_without_re_running_nlp() {
    let dir = tempdir("nlp_wal");
    let primary_path = dir.join("primary.idx");
    let replayed_path = dir.join("replayed.idx");
    let spec = body_spec();

    let nlp_spec = NlpColumnSpec {
        money_column: "money".into(),
        quantity_column: "qty".into(),
        date_column: "doc_date".into(),
        geo_spine_column: "geo_spine".into(),
        geo_point_column: "geo_loc".into(),
        sentiment_column: "sentiment".into(),
        sentiment_label_column: "sentiment_label".into(),
        affirmed_entity_column: "affirmed_ent".into(),
        negated_entity_column: "negated_ent".into(),
        simhash_column: "simhash".into(),
    };

    let nlp_node_config = |path: PathBuf, analysis_addr: Option<String>, wal_on: bool| NodeConfig {
        index_path: Some(path),
        wal: wal_on,
        wal_buckets: 2,
        analysis_addr,
        map_numeric_fields: vec!["money".into(), "qty".into()],
        integer_fields: vec!["doc_date".into()],
        facet_fields: vec!["sentiment_label".into()],
        geo_fields: vec!["geo_loc".into()],
        numeric_fields: vec!["sentiment".into()],
        map_facet_fields: vec![
            "geo_spine".into(),
            "affirmed_ent".into(),
            "negated_ent".into(),
        ],
        unsigned_integer_fields: vec!["simhash".into()],
        ..Default::default()
    };

    // 1. Ingest with `wal: true` WITHOUT flushing so only the WAL records exist on disk!
    {
        let node = Arc::new(
            NodeServiceImpl::new(
                None,
                nlp_node_config(
                    primary_path.clone(),
                    Some(TURBO_ANALYSIS_BACKEND.to_string()),
                    true,
                ),
            )
            .with_nlp_columns(nlp_spec.clone()),
        );

        let docs = [
            "On 2024-05-15 in Berlin, Germany, Acme paid $5000 for 25 kg of batteries with excellent reliability; visit www.acme.com today.",
            "On 2023-01-10 in Paris, France, Beta did not visit www.evil.org, and instead paid $120 for 2 kg of cable.",
        ];
        let mut link = NodeLink::local(node);
        let reqs: Vec<AddDocumentsRequest> = docs
            .into_iter()
            .map(|text| AddDocumentsRequest {
                text: text.to_string(),
                analysis: Some(spec.clone()),
                ..Default::default()
            })
            .collect();
        let added = link
            .add_documents(tokio_stream::iter(reqs))
            .await
            .unwrap()
            .into_inner()
            .added;
        assert_eq!(added, 2);
    }

    // 2. Inspect raw WAL records on disk and verify pre-WAL materialization wrote all typed columns!
    let gen_dir = reshard::resolve_gen(&wal::wal_dir(&primary_path)).unwrap();
    let wal_records = wal::read_clocked_records(&gen_dir, 0).unwrap();
    let mut logged_docs: Vec<(u64, AddDocumentsRequest)> = Vec::new();
    for rec in wal_records {
        if let Some(wal_record::Op::AddDocuments(batch)) = rec.op {
            for (offset, d) in batch.documents.into_iter().enumerate() {
                logged_docs.push((batch.first_id + offset as u64, d));
            }
        }
    }
    logged_docs.sort_by_key(|(id, _)| *id);
    assert_eq!(logged_docs.len(), 2);
    assert!(logged_docs
        .iter()
        .all(|(_, d)| d.nlp_materialization.starts_with("v1:")));
    assert!(
        !logged_docs[0].1.map_numerics.is_empty()
            && !logged_docs[0].1.integers.is_empty()
            && !logged_docs[0].1.map_facets.is_empty()
            && !logged_docs[0].1.geo_points.is_empty()
            && !logged_docs[0].1.unsigned_integers.is_empty(),
        "WAL record must carry pre-materialized NLP typed columns"
    );

    // 3. Replay those exact WAL `AddDocumentsRequest` records onto a lexical-only `native` node
    //    (which refuses all NLP layers!) with the SAME `nlp_columns` configured, flush, and reopen with
    //    `analysis_addr: None`!
    {
        let replay_node = Arc::new(
            NodeServiceImpl::new(
                None,
                nlp_node_config(
                    replayed_path.clone(),
                    Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                    false,
                ),
            )
            .with_nlp_columns(nlp_spec.clone()),
        );
        let mut link = NodeLink::local(replay_node);
        let replay_reqs: Vec<AddDocumentsRequest> =
            logged_docs.into_iter().map(|(_, d)| d).collect();
        link.add_documents(tokio_stream::iter(replay_reqs))
            .await
            .unwrap();
        link.flush(FlushRequest::default()).await.unwrap();
    }

    let reopened = Arc::new(
        NodeServiceImpl::open(nlp_node_config(replayed_path, None, false), None, false).unwrap(),
    );
    let coord = CoordinatorServiceImpl::with_local_nodes(vec![reopened]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );

    let run_cel = |cel: &str| {
        let spec_clone = spec.clone();
        let cel_owned = cel.to_string();
        let coord_clone = coord.clone();
        async move {
            SearchService::query(
                &coord_clone,
                Request::new(QueryRequest {
                    selection: Some(SelectionQuery {
                        node: Some(selection_query::Node::Boolean(BooleanQuery {
                            must: vec![
                                SelectionQuery {
                                    node: Some(selection_query::Node::Search(SearchQuery {
                                        id: "lex".to_string(),
                                        query: Some(search_query::Query::Lexical(LexicalQuery {
                                            text: "paid".to_string(),
                                            analysis: Some(spec_clone),
                                            ..Default::default()
                                        })),
                                    })),
                                },
                                SelectionQuery {
                                    node: Some(selection_query::Node::Filter(FilterQuery {
                                        id: "nlp_cel".to_string(),
                                        predicate: Some(filter_query::Predicate::Cel(cel_owned)),
                                    })),
                                },
                            ],
                            ..Default::default()
                        })),
                    }),
                    k: 10,
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_inner()
        }
    };

    let r_money = run_cel("money[\"USD\"] >= 1000.0 && qty[\"kg\"] >= 20.0 && simhash > 0u").await;
    assert_eq!(r_money.hits.len(), 1);
    assert_eq!(r_money.hits[0].doc_id, 0);

    // Affirmed vs negated entity polarity isolation:
    let r_affirmed = run_cel("\"id:www.acme.com\" in affirmed_ent").await;
    assert_eq!(r_affirmed.hits.len(), 1);
    assert_eq!(r_affirmed.hits[0].doc_id, 0);

    let r_neg_isolated = run_cel("\"id:www.evil.org\" in affirmed_ent").await;
    assert!(
        r_neg_isolated.hits.is_empty(),
        "negated entity must NEVER leak into affirmed_ent column"
    );
    let r_neg = run_cel("\"id:www.evil.org\" in negated_ent").await;
    assert_eq!(r_neg.hits.len(), 1);
    assert_eq!(r_neg.hits[0].doc_id, 1);

    // Geo containment spine + document date (`2024-05-15` > `2024-01-01` = 1704067200_000_000 us)
    let r_geo_date = run_cel("\"DE\" in geo_spine && doc_date >= 1704067200000000").await;
    assert_eq!(r_geo_date.hits.len(), 1);
    assert_eq!(r_geo_date.hits[0].doc_id, 0);
}

#[tokio::test]
async fn handshake_refuses_lexical_and_neural_bundle_drift_with_failed_precondition() {
    let expected_spec = lattice_spec();
    let expected_bundle = BundleScoringFingerprint::from_spec(&BundleScoringSpec {
        model_id: "bge-small-en-v1.5".into(),
        revision: "rev-1".into(),
        manifest_sha256: "aaaa".repeat(16),
        task: "embed".into(),
        dim: 384,
        pooling: "cls".into(),
        normalize: "l2".into(),
    });
    let hs: IndexGenerationHandshake =
        turbo_handshake(Some(&expected_spec), Some(expected_bundle)).unwrap();

    let node = Arc::new(
        NodeServiceImpl::new(
            None,
            NodeConfig {
                analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
                ..Default::default()
            },
        )
        .with_turbo_handshake(hs),
    );

    // 1. Matching neural bundle passes:
    node.verify_turbo_bundle_handshake(&expected_bundle)
        .expect("matching bundle fingerprint succeeds");

    // 2. Drifted neural bundle (`dim: 768`) refuses with `FAILED_PRECONDITION`:
    let drifted_bundle = BundleScoringFingerprint::from_spec(&BundleScoringSpec {
        model_id: "bge-small-en-v1.5".into(),
        revision: "rev-2".into(),
        manifest_sha256: "bbbb".repeat(16),
        task: "embed".into(),
        dim: 768,
        pooling: "cls".into(),
        normalize: "l2".into(),
    });
    let bundle_err = node
        .verify_turbo_bundle_handshake(&drifted_bundle)
        .unwrap_err();
    assert_eq!(bundle_err.code(), Code::FailedPrecondition);

    // 3. Drifted lexical `AnalysisSpec` at ingest refuses with `FAILED_PRECONDITION`:
    let drifted_spec = pipestream_search::analyzer::cased_body_spec();
    let mut link = NodeLink::local(node);
    let ingest_err = link
        .add_documents(tokio_stream::iter(vec![AddDocumentsRequest {
            text: "Handshake drift check".into(),
            analysis: Some(drifted_spec),
            ..Default::default()
        }]))
        .await
        .unwrap_err();
    assert_eq!(ingest_err.code(), Code::FailedPrecondition);
}

#[test]
fn single_pass_multi_res_vector_output_normalizes_tiers_and_binds_sha256() {
    let raw_full: Vec<f32> = (1..=384).map(|i| (i as f32) * 0.01).collect();
    let sent_vecs = vec![vec![0.6_f32, 0.8_f32], vec![1.0_f32, 0.0_f32]];
    let out = MultiResVectorOutput::from_full_and_sentences(
        &raw_full,
        &[128, 256, 384],
        Some(vec![0xAA, 0x55, 0x0F]),
        sent_vecs.clone(),
    );

    assert_eq!(out.prefixes.len(), 3);
    for (expected_dim, (dim, slice)) in [128u32, 256, 384].into_iter().zip(&out.prefixes) {
        assert_eq!(*dim, expected_dim);
        assert_eq!(slice.len(), expected_dim as usize);
        let norm: f32 = slice.iter().map(|v| v * v).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 1e-5,
            "Matryoshka tier {expected_dim} must be unit L2 normalized, got {norm}"
        );
    }
    let full_norm: f32 = out.full.iter().map(|v| v * v).sum::<f32>().sqrt();
    assert!((full_norm - 1.0).abs() < 1e-5);
    assert!(out.inv_norm > 0.0 && out.inv_norm.is_finite());
    assert_eq!(out.sentence_vectors, sent_vecs);
    assert_ne!(out.sha256, [0u8; 32]);

    let out2 = MultiResVectorOutput::from_full_and_sentences(
        &raw_full,
        &[128, 256, 384],
        Some(vec![0xAA, 0x55, 0x0F]),
        sent_vecs,
    );
    assert_eq!(out.sha256, out2.sha256);
}

#[tokio::test]
async fn empty_nlp_results_and_mixed_replay_streams_keep_materialization_markers() {
    use pipestream_search::turbo_bridge::materialize_nlp_columns;
    use prost::Message;
    let dir = tempdir("nlp_mixed_replay");
    let mapping = NlpColumnSpec {
        money_column: "money".into(),
        ..Default::default()
    };
    let config = |name: &str| NodeConfig {
        index_path: Some(dir.join(name)),
        wal: true,
        wal_buckets: 1,
        analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
        map_numeric_fields: vec!["money".into()],
        ..Default::default()
    };
    let projection = turbo_client::OnePassDocumentNlp::builder()
        .build()
        .unwrap()
        .analyze("")
        .unwrap();
    let empty = materialize_nlp_columns(
        AddDocumentsRequest {
            text: "ordinary words".into(),
            analysis: Some(body_spec()),
            ..Default::default()
        },
        Some(&projection),
        Some(&mapping),
    )
    .unwrap();
    assert!(empty.map_numerics.is_empty());
    assert!(!empty.nlp_materialization.is_empty());
    let decoded = AddDocumentsRequest::decode(empty.encode_to_vec().as_slice()).unwrap();
    assert_eq!(
        materialize_nlp_columns(decoded, None, Some(&mapping)).unwrap(),
        empty
    );
    let drifted = NlpColumnSpec {
        money_column: "renamed_money".into(),
        ..Default::default()
    };
    assert_eq!(
        materialize_nlp_columns(empty.clone(), None, Some(&drifted))
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let mut unknown = empty.clone();
    unknown.nlp_materialization.replace_range(..2, "v2");
    assert_eq!(
        materialize_nlp_columns(unknown, None, Some(&mapping))
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    // Cross both session transitions: replay -> fresh -> replay. Money in
    // the fresh record proves the middle session really requested extraction.
    let path = config("mixed.idx").index_path.unwrap();
    {
        let node = Arc::new(
            NodeServiceImpl::new(None, config("mixed.idx")).with_nlp_columns(mapping.clone()),
        );
        let mut link = NodeLink::local(node);
        let fresh = AddDocumentsRequest {
            text: "Paid $500 for goods.".into(),
            analysis: Some(body_spec()),
            ..Default::default()
        };
        assert_eq!(
            link.add_documents(tokio_stream::iter(vec![
                empty.clone(),
                fresh,
                empty.clone()
            ]))
            .await
            .unwrap()
            .into_inner()
            .added,
            3
        );
    }
    let gen = reshard::resolve_gen(&wal::wal_dir(&path)).unwrap();
    let logged: Vec<_> = wal::read_clocked_records(&gen, 0)
        .unwrap()
        .into_iter()
        .filter_map(|r| match r.op {
            Some(wal_record::Op::AddDocuments(b)) => Some(b.documents),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(logged.len(), 3);
    assert!(logged
        .iter()
        .all(|d| d.nlp_materialization == empty.nlp_materialization));
    assert_eq!(logged[0].map_numerics, empty.map_numerics);
    assert!(!logged[1].map_numerics.is_empty());
    assert_eq!(logged[2].map_numerics, empty.map_numerics);

    // Empty extraction is still materialized: the native backend cannot
    // satisfy an NLP request, so successful replay demonstrates no reanalysis.
    let mut native_config = config("native.idx");
    native_config.analysis_addr = Some(NATIVE_ANALYSIS_BACKEND.to_string());
    let node = Arc::new(NodeServiceImpl::new(None, native_config).with_nlp_columns(mapping));
    let mut link = NodeLink::local(node);
    assert_eq!(
        link.add_documents(tokio_stream::iter(logged))
            .await
            .unwrap()
            .into_inner()
            .added,
        3
    );
}
