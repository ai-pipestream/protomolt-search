//! End-to-end `#104` query-path integration tests:
//! 1. One-pass `AnalyzeQuery` structured filter binding (`Money`, `Quantity`,
//!    `Temporal`, `Place`, `Entity` -> native CEL / `FilterExpr`) across
//!    `Lexical`, `Dense`, and `Composite` queries, plus `#94` filter-selectivity
//!    HNSW budget escalation (`FilteredAnnStrategy`).
//! 2. Multi-word query synonyms (`SynonymTable` multi-token contiguous triggers
//!    and `SynonymExpansion` reporting).
//! 3. Zero-copy `MappedSurfaceLexicon` (`TSUGLEX1`) stemmed `SearchService.Suggest`
//!    (`from_bytes` and `open_mmap`) returning genuine surface completions when
//!    stems are shorter than the typed prefix.
//! 4. Contextual bigram `QuerySpellchecker` (`SearchService.TermSuggest`).
//! 5. Quantized `u8` and `u16` SPLADE 128-doc Block-Max postings
//!    (`COLUMN_KIND_SPLADE = 19`) across `Bm25Store`, `SpillBuilder`,
//!    `Bm25Reader`, `SegmentedShard`, and multi-shard `CoordinatorServiceImpl::score_splade`
//!    with CEL filter admission and L1 rounding slack.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use common::{fit_calibration, unit_vectors};
use pipestream_search::analyzer::{body_spec, TURBO_ANALYSIS_BACKEND};
use pipestream_search::coordinator::{CoordinatorServiceImpl, MAX_SUGGEST_LIMIT};
use pipestream_search::link::NodeLink;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, selection_score_strategy, AddDocumentsRequest,
    AddVectorsRequest, Bm25SearchRequest, CompositeSearchStrategy, DenseQuery, IntegerValue,
    LexicalQuery, QueryRequest, RrfScore, SearchQuery, SelectionOperator, SelectionQuery,
    SelectionScoreStrategy, SetCalibrationRequest, SuggestRequest, SynonymRule, TermSuggestMode,
    TermSuggestRequest,
};
use pipestream_search::postings::{
    AnalyzedDoc, AnalyzedField, Bm25Index, Bm25Reader, Bm25Store, SpillBuilder,
};
use pipestream_search::segmented::SegmentedShard;
use pipestream_search::synonyms::SynonymTable;
use pipestream_search::turbo_bridge::query_plan::{
    bind_one_pass_query, select_filtered_ann_strategy, FilteredAnnStrategy, SharedSurfaceLexicon,
};
use pipestream_search::turbo_bridge::rerank::{RerankBackend, RerankSpec};
use pipestream_search::turbo_bridge::splade::quantize_splade_impacts;
use pipestream_search::turbo_bridge::NlpColumnSpec;
use tonic::Request;
use turbo_client::{
    OnePassQueryBuilder, QuerySpellchecker, SpellcheckConfig, SurfaceLexiconBuilder,
};

const DIM: usize = 32;

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "turboembed_query_{tag}_{}_{}",
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

fn full_nlp_spec() -> NlpColumnSpec {
    NlpColumnSpec {
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
        minhash_column: "minhash".into(),
    }
}

fn nlp_node_config(slot_offset: u64) -> NodeConfig {
    NodeConfig {
        slot_offset,
        analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
        sentence_fields: vec!["body".into()],
        map_numeric_fields: vec!["money".into(), "qty".into()],
        integer_fields: vec!["doc_date".into(), "tier".into()],
        facet_fields: vec!["sentiment_label".into()],
        geo_fields: vec!["geo_loc".into()],
        numeric_fields: vec!["sentiment".into()],
        map_facet_fields: vec![
            "geo_spine".into(),
            "affirmed_ent".into(),
            "negated_ent".into(),
        ],
        unsigned_integer_fields: vec!["simhash".into()],
        map_unsigned_integer_fields: vec!["minhash".into()],
        ..Default::default()
    }
}

#[test]
fn filtered_ann_strategy_escalates_budget_and_switches_to_prefiltered_exact_at_low_selectivity() {
    // Unfiltered (~100% selectivity): keeps base ef_search.
    assert_eq!(
        select_filtered_ann_strategy(64, 10, 50_000, 1.0),
        FilteredAnnStrategy::Graph { ef_search: 64 }
    );

    // Moderate selectivity (10% of 50,000 = 5,000 survivors): escalates ef_search ~10x -> 640.
    assert_eq!(
        select_filtered_ann_strategy(64, 10, 50_000, 0.10),
        FilteredAnnStrategy::Graph { ef_search: 640 }
    );

    // High selectivity / tiny survivor count (0.2% of 50,000 = 100 survivors): prefiltered exact scan!
    assert_eq!(
        select_filtered_ann_strategy(64, 10, 50_000, 0.002),
        FilteredAnnStrategy::PrefilteredExact
    );
}

#[tokio::test]
async fn one_pass_analyze_query_binds_structured_filters_across_lexical_dense_and_composite() {
    let nlp_spec = full_nlp_spec();
    let analyzer = OnePassQueryBuilder::new().build().unwrap();

    // Direct unit check on `bind_one_pass_query` across Money + Quantity + Year + Place + Entity + caller CEL.
    let plan = analyzer
        .analyze("lithium batteries $5000 25 kg in 2024 Berlin ops@acme.example")
        .unwrap();
    let bound = bind_one_pass_query(&plan, &nlp_spec, Some("tier == 1")).unwrap();
    assert!(
        bound.synthesized_cel.contains("money[\"USD\"] == 5000.0"),
        "synthesized CEL: {}",
        bound.synthesized_cel
    );
    assert!(
        bound.synthesized_cel.contains("qty[\"kg\"] == 25.0"),
        "synthesized CEL: {}",
        bound.synthesized_cel
    );
    assert!(
        bound.synthesized_cel.contains("doc_date >="),
        "synthesized CEL: {}",
        bound.synthesized_cel
    );
    assert!(
        bound.synthesized_cel.contains("in geo_spine"),
        "synthesized CEL: {}",
        bound.synthesized_cel
    );
    assert!(bound.filter_expr.is_some());
    assert!(bound.estimated_selectivity < 0.25);

    // End-to-end 2-shard hybrid cluster with pre-WAL NLP columns + 4-bit calibrated vectors.
    let docs = [
        // Doc 0: matches $5000, 25 kg, 2024, Berlin
        "On 2024-05-15 in Berlin, Germany, Acme purchased lithium batteries for $5000 weighing 25 kg.",
        // Doc 1: same lexical words ("lithium batteries"), wrong price ($1200) and wrong weight (5 kg)
        "On 2024-05-15 in Berlin, Germany, Beta purchased lithium batteries for $1200 weighing 5 kg.",
        // Doc 2: matches $5000 and 25 kg, wrong year (2021) and city (Paris)
        "On 2021-03-10 in Paris, France, Gamma purchased lithium batteries for $5000 weighing 25 kg.",
        // Doc 3: unrelated text, vector closest to query vector if unfiltered!
        "Unrelated industrial transformer maintenance manual.",
    ];

    let corpus_vecs = unit_vectors(4, DIM, 0x1040_BEEF);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus_vecs);

    let mut nodes = Vec::new();
    for shard in 0..2usize {
        let node = Arc::new(
            NodeServiceImpl::new(None, nlp_node_config((shard * 2) as u64))
                .with_nlp_columns(nlp_spec.clone()),
        );
        let mut link = NodeLink::local(Arc::clone(&node));
        link.set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.clone(),
            scale: scale.clone(),
        })
        .await
        .unwrap();

        let doc_reqs: Vec<AddDocumentsRequest> = (0..2usize)
            .map(|local| {
                let id = shard * 2 + local;
                AddDocumentsRequest {
                    text: docs[id].to_string(),
                    analysis: Some(body_spec()),
                    integers: vec![IntegerValue {
                        field: "tier".into(),
                        value: 1,
                    }],
                    ..Default::default()
                }
            })
            .collect();
        link.add_documents(tokio_stream::iter(doc_reqs))
            .await
            .unwrap();

        let start = shard * 2 * DIM;
        link.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
            vectors: corpus_vecs[start..start + 2 * DIM].to_vec(),
            dim: DIM as u32,
        }]))
        .await
        .unwrap();

        nodes.push(node);
    }

    let coord = CoordinatorServiceImpl::with_local_nodes(nodes)
        .with_bm25(Some(TURBO_ANALYSIS_BACKEND.to_string()), Default::default())
        .with_one_pass_query(OnePassQueryBuilder::new().build().unwrap(), nlp_spec);

    // 1. Lexical shape: inline `$5000 25 kg in 2024` filters out Docs 1 & 2 even though all three
    //    contain `"lithium batteries"`, leaving ONLY Doc 0!
    let lex_resp = SearchService::query(
        &coord,
        Request::new(QueryRequest {
            k: 10,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "lex".into(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "lithium batteries $5000 25 kg in 2024".into(),
                        analysis: Some(body_spec()),
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        lex_resp.hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        vec![0]
    );

    // 2. Composite (Dense + Lexical RRF) shape: even with `SelectionOperator::Or` where Doc 3's
    //    exact vector is queried, the synthesized one-pass CEL filter gates the entire selection
    //    so only Doc 0 survives!
    let doc3_vec = corpus_vecs[3 * DIM..4 * DIM].to_vec();
    let comp_resp = SearchService::query(
        &coord,
        Request::new(QueryRequest {
            k: 10,
            selection_k: 10,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                    operator: SelectionOperator::Or as i32,
                    clauses: vec![
                        SelectionQuery {
                            node: Some(selection_query::Node::Search(SearchQuery {
                                id: "vec".into(),
                                query: Some(search_query::Query::Dense(DenseQuery {
                                    vector: doc3_vec.clone(),
                                    ..Default::default()
                                })),
                            })),
                        },
                        SelectionQuery {
                            node: Some(selection_query::Node::Search(SearchQuery {
                                id: "lex".into(),
                                query: Some(search_query::Query::Lexical(LexicalQuery {
                                    text: "lithium batteries $5000 25 kg in 2024".into(),
                                    analysis: Some(body_spec()),
                                    ..Default::default()
                                })),
                            })),
                        },
                    ],
                    scoring: Some(SelectionScoreStrategy {
                        strategy: Some(
                            selection_score_strategy::Strategy::Rrf(RrfScore::default()),
                        ),
                    }),
                })),
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        comp_resp.hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        vec![0]
    );

    // 3. Pure Dense shape with `RerankSpec::query_text_override`: binds inline `$5000 25 kg in 2024`
    //    before vector scan so Doc 3 (exact vector match!) and Docs 1..2 are filtered out!
    let mut rerank_spec = RerankSpec::new(RerankBackend::DeterministicCalibrated {
        platt_slope: 2.0,
        platt_intercept: -0.5,
    });
    rerank_spec.query_text_override = Some("lithium batteries $5000 25 kg in 2024".into());
    let dense_coord = coord.with_rerank_spec(rerank_spec);
    let dense_resp = SearchService::query(
        &dense_coord,
        Request::new(QueryRequest {
            k: 10,
            selection_k: 10,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "vec".into(),
                    query: Some(search_query::Query::Dense(DenseQuery {
                        vector: doc3_vec,
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        dense_resp.hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        vec![0]
    );
}

#[tokio::test]
async fn multi_word_query_synonyms_expand_contiguous_phrases_without_false_single_word_triggers() {
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
            ..Default::default()
        },
    ));

    let docs = [
        "NYC municipal bond issuance report",
        "New Jersey state highway budget",
        "High tribunal constitutional opinion",
    ];
    let mut link = NodeLink::local(Arc::clone(&node));
    link.add_documents(tokio_stream::iter(docs.into_iter().map(|text| {
        AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(body_spec()),
            ..Default::default()
        }
    })))
    .await
    .unwrap();

    let table = SynonymTable::from_rules(vec![
        // Symmetric multi-word <-> abbreviation rule
        SynonymRule {
            terms: vec!["new york city".into(), "nyc".into()],
            to: Vec::new(),
        },
        // One-way multi-word rule
        SynonymRule {
            terms: vec!["supreme court".into()],
            to: vec!["high tribunal".into()],
        },
    ])
    .unwrap();

    let coord = CoordinatorServiceImpl::with_local_nodes(vec![node])
        .with_bm25(Some(TURBO_ANALYSIS_BACKEND.to_string()), Default::default())
        .with_synonyms(table);

    // 1. Querying `"new york city"` matches the 3-token sequence and expands to `"nyc"`,
    //    retrieving Doc 0 (`"NYC municipal bond issuance report"`).
    let nyc_resp = SearchService::bm25_search(
        &coord,
        Request::new(Bm25SearchRequest {
            text: "new york city".into(),
            k: 10,
            analysis: Some(body_spec()),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let nyc_ids: BTreeSet<u64> = nyc_resp.hits.iter().map(|h| h.doc_id).collect();
    assert!(
        nyc_ids.contains(&0),
        "expected Doc 0 (NYC), got {nyc_ids:?}"
    );
    assert_eq!(nyc_resp.synonym_expansions.len(), 1);
    assert_eq!(
        nyc_resp.synonym_expansions[0].terms,
        vec!["nyc".to_string()]
    );

    // 2. Querying `"new jersey"` shares `"new"` with `"new york city"` but MUST NOT trigger `"nyc"`!
    let nj_resp = SearchService::bm25_search(
        &coord,
        Request::new(Bm25SearchRequest {
            text: "new jersey".into(),
            k: 10,
            analysis: Some(body_spec()),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(nj_resp.synonym_expansions.is_empty());
    assert_eq!(
        nj_resp.hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        vec![1]
    );

    // 3. One-way multi-word rule `"supreme court"` -> `"high tribunal"` retrieves Doc 2,
    //    while `"high tribunal"` does NOT expand backward.
    let sc_resp = SearchService::bm25_search(
        &coord,
        Request::new(Bm25SearchRequest {
            text: "supreme court".into(),
            k: 10,
            analysis: Some(body_spec()),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        sc_resp.hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(sc_resp.synonym_expansions.len(), 1);

    let reverse_resp = SearchService::bm25_search(
        &coord,
        Request::new(Bm25SearchRequest {
            text: "high tribunal".into(),
            k: 10,
            analysis: Some(body_spec()),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(reverse_resp.synonym_expansions.is_empty());
}

#[tokio::test]
async fn mapped_surface_lexicon_and_contextual_spellchecker_serve_suggest_and_term_suggest() {
    let dir = tempdir("lexicon_spell");
    let corpus = [
        "International arbitration treaties govern international trade disputes.",
        "Internationally recognized supreme court precedent governs constitutional appeals.",
        "The supreme court heard arguments on international maritime jurisdiction.",
        "Supreme court justices reviewed appellate jurisdiction.",
    ];

    // 2-shard cluster where `body` is Porter-stemmed (`"international"` -> `"intern"`!).
    let mut nodes = Vec::new();
    for shard in 0..2usize {
        let node = Arc::new(NodeServiceImpl::new(
            None,
            NodeConfig {
                slot_offset: (shard * 2) as u64,
                analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
                ..Default::default()
            },
        ));
        let mut link = NodeLink::local(Arc::clone(&node));
        let reqs: Vec<AddDocumentsRequest> = corpus[shard * 2..(shard + 1) * 2]
            .iter()
            .map(|text| AddDocumentsRequest {
                text: (*text).to_string(),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .collect();
        link.add_documents(tokio_stream::iter(reqs)).await.unwrap();
        nodes.push(node);
    }

    // Build `TSUGLEX1` surface lexicon from observed `(surface, stem, df)` pairs.
    let mut lex_builder = SurfaceLexiconBuilder::new();
    lex_builder.observe("international", "intern", 2);
    lex_builder.observe("internationally", "intern", 1);
    lex_builder.observe("supreme", "suprem", 3);
    lex_builder.observe("court", "court", 3);
    let lex_bytes = lex_builder.build_bytes().unwrap();

    // Verify both `SharedSurfaceLexicon::from_bytes` AND zero-copy `SharedSurfaceLexicon::open_mmap`.
    let lex_from_bytes = SharedSurfaceLexicon::from_bytes(&lex_bytes).unwrap();
    assert!(!lex_from_bytes
        .view()
        .suggest_prefix("internationa", 8)
        .is_empty());

    let lex_path = dir.join("surface.tsuglex");
    std::fs::write(&lex_path, &lex_bytes).unwrap();
    let mmap_lex = Arc::new(SharedSurfaceLexicon::open_mmap(&lex_path).unwrap());

    // Build contextual bigram `QuerySpellchecker` from the corpus.
    let spellchecker = Arc::new(QuerySpellchecker::from_documents(
        &corpus,
        SpellcheckConfig::default(),
    ));

    let coord = CoordinatorServiceImpl::with_local_nodes(nodes)
        .with_bm25(Some(TURBO_ANALYSIS_BACKEND.to_string()), Default::default())
        .with_surface_lexicon(mmap_lex)
        .with_spellchecker(spellchecker);

    // 1. Stemmed `SearchService.Suggest`: typing `"internationa"` (longer than Porter stem `"intern"`)
    //    returns `"international"` and `"internationally"` instead of empty results!
    let sug = SearchService::suggest(
        &coord,
        Request::new(SuggestRequest {
            collection: String::new(),
            field: "body".into(),
            prefix: "Internationa".into(),
            limit: MAX_SUGGEST_LIMIT as u32,
            max_scan: 0,
            analysis: Some(body_spec()),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let sug_terms: Vec<&str> = sug.suggestions.iter().map(|s| s.term.as_str()).collect();
    assert_eq!(sug_terms, vec!["international", "internationally"]);
    assert_eq!(sug.suggestions[0].df, 2);

    // 2. Contextual `SearchService.TermSuggest`: `"supreme cort"` corrects `"cort"` -> `"court"`
    //    using token-bigram context (`"supreme court"`).
    let term_sug = SearchService::term_suggest(
        &coord,
        Request::new(TermSuggestRequest {
            collection: String::new(),
            field: "body".into(),
            text: "supreme cort".into(),
            mode: TermSuggestMode::Missing as i32,
            max_edits: 2,
            limit: 5,
            prefix_length: 1,
            max_scan: 0,
            analysis: Some(body_spec()),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(term_sug.terms.len(), 2);
    let cort_sug = term_sug
        .terms
        .iter()
        .find(|t| t.term == "cort")
        .expect("cort suggestion entry");
    assert_eq!(cort_sug.candidates[0].term, "court");
}

#[tokio::test]
async fn quantized_u8_and_u16_splade_128_doc_block_max_matches_exhaustive_oracle_and_skips_blocks()
{
    let dir = tempdir("splade_block_max");
    let n_docs = 420usize; // > 3 full 128-doc blocks (4 blocks per high-df term!)

    for bits in [8u8, 16u8] {
        // Fix a uniform positive reference weight (`2.55`) per doc so `scale` is identical across
        // all documents on a shard, letting us compare against an exact quantized oracle.
        let mut docs: Vec<AnalyzedDoc> = Vec::with_capacity(n_docs);
        let mut oracle_scale = 0.0_f32;
        let mut oracle_postings: Vec<BTreeMap<String, u16>> = Vec::with_capacity(n_docs);

        for d in 0..n_docs {
            // Spike 24 high-weight documents (12 even + 12 odd) in Block 0 (`d = 10..34`) so
            // top-10 threshold saturates in Block 0 and 128-doc Block-Max skips Blocks 1..3!
            let rare_w = if (10..34).contains(&d) {
                2.50 - (d - 10) as f32 * 0.03
            } else if d % 7 == 0 {
                0.15
            } else {
                0.04
            };
            let bg_w = 0.02 + ((d % 11) as f32) * 0.01;
            let raw = vec![
                ("__anchor".to_string(), 2.55_f32),
                ("neural_target".to_string(), rare_w),
                ("background_noise".to_string(), bg_w),
                (format!("tok_{}", d % 17), 0.12_f32),
            ];
            let (scale, quantized, l1_bound) = quantize_splade_impacts(&raw, 16, bits);
            assert!(scale > 0.0 && l1_bound >= 0.0);
            if d == 0 {
                oracle_scale = scale;
            } else {
                assert!((scale - oracle_scale).abs() < 1e-7);
            }
            oracle_postings.push(quantized.iter().cloned().collect());
            docs.push(AnalyzedDoc {
                fields: vec![AnalyzedField {
                    terms: vec![("doc".into(), 1, Vec::new())],
                    length: 1,
                    splade: quantized,
                    splade_scale_bits: scale.to_bits(),
                    ..Default::default()
                }],
                ..Default::default()
            });
        }

        let query_weights = vec![
            ("neural_target".to_string(), 1.25_f32),
            ("background_noise".to_string(), 0.20_f32),
            ("tok_3".to_string(), 0.35_f32),
        ];

        // Compute exact exhaustive quantized dot-product top-10 oracle (for unfiltered and filtered `d % 2 == 0`).
        let exhaustive_top_k = |even_only: bool, k: usize| -> Vec<(u64, f32)> {
            let mut all: Vec<(u64, f32)> = (0..n_docs)
                .filter(|&d| !even_only || d.is_multiple_of(2))
                .filter_map(|d| {
                    let map = &oracle_postings[d];
                    let mut acc = 0.0_f32;
                    for (qt, qw) in &query_weights {
                        if let Some(&q_imp) = map.get(qt) {
                            acc += *qw * (f32::from(q_imp) * oracle_scale);
                        }
                    }
                    (acc > 0.0).then_some((d as u64, acc))
                })
                .collect();
            all.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            all.truncate(k);
            all
        };

        let expected_unfiltered = exhaustive_top_k(false, 10);
        let expected_even = exhaustive_top_k(true, 10);

        // 1. Verify `Bm25Store` -> `.bm25` `COLUMN_KIND_SPLADE = 19` -> `Bm25Reader` zero-copy mmap
        //    AND verify `SpladeTraversalStats.blocks_skipped > 0`!
        let mut store = Bm25Store::new();
        for (d, doc) in docs.iter().enumerate() {
            store.add_document(d as u32, format!("doc {d}"), doc.clone());
        }
        let store_path = dir.join(format!("store_u{bits}.bm25"));
        store.save(&store_path).unwrap();
        let reader = Bm25Reader::open(&store_path).unwrap();
        let (mmap_hits, stats) = reader
            .field(0)
            .score_splade(&query_weights, 0.001, 10, &|_| true);
        assert!(
            stats.total_blocks >= 8 && stats.blocks_skipped > 0,
            "bits={bits}: expected 128-doc Block-Max to skip low-impact blocks, got {stats:?}"
        );
        let mmap_ids: Vec<u64> = mmap_hits.iter().map(|&(d, _)| u64::from(d)).collect();
        let exp_ids: Vec<u64> = expected_unfiltered.iter().map(|&(d, _)| d).collect();
        assert_eq!(mmap_ids, exp_ids, "bits={bits} mmap top-10 IDs mismatch");
        for ((_, got_s), (_, exp_s)) in mmap_hits.iter().zip(&expected_unfiltered) {
            assert!(
                (got_s - exp_s).abs() < 1e-5,
                "got {got_s} vs expected {exp_s}"
            );
        }

        // 2. Verify `SpillBuilder` external merge + `Bm25Reader::transpose` preserve SPLADE postings!
        let spill_dir = dir.join(format!("spill_work_u{bits}"));
        let spill_path = dir.join(format!("spill_u{bits}.bm25"));
        let mut spill = SpillBuilder::create(&spill_dir)
            .unwrap()
            .with_buffer_bytes(2048);
        for (d, doc) in docs.iter().enumerate() {
            spill
                .add_document_with_lineage(d as u32, format!("doc {d}"), doc.clone(), None)
                .unwrap();
        }
        spill.finish(&spill_path).unwrap();
        let spill_reader = Bm25Reader::open(&spill_path).unwrap();
        let (spill_hits, _) = spill_reader
            .field(0)
            .score_splade(&query_weights, 0.0, 10, &|_| true);
        assert_eq!(
            spill_hits
                .iter()
                .map(|&(d, _)| u64::from(d))
                .collect::<Vec<_>>(),
            exp_ids
        );
        let transposed_doc = spill_reader
            .field(0)
            .transpose()
            .unwrap()
            .field(300)
            .unwrap();
        assert_eq!(transposed_doc.splade, docs[300].fields[0].splade);

        // 3. Verify `SegmentedShard` + 2-shard `CoordinatorServiceImpl::score_splade`!
        let seg_dir = dir.join(format!("seg_u{bits}"));
        let mut seg = SegmentedShard::open(&seg_dir, Bm25Store::new()).unwrap();
        for (d, doc) in docs.iter().enumerate() {
            seg.add_document(d as u32, format!("doc {d}"), doc.clone(), None)
                .unwrap();
        }
        let (seg_hits, _) = seg
            .field(0)
            .score_splade(&query_weights, 0.0, 10, &|_| true);
        assert_eq!(
            seg_hits
                .iter()
                .map(|&(d, _)| u64::from(d))
                .collect::<Vec<_>>(),
            exp_ids
        );

        let split = n_docs / 2;
        let mut shard_nodes = Vec::new();
        for (shard_idx, chunk) in docs.chunks(split).enumerate() {
            let mut shard_store = Bm25Store::new();
            for (local, doc) in chunk.iter().enumerate() {
                shard_store.add_document(local as u32, format!("doc {local}"), doc.clone());
            }
            let idx_base = dir.join(format!("s{shard_idx}_{bits}.idx"));
            let sidecar_path = pipestream_search::node::bm25_sidecar_path(&idx_base);
            shard_store.save(&sidecar_path).unwrap();
            let opened = Arc::new(
                NodeServiceImpl::open(
                    NodeConfig {
                        index_path: Some(idx_base),
                        slot_offset: (shard_idx * split) as u64,
                        ..Default::default()
                    },
                    None,
                    true,
                )
                .unwrap(),
            );
            shard_nodes.push(opened);
        }

        let coord = CoordinatorServiceImpl::with_local_nodes(shard_nodes);
        let fleet_hits = coord
            .score_splade("body", &query_weights, 0.001, 10, "")
            .await
            .unwrap();
        assert_eq!(
            fleet_hits.iter().map(|&(d, _)| d).collect::<Vec<_>>(),
            exp_ids,
            "bits={bits} 2-shard coordinator SPLADE top-10 mismatch"
        );

        // Also verify filtered admission on `Bm25Reader::score_splade` (`d % 2 == 0`) matches `expected_even`!
        let (even_hits, _) = reader
            .field(0)
            .score_splade(&query_weights, 0.001, 10, &|d| {
                (d as usize).is_multiple_of(2)
            });
        assert_eq!(
            even_hits
                .iter()
                .map(|&(d, _)| u64::from(d))
                .collect::<Vec<_>>(),
            expected_even.iter().map(|&(d, _)| d).collect::<Vec<_>>()
        );
    }
}
