//! End-to-end hardware benchmark for Epic `#102` (`#103`, `#104`, `#105`).
//!
//! Measures single-host release performance across all three unification pillars:
//! 1. `#103`: One-pass `turbo` lexical lattice + NLP extraction + pre-WAL typed
//!    column materialization (`analyze_document_with_options` + `materialize_nlp_columns`),
//!    and `(pos, pos_len)` token-lattice phrase evaluation over a mapped `.bm25` shard.
//! 2. `#104`: One-pass `AnalyzeQuery` structured filter binding (`OnePassQueryBuilder` +
//!    `bind_one_pass_query`), zero-copy `MappedSurfaceLexicon` (`TSUGLEX1`) stemmed
//!    prefix completion, and quantized `u8` / `u16` 128-doc Block-Max SPLADE traversal
//!    (`COLUMN_KIND_SPLADE = 19`) with measured block-skip rates.
//! 3. `#105`: Stage-2 calibrated `RerankAndHighlight` shortlist scoring (`C = 50 -> k = 10`)
//!    with `DocLineage.span_start` parent-document UTF-8 byte offset composition.
//!
//! ```text
//! cargo run --release --example turboembed_bench -- --docs=2000 --splade-docs=20000 --queries=200
//! ```

use std::path::PathBuf;
use std::time::Instant;

use pipestream_search::analyzer::{
    analyze_document_with_options, RemoteCallOptions, SessionLayers, CHAR_FILTER_ACCENT_FOLD,
    CHAR_FILTER_DEHYPHENATE, CHAR_FILTER_FULL_CASE_FOLD, CHAR_FILTER_STRIP_INVISIBLE,
    CHAR_FILTER_WHITESPACE, SOURCE_NORMALIZED_STEMS, STEMMER_PORTER, TERM_VECTOR_MODE_FULL,
    TOKENIZER_UAX29, TURBO_ANALYSIS_BACKEND,
};
use pipestream_search::pb::{AddDocumentsRequest, AnalysisSpec};
use pipestream_search::postings::{AnalyzedDoc, AnalyzedField, Bm25Index, Bm25Reader, Bm25Store};
use pipestream_search::proximity;
use pipestream_search::turbo_bridge::query_plan::{bind_one_pass_query, SharedSurfaceLexicon};
use pipestream_search::turbo_bridge::rerank::{
    rerank_and_highlight_shortlist, RerankBackend, RerankCandidate, RerankSpec,
};
use pipestream_search::turbo_bridge::splade::quantize_splade_impacts;
use pipestream_search::turbo_bridge::{materialize_nlp_columns, NlpColumnSpec};
use turbo_client::{OnePassQueryBuilder, SurfaceLexiconBuilder};

fn arg_usize(key: &str, default: usize) -> usize {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).and_then(|v| v.parse().ok()))
        .unwrap_or(default)
}

fn quantiles_us(samples_us: &mut [f64]) -> (f64, f64, f64) {
    if samples_us.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    samples_us.sort_by(|a, b| a.total_cmp(b));
    let n = samples_us.len();
    let p50 = samples_us[(n - 1) * 50 / 100];
    let p95 = samples_us[(n - 1) * 95 / 100];
    let p99 = samples_us[(n - 1) * 99 / 100];
    (p50, p95, p99)
}

fn bench_tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "psearch_turboembed_bench_{}_{}",
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

fn nlp_spec() -> NlpColumnSpec {
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
    }
}

#[tokio::main]
async fn main() {
    let n_docs = arg_usize("docs", 2_000);
    let splade_docs = arg_usize("splade-docs", 20_000);
    let n_queries = arg_usize("queries", 200);
    let dir = bench_tempdir();

    println!(
        "=== turboembed_bench (arch={}, docs={n_docs}, splade_docs={splade_docs}, queries={n_queries}) ===",
        std::env::consts::ARCH
    );

    // -----------------------------------------------------------------------
    // 1. #103: One-pass in-process `turbo` ingest + pre-WAL NLP materialization
    //    + mapped `(pos, pos_len)` lattice phrase evaluation
    // -----------------------------------------------------------------------
    let spec = lattice_spec();
    let cols = nlp_spec();
    let layers = SessionLayers {
        sentences: true,
        dual_cased: true,
        nlp_columns: true,
        ..Default::default()
    };
    let cities = [
        "Berlin, Germany",
        "Paris, France",
        "London, United Kingdom",
        "Madrid, Spain",
    ];
    let corpus: Vec<String> = (0..n_docs)
        .map(|i| {
            let city = cities[i % cities.len()];
            let price = 500 + (i % 40) * 250;
            let kg = 5 + (i % 50);
            format!(
                "On 2024-05-{:02} in {city}, the inter-\nnational procurement board approved ${price} \
                 for {kg} kg of lithium-ion storage modules with reliable thermal safety; \
                 contact procurement-{i}@acme.example rather than legacy@vendor.example.",
                1 + (i % 28)
            )
        })
        .collect();

    // Warm up shared analyzer caches once before timing.
    let _ = analyze_document_with_options(
        TURBO_ANALYSIS_BACKEND,
        &corpus[0],
        Some(&spec),
        layers,
        &RemoteCallOptions::default(),
    )
    .await
    .unwrap();

    let mut ingest_us = Vec::with_capacity(n_docs);
    let mut store = Bm25Store::new()
        .with_positions(&["body"])
        .with_sentences(&["body"]);
    let ingest_wall = Instant::now();
    for (i, text) in corpus.iter().enumerate() {
        let t0 = Instant::now();
        let analyzed = analyze_document_with_options(
            TURBO_ANALYSIS_BACKEND,
            text,
            Some(&spec),
            layers,
            &RemoteCallOptions::default(),
        )
        .await
        .unwrap();
        let materialized = materialize_nlp_columns(
            AddDocumentsRequest {
                text: text.clone(),
                ..Default::default()
            },
            analyzed.nlp.as_deref(),
            Some(&cols),
        )
        .unwrap();
        std::hint::black_box(&materialized);
        ingest_us.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
        store.add_document(i as u32, text.clone(), analyzed);
    }
    let ingest_secs = ingest_wall.elapsed().as_secs_f64();
    let (ing_p50, ing_p95, ing_p99) = quantiles_us(&mut ingest_us);
    println!(
        "[#103 ingest+nlp] {:.1} docs/s ({:.3} s total) | p50={:.1} us  p95={:.1} us  p99={:.1} us",
        n_docs as f64 / ingest_secs,
        ingest_secs,
        ing_p50,
        ing_p95,
        ing_p99
    );

    let lattice_path = dir.join("lattice.bm25");
    store.save(&lattice_path).unwrap();
    let lattice_reader = Bm25Reader::open(&lattice_path).unwrap();
    let lattice_view = lattice_reader.field(0);
    let mut lattice_us = Vec::with_capacity(n_queries);
    let joined_terms = ["intern".to_string(), "procur".to_string()];
    let split_terms = [
        "inter".to_string(),
        "nation".to_string(),
        "procur".to_string(),
    ];
    for q in 0..n_queries {
        let doc_slot = (q % n_docs) as u32;
        let t0 = Instant::now();
        let ok_joined =
            proximity::phrase_matches(&lattice_view, &joined_terms, &[0, 1], doc_slot, 0);
        let ok_split =
            proximity::phrase_matches(&lattice_view, &split_terms, &[0, 1, 2], doc_slot, 0);
        assert!(ok_joined && ok_split);
        lattice_us.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let (lat_p50, lat_p95, lat_p99) = quantiles_us(&mut lattice_us);
    println!(
        "[#103 pos_len lattice] joined+split phrase verify | p50={:.2} us  p95={:.2} us  p99={:.2} us",
        lat_p50, lat_p95, lat_p99
    );

    // -----------------------------------------------------------------------
    // 2. #104: One-pass `AnalyzeQuery`, `MappedSurfaceLexicon`, and 128-doc
    //    Block-Max SPLADE (`u8` and `u16`)
    // -----------------------------------------------------------------------
    let one_pass = OnePassQueryBuilder::new().build().unwrap();
    let mut qp_us = Vec::with_capacity(n_queries);
    for _ in 0..n_queries {
        let t0 = Instant::now();
        let plan = one_pass
            .analyze("lithium storage modules $5000 25 kg in 2024 Berlin ops@acme.example")
            .unwrap();
        let bound = bind_one_pass_query(&plan, &cols, Some("simhash > 0u")).unwrap();
        std::hint::black_box(&bound);
        qp_us.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let (qp_p50, qp_p95, qp_p99) = quantiles_us(&mut qp_us);
    println!(
        "[#104 one-pass query+CEL] | p50={:.1} us  p95={:.1} us  p99={:.1} us",
        qp_p50, qp_p95, qp_p99
    );

    let mut lex_builder = SurfaceLexiconBuilder::new();
    for i in 0..5_000u64 {
        lex_builder.observe(&format!("international_{i:04}"), "intern", 100 + (i % 50));
        lex_builder.observe(&format!("interagency_{i:04}"), "interag", 50 + (i % 25));
    }
    let lex_bytes = lex_builder.build_bytes().unwrap();
    let shared_lex = SharedSurfaceLexicon::from_bytes(&lex_bytes).unwrap();
    let lex_view = shared_lex.view();
    let mut sug_us = Vec::with_capacity(n_queries);
    for _ in 0..n_queries {
        let t0 = Instant::now();
        let hits = lex_view.suggest_prefix("internationa", 10);
        assert_eq!(hits.len(), 10);
        sug_us.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let (sug_p50, sug_p95, sug_p99) = quantiles_us(&mut sug_us);
    println!(
        "[#104 TSUGLEX1 suggest] 10k surfaces mmap | p50={:.2} us  p95={:.2} us  p99={:.2} us",
        sug_p50, sug_p95, sug_p99
    );

    for bits in [8u8, 16u8] {
        let mut splade_store = Bm25Store::new();
        for d in 0..splade_docs {
            let rare_signal = if d % 257 == 3 {
                2.40 - ((d / 257) % 10) as f32 * 0.05
            } else if d % 19 == 0 {
                0.18
            } else {
                0.03
            };
            let mut raw = Vec::with_capacity(10);
            raw.push(("__anchor".to_string(), 2.55_f32));
            raw.push((format!("topic_{}", d % 8), rare_signal));
            for j in 0..8usize {
                let term = format!("bg_{}", (d + j * 13) % 64);
                let weight = 0.02 + (((d + j) % 9) as f32) * 0.015;
                raw.push((term, weight));
            }
            let (scale, quantized, _) = quantize_splade_impacts(&raw, 16, bits);
            splade_store.add_document(
                d as u32,
                String::new(),
                AnalyzedDoc {
                    fields: vec![AnalyzedField {
                        terms: vec![("d".into(), 1, Vec::new())],
                        length: 1,
                        splade: quantized,
                        splade_scale_bits: scale.to_bits(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            );
        }
        let splade_path = dir.join(format!("splade_u{bits}.bm25"));
        splade_store.save(&splade_path).unwrap();
        let splade_reader = Bm25Reader::open(&splade_path).unwrap();
        let splade_view = splade_reader.field(0);

        let mut sp_us = Vec::with_capacity(n_queries);
        let mut total_blocks = 0u64;
        let mut skipped_blocks = 0u64;
        for q in 0..n_queries {
            let mut q_terms = Vec::with_capacity(12);
            q_terms.push((format!("topic_{}", q % 8), 1.35_f32));
            for j in 0..11usize {
                q_terms.push((
                    format!("bg_{}", (q * 5 + j * 7) % 64),
                    0.08 + (j as f32) * 0.01,
                ));
            }
            let t0 = Instant::now();
            let (hits, stats) = splade_view.score_splade(&q_terms, 0.002, 10, &|_| true);
            assert_eq!(hits.len(), 10);
            sp_us.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
            total_blocks += stats.total_blocks;
            skipped_blocks += stats.blocks_skipped;
        }
        let (sp_p50, sp_p95, sp_p99) = quantiles_us(&mut sp_us);
        let skip_pct = if total_blocks > 0 {
            100.0 * skipped_blocks as f64 / total_blocks as f64
        } else {
            0.0
        };
        println!(
            "[#104 SPLADE u{bits:<2} Block-Max] {splade_docs} docs, 12-term query -> top-10 | p50={:.1} us  p95={:.1} us  p99={:.1} us | blocks skipped={}/{} ({:.1}%)",
            sp_p50, sp_p95, sp_p99, skipped_blocks, total_blocks, skip_pct
        );
    }

    // -----------------------------------------------------------------------
    // 3. #105: Stage-2 calibrated `RerankAndHighlight` (`C = 50 -> k = 10`)
    // -----------------------------------------------------------------------
    let shortlist: Vec<RerankCandidate> = (0..50u64)
        .map(|i| {
            let s0 = " introductory procedural background regarding commercial supply contracts. ";
            let s1 = format!(
                "Clause {} establishes calibrated lithium-ion battery warranty remedies and thermal inspection schedules.",
                i + 1
            );
            let s2 = " Boilerplate governing venue and severability provisions.";
            let text = format!("{s0}{s1}{s2}");
            let b0 = s0.len() as u32;
            let b1 = b0 + s1.len() as u32;
            let b2 = text.len() as u32;
            RerankCandidate {
                doc_id: i,
                prior_score: 0.9 - (i as f64) * 0.01,
                text,
                sentence_spans: vec![(0, b0), (b0, b1), (b1, b2)],
                lineage_span_start: (i as u32) * 4096,
            }
        })
        .collect();
    let mut rerank_spec = RerankSpec::new(RerankBackend::DeterministicCalibrated {
        platt_slope: 1.8,
        platt_intercept: -0.35,
    });
    rerank_spec.shortlist_size = 50;
    rerank_spec.highlight_top_k = 10;
    rerank_spec.max_snippets_per_candidate = 2;

    let mut rr_us = Vec::with_capacity(n_queries);
    for _ in 0..n_queries {
        let t0 = Instant::now();
        let out = rerank_and_highlight_shortlist(
            &rerank_spec,
            "lithium battery warranty thermal inspection",
            &shortlist,
            0,
        )
        .await
        .unwrap();
        assert_eq!(out.len(), 50);
        assert!(!out[0].snippets.is_empty());
        rr_us.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let (rr_p50, rr_p95, rr_p99) = quantiles_us(&mut rr_us);
    println!(
        "[#105 Stage-2 RerankAndHighlight] C=50 -> top-10 snippets + lineage | p50={:.1} us  p95={:.1} us  p99={:.1} us",
        rr_p50, rr_p95, rr_p99
    );

    let _ = std::fs::remove_dir_all(&dir);
}
