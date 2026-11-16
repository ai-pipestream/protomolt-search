//! Distributed and shard-absent-term acceptance gates for multi-field fused pruning.

use std::path::PathBuf;

use pipestream_search::bm25::{
    self, Bm25Params, CorpusStats, FieldQuery, FusedDoc, PruneStats, TopK,
};
use pipestream_search::postings::{AnalyzedDoc, AnalyzedField, Bm25Index, Bm25Reader, Bm25Store};

fn test_dir(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/tmp")
        .join(format!("fused_dist_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

type Sig = Vec<(u32, u64, Vec<(usize, usize, Vec<(u32, u32)>)>)>;

fn sig(hits: &[FusedDoc]) -> Sig {
    hits.iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.term_offsets.clone()))
        .collect()
}

fn field_stats(store: &Bm25Store, f: usize, terms: &[String]) -> CorpusStats {
    CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.field(f).total_doc_length(),
        dfs: terms.iter().map(|t| store.field(f).df(t)).collect(),
    }
}

fn doc_for_global_id(g: u32) -> AnalyzedDoc {
    let body = vec![
        ("rust".to_string(), 1 + g % 3, vec![(g, g + 2)]),
        (format!("t{}", g % 4), 1, vec![(g + 3, g + 5)]),
    ];
    let body_len: u32 = body.iter().map(|t| t.1).sum();
    let mut fields = vec![AnalyzedField {
        terms: body,
        length: body_len,
        ..Default::default()
    }];
    if g % 3 != 1 {
        fields.push(AnalyzedField {
            terms: vec![
                ("smith".to_string(), 1, vec![(0, 5)]),
                (format!("n{}", g % 2), 1, vec![]),
            ],
            length: 2,
            ..Default::default()
        });
    }
    AnalyzedDoc {
        fields,
        ..Default::default()
    }
}

fn build_shard(range: std::ops::Range<u32>, offset: u32) -> Bm25Store {
    let mut store = Bm25Store::with_text_fields(&["body", "name"]);
    for g in range {
        store.add_document(g - offset, format!("doc {g}"), doc_for_global_id(g));
    }
    store
}

fn merge_global_field_stats(
    shards: &[(Bm25Store, u32)],
    field_idx: usize,
    terms: &[String],
) -> CorpusStats {
    let shares: Vec<(u64, u64, Vec<u32>)> = shards
        .iter()
        .map(|(s, _)| {
            (
                s.doc_count(),
                s.field(field_idx).total_doc_length(),
                terms.iter().map(|t| s.field(field_idx).df(t)).collect(),
            )
        })
        .collect();
    bm25::merge_stats(&shares)
}

fn merge_fused_hits(mut hits: Vec<FusedDoc>, k: usize) -> Vec<FusedDoc> {
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.doc_id.cmp(&b.doc_id))
    });
    hits.truncate(k);
    hits
}

fn two_field_queries<'a>(
    body: &'a dyn Bm25Index,
    name: &'a dyn Bm25Index,
    body_q: (&'a [String], &CorpusStats, Bm25Params, f64),
    name_q: (&'a [String], &CorpusStats, Bm25Params, f64),
) -> [FieldQuery<'a>; 2] {
    [
        FieldQuery {
            index: body,
            terms: body_q.0,
            stats: body_q.1.clone(),
            params: body_q.2,
            weight: body_q.3,
        },
        FieldQuery {
            index: name,
            terms: name_q.0,
            stats: name_q.1.clone(),
            params: name_q.2,
            weight: name_q.3,
        },
    ]
}

#[test]
fn fused_pruned_distributed_equals_monolithic() {
    let dir = test_dir("dist");
    let monolith = build_shard(0..700, 0);
    let shards = [
        (build_shard(0..311, 0), 0u32),
        (build_shard(311..700, 311), 311u32),
    ];
    let body_terms = vec!["rust".to_string(), "t2".to_string()];
    let name_terms = vec!["smith".to_string(), "n1".to_string()];
    let body_params = Bm25Params::default();
    let name_params = Bm25Params { k1: 0.8, b: 0.3 };
    let body_stats = merge_global_field_stats(&shards, 0, &body_terms);
    let name_stats = merge_global_field_stats(&shards, 1, &name_terms);

    let readers: Vec<Bm25Reader> = std::iter::once(&monolith)
        .chain(shards.iter().map(|(s, _)| s))
        .enumerate()
        .map(|(i, s)| {
            let path = dir.join(format!("{i}.bm25"));
            s.save(&path).unwrap();
            Bm25Reader::open(&path).unwrap()
        })
        .collect();

    let k = 12;
    let run = |reader: &Bm25Reader, floor: f64| {
        let (r0, r1) = (reader.field(0), reader.field(1));
        let q = two_field_queries(
            &r0,
            &r1,
            (&body_terms, &body_stats, body_params, 1.0),
            (&name_terms, &name_stats, name_params, 1.7),
        );
        bm25::top_k_fused(&q, TopK::new(k).floor(floor))
    };

    let want = run(&readers[0], f64::NEG_INFINITY);
    let (m0, m1) = (monolith.field(0), monolith.field(1));
    let mq = two_field_queries(
        &m0,
        &m1,
        (&body_terms, &body_stats, body_params, 1.0),
        (&name_terms, &name_stats, name_params, 1.7),
    );
    let oracle = bm25::top_k_fused_exhaustive(&mq, k, None);
    assert_eq!(sig(&oracle), sig(&want), "monolith pruned vs exhaustive");

    for (label, floor) in [
        ("unseeded", f64::NEG_INFINITY),
        ("seeded", want[k - 1].score),
    ] {
        let hits: Vec<FusedDoc> = readers[1..]
            .iter()
            .zip(shards.iter().map(|&(_, off)| off))
            .flat_map(|(r, off)| {
                run(r, floor).into_iter().map(move |mut d| {
                    d.doc_id += off;
                    d
                })
            })
            .collect();
        let merged = merge_fused_hits(hits, k);
        assert_eq!(sig(&want), sig(&merged), "{label} distributed merge");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn build_absent_term_shard() -> Bm25Store {
    let mut store = Bm25Store::with_text_fields(&["body", "name"]);
    for i in 0..2000u32 {
        let tf = if i % 97 == 0 { 5 } else { 1 };
        store.add_document(
            i,
            format!("doc {i}"),
            AnalyzedDoc {
                fields: vec![
                    AnalyzedField {
                        terms: vec![("court".to_string(), tf, vec![(0, 5)])],
                        length: 4,
                        ..Default::default()
                    },
                    AnalyzedField {
                        terms: vec![("smith".to_string(), 1, vec![(0, 5)])],
                        length: 1,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        );
    }
    store
}

#[test]
fn a_term_absent_from_this_shard_does_not_disable_fused_pruning() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("fused_absent_term");
    std::fs::create_dir_all(&dir).unwrap();
    let store = build_absent_term_shard();
    let path = dir.join("absent.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();

    let body_terms = vec!["court".to_string(), "12b6".to_string()];
    let name_terms = vec!["smith".to_string()];
    let params = Bm25Params::default();
    let body_stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.field(0).total_doc_length(),
        dfs: vec![2000, 10],
    };
    let name_stats = field_stats(&store, 1, &name_terms);
    let (s0, s1, r0, r1) = (
        store.field(0),
        store.field(1),
        reader.field(0),
        reader.field(1),
    );
    let sq = two_field_queries(
        &s0,
        &s1,
        (&body_terms, &body_stats, params, 1.0),
        (&name_terms, &name_stats, params, 2.0),
    );
    let rq = two_field_queries(
        &r0,
        &r1,
        (&body_terms, &body_stats, params, 1.0),
        (&name_terms, &name_stats, params, 2.0),
    );

    let oracle = bm25::top_k_fused_exhaustive(&sq, 10, None);
    let mut prune = PruneStats::default();
    let got = bm25::top_k_fused(&rq, TopK::new(10).prune_stats(&mut prune));
    assert_eq!(sig(&oracle), sig(&got));
    assert!(prune.candidates_evaluated > 0);

    let floor = oracle[9].score;
    let seeded = bm25::top_k_fused(&rq, TopK::new(10).floor(floor));
    assert_eq!(
        sig(&bm25::filter_fused_to_floor(oracle, floor)),
        sig(&seeded)
    );

    let present_only = vec!["court".to_string()];
    let solo_body_stats = CorpusStats {
        doc_count: store.doc_count(),
        total_doc_length: store.field(0).total_doc_length(),
        dfs: vec![2000],
    };
    let solo_q = two_field_queries(
        &r0,
        &r1,
        (&present_only, &solo_body_stats, params, 1.0),
        (&name_terms, &name_stats, params, 2.0),
    );
    let mut solo = PruneStats::default();
    let _ = bm25::top_k_fused(&solo_q, TopK::new(10).prune_stats(&mut solo));
    assert_eq!(prune.blocks_total, solo.blocks_total);
    let _ = std::fs::remove_dir_all(&dir);
}
