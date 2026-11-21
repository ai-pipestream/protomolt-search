//! Block-max pruned vs exhaustive bitwise equivalence under geo filters and decay.

use super::*;

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

fn build_geo_pruned_corpus(n: u32, path: &std::path::Path) -> u64 {
    let tf_a = |d: u32| 1 + (u64::from(d) * 2654435761 % 7) as u32;
    let lat = |d: u32| -60.0 + f64::from(d % 121);
    let lon = |d: u32| -179.0 + f64::from(d % 359);
    let mut store = Bm25Store::with_text_fields(&["body"]).with_columns(Geo, &["courthouse"]);
    let mut total = 0u64;
    for doc in 0..n {
        let mut terms = vec![("a".to_string(), tf_a(doc), Vec::new())];
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
            store.set_geo(0, doc, lat(doc), lon(doc));
        }
    }
    store.save(path).unwrap();
    total
}

fn geo_pruned_regions() -> [GeoRegion; 4] {
    [
        GeoRegion::Bbox {
            min_lat: -90.0,
            max_lat: 90.0,
            min_lon: -180.0,
            max_lon: 180.0,
        },
        GeoRegion::Bbox {
            min_lat: 0.0,
            max_lat: 20.0,
            min_lon: -180.0,
            max_lon: 0.0,
        },
        GeoRegion::Radius {
            lat: 38.8977,
            lon: -77.0365,
            meters: 3_000_000.0,
            metric: GeoMetric::Haversine,
        },
        GeoRegion::Radius {
            lat: 0.0,
            lon: 0.0,
            meters: 900_000.0,
            metric: GeoMetric::Manhattan,
        },
    ]
}

fn assert_geo_decayed_k(
    reader: &Bm25Reader,
    stats: &CorpusStats,
    chain: &ScoreChain,
    filters: &pipestream_search::filter::DocFilter,
    region: GeoRegion,
    k: usize,
) {
    let body = reader.field(0);
    let cols = ReaderNumerics(reader);
    let gi = reader.geo_index("courthouse").unwrap();
    let terms: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
    let p = Bm25Params::default();
    let sig = |docs: &[bm25::ScoredDoc]| -> Vec<(u32, u64)> {
        docs.iter().map(|d| (d.doc_id, d.score.to_bits())).collect()
    };
    let chain_ctx = Some((chain, &cols as &dyn NumericRead));
    let filter_ctx = Some((filters, &cols as &dyn NumericRead));
    let q_ex = bm25::TopK::new(k).chain(chain_ctx).filter(filter_ctx);
    let exhaustive = bm25::top_k_exhaustive(&body, &terms, stats, p, q_ex);
    let mut prune = bm25::PruneStats::default();
    let q_pr = bm25::TopK::new(k)
        .chain(chain_ctx)
        .filter(filter_ctx)
        .prune_stats(&mut prune);
    let pruned = bm25::top_k(&body, &terms, stats, p, q_pr);
    assert_eq!(sig(&exhaustive), sig(&pruned), "k={k} {region:?}");
    if let Some(kth) = exhaustive.last() {
        let mut prune = bm25::PruneStats::default();
        let q_s = bm25::TopK::new(k)
            .floor(kth.score)
            .chain(chain_ctx)
            .filter(filter_ctx)
            .prune_stats(&mut prune);
        let seeded = bm25::top_k(&body, &terms, stats, p, q_s);
        assert_eq!(sig(&exhaustive), sig(&seeded), "k={k}: seeded");
    }
    for d in &pruned {
        let (dlat, dlon) = cols.geo_value(gi, d.doc_id).expect("survivor has a point");
        assert!(region.contains(dlat, dlon), "survivor outside the region");
    }
    let mut un_prune = bm25::PruneStats::default();
    let q_un = bm25::TopK::new(k)
        .chain(chain_ctx)
        .prune_stats(&mut un_prune);
    let unfiltered = bm25::top_k(&body, &terms, stats, p, q_un);
    assert_ne!(sig(&unfiltered), sig(&pruned), "k={k} {region:?}");
}

fn assert_geo_decayed_region(
    reader: &Bm25Reader,
    stats: &CorpusStats,
    metric: GeoMetric,
    region: GeoRegion,
) {
    let gi = reader.geo_index("courthouse").unwrap();
    let chain = ScoreChain {
        stages: vec![Stage {
            op: StageOp::MultGeoDecay {
                metric,
                origin_lat: 38.8977,
                origin_lon: -77.0365,
                scale: 1_500_000.0,
            },
            column: Some(ColumnRef::Geo(gi)),
            min_max: (f64::NAN, f64::NAN),
        }],
    };
    let filters = pipestream_search::filter::DocFilter {
        deleted: None,
        geo: GeoFilters {
            filters: vec![pipestream_search::geo::GeoFilter {
                column: Some(gi),
                region,
            }],
        },
        pred: None,
        phrase: Vec::new(),
    };
    for k in [1usize, 5, 50] {
        assert_geo_decayed_k(reader, stats, &chain, &filters, region, k);
    }
}

/// The exactness gate: on a file-backed shard (impacts present, so the
/// block-max path really prunes) the filtered, geo-decayed pruned
/// scorer is bitwise identical to the filtered, geo-decayed exhaustive
/// oracle — at several k, under seeded floors, and with the filter both
/// wide open and narrow enough to remove most of the corpus.
#[test]
fn geo_filtered_decayed_pruned_matches_exhaustive_bitwise() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("geo_pruned_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let n = 3000u32;
    let path = dir.join("geo.bm25");
    let total_doc_length = build_geo_pruned_corpus(n, &path);
    let reader = Bm25Reader::open(&path).unwrap();
    let stats = CorpusStats {
        doc_count: u64::from(n),
        total_doc_length,
        dfs: vec![n, n.div_ceil(3), n.div_ceil(61)],
    };
    for metric in [GeoMetric::Haversine, GeoMetric::Manhattan] {
        for region in geo_pruned_regions() {
            assert_geo_decayed_region(&reader, &stats, metric, region);
        }
    }
    let body = reader.field(0);
    let terms: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
    let params = Bm25Params::default();
    let sig = |docs: &[bm25::ScoredDoc]| -> Vec<(u32, u64)> {
        docs.iter().map(|d| (d.doc_id, d.score.to_bits())).collect()
    };
    for k in [1usize, 10] {
        let plain = bm25::top_k(&body, &terms, &stats, params, k);
        let mut prune = bm25::PruneStats::default();
        let gated = bm25::top_k(
            &body,
            &terms,
            &stats,
            params,
            bm25::TopK::new(k).prune_stats(&mut prune),
        );
        assert_eq!(sig(&plain), sig(&gated), "k={k}: ungated drift");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn assert_geo_fused_region(
    reader: &Bm25Reader,
    fields: &[bm25::FieldQuery<'_>],
    region: GeoRegion,
) {
    let cols = ReaderNumerics(reader);
    let gi = reader.geo_index("courthouse").unwrap();
    let sig = |docs: &[bm25::FusedDoc]| -> Vec<(u32, u64)> {
        docs.iter().map(|d| (d.doc_id, d.score.to_bits())).collect()
    };
    let filters = pipestream_search::filter::DocFilter {
        deleted: None,
        geo: GeoFilters {
            filters: vec![pipestream_search::geo::GeoFilter {
                column: Some(gi),
                region,
            }],
        },
        pred: None,
        phrase: Vec::new(),
    };
    let filter_ctx = Some((&filters, &cols as &dyn NumericRead));
    for k in [1usize, 5, 50] {
        let exhaustive =
            bm25::top_k_fused_exhaustive(fields, bm25::TopK::new(k).filter(filter_ctx), None);
        let mut prune = bm25::PruneStats::default();
        let pruned = bm25::top_k_fused(
            fields,
            bm25::TopK::new(k)
                .filter(filter_ctx)
                .prune_stats(&mut prune),
        );
        assert_eq!(sig(&exhaustive), sig(&pruned), "k={k} {region:?}");
        if let Some(kth) = exhaustive.last() {
            let seeded = bm25::top_k_fused(
                fields,
                bm25::TopK::new(k)
                    .floor(kth.score)
                    .filter(filter_ctx)
                    .prune_stats(&mut bm25::PruneStats::default()),
            );
            assert_eq!(sig(&exhaustive), sig(&seeded), "k={k} {region:?}: seeded");
        }
        for d in &pruned {
            let (dlat, dlon) = cols.geo_value(gi, d.doc_id).expect("survivor has a point");
            assert!(region.contains(dlat, dlon), "survivor outside the region");
        }
        let unfiltered = bm25::top_k_fused(
            fields,
            bm25::TopK::new(k).prune_stats(&mut bm25::PruneStats::default()),
        );
        assert_ne!(sig(&unfiltered), sig(&pruned), "k={k} {region:?}");
    }
}

/// The FUSED twin of the exactness gate above: on a file-backed shard
/// (impacts present, so the fused block-max path really prunes), the
/// filtered fused pruned scorer is bitwise identical to the filtered
/// fused exhaustive oracle.
#[test]
fn geo_filtered_fused_pruned_matches_exhaustive_bitwise() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("geo_fused_pruned_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let n = 3000u32;
    let path = dir.join("geo_fused.bm25");
    let total_doc_length = build_geo_pruned_corpus(n, &path);
    let reader = Bm25Reader::open(&path).unwrap();
    let body = reader.field(0);

    let leg1_terms: Vec<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
    let leg2_terms: Vec<String> = vec!["c".to_string()];
    let fields = vec![
        bm25::FieldQuery {
            index: &body,
            terms: &leg1_terms,
            stats: CorpusStats {
                doc_count: u64::from(n),
                total_doc_length,
                dfs: vec![n, n.div_ceil(3)],
            },
            params: Bm25Params::default(),
            weight: 0.75,
        },
        bm25::FieldQuery {
            index: &body,
            terms: &leg2_terms,
            stats: CorpusStats {
                doc_count: u64::from(n),
                total_doc_length,
                dfs: vec![n.div_ceil(61)],
            },
            params: Bm25Params::default(),
            weight: 1.25,
        },
    ];
    for region in geo_pruned_regions() {
        assert_geo_fused_region(&reader, &fields, region);
    }
    let sig = |docs: &[bm25::FusedDoc]| -> Vec<(u32, u64)> {
        docs.iter().map(|d| (d.doc_id, d.score.to_bits())).collect()
    };
    for k in [1usize, 10] {
        let plain = bm25::top_k_fused(&fields, k);
        let gated = bm25::top_k_fused(
            &fields,
            bm25::TopK::new(k).prune_stats(&mut bm25::PruneStats::default()),
        );
        assert_eq!(sig(&plain), sig(&gated), "k={k}: ungated drift");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
