//! Configuration round trips, build recall, FixedOrder reproducibility, and levels.

use super::*;

fn sample_config() -> HnswConfig {
    HnswConfig {
        m: 24,
        ef_construction: 150,
        build_mode: BuildMode::FixedOrder,
        seed: 0xDEAD_BEEF,
        locality_order: super::super::config::LocalityOrder::None,
    }
}

fn out_of_range_parameters_are_refused(config: HnswConfig) {
    let refused = |c: HnswConfig| c.backend_config().unwrap_err().to_string();
    assert!(refused(HnswConfig { m: 2, ..config }).contains("m=2"));
    assert!(refused(HnswConfig { m: 1000, ..config }).contains("m=1000"));
    assert!(refused(HnswConfig {
        ef_construction: 10,
        ..config
    })
    .contains("ef_construction=10 is below m=24"));
    assert!(refused(HnswConfig {
        ef_construction: 1 << 20,
        ..config
    })
    .contains("exceeds"));
}

fn unknown_proto_enums_are_refused() {
    let unspecified = crate::pb::HnswConfig {
        m: 16,
        ef_construction: 100,
        build_mode: 0,
        seed: 1,
        locality_order: 0,
    };
    assert!(HnswConfig::from_proto(&unspecified)
        .unwrap_err()
        .to_string()
        .contains("unspecified"));
    let unknown_mode = crate::pb::HnswConfig {
        build_mode: 99,
        ..unspecified
    };
    assert!(HnswConfig::from_proto(&unknown_mode)
        .unwrap_err()
        .to_string()
        .contains("unknown build_mode 99"));
    let unknown_locality = crate::pb::HnswConfig {
        build_mode: 1,
        locality_order: 99,
        ..unspecified
    };
    assert!(HnswConfig::from_proto(&unknown_locality)
        .unwrap_err()
        .to_string()
        .contains("unknown locality_order 99"));
}

fn foreign_backend_configs_are_refused(wire: VectorBackendConfig) {
    let other_format = VectorBackendConfig {
        config_format: "text/plain".into(),
        ..wire.clone()
    };
    assert!(HnswConfig::from_backend_config(&other_format)
        .unwrap_err()
        .to_string()
        .contains("unsupported hnsw config format"));
    let other_kind = VectorBackendConfig {
        backend_kind: EMBEDDED_TURBOVEC.into(),
        ..wire.clone()
    };
    assert!(HnswConfig::from_backend_config(&other_kind)
        .unwrap_err()
        .to_string()
        .contains("names"));
    let garbage = VectorBackendConfig {
        payload: vec![0xFF; 7],
        ..wire
    };
    assert!(HnswConfig::from_backend_config(&garbage)
        .unwrap_err()
        .to_string()
        .contains("not HnswConfig"));
}

fn the_fingerprint_binds_every_field(config: HnswConfig) {
    // The fingerprint binds every field.
    let fingerprint = |c: HnswConfig| {
        HnswIndex::new(8, c)
            .unwrap()
            .descriptor()
            .scoring_fingerprint
    };
    let base = fingerprint(config);
    assert_ne!(base, fingerprint(HnswConfig { m: 32, ..config }));
    assert_ne!(
        base,
        fingerprint(HnswConfig {
            ef_construction: 151,
            ..config
        })
    );
    assert_ne!(
        base,
        fingerprint(HnswConfig {
            build_mode: BuildMode::Parallel,
            ..config
        })
    );
    assert_ne!(base, fingerprint(HnswConfig { seed: 1, ..config }));
    assert_ne!(
        base,
        fingerprint(HnswConfig {
            locality_order: super::super::config::LocalityOrder::Rcm,
            ..config
        })
    );
    assert_ne!(
        base,
        fingerprint(HnswConfig {
            locality_order: super::super::config::LocalityOrder::RecursiveBisection,
            ..config
        })
    );
    assert_ne!(
        base,
        HnswIndex::new(9, config)
            .unwrap()
            .descriptor()
            .scoring_fingerprint
    );
    assert!(HnswIndex::new(0, config).is_err());
}

#[test]
fn config_round_trips_and_refuses_bad_configs() {
    let config = sample_config();
    let wire = config.backend_config().unwrap();
    assert_eq!(wire.backend_kind, HNSW);
    assert_eq!(wire.config_format, CONFIG_FORMAT);
    assert_eq!(HnswConfig::from_backend_config(&wire).unwrap(), config);
    assert_eq!(HnswConfig::decode(&config.encode()).unwrap(), config);
    out_of_range_parameters_are_refused(config);
    unknown_proto_enums_are_refused();
    foreign_backend_configs_are_refused(wire);
    the_fingerprint_binds_every_field(config);
}

#[test]
fn recall_at_10_clears_0_95_in_both_build_modes() {
    let dim = 32;
    let (vectors, queries) = corpus_with_queries(4000, 100, dim, 0x1234);
    for mode in [BuildMode::FixedOrder, BuildMode::Parallel] {
        let index = built(&vectors, dim, mode);
        assert_eq!(index.len(), 4000);
        let r = recall(&index, &vectors, &queries, dim, 10, 64);
        assert!(r >= 0.95, "{mode:?}: recall@10 {r} at ef 64");
        // Scores are the exact inner products of the stored rows.
        let (hits, stats) = index
            .search_with_stats(&queries[..dim], 10, 64, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(hits.len(), 10);
        assert!(stats.stats.hops > 0 && stats.stats.scored >= 10);
        for (row, score) in hits {
            let exact: f32 = vectors[row as usize * dim..][..dim]
                .iter()
                .zip(&queries[..dim])
                .map(|(a, b)| a * b)
                .sum();
            assert!((score - exact).abs() <= 1.0e-5, "{score} vs {exact}");
        }
    }
}

#[test]
fn fixed_order_build_is_reproducible_bit_for_bit() {
    let dim = 24;
    let vectors = corpus(1500, dim, 0x42);
    let dir = temp_dir("fixed-order");
    let image = |tag: &str, batches: &[usize]| {
        let mut index = HnswIndex::new(dim, config(BuildMode::FixedOrder)).unwrap();
        let mut at = 0;
        for &rows in batches {
            index
                .add(&vectors[at * dim..(at + rows) * dim], dim)
                .unwrap();
            at += rows;
        }
        index.prepare().unwrap();
        let path = dir.join(tag);
        index.write(&path).unwrap();
        std::fs::read(&path).unwrap()
    };
    let once = image("a", &[1500]);
    assert_eq!(once, image("b", &[1500]), "two builds differ");
    // Tail insertion in row order is the same insertion loop, so batching
    // does not change the fixed-order graph either.
    assert_eq!(once, image("c", &[1000, 400, 100]), "batched build differs");
    // Another seed draws other levels and another graph.
    let mut other = HnswIndex::new(
        dim,
        HnswConfig {
            seed: 8,
            ..config(BuildMode::FixedOrder)
        },
    )
    .unwrap();
    other.add(&vectors, dim).unwrap();
    other.write(&dir.join("d")).unwrap();
    assert_ne!(once, std::fs::read(dir.join("d")).unwrap());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn levels_are_geometric_capped_and_order_independent() {
    let mult = 1.0 / 16f64.ln();
    let mut counts = [0usize; 8];
    for node in 0..100_000u32 {
        let level = level_for(7, node, mult);
        assert!(level <= MAX_LEVEL);
        counts[usize::from(level).min(7)] += 1;
        assert_eq!(level, level_for(7, node, mult));
    }
    // About 1/16 of nodes reach level 1, 1/256 level 2.
    assert!(
        (5_000..7_500).contains(&(100_000 - counts[0])),
        "{counts:?}"
    );
    assert!(
        (200..600).contains(&(counts[2..].iter().sum::<usize>())),
        "{counts:?}"
    );
    assert_ne!(
        (0..64).map(|n| level_for(7, n, mult)).collect::<Vec<_>>(),
        (0..64).map(|n| level_for(8, n, mult)).collect::<Vec<_>>()
    );
}
