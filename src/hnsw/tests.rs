//! Unit tests of the `hnsw` provider. Everything is seeded.

use std::path::PathBuf;

use super::config::{BuildMode, HnswConfig, CONFIG_FORMAT};
use super::graph::{level_for, MAX_LEVEL};
use super::{HnswIndex, HNSW};
use crate::vector::{
    QualityContract, VectorBackendConfig, VectorCapability, VectorIndex, VectorProvider,
    VectorSearchOptions, VectorStreamControl, EMBEDDED_TURBOVEC,
};

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Approximately normal: the sum of twelve uniforms, centered.
    fn normal(&mut self) -> f32 {
        ((0..12).map(|_| self.unit()).sum::<f64>() - 6.0) as f32
    }
}

/// `rows + queries` unit vectors of `dim` values drawn around sixteen
/// seeded topic centers (intra-topic cosine about 0.9, as tight as the
/// challenge fixture's topics), the queries from the same centers, so they
/// have the neighborhoods a real query has.
fn corpus_with_queries(rows: usize, queries: usize, dim: usize, seed: u64) -> (Vec<f32>, Vec<f32>) {
    let mut rng = Rng(seed);
    let topics: Vec<Vec<f32>> = (0..16)
        .map(|_| (0..dim).map(|_| rng.normal()).collect())
        .collect();
    let mut draw = |count: usize| {
        let mut out = Vec::with_capacity(count * dim);
        for _ in 0..count {
            let topic = &topics[(rng.next_u64() % 16) as usize];
            let mut row: Vec<f32> = topic.iter().map(|t| t + 0.3 * rng.normal()).collect();
            let norm = row.iter().map(|x| x * x).sum::<f32>().sqrt();
            for x in &mut row {
                *x /= norm;
            }
            out.extend_from_slice(&row);
        }
        out
    };
    let base = draw(rows);
    let queries = draw(queries);
    (base, queries)
}

/// Rows only; see [`corpus_with_queries`].
fn corpus(rows: usize, dim: usize, seed: u64) -> Vec<f32> {
    corpus_with_queries(rows, 0, dim, seed).0
}

fn exact_top(vectors: &[f32], dim: usize, query: &[f32], k: usize) -> Vec<u32> {
    let mut all: Vec<(f32, u32)> = vectors
        .chunks_exact(dim)
        .enumerate()
        .map(|(i, row)| (row.iter().zip(query).map(|(a, b)| a * b).sum(), i as u32))
        .collect();
    all.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    all.truncate(k);
    all.into_iter().map(|(_, i)| i).collect()
}

fn recall(
    index: &HnswIndex,
    vectors: &[f32],
    queries: &[f32],
    dim: usize,
    k: usize,
    beam: usize,
) -> f64 {
    let mut found = 0usize;
    let mut total = 0usize;
    for query in queries.chunks_exact(dim) {
        let truth = exact_top(vectors, dim, query, k);
        let (hits, _) = index
            .search_with_stats(query, k, beam, VectorSearchOptions::new())
            .unwrap();
        found += hits.iter().filter(|(row, _)| truth.contains(row)).count();
        total += truth.len();
    }
    found as f64 / total as f64
}

fn config(mode: BuildMode) -> HnswConfig {
    HnswConfig {
        m: 16,
        ef_construction: 100,
        build_mode: mode,
        seed: 7,
    }
}

fn built(vectors: &[f32], dim: usize, mode: BuildMode) -> HnswIndex {
    let mut index = HnswIndex::new(dim, config(mode)).unwrap();
    index.add(vectors, dim).unwrap();
    index.prepare().unwrap();
    index
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("hnsw-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn config_round_trips_and_refuses_bad_configs() {
    let config = HnswConfig {
        m: 24,
        ef_construction: 150,
        build_mode: BuildMode::FixedOrder,
        seed: 0xDEAD_BEEF,
    };
    let wire = config.backend_config().unwrap();
    assert_eq!(wire.backend_kind, HNSW);
    assert_eq!(wire.config_format, CONFIG_FORMAT);
    assert_eq!(HnswConfig::from_backend_config(&wire).unwrap(), config);
    assert_eq!(HnswConfig::decode(&config.encode()).unwrap(), config);

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

    let unspecified = crate::pb::HnswConfig {
        m: 16,
        ef_construction: 100,
        build_mode: 0,
        seed: 1,
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
        HnswIndex::new(9, config)
            .unwrap()
            .descriptor()
            .scoring_fingerprint
    );
    assert!(HnswIndex::new(0, config).is_err());
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
        assert!(stats.hops > 0 && stats.scored >= 10);
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

#[test]
fn image_round_trips_and_mapped_load_equals_heap_load() {
    let dim = 16;
    let (vectors, queries) = corpus_with_queries(2000, 20, dim, 0x99);
    let index = built(&vectors, dim, BuildMode::Parallel);
    let dir = temp_dir("round-trip");
    let path = dir.join("image");
    index.write(&path).unwrap();
    let heap = HnswIndex::load(&path).unwrap();
    let mapped = HnswIndex::load_mapped(&path).unwrap();
    assert!(!heap.is_mapped() && mapped.is_mapped());
    assert_eq!(heap.len(), 2000);
    assert_eq!(mapped.len(), 2000);
    assert_eq!(heap.descriptor(), index.descriptor());
    assert_eq!(mapped.descriptor(), index.descriptor());
    assert_eq!(heap.config(), index.config());
    let bytes = std::fs::metadata(&path).unwrap().len() as f64 / 2000.0;
    assert!((mapped.image_bytes_per_row().unwrap() - bytes).abs() < 1.0e-9);
    // Rows: 64 B, inv_norm 4, level 1, upper_slot 4, layer 0 (2M+1)*4 = 132 B.
    assert!(bytes > 200.0 && bytes < 260.0, "{bytes} bytes per row");

    let search = |index: &HnswIndex| {
        let results = index
            .search(&queries, 10, VectorSearchOptions::new())
            .unwrap();
        results
            .scores
            .iter()
            .map(|s| s.to_bits())
            .zip(results.slots.iter().copied())
            .collect::<Vec<_>>()
    };
    let expected = search(&index);
    assert_eq!(search(&heap), expected);
    assert_eq!(search(&mapped), expected);

    let transcript = |index: &HnswIndex, row: usize| {
        let mut out = None;
        index
            .row_transcripts(row..row + 1, &mut |_, t| out = Some(t.to_vec()))
            .unwrap();
        out.unwrap()
    };
    for row in [0, 1, 777, 1999] {
        let expected = transcript(&index, row);
        assert_eq!(expected.len(), 12 + 4 * dim);
        assert_eq!(&expected[..4], b"HNSW");
        let stored: Vec<f32> = expected[12..]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect();
        assert_eq!(stored, &vectors[row * dim..(row + 1) * dim]);
        assert_eq!(transcript(&heap, row), expected);
        assert_eq!(transcript(&mapped, row), expected);
    }
    assert!(index.row_transcripts(1999..2001, &mut |_, _| {}).is_err());

    // A mapped image is sealed.
    let mut mapped = mapped;
    assert!(mapped
        .add(&vectors[..dim], dim)
        .unwrap_err()
        .to_string()
        .contains("sealed"));
    assert!(mapped.write(&dir.join("copy")).is_err());

    // A loaded heap image keeps growing like the original.
    let mut heap = heap;
    heap.add(&corpus(50, dim, 0x97), dim).unwrap();
    assert_eq!(heap.len(), 2050);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_empty_image_round_trips() {
    let dim = 8;
    let index = HnswIndex::new(dim, config(BuildMode::FixedOrder)).unwrap();
    let results = index
        .search(&[1.0; 16], 3, VectorSearchOptions::new())
        .unwrap();
    assert_eq!(results.slots, vec![-1; 6]);
    assert_eq!(results.query_count, 2);
    let dir = temp_dir("empty");
    let path = dir.join("image");
    index.write(&path).unwrap();
    for loaded in [
        HnswIndex::load(&path).unwrap(),
        HnswIndex::load_mapped(&path).unwrap(),
    ] {
        assert_eq!(loaded.len(), 0);
        assert_eq!(loaded.dimension(), Some(dim));
        assert!(loaded.image_bytes_per_row().is_none());
        let results = loaded
            .search(&[1.0; 8], 2, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(results.slots, vec![-1, -1]);
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn corrupted_and_hostile_images_are_refused() {
    let dim = 8;
    let vectors = corpus(300, dim, 0x77);
    let index = built(&vectors, dim, BuildMode::FixedOrder);
    let dir = temp_dir("hostile");
    let good = dir.join("good");
    index.write(&good).unwrap();
    let bytes = std::fs::read(&good).unwrap();
    let header_len = super::image::header_len();
    let u64_at = |b: &[u8], at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap());

    let refuse = |tag: &str, body: &[u8], expect: &str| {
        let path = dir.join(tag);
        std::fs::write(&path, body).unwrap();
        for (how, result) in [
            ("load", HnswIndex::load(&path)),
            ("map", HnswIndex::load_mapped(&path)),
        ] {
            let error = result
                .err()
                .unwrap_or_else(|| panic!("{how} accepted {tag}"));
            assert!(
                error.to_string().contains(expect),
                "{how} {tag}: {error} (expected {expect:?})"
            );
        }
    };
    let recrc = |b: &mut [u8]| {
        let crc = crate::wal::crc32(&b[..136]);
        b[136..140].copy_from_slice(&crc.to_le_bytes());
    };
    let reseal = |b: &mut [u8]| {
        let n = b.len() - 32;
        let digest = crate::sha256::digest(&b[..n]);
        b[n..].copy_from_slice(&digest);
    };

    refuse("empty", &[], "truncated");
    refuse("short", &bytes[..header_len - 1], "truncated");
    refuse("header-only", &bytes[..header_len], "declares");
    refuse("truncated", &bytes[..bytes.len() - 1], "declares");
    let mut magic = bytes.clone();
    magic[0] ^= 1;
    refuse("magic", &magic, "unknown magic");
    let mut format = bytes.clone();
    format[8] = 9;
    recrc(&mut format);
    refuse("format", &format, "format 9");
    let mut flipped = bytes.clone();
    flipped[20] ^= 1; // m, without fixing the CRC
    refuse("header-bit", &flipped, "header checksum");
    let mut body = bytes.clone();
    let rows_off = u64_at(&bytes, 72) as usize;
    body[rows_off + 5] ^= 0x80;
    refuse("body-bit", &body, "trailer");
    let mut trailer = bytes.clone();
    let last = trailer.len() - 1;
    trailer[last] ^= 1;
    refuse("trailer-bit", &trailer, "trailer");
    // A header that claims a huge row count lays out past the file: refused
    // from the header alone.
    let mut huge = bytes.clone();
    huge[40..48].copy_from_slice(&(1u64 << 30).to_le_bytes());
    recrc(&mut huge);
    refuse("huge-rows", &huge, "section offsets");
    let mut too_many = bytes.clone();
    too_many[40..48].copy_from_slice(&(u64::from(u32::MAX)).to_le_bytes());
    recrc(&mut too_many);
    refuse("too-many-rows", &too_many, "limit");
    let mut bad_m = bytes.clone();
    bad_m[20..24].copy_from_slice(&1u32.to_le_bytes());
    recrc(&mut bad_m);
    refuse("bad-m", &bad_m, "m=1");
    let mut bad_mode = bytes.clone();
    bad_mode[28..32].copy_from_slice(&0u32.to_le_bytes());
    recrc(&mut bad_mode);
    refuse("bad-mode", &bad_mode, "unspecified");
    let mut bad_entry = bytes.clone();
    bad_entry[64..68].copy_from_slice(&5000u32.to_le_bytes());
    recrc(&mut bad_entry);
    refuse("bad-entry", &bad_entry, "entry 5000");
    // Structurally hostile with a valid header and trailer: a neighbor id
    // past the rows, a count past the cap, a self link, a level past the
    // header's top.
    let links0_off = u64_at(&bytes, 112) as usize;
    let mut wild = bytes.clone();
    wild[links0_off + 4..links0_off + 8].copy_from_slice(&40_000u32.to_le_bytes());
    reseal(&mut wild);
    refuse("wild-neighbor", &wild, "names neighbor 40000");
    let mut count = bytes.clone();
    count[links0_off..links0_off + 4].copy_from_slice(&33u32.to_le_bytes());
    reseal(&mut count);
    refuse("wild-count", &count, "lists 33 neighbors");
    let mut selfie = bytes.clone();
    selfie[links0_off + 4..links0_off + 8].copy_from_slice(&0u32.to_le_bytes());
    reseal(&mut selfie);
    refuse("self-link", &selfie, "names neighbor 0");
    let level_off = u64_at(&bytes, 88) as usize;
    let mut tall = bytes.clone();
    tall[level_off] = 9;
    reseal(&mut tall);
    refuse("tall-row", &tall, "upper slot");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn tail_insertion_keeps_recall_in_both_modes() {
    let dim = 32;
    let (vectors, queries) = corpus_with_queries(5000, 100, dim, 0x3141);
    for mode in [BuildMode::FixedOrder, BuildMode::Parallel] {
        let mut index = HnswIndex::new(dim, config(mode)).unwrap();
        index.add(&vectors[..3000 * dim], dim).unwrap();
        // One by one, then a small batch, then one below and one above the
        // parallel threshold.
        for row in 3000..3010 {
            index
                .add(&vectors[row * dim..(row + 1) * dim], dim)
                .unwrap();
        }
        index.add(&vectors[3010 * dim..3100 * dim], dim).unwrap();
        index.add(&vectors[3100 * dim..3300 * dim], dim).unwrap();
        index.add(&vectors[3300 * dim..5000 * dim], dim).unwrap();
        assert_eq!(index.len(), 5000);
        let r = recall(&index, &vectors, &queries, dim, 10, 64);
        assert!(r >= 0.95, "{mode:?}: recall@10 {r} after tail insertion");
        // The tail rows themselves are found.
        let mut tail_found = 0;
        for row in 4900..5000 {
            let (hits, _) = index
                .search_with_stats(
                    &vectors[row * dim..(row + 1) * dim],
                    1,
                    16,
                    VectorSearchOptions::new(),
                )
                .unwrap();
            tail_found += usize::from(hits.first().map(|h| h.0) == Some(row as u32));
        }
        assert!(
            tail_found >= 95,
            "{mode:?}: {tail_found}/100 tail rows found as their own nearest"
        );
    }
    let mut index = HnswIndex::new(dim, config(BuildMode::FixedOrder)).unwrap();
    assert!(index.add(&vectors[..dim + 1], dim).is_err());
    assert!(index.add(&vectors[..dim], dim + 1).is_err());
    assert!(index.add(&[f32::NAN; 32], dim).is_err());
    index.add(&[], dim).unwrap();
    assert_eq!(index.len(), 0);
}

#[test]
fn masks_and_floors_filter_results_only() {
    let dim = 16;
    let (vectors, queries) = corpus_with_queries(2000, 10, dim, 0x2718);
    let index = built(&vectors, dim, BuildMode::FixedOrder);
    let allow: Vec<bool> = (0..2000).map(|i| i % 3 == 0).collect();
    let results = index
        .search(&queries, 5, VectorSearchOptions::new().with_mask(&allow))
        .unwrap();
    assert_eq!(results.result_count, 5);
    for q in 0..10 {
        let query = &queries[q * dim..(q + 1) * dim];
        let slots = results.slots_for_query(q);
        assert!(
            slots.iter().all(|&s| s >= 0 && allow[s as usize]),
            "{slots:?}"
        );
        // Masked recall against the exact masked truth.
        let truth: Vec<u32> = {
            let mut all: Vec<(f32, u32)> = vectors
                .chunks_exact(dim)
                .enumerate()
                .filter(|(i, _)| allow[*i])
                .map(|(i, row)| (row.iter().zip(query).map(|(a, b)| a * b).sum(), i as u32))
                .collect();
            all.sort_by(|a, b| b.0.total_cmp(&a.0));
            all.iter().take(5).map(|h| h.1).collect()
        };
        let hit = slots
            .iter()
            .filter(|s| truth.contains(&(**s as u32)))
            .count();
        assert!(hit >= 3, "query {q}: masked hits {hit}/5");
    }
    // A mask that admits nothing returns padding; a mask of the wrong
    // length refuses.
    let none = vec![false; 2000];
    let results = index
        .search(
            &queries[..dim],
            3,
            VectorSearchOptions::new().with_mask(&none),
        )
        .unwrap();
    assert_eq!(results.slots, vec![-1, -1, -1]);
    assert!(results.scores.iter().all(|s| *s == f32::NEG_INFINITY));
    assert!(index
        .search(
            &queries[..dim],
            3,
            VectorSearchOptions::new().with_mask(&allow[..10])
        )
        .is_err());

    // The floor filters: the top-k above the unfloored k-th best is the
    // same set, and a floor above everything yields padding.
    let plain = index
        .search(&queries[..dim], 5, VectorSearchOptions::new())
        .unwrap();
    let kth = plain.scores[4];
    let floored = index
        .search(
            &queries[..dim],
            5,
            VectorSearchOptions::new().with_minimum_score(kth),
        )
        .unwrap();
    assert_eq!(floored.slots, plain.slots);
    let above = index
        .search(
            &queries[..dim],
            5,
            VectorSearchOptions::new().with_minimum_score(plain.scores[0] + 1.0),
        )
        .unwrap();
    assert_eq!(above.slots, vec![-1; 5]);
    // A floor between the 2nd and 3rd best keeps two and pads three.
    let between = (plain.scores[1] + plain.scores[2]) / 2.0;
    let two = index
        .search(
            &queries[..dim],
            5,
            VectorSearchOptions::new().with_minimum_score(between),
        )
        .unwrap();
    assert_eq!(&two.slots[..2], &plain.slots[..2]);
    assert_eq!(&two.slots[2..], &[-1, -1, -1]);
    // k = 0 is an empty, well-formed answer.
    let zero = index
        .search(&queries[..dim], 0, VectorSearchOptions::new())
        .unwrap();
    assert_eq!(zero.result_count, 0);
    assert!(zero.slots.is_empty());
    // A candidate depth widens the beam.
    let (_, narrow) = index
        .search_with_stats(&queries[..dim], 1, 1, VectorSearchOptions::new())
        .unwrap();
    let (_, wide) = index
        .search_with_stats(&queries[..dim], 1, 200, VectorSearchOptions::new())
        .unwrap();
    assert!(wide.scored > narrow.scored);
    let deep = index
        .search(
            &queries[..dim],
            1,
            VectorSearchOptions::new().with_candidate_depth(200),
        )
        .unwrap();
    assert_eq!(deep.slots[0], plain.slots[0]);
}

#[test]
fn streaming_emits_scored_rows_and_honors_stop_and_raised_floors() {
    let dim = 16;
    let (vectors, queries) = corpus_with_queries(3000, 3, dim, 0x1618);
    let index = built(&vectors, dim, BuildMode::Parallel);
    let allow: Vec<bool> = (0..3000).map(|i| i % 2 == 1).collect();

    // Without a depth the provider refuses.
    let error = index
        .search_streaming_controlled(
            &queries[..dim],
            VectorSearchOptions::new(),
            &mut |_| VectorStreamControl::Continue,
            &mut || VectorStreamControl::Continue,
        )
        .unwrap_err();
    assert!(error.to_string().contains("candidate depth"), "{error}");

    // Every emitted row is allowed, exactly scored, and the emission covers
    // the beam's top-k.
    let options = VectorSearchOptions::new()
        .with_mask(&allow)
        .with_candidate_depth(64);
    let mut emitted: Vec<(usize, i64, f32)> = Vec::new();
    let mut batches = 0;
    let mut polls = 0;
    let summary = index
        .search_streaming_controlled(
            &queries,
            options,
            &mut |batch| {
                batches += 1;
                assert_eq!(batch.scores.len(), batch.slots.len());
                assert!(!batch.scores.is_empty());
                for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                    emitted.push((batch.query_index, slot, score));
                }
                VectorStreamControl::Continue
            },
            &mut || {
                polls += 1;
                VectorStreamControl::Continue
            },
        )
        .unwrap();
    assert!(summary.completed);
    assert_eq!(summary.query_count, 3);
    assert_eq!(summary.emitted, emitted.len());
    assert!(summary.units_scanned >= emitted.len());
    assert!(batches > 3 && polls > 3, "{batches} batches, {polls} polls");
    for &(q, slot, score) in &emitted {
        assert!(slot >= 0 && allow[slot as usize]);
        let exact: f32 = vectors[slot as usize * dim..][..dim]
            .iter()
            .zip(&queries[q * dim..(q + 1) * dim])
            .map(|(a, b)| a * b)
            .sum();
        assert!((score - exact).abs() <= 1.0e-5);
    }
    for q in 0..3 {
        let seen: Vec<i64> = emitted.iter().filter(|e| e.0 == q).map(|e| e.1).collect();
        let mut sorted = seen.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), seen.len(), "query {q} emitted a row twice");
        let top = index
            .search(
                &queries[q * dim..(q + 1) * dim],
                10,
                VectorSearchOptions::new()
                    .with_mask(&allow)
                    .with_candidate_depth(64),
            )
            .unwrap();
        for slot in top.slots {
            assert!(
                seen.contains(&slot),
                "query {q}: top row {slot} never emitted"
            );
        }
    }

    // Stop from the sink ends the stream uncompleted after one batch.
    let mut seen = 0;
    let summary = index
        .search_streaming_controlled(
            &queries,
            options,
            &mut |batch| {
                seen += batch.slots.len();
                VectorStreamControl::Stop
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    assert!(!summary.completed);
    assert_eq!(summary.emitted, seen);
    assert!(seen > 0 && seen <= 64);

    // Stop from the control ends it too, with nothing more emitted.
    let mut after_stop = 0;
    let stopped = std::cell::Cell::new(false);
    let summary = index
        .search_streaming_controlled(
            &queries,
            options,
            &mut |batch| {
                if stopped.get() {
                    after_stop += batch.slots.len();
                }
                VectorStreamControl::Continue
            },
            &mut || {
                stopped.set(true);
                VectorStreamControl::Stop
            },
        )
        .unwrap();
    assert!(!summary.completed);
    assert_eq!(after_stop, 0);

    // A floor raised by the control gates what follows, and a floor raised
    // by the sink carries into the next query.
    let plain = index
        .search(&queries, 1, VectorSearchOptions::new().with_mask(&allow))
        .unwrap();
    let raise_to = plain.scores[1] - 1.0e-3; // just under query 1's best
    let mut polls = 0;
    let mut per_query: Vec<Vec<f32>> = vec![Vec::new(); 3];
    let summary = index
        .search_streaming_controlled(
            &queries,
            options,
            &mut |batch| {
                per_query[batch.query_index].extend_from_slice(batch.scores);
                if batch.query_index == 0 {
                    VectorStreamControl::RaiseFloor(raise_to)
                } else {
                    VectorStreamControl::Continue
                }
            },
            &mut || {
                polls += 1;
                if polls == 2 {
                    VectorStreamControl::RaiseFloor(-0.5)
                } else {
                    VectorStreamControl::Continue
                }
            },
        )
        .unwrap();
    assert!(summary.completed);
    // Query 0's first batch raised the floor to just under query 1's best,
    // so query 1 emits its best row (and anything tied) and nothing below.
    assert!(
        per_query[1].iter().all(|&s| s >= raise_to),
        "{:?}",
        per_query[1]
    );
    assert!(per_query[1].contains(&plain.scores[1]));
    assert!(per_query[2].iter().all(|&s| s >= raise_to));
    assert!(per_query[0].iter().all(|&s| s >= -0.5));
    // An initial floor from the options behaves like a raised one.
    let mut below = 0;
    index
        .search_streaming_controlled(
            &queries[..dim],
            options.with_minimum_score(0.5),
            &mut |batch| {
                below += batch.scores.iter().filter(|s| **s < 0.5).count();
                VectorStreamControl::Continue
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    assert_eq!(below, 0);
}

#[test]
fn vectors_of_varied_length_keep_recall_under_inner_product() {
    let dim = 32;
    let (mut vectors, queries) = corpus_with_queries(4000, 100, dim, 0x8080);
    let mut rng = Rng(0x8081);
    for row in vectors.chunks_exact_mut(dim) {
        let scale = (0.25 + 3.75 * rng.unit()) as f32; // lengths in 0.25..4
        for x in row {
            *x *= scale;
        }
    }
    for mode in [BuildMode::FixedOrder, BuildMode::Parallel] {
        let index = built(&vectors, dim, mode);
        let r = recall(&index, &vectors, &queries, dim, 10, 128);
        assert!(
            r >= 0.9,
            "{mode:?}: recall@10 {r} with varied lengths at ef 128"
        );
    }
    // Zero rows are tolerated: they score 0 against everything.
    let mut index = HnswIndex::new(dim, config(BuildMode::FixedOrder)).unwrap();
    let mut with_zero = vectors[..100 * dim].to_vec();
    with_zero.extend(std::iter::repeat_n(0.0, dim));
    index.add(&with_zero, dim).unwrap();
    assert_eq!(index.len(), 101);
    let results = index
        .search(
            &queries[..dim],
            101,
            VectorSearchOptions::new().with_candidate_depth(400),
        )
        .unwrap();
    assert!(results.slots.contains(&100));
}

#[test]
fn provider_dispatches_by_kind_and_advertises_configured_ann() {
    let dim = 16;
    let mut index = VectorIndex::create(HNSW, dim, 4).unwrap();
    let descriptor = index.descriptor();
    assert_eq!(descriptor.backend_kind, HNSW);
    assert_eq!(descriptor.quality_contract, QualityContract::ConfiguredAnn);
    assert_eq!(descriptor.metric, "inner_product");
    assert_eq!(descriptor.bits_per_dimension, Some(32));
    assert_eq!(descriptor.dimension, Some(dim));
    assert!(!descriptor
        .capabilities
        .contains(&VectorCapability::ExhaustiveCompletion));
    assert!(descriptor
        .capabilities
        .contains(&VectorCapability::RawVectorRebuild));
    assert!(descriptor
        .capabilities
        .contains(&VectorCapability::DenseMask));
    assert!(!descriptor.scoring_fingerprint.is_empty());
    assert!(!index.is_mapped());

    let config = index.backend_config().unwrap();
    assert_eq!(config.backend_kind, HNSW);
    assert_eq!(
        HnswConfig::from_backend_config(&config).unwrap(),
        HnswConfig::default()
    );
    let rebuilt = VectorIndex::from_backend_config(dim, &config).unwrap();
    assert_eq!(rebuilt.descriptor(), descriptor);
    let fitted = VectorIndex::fit_backend_config(HNSW, dim, 4, &[0.0; 64]).unwrap();
    assert_eq!(fitted, config);
    assert!(VectorIndex::from_backend_config(
        dim,
        &VectorBackendConfig {
            payload: vec![1, 2, 3, 4, 5],
            ..config
        }
    )
    .is_err());

    let vectors = corpus(500, dim, 0x0F0F);
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();
    assert_eq!(index.len(), 500);
    let dir = temp_dir("dispatch");
    let path = dir.join("image");
    index.write(&path).unwrap();
    let loaded = VectorIndex::load(HNSW, &path).unwrap();
    let mapped = VectorIndex::load_mapped(HNSW, &path).unwrap();
    assert!(!loaded.is_mapped() && mapped.is_mapped());
    assert_eq!(loaded.len(), 500);
    let results = mapped.search(&vectors[..dim], 1, VectorSearchOptions::new());
    assert_eq!(results.slots, vec![0]);
    assert_eq!(
        mapped.row_transcript(3).unwrap(),
        index.row_transcript(3).unwrap()
    );
    assert!(VectorIndex::load(EMBEDDED_TURBOVEC, &path).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn query_validation_refuses_malformed_input() {
    let dim = 8;
    let index = built(&corpus(100, dim, 1), dim, BuildMode::FixedOrder);
    assert!(index
        .search(&[1.0; 9], 1, VectorSearchOptions::new())
        .is_err());
    assert!(index
        .search(&[f32::INFINITY; 8], 1, VectorSearchOptions::new())
        .is_err());
    assert!(index
        .search_with_stats(&[1.0; 16], 1, 16, VectorSearchOptions::new())
        .is_err());
    let depth_zero = index
        .search_streaming_controlled(
            &[1.0; 8],
            VectorSearchOptions::new().with_candidate_depth(0),
            &mut |_| VectorStreamControl::Continue,
            &mut || VectorStreamControl::Continue,
        )
        .unwrap_err();
    assert!(depth_zero.to_string().contains("depth 0"));
}

/// Rows of varied length and sign at dimensions that straddle every kernel
/// lane width: every score the provider reports, unary or streamed, is bit
/// for bit `exact_vectors::dot` of the stored row and the query, on
/// whichever kernel this machine runs. Equal scores order by row.
#[test]
fn reported_scores_are_bit_identical_to_the_scalar_scorer_and_ties_order_by_row() {
    for &dim in &[17usize, 65] {
        let mut rng = Rng(0xB175 + dim as u64);
        let rows = 1200;
        let mut vectors: Vec<f32> = (0..rows * dim)
            .map(|_| rng.normal() * (1.0 + 3.0 * rng.unit() as f32))
            .collect();
        // Exact duplicates: rows 40..60 repeat rows 20..40, so the top of
        // every result holds ties that only the row order can break.
        let dup: Vec<f32> = vectors[20 * dim..40 * dim].to_vec();
        vectors[40 * dim..60 * dim].copy_from_slice(&dup);
        let queries: Vec<f32> = (0..4 * dim).map(|_| rng.normal()).collect();
        let index = built(&vectors, dim, BuildMode::Parallel);
        let exact = |row: i64, query: &[f32]| {
            crate::exact_vectors::dot(&vectors[row as usize * dim..][..dim], query)
        };
        let results = index
            .search(
                &queries,
                50,
                VectorSearchOptions::new().with_candidate_depth(128),
            )
            .unwrap();
        for q in 0..4 {
            let query = &queries[q * dim..(q + 1) * dim];
            let slots = results.slots_for_query(q);
            let scores = results.scores_for_query(q);
            for (&slot, &score) in slots.iter().zip(scores) {
                assert!(slot >= 0, "dim {dim} query {q} came back short");
                assert_eq!(
                    score.to_bits(),
                    exact(slot, query).to_bits(),
                    "dim {dim} query {q} row {slot}: {score} is not the scalar score"
                );
            }
            for w in slots.windows(2).zip(scores.windows(2)) {
                let ((a, b), (sa, sb)) = ((w.0[0], w.0[1]), (w.1[0], w.1[1]));
                assert!(
                    sa > sb || (sa == sb && a < b),
                    "dim {dim} query {q}: ({a}, {sa}) before ({b}, {sb})"
                );
            }
        }
        let mut streamed: Vec<(usize, i64, f32)> = Vec::new();
        let summary = index
            .search_streaming_controlled(
                &queries,
                VectorSearchOptions::new().with_candidate_depth(128),
                &mut |batch| {
                    for w in batch.slots.windows(2).zip(batch.scores.windows(2)) {
                        let ((a, b), (sa, sb)) = ((w.0[0], w.0[1]), (w.1[0], w.1[1]));
                        assert!(sa > sb || (sa == sb && a < b), "batch order");
                    }
                    for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                        streamed.push((batch.query_index, slot, score));
                    }
                    VectorStreamControl::Continue
                },
                &mut || VectorStreamControl::Continue,
            )
            .unwrap();
        assert!(summary.completed && !streamed.is_empty());
        for &(q, slot, score) in &streamed {
            let query = &queries[q * dim..(q + 1) * dim];
            assert_eq!(score.to_bits(), exact(slot, query).to_bits());
        }
    }
}

/// The rows behind the graph carry the identity a shared store must match
/// (#71): the same dimension, count and content digest whether the index is
/// on the heap, loaded from its image or served from the map, and the digest
/// is the one the `.exact` sidecar keeps over the same rows.
#[test]
fn row_source_identity_is_stable_and_matches_the_exact_sidecar() {
    let dim = 24;
    let vectors = corpus(700, dim, 0x1D);
    let index = built(&vectors, dim, BuildMode::FixedOrder);
    let identity = index.row_identity();
    assert_eq!((identity.dim, identity.rows), (dim, 700));
    index.with_row_source(|rows| {
        assert_eq!(rows.row(3), &vectors[3 * dim..4 * dim]);
        assert_eq!(rows.values(), &vectors[..]);
    });
    let dir = temp_dir("rows");
    let image = dir.join("image");
    index.write(&image).unwrap();
    assert_eq!(HnswIndex::load(&image).unwrap().row_identity(), identity);
    assert_eq!(
        HnswIndex::load_mapped(&image).unwrap().row_identity(),
        identity
    );
    let exact_path = dir.join("rows.exact");
    crate::exact_vectors::ExactVectorStore::from_values(dim, vectors.clone())
        .unwrap()
        .write(&exact_path)
        .unwrap();
    let sidecar = crate::exact_vectors::ExactVectorStore::open(&exact_path).unwrap();
    assert_eq!(sidecar.payload_sha256(), Some(identity.content_digest));
    // Different rows, different digest.
    let other = built(&corpus(700, dim, 0x1E), dim, BuildMode::FixedOrder).row_identity();
    assert_ne!(other.content_digest, identity.content_digest);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A beam as wide as the graph reaches every row: the unary result is the
/// whole corpus in exact score order, and the stream emits every row once.
/// A hop that finds no unvisited neighbor does not end the walk early.
#[test]
fn a_full_beam_visits_every_reachable_row() {
    let dim = 12;
    let n = 900;
    let (vectors, queries) = corpus_with_queries(n, 2, dim, 0xF011);
    for mode in [BuildMode::FixedOrder, BuildMode::Parallel] {
        let index = built(&vectors, dim, mode);
        for query in queries.chunks_exact(dim) {
            let (hits, stats) = index
                .search_with_stats(query, n, n, VectorSearchOptions::new())
                .unwrap();
            assert_eq!(hits.len(), n, "{mode:?}: not every row was reached");
            // Layer 0 scores every row once; the descent above it scores a few more.
            assert!(stats.scored as usize >= n);
            let truth = exact_top(&vectors, dim, query, n);
            let got: Vec<u32> = hits.iter().map(|h| h.0).collect();
            assert_eq!(got, truth, "{mode:?}: full-beam order is the exact order");
            let mut emitted: Vec<i64> = Vec::new();
            let summary = index
                .search_streaming_controlled(
                    query,
                    VectorSearchOptions::new().with_candidate_depth(n),
                    &mut |batch| {
                        emitted.extend_from_slice(batch.slots);
                        VectorStreamControl::Continue
                    },
                    &mut || VectorStreamControl::Continue,
                )
                .unwrap();
            assert!(summary.completed);
            emitted.sort_unstable();
            assert_eq!(emitted, (0..n as i64).collect::<Vec<_>>(), "{mode:?}");
        }
    }
}
