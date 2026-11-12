//! Unit tests of the `hnsw` provider. Everything is seeded.

use std::path::PathBuf;

use super::config::{BuildMode, HnswConfig, CONFIG_FORMAT};
use super::graph::{level_for, MAX_LEVEL};
use super::{HnswIndex, HNSW};
use crate::boolean_bits::Bits;
use crate::vector::{
    FilterRegime, QualityContract, VectorBackendConfig, VectorCapability, VectorIndex,
    VectorProvider, VectorSearchOptions, VectorStreamControl, EMBEDDED_TURBOVEC,
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
        // parallel threshold: appends stay unlinked until `prepare()` / `write()`.
        for row in 3000..3010 {
            index
                .add(&vectors[row * dim..(row + 1) * dim], dim)
                .unwrap();
        }
        index.add(&vectors[3010 * dim..3100 * dim], dim).unwrap();
        index.add(&vectors[3100 * dim..3300 * dim], dim).unwrap();
        index.add(&vectors[3300 * dim..5000 * dim], dim).unwrap();
        assert_eq!(index.len(), 5000);
        assert!(!index.is_graph_linked());

        // Before `prepare()`, the unlinked tail enumerates with exact recall.
        let unlinked = index
            .search(&queries[..dim], 10, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(unlinked.tail_enumerated_rows, 5000);
        let r_unlinked = recall(&index, &vectors, &queries, dim, 10, 64);
        assert_eq!(r_unlinked, 1.0, "{mode:?}: unlinked recall@10 {r_unlinked}");

        // Sealing / preparing builds the graph in bulk across all appended rows.
        index.prepare().unwrap();
        assert!(index.is_graph_linked());
        let linked = index
            .search(&queries[..dim], 10, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(linked.tail_enumerated_rows, 0);
        let r = recall(&index, &vectors, &queries, dim, 10, 64);
        assert!(r >= 0.95, "{mode:?}: recall@10 {r} after seal-time build");
        // The tail rows themselves are found in the sealed graph.
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
    assert!(wide.stats.scored > narrow.stats.scored);
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
    // Zero rows are tolerated both before and after graph build: they score 0
    // against everything.
    let mut index = HnswIndex::new(dim, config(BuildMode::FixedOrder)).unwrap();
    let mut with_zero = vectors[..300 * dim].to_vec();
    with_zero.extend(std::iter::repeat_n(0.0, dim));
    index.add(&with_zero, dim).unwrap();
    assert_eq!(index.len(), 301);
    assert!(!index.is_graph_linked());
    let results = index
        .search(
            &queries[..dim],
            301,
            VectorSearchOptions::new().with_candidate_depth(400),
        )
        .unwrap();
    assert!(results.slots.contains(&300));
    index.prepare().unwrap();
    assert!(index.is_graph_linked());
    let results = index
        .search(
            &queries[..dim],
            301,
            VectorSearchOptions::new().with_candidate_depth(400),
        )
        .unwrap();
    assert!(results.slots.contains(&300));
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
            assert!(stats.stats.scored as usize >= n);
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

/// The exact top `k` of the allowed rows, by the scalar scorer: row and
/// score, best first, ties by row.
fn exact_top_masked(
    vectors: &[f32],
    dim: usize,
    query: &[f32],
    allow: &[bool],
    k: usize,
) -> Vec<(u32, f32)> {
    let mut all: Vec<(u32, f32)> = vectors
        .chunks_exact(dim)
        .enumerate()
        .filter(|(i, _)| allow[*i])
        .map(|(i, row)| (i as u32, crate::exact_vectors::dot(row, query)))
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    all.truncate(k);
    all
}

/// The `percent` of rows whose direction is closest to `anchor`: a filter
/// correlated with a region of the space.
fn region_mask(vectors: &[f32], dim: usize, anchor: &[f32], percent: usize) -> Vec<bool> {
    let n = vectors.len() / dim;
    let keep = exact_top(vectors, dim, anchor, n * percent / 100);
    let mut allow = vec![false; n];
    for row in keep {
        allow[row as usize] = true;
    }
    allow
}

#[test]
fn the_regime_follows_the_mask_cardinality_and_a_pinned_regime_is_honored() {
    let dim = 16;
    let n = 20_000;
    let (vectors, queries) = corpus_with_queries(n, 1, dim, 0x55A1);
    let index = built(&vectors, dim, BuildMode::Parallel);
    let query = &queries[..dim];
    let regime_at = |allowed_every: usize, invert: bool| {
        let allow: Vec<bool> = (0..n).map(|i| (i % allowed_every == 0) != invert).collect();
        index
            .search_with_stats(query, 10, 16, VectorSearchOptions::new().with_mask(&allow))
            .unwrap()
            .1
            .regime
    };
    // ef 16 at M 16: enumerate up to 4 * 16 * 32 = 2048 allowed rows.
    assert_eq!(regime_at(100, false), Some(FilterRegime::Enumerate)); // 200 rows
    assert_eq!(regime_at(4, false), Some(FilterRegime::FilterAware)); // 25%
                                                                      // The plain walk is never chosen, however much the mask allows.
    assert_eq!(regime_at(20, true), Some(FilterRegime::FilterAware)); // 95%
                                                                      // A mask that allows everything is no filter at all.
    assert_eq!(regime_at(1, false), None);
    // A wider beam moves the enumerate threshold with it.
    let allow: Vec<bool> = (0..n).map(|i| i % 4 == 0).collect();
    let (_, answer) = index
        .search_with_stats(query, 10, 64, VectorSearchOptions::new().with_mask(&allow))
        .unwrap();
    assert_eq!(answer.regime, Some(FilterRegime::Enumerate)); // 5000 <= 8192

    for regime in [
        FilterRegime::Enumerate,
        FilterRegime::FilterAware,
        FilterRegime::Plain,
    ] {
        let options = VectorSearchOptions::new()
            .with_mask(&allow)
            .with_filter_regime(regime);
        let (hits, answer) = index.search_with_stats(query, 10, 16, options).unwrap();
        assert_eq!(answer.regime, Some(regime));
        assert_eq!(hits.len(), 10);
        // The mask as a bitmap is the same search.
        let bits = Bits::from_bools(&allow);
        let options = VectorSearchOptions::new()
            .with_bits(&bits)
            .with_filter_regime(regime);
        let (same, as_bits) = index.search_with_stats(query, 10, 16, options).unwrap();
        assert_eq!(same, hits, "{regime:?}");
        assert_eq!(as_bits, answer, "{regime:?}");
        assert!(
            hits.iter().all(|(row, _)| allow[*row as usize]),
            "{regime:?}"
        );
        // No mask, no regime to pin.
        let error = index
            .search_with_stats(
                query,
                10,
                16,
                VectorSearchOptions::new().with_filter_regime(regime),
            )
            .unwrap_err();
        assert!(error.to_string().contains("no mask"), "{error}");
        // An empty mask returns nothing in every regime.
        let none = vec![false; n];
        let options = VectorSearchOptions::new()
            .with_mask(&none)
            .with_filter_regime(regime);
        let (hits, _) = index.search_with_stats(query, 10, 16, options).unwrap();
        assert!(hits.is_empty(), "{regime:?}");
    }
}

#[test]
fn enumerate_is_the_exact_top_k_of_the_mask_on_every_shape() {
    let dim = 17;
    let n = 6000;
    let (mut vectors, queries) = corpus_with_queries(n, 6, dim, 0xE0E0);
    // Rows of very different lengths, negated rows, and a zero row: the
    // kernel bound has to hold for all of them.
    let mut rng = Rng(0xBEEF);
    for (i, row) in vectors.chunks_exact_mut(dim).enumerate() {
        let scale = match i % 5 {
            0 => 1e-3,
            1 => 1.0,
            2 => -7.5,
            3 => 250.0,
            _ => 1.0 + rng.unit() as f32,
        };
        for x in row.iter_mut() {
            *x *= scale;
        }
    }
    vectors[dim * 40..dim * 41].fill(0.0);
    // Exact duplicates: ties are settled by row.
    let (head, tail) = vectors.split_at_mut(dim * 100);
    tail[..dim].copy_from_slice(&head[dim * 7..dim * 8]);
    for mode in [BuildMode::FixedOrder, BuildMode::Parallel] {
        let index = built(&vectors, dim, mode);
        for (every, k) in [(3usize, 10usize), (10, 1), (10, 100), (50, 200), (997, 10)] {
            let allow: Vec<bool> = (0..n)
                .map(|i| i % every == 0 || i == 7 || i == 100)
                .collect();
            let allowed = allow.iter().filter(|&&on| on).count();
            for query in queries.chunks_exact(dim) {
                let options = VectorSearchOptions::new()
                    .with_mask(&allow)
                    .with_filter_regime(FilterRegime::Enumerate);
                let (hits, answer) = index.search_with_stats(query, k, 16, options).unwrap();
                let truth = exact_top_masked(&vectors, dim, query, &allow, k);
                assert_eq!(hits.len(), k.min(allowed));
                assert_eq!(answer.stats.scored as usize, allowed);
                for (got, want) in hits.iter().zip(&truth) {
                    assert_eq!(got.0, want.0, "{mode:?} every {every} k {k}");
                    assert_eq!(got.1.to_bits(), want.1.to_bits());
                }
                // A floor keeps exactly the rows at or above it.
                if let Some(&(_, mid)) = truth.get(truth.len() / 2) {
                    let (floored, _) = index
                        .search_with_stats(query, k, 16, options.with_minimum_score(mid))
                        .unwrap();
                    let want: Vec<u32> = truth.iter().filter(|t| t.1 >= mid).map(|t| t.0).collect();
                    let got: Vec<u32> = floored.iter().map(|h| h.0).collect();
                    assert_eq!(got, want);
                }
            }
        }
    }
}

#[test]
fn the_filter_aware_walk_keeps_recall_and_scores_only_allowed_rows() {
    let dim = 24;
    let n = 30_000;
    let (vectors, queries) = corpus_with_queries(n, 24, dim, 0xAC02);
    let index = built(&vectors, dim, BuildMode::Parallel);
    let k = 10;
    let mut rng = Rng(0x5EED);
    let uncorrelated: Vec<bool> = (0..n).map(|_| rng.next_u64() % 10 < 3).collect();
    // Beam 64 at M 16: the provider enumerates a mask of up to this many
    // rows and walks a larger one.
    let enumerated = super::ENUMERATE_ROWS_PER_WALK_ROW * 64 * 32;
    for (name, percent) in [("uncorrelated", 30), ("near", 30), ("far", 30), ("far", 60)] {
        // The provider's own choice, then the two walks pinned.
        let regimes = [
            None,
            Some(FilterRegime::FilterAware),
            Some(FilterRegime::Plain),
        ];
        let mut found = [0usize; 3];
        let mut scored = [0u64; 3];
        let mut bridges = 0u64;
        let mut total = 0usize;
        for (qi, query) in queries.chunks_exact(dim).enumerate() {
            // "near": the region around the query. "far": the region around
            // the row least like the query, a desert away from where the
            // unfiltered descent lands.
            let allow = match name {
                "uncorrelated" => uncorrelated.clone(),
                "near" => region_mask(&vectors, dim, query, percent),
                _ => {
                    let opposite: Vec<f32> = query.iter().map(|x| -x).collect();
                    let far = exact_top(&vectors, dim, &opposite, 1)[0] as usize;
                    region_mask(&vectors, dim, &vectors[far * dim..][..dim], percent)
                }
            };
            let truth = exact_top_masked(&vectors, dim, query, &allow, k);
            total += truth.len();
            for (slot, regime) in regimes.into_iter().enumerate() {
                let mut options = VectorSearchOptions::new().with_mask(&allow);
                options.filter_regime = regime;
                let (hits, answer) = index.search_with_stats(query, k, 64, options).unwrap();
                assert!(
                    hits.iter().all(|(row, _)| allow[*row as usize]),
                    "query {qi}"
                );
                for (row, score) in &hits {
                    let exact =
                        crate::exact_vectors::dot(&vectors[*row as usize * dim..][..dim], query);
                    assert_eq!(score.to_bits(), exact.to_bits());
                }
                found[slot] += hits
                    .iter()
                    .filter(|h| truth.iter().any(|t| t.0 == h.0))
                    .count();
                scored[slot] += answer.stats.scored;
                match regime {
                    // A mask away from the query is handed to enumerate
                    // before the walk starts: the probe reads links only.
                    None if name == "far" => {
                        assert!(allow.iter().filter(|&&on| on).count() > enumerated);
                        assert_eq!(answer.regime, Some(FilterRegime::Enumerate), "query {qi}");
                        assert!(answer.stats.probed > 0);
                        assert_eq!(answer.stats.bridges, 0);
                        assert_eq!(hits, truth, "query {qi}");
                    }
                    None => {
                        let allowed = allow.iter().filter(|&&on| on).count();
                        let want = if allowed <= enumerated {
                            FilterRegime::Enumerate
                        } else {
                            FilterRegime::FilterAware
                        };
                        assert_eq!(answer.regime, Some(want), "query {qi}");
                    }
                    Some(FilterRegime::FilterAware) => {
                        assert_eq!(answer.regime, regime);
                        assert_eq!(answer.stats.probed, 0);
                        bridges += answer.stats.bridges;
                    }
                    Some(_) => assert_eq!(answer.stats.bridges, 0),
                }
            }
        }
        let recall = found[0] as f64 / total as f64;
        assert!(recall >= 0.95, "{name} {percent}%: recall {recall}");
        if name == "uncorrelated" {
            // The plain walk pays for every disallowed row it routes
            // through; the filter-aware walk looks through them.
            assert!(bridges > 0, "no bridge was looked through");
            assert!(
                scored[1] * 2 < scored[2],
                "filter-aware scored {} rows, plain {}",
                scored[1],
                scored[2]
            );
        }
    }
}

#[test]
fn a_walk_stranded_on_an_island_is_finished_by_enumerate() {
    // A narrow graph and a mask of two kinds of rows: the three nearest the
    // query, and a region away from it too small a share of the rows for
    // the probe to judge. The walk reaches the three and runs dry.
    let dim = 16;
    let n = 8000;
    let (vectors, queries) = corpus_with_queries(n, 4, dim, 0x15A0);
    let mut config = config(BuildMode::FixedOrder);
    config.m = 4;
    let mut index = HnswIndex::new(dim, config).unwrap();
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();
    let k = 10;
    for query in queries.chunks_exact(dim) {
        let opposite: Vec<f32> = query.iter().map(|x| -x).collect();
        let far = exact_top(&vectors, dim, &opposite, 1)[0] as usize;
        let mut allow = region_mask(&vectors, dim, &vectors[far * dim..][..dim], 10);
        for row in exact_top(&vectors, dim, query, 3) {
            allow[row as usize] = true;
        }
        let allowed = allow.iter().filter(|&&on| on).count();
        let truth = exact_top_masked(&vectors, dim, query, &allow, k);
        let options = VectorSearchOptions::new().with_mask(&allow);
        let (hits, answer) = index.search_with_stats(query, k, 16, options).unwrap();
        assert_eq!(answer.regime, Some(FilterRegime::Enumerate));
        // The walk ran (the probe let it) before enumerate scored the mask.
        assert!(answer.stats.bridges > 0);
        assert!(answer.stats.scored as usize > allowed);
        assert_eq!(hits, truth);
        // Pinned, the walk keeps what it reached.
        let pinned = options.with_filter_regime(FilterRegime::FilterAware);
        let (short, answer) = index.search_with_stats(query, k, 16, pinned).unwrap();
        assert_eq!(answer.regime, Some(FilterRegime::FilterAware));
        assert!(short.len() < k);

        // A stream emits what the walk reached, then every other allowed
        // row: each once, each exactly scored.
        let mut rows: Vec<(i64, f32)> = Vec::new();
        let summary = index
            .search_streaming_controlled(
                query,
                options.with_candidate_depth(16),
                &mut |batch| {
                    rows.extend(
                        batch
                            .slots
                            .iter()
                            .copied()
                            .zip(batch.scores.iter().copied()),
                    );
                    VectorStreamControl::Continue
                },
                &mut || VectorStreamControl::Continue,
            )
            .unwrap();
        assert!(summary.completed);
        assert_eq!(summary.emitted, allowed);
        let mut seen: Vec<i64> = rows.iter().map(|r| r.0).collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), allowed);
        for (slot, score) in &rows {
            assert!(allow[*slot as usize]);
            let exact = crate::exact_vectors::dot(&vectors[*slot as usize * dim..][..dim], query);
            assert_eq!(score.to_bits(), exact.to_bits());
        }
    }
}

#[test]
fn streaming_follows_the_regimes() {
    let dim = 16;
    let n = 8000;
    let (vectors, queries) = corpus_with_queries(n, 1, dim, 0x57AE);
    let index = built(&vectors, dim, BuildMode::Parallel);
    let query = &queries[..dim];
    let allow: Vec<bool> = (0..n).map(|i| i % 5 == 0).collect();
    let stream = |regime: FilterRegime, stop_after: Option<usize>| {
        let mut rows: Vec<(i64, f32)> = Vec::new();
        let mut batches = 0usize;
        let options = VectorSearchOptions::new()
            .with_mask(&allow)
            .with_candidate_depth(32)
            .with_filter_regime(regime);
        let summary = index
            .search_streaming_controlled(
                query,
                options,
                &mut |batch| {
                    batches += 1;
                    rows.extend(
                        batch
                            .slots
                            .iter()
                            .copied()
                            .zip(batch.scores.iter().copied()),
                    );
                    match stop_after {
                        Some(limit) if batches >= limit => VectorStreamControl::Stop,
                        _ => VectorStreamControl::Continue,
                    }
                },
                &mut || VectorStreamControl::Continue,
            )
            .unwrap();
        (rows, summary)
    };
    // Enumerate streams every allowed row, each exactly scored, once.
    let (rows, summary) = stream(FilterRegime::Enumerate, None);
    assert!(summary.completed);
    assert_eq!(rows.len(), n / 5);
    assert_eq!(summary.emitted, n / 5);
    let mut seen: Vec<i64> = rows.iter().map(|r| r.0).collect();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen.len(), n / 5);
    for (slot, score) in &rows {
        assert!(allow[*slot as usize]);
        let exact = crate::exact_vectors::dot(&vectors[*slot as usize * dim..][..dim], query);
        assert_eq!(score.to_bits(), exact.to_bits());
    }
    // And stops when told to.
    let (rows, summary) = stream(FilterRegime::Enumerate, Some(2));
    assert!(!summary.completed);
    assert!(rows.len() < n / 5);
    // The walks emit allowed rows only, and the filter-aware one covers the
    // exact top of the mask.
    for regime in [FilterRegime::FilterAware, FilterRegime::Plain] {
        let (rows, summary) = stream(regime, None);
        assert!(summary.completed);
        assert!(
            rows.iter().all(|(slot, _)| allow[*slot as usize]),
            "{regime:?}"
        );
        let truth = exact_top_masked(&vectors, dim, query, &allow, 5);
        let hit = truth
            .iter()
            .filter(|t| rows.iter().any(|r| r.0 == i64::from(t.0)))
            .count();
        assert!(hit >= 4, "{regime:?}: stream covers {hit} of the top 5");
    }
}

#[test]
fn bulk_walk_and_enumerate_visit_identical_nodes_and_scores_to_single_row_dot() {
    use super::distance::Kernel;
    use super::graph::{with_scratch, Filter, Hook, LinkCells, Scored, Store};
    use crate::vector::Mask;
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    struct Trace(Vec<(u32, u32)>);
    impl Hook for Trace {
        fn scored(&mut self, node: u32, score: f32, _row: &[f32]) {
            self.0.push((node, score.to_bits()));
        }
        fn hop(&mut self) -> bool {
            true
        }
    }

    type ScoreTrace = Vec<(u32, u32)>;

    // Reference single-row-dot walk (unfiltered, plain, and filter-aware) to
    // pin that bulk neighbor-list scoring changes neither the visited path
    // nor a single traversal score bit on any kernel or odd dimension.
    fn single_row_walk<L: LinkCells + ?Sized>(
        view: &super::graph::View<'_, L>,
        q: &[f32],
        ef: usize,
        allow: Option<(Mask<'_>, bool)>,
    ) -> (ScoreTrace, ScoreTrace) {
        let row = |n: u32| view.row(n);
        let links = |node: u32, layer: usize| -> Vec<u32> {
            let (cells, cap, start) = if layer == 0 {
                (view.links0, view.m0, node as usize * (view.m0 + 1))
            } else {
                let u = view.upper_slot[node as usize] as usize;
                let base = view.upper_base[u] as usize + (layer - 1) * (view.m + 1);
                (view.upper, view.m, base)
            };
            let count = (cells.get(start) as usize).min(cap);
            (0..count).map(|i| cells.get(start + 1 + i)).collect()
        };
        let mut ep = Scored {
            score: view.kernel.dot(q, row(view.entry)),
            node: view.entry,
        };
        for layer in (1..=view.top as usize).rev() {
            loop {
                let mut improved = false;
                for v in links(ep.node, layer) {
                    let s = view.kernel.dot(q, row(v));
                    if s > ep.score {
                        ep = Scored { score: s, node: v };
                        improved = true;
                    }
                }
                if !improved {
                    break;
                }
            }
        }
        let mut visited = vec![false; view.n()];
        let mut cand = BinaryHeap::new();
        let mut res: BinaryHeap<Reverse<Scored>> = BinaryHeap::new();
        let mut trace = Vec::new();
        visited[ep.node as usize] = true;
        cand.push(ep);
        if allow.is_none_or(|(m, _)| m.test(ep.node as usize)) {
            trace.push((ep.node, ep.score.to_bits()));
            res.push(Reverse(ep));
        }
        while let Some(c) = cand.pop() {
            if res.len() >= ef && c.score < res.peek().unwrap().0.score {
                break;
            }
            match allow {
                Some((mask, true)) => {
                    let mut routes = Vec::new();
                    let mut fresh = Vec::new();
                    for v in links(c.node, 0) {
                        if mask.test(v as usize) {
                            if !visited[v as usize] {
                                visited[v as usize] = true;
                                fresh.push(v);
                            }
                        } else if !visited[v as usize] {
                            routes.push(v);
                        }
                    }
                    for bridge in routes {
                        if fresh.len() >= view.m0 {
                            break;
                        }
                        if visited[bridge as usize] {
                            continue;
                        }
                        visited[bridge as usize] = true;
                        for w in links(bridge, 0) {
                            if fresh.len() >= view.m0 {
                                break;
                            }
                            if mask.test(w as usize) && !visited[w as usize] {
                                visited[w as usize] = true;
                                fresh.push(w);
                            }
                        }
                    }
                    for v in fresh {
                        let s = view.kernel.dot(q, row(v));
                        trace.push((v, s.to_bits()));
                        if res.len() < ef || s > res.peek().unwrap().0.score {
                            let sc = Scored { score: s, node: v };
                            cand.push(sc);
                            res.push(Reverse(sc));
                            if res.len() > ef {
                                res.pop();
                            }
                        }
                    }
                }
                plain => {
                    for v in links(c.node, 0) {
                        if visited[v as usize] {
                            continue;
                        }
                        visited[v as usize] = true;
                        let s = view.kernel.dot(q, row(v));
                        let ok = plain.is_none_or(|(m, _)| m.test(v as usize));
                        if ok {
                            trace.push((v, s.to_bits()));
                        }
                        if res.len() < ef || s > res.peek().unwrap().0.score {
                            let sc = Scored { score: s, node: v };
                            cand.push(sc);
                            if ok {
                                res.push(Reverse(sc));
                                if res.len() > ef {
                                    res.pop();
                                }
                            }
                        }
                    }
                }
            }
        }
        let mut out: Vec<Scored> = res.into_iter().map(|r| r.0).collect();
        out.sort_unstable_by(|a, b| b.cmp(a));
        (
            trace,
            out.into_iter()
                .map(|s| (s.node, s.score.to_bits()))
                .collect(),
        )
    }

    for dim in [17usize, 33, 64, 65] {
        let (vectors, queries) = corpus_with_queries(600, 6, dim, 0x9600 + dim as u64);
        let allow_flags: Vec<bool> = (0..600).map(|i| i % 3 != 0).collect();
        let mask = Mask::Bools(&allow_flags);
        let allowed = mask.count();
        for kernel in Kernel::available() {
            let mut store = Store::new(dim, 16, 64, 7, kernel);
            store.append_unlinked(&vectors).unwrap();
            let links = store.build_links(false).unwrap();
            store.install_links(links);
            let view = store.view();
            for q in queries.chunks_exact(dim) {
                for (filter, spec) in [
                    (Filter::None, None),
                    (Filter::Plain(mask), Some((mask, false))),
                    (
                        Filter::Aware {
                            allow: mask,
                            allowed,
                            adaptive: false,
                            budget: None,
                        },
                        Some((mask, true)),
                    ),
                ] {
                    let mut trace = Trace(Vec::new());
                    let hits = with_scratch(|scratch| {
                        view.walk(
                            q,
                            24,
                            filter,
                            super::graph::Patience::OFF,
                            scratch,
                            &mut trace,
                        )
                        .0
                    });
                    let got_hits: Vec<(u32, u32)> = hits
                        .into_iter()
                        .map(|s| (s.node, s.score.to_bits()))
                        .collect();
                    let (exp_trace, exp_hits) = single_row_walk(&view, q, 24, spec);
                    assert_eq!(trace.0, exp_trace, "{} dim={dim}", kernel.name());
                    assert_eq!(got_hits, exp_hits, "{} dim={dim}", kernel.name());
                }
                let mut surv = Vec::new();
                let count = view.enumerate(q, mask, 10, 1.0e-5, &mut surv);
                assert_eq!(count, allowed as u64);
                for s in &surv {
                    assert_eq!(
                        s.score.to_bits(),
                        kernel.dot(q, view.row(s.node)).to_bits(),
                        "{} dim={dim} enumerate",
                        kernel.name()
                    );
                }
            }
        }
    }
}

#[test]
fn a_walk_that_defeats_the_probe_stops_at_its_visit_budget() {
    use super::graph::{with_scratch, Filter, LinkCells, NoHook, Scored, Walk};
    use crate::vector::{FilterRegimeCounts, Mask};
    use std::collections::HashSet;

    let dim = 32;
    let n = 12_000;
    let mut rng = Rng(0x94B0);
    let mut draw_isotropic = |count: usize| -> Vec<f32> {
        let mut out = Vec::with_capacity(count * dim);
        for _ in 0..count {
            let mut row: Vec<f32> = (0..dim).map(|_| rng.normal()).collect();
            let norm = row.iter().map(|x| x * x).sum::<f32>().sqrt();
            for x in &mut row {
                *x /= norm;
            }
            out.extend_from_slice(&row);
        }
        out
    };
    let vectors = draw_isotropic(n);
    let queries = draw_isotropic(1);
    let mut cfg = config(BuildMode::FixedOrder);
    cfg.m = 6;
    cfg.ef_construction = 48;
    let mut index = HnswIndex::new(dim, cfg).unwrap();
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();
    let query = &queries[..dim];

    // Inspect the upper-layer descent landing point and layer-0 adjacency so
    // the mask deterministically defeats both `Walk::Far` (2-hop probe) and
    // `Walk::Short` (unfilled beam) while walling off the true top-k pocket.
    let (ep, descent_scored, allow) = {
        let super::Backing::Heap(store) = &index.backing else {
            unreachable!()
        };
        let view = store.view();
        let neighbors0 = |node: u32| -> Vec<u32> {
            let start = node as usize * (view.m0 + 1);
            let count = (LinkCells::get(view.links0, start) as usize).min(view.m0);
            (0..count)
                .map(|i| LinkCells::get(view.links0, start + 1 + i))
                .collect()
        };
        let ball_2hop = |seeds: &[u32]| -> HashSet<u32> {
            let mut out: HashSet<u32> = seeds.iter().copied().collect();
            let mut ring1 = Vec::new();
            for &u in seeds {
                for v in neighbors0(u) {
                    if out.insert(v) {
                        ring1.push(v);
                    }
                }
            }
            for u in ring1 {
                for v in neighbors0(u) {
                    out.insert(v);
                }
            }
            out
        };

        // Run a zero-budget walk to read the exact descent score count and `ep`.
        let mut ep = Scored {
            score: view.kernel.dot(query, view.row(view.entry)),
            node: view.entry,
        };
        for layer in (1..=view.top as usize).rev() {
            loop {
                let u = view.upper_slot[ep.node as usize] as usize;
                let start = view.upper_base[u] as usize + (layer - 1) * (view.m + 1);
                let count = (LinkCells::get(view.upper, start) as usize).min(view.m);
                let mut improved = false;
                for i in 0..count {
                    let v = LinkCells::get(view.upper, start + 1 + i);
                    let s = view.kernel.dot(query, view.row(v));
                    if s > ep.score {
                        ep = Scored { score: s, node: v };
                        improved = true;
                    }
                }
                if !improved {
                    break;
                }
            }
        }
        let dummy = vec![false; n];
        let (_, zero_stats, zero_walk) = with_scratch(|scratch| {
            view.walk(
                query,
                16,
                Filter::Aware {
                    allow: Mask::Bools(&dummy),
                    allowed: 0,
                    adaptive: false,
                    budget: Some(0),
                },
                super::graph::Patience::OFF,
                scratch,
                &mut NoHook,
            )
        });
        assert_eq!(zero_walk, Walk::Budget);
        let descent_scored = zero_stats.scored;

        let ranked = exact_top(&vectors, dim, query, n);
        let exact_score =
            |row: u32| crate::exact_vectors::dot(&vectors[row as usize * dim..][..dim], query);

        // Allow 20 mid-low-scoring nodes in `ep`'s 2-hop layer-0 ball (`>= ef = 16`,
        // so `Walk::Short` cannot fire and the 2-hop `far` probe clears easily),
        // leaving thousands of rows strictly below `lowest_decoy_score` for
        // non-interfering cardinality padding and thousands strictly above
        // `best_decoy_score` for uphill wave steps.
        let ep_reach = ball_2hop(&[ep.node]);
        let mut ep_ball: Vec<u32> = ep_reach.iter().copied().collect();
        ep_ball.sort_by(|&a, &b| exact_score(a).total_cmp(&exact_score(b)).then(a.cmp(&b)));
        let decoy_offset = ep_ball.len() / 4;
        let decoys: Vec<u32> = ep_ball.into_iter().skip(decoy_offset).take(20).collect();
        assert_eq!(decoys.len(), 20);
        let best_decoy_score = decoys
            .iter()
            .map(|&u| exact_score(u))
            .fold(f32::NEG_INFINITY, f32::max);
        let mut start_reach = ball_2hop(&decoys);
        start_reach.extend(ep_reach);

        // Isolate a single top hit outside the 2-hop outgoing reach of `ep`
        // and `decoys`, walled off by its 2-hop incoming predecessors (~100
        // rows) so no directed 1-hop bridge step `c -> bridge -> p` can reach
        // it while leaving 99.5% of the positive hemisphere open to the beam.
        let pocket: Vec<u32> = ranked
            .iter()
            .copied()
            .filter(|u| exact_score(*u) > best_decoy_score && !start_reach.contains(u))
            .take(1)
            .collect();
        assert_eq!(pocket.len(), 1, "expected an isolated top-1 pocket row");
        let pocket_set: HashSet<u32> = pocket.iter().copied().collect();
        let mut in_1hop: HashSet<u32> = HashSet::new();
        for u in 0..n as u32 {
            if !pocket_set.contains(&u) && neighbors0(u).iter().any(|v| pocket_set.contains(v)) {
                in_1hop.insert(u);
            }
        }
        let mut moat = in_1hop.clone();
        for u in 0..n as u32 {
            if !pocket_set.contains(&u)
                && neighbors0(u)
                    .iter()
                    .any(|v| pocket_set.contains(v) || in_1hop.contains(v))
            {
                moat.insert(u);
            }
        }

        // Default threshold at `m = 6` (`m0 = 12`), `ef = 32` is `4 * 32 * 12 = 1536`,
        // so `target_allowed = 1550` (`12.9%` uncorrelated sparsity outside `moat`)
        // sets `budget = ceil(1550 / 3.5) = 443` layer-0 work units
        // (`layer0_scored + bridges`), while at `dim = 64` full `m0` lists drive
        // ~40 hops of bridge + neighbor expansions (~700+ work units >> 443).
        let target_allowed = 1_550usize;
        let mut allow = vec![false; n];
        for &u in &pocket {
            allow[u as usize] = true;
        }
        for &u in &decoys {
            allow[u as usize] = true;
        }
        for u in 0..n as u32 {
            if allow.iter().filter(|&&on| on).count() >= target_allowed {
                break;
            }
            if !allow[u as usize] && !moat.contains(&u) && u % 8 == 0 {
                allow[u as usize] = true;
            }
        }
        for &u in ranked.iter().rev() {
            if allow.iter().filter(|&&on| on).count() >= target_allowed {
                break;
            }
            if !allow[u as usize] && !moat.contains(&u) {
                allow[u as usize] = true;
            }
        }
        (ep.node, descent_scored, allow)
    };

    let ep_allowed = u64::from(allow[ep as usize]);
    let k = 10;
    let ef = 32;
    let allowed = allow.iter().filter(|&&on| on).count();
    assert_eq!(allowed, 1_550);
    let truth = exact_top_masked(&vectors, dim, query, &allow, k);

    // 1. Pinned `FilterAware` ignores both the probe and the budget: it fills
    // the beam (`>= k`, not stranded), burns heavy work, and misses the
    // moated pocket.
    let pinned_opts = VectorSearchOptions::new()
        .with_mask(&allow)
        .with_filter_regime(FilterRegime::FilterAware);
    let (pinned_hits, pinned_answer) = index.search_with_stats(query, k, ef, pinned_opts).unwrap();
    assert_eq!(pinned_answer.regime, Some(FilterRegime::FilterAware));
    assert!(!pinned_answer.budget_handoff);
    assert_eq!(pinned_hits.len(), k);
    assert_ne!(
        pinned_hits, truth,
        "moated pocket should be unreachable without budget handoff"
    );

    // 2. Adaptive (`ANN` default): `far` probe runs and does NOT trip
    // (`hops > 0`, `bridges > 0`), `Walk::Budget` fires mid-walk, skips every
    // allowed row the walk already scored, and returns the exact top `k`.
    let adaptive_opts = VectorSearchOptions::new().with_mask(&allow);
    let (hits, answer) = index
        .search_with_stats(query, k, ef, adaptive_opts)
        .unwrap();
    assert_eq!(
        answer.regime,
        Some(FilterRegime::Enumerate),
        "answer={answer:?}, pinned={pinned_answer:?}, descent={descent_scored}"
    );
    assert!(
        answer.budget_handoff,
        "expected Walk::Budget handoff: {answer:?}"
    );
    assert!(
        answer.stats.probed > 0,
        "far probe must have run and cleared"
    );
    assert!(answer.stats.hops > 0 && answer.stats.bridges > 0);
    assert!(
        answer.stats.hops < pinned_answer.stats.hops,
        "budget must stop the walk earlier than pinned ({} vs {})",
        answer.stats.hops,
        pinned_answer.stats.hops
    );
    assert_eq!(
        answer.stats.scored - descent_scored + ep_allowed,
        allowed as u64,
        "every allowed row must be scored at descent/layer-0 + enumerate_skipping exactly once"
    );
    assert_eq!(hits, truth);

    // `VectorProvider::search` reports both `enumerate` and `budget_handoffs`.
    let unary = index
        .search(
            query,
            k,
            VectorSearchOptions::new()
                .with_mask(&allow)
                .with_candidate_depth(ef),
        )
        .unwrap();
    assert_eq!(
        unary.regimes,
        FilterRegimeCounts {
            enumerate: 1,
            filter_aware: 0,
            plain: 0,
            budget_handoffs: 1,
        }
    );

    // 3. Streaming: `Walk::Budget` emits what the walk reached and streams the
    // remaining allowed rows with zero duplicates, reporting `budget_handoffs = 1`.
    let mut streamed: Vec<(i64, f32)> = Vec::new();
    let summary = index
        .search_streaming_controlled(
            query,
            VectorSearchOptions::new()
                .with_mask(&allow)
                .with_candidate_depth(ef),
            &mut |batch| {
                streamed.extend(
                    batch
                        .slots
                        .iter()
                        .copied()
                        .zip(batch.scores.iter().copied()),
                );
                VectorStreamControl::Continue
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    assert!(summary.completed);
    assert_eq!(summary.emitted, allowed);
    assert_eq!(
        summary.regimes,
        FilterRegimeCounts {
            enumerate: 1,
            filter_aware: 0,
            plain: 0,
            budget_handoffs: 1,
        }
    );
    let mut seen_slots: Vec<i64> = streamed.iter().map(|r| r.0).collect();
    seen_slots.sort_unstable();
    seen_slots.dedup();
    assert_eq!(seen_slots.len(), allowed, "stream emitted a row twice");
    for (slot, score) in &streamed {
        assert!(allow[*slot as usize]);
        let exact = crate::exact_vectors::dot(&vectors[*slot as usize * dim..][..dim], query);
        assert_eq!(score.to_bits(), exact.to_bits());
    }

    // 4. Policy `enumerate_max_rows` scales the budget: a higher threshold
    // implies cheaper enumeration relative to walking and stops in fewer hops.
    let (_, loose) = index
        .search_with_stats(
            query,
            k,
            ef,
            VectorSearchOptions::new()
                .with_mask(&allow)
                .with_enumerate_max_rows(400),
        )
        .unwrap();
    let (_, tight) = index
        .search_with_stats(
            query,
            k,
            ef,
            VectorSearchOptions::new()
                .with_mask(&allow)
                .with_enumerate_max_rows(1_536),
        )
        .unwrap();
    assert!(tight.budget_handoff);
    assert!(
        tight.stats.hops < loose.stats.hops,
        "tighter enumerate_max_rows budget should stop in fewer hops ({} vs {})",
        tight.stats.hops,
        loose.stats.hops
    );
}

#[test]
fn tiny_segments_persist_unlinked_and_refuse_hostile_unlinked_headers() {
    use super::MIN_GRAPH_ROWS;

    let dim = 16;
    let rows = 100;
    assert!(rows < MIN_GRAPH_ROWS);
    let (vectors, queries) = corpus_with_queries(rows, 8, dim, 0x9701);
    let mut index = HnswIndex::new(dim, config(BuildMode::Parallel)).unwrap();
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();
    assert!(!index.is_graph_linked());

    let dir = temp_dir("tiny-unlinked");
    let path = dir.join("tiny.hnsw");
    index.write(&path).unwrap();
    let file_len = std::fs::metadata(&path).unwrap().len();
    let bytes_per_row = file_len as f64 / rows as f64;
    // Rows (64 B) + inv_norm (4 B) + 192 B header + 32 B trailer + 64 B section
    // alignment: well under 115 B/row, vs > 200 B/row when links are written.
    assert!(
        bytes_per_row < 115.0,
        "unlinked tiny image wrote {bytes_per_row} B/row"
    );

    let heap = HnswIndex::load(&path).unwrap();
    let mapped = HnswIndex::load_mapped(&path).unwrap();
    assert!(!heap.is_graph_linked() && !mapped.is_graph_linked());
    assert!(!heap.is_mapped() && mapped.is_mapped());
    assert_eq!(heap.row_identity(), index.row_identity());
    assert_eq!(mapped.row_identity(), index.row_identity());

    let all_true = vec![true; rows];
    let allow: Vec<bool> = (0..rows).map(|i| i % 2 == 0).collect();
    let allowed = allow.iter().filter(|&&b| b).count();
    for query in queries.chunks_exact(dim) {
        let truth_all = exact_top_masked(&vectors, dim, query, &all_true, 10);
        let truth_masked = exact_top_masked(&vectors, dim, query, &allow, 10);
        for candidate in [&index, &heap, &mapped] {
            let res = candidate
                .search(query, 10, VectorSearchOptions::new())
                .unwrap();
            assert_eq!(res.tail_enumerated_rows, rows as u64);
            let got: Vec<(u32, f32)> = res
                .slots_for_query(0)
                .iter()
                .zip(res.scores_for_query(0))
                .map(|(&id, &s)| (id as u32, s))
                .collect();
            assert_eq!(got, truth_all);

            let masked = candidate
                .search(query, 10, VectorSearchOptions::new().with_mask(&allow))
                .unwrap();
            assert_eq!(masked.tail_enumerated_rows, allowed as u64);
            assert_eq!(masked.regimes.enumerate, 1);
            let got_masked: Vec<(u32, f32)> = masked
                .slots_for_query(0)
                .iter()
                .zip(masked.scores_for_query(0))
                .map(|(&id, &s)| (id as u32, s))
                .collect();
            assert_eq!(got_masked, truth_masked);

            // Streaming raises floor and counts `tail_enumerated_rows`.
            let floor = truth_all[4].1;
            let mut streamed = Vec::new();
            let summary = candidate
                .search_streaming_controlled(
                    query,
                    VectorSearchOptions::new()
                        .with_candidate_depth(32)
                        .with_minimum_score(floor),
                    &mut |batch| {
                        streamed.extend(
                            batch
                                .slots
                                .iter()
                                .copied()
                                .zip(batch.scores.iter().copied()),
                        );
                        VectorStreamControl::Continue
                    },
                    &mut || VectorStreamControl::Continue,
                )
                .unwrap();
            assert!(summary.completed);
            assert_eq!(summary.tail_enumerated_rows, rows as u64);
            assert!(streamed.iter().all(|&(_, s)| s >= floor));
            for &(id, _) in &truth_all[..5] {
                assert!(streamed.iter().any(|&(slot, _)| slot == i64::from(id)));
            }
        }
    }

    // Hostile `entry == NO_NODE` headers with non-zero `top`, `upper_nodes`,
    // or `upper_cells` must be refused before mapping or allocating links.
    let bytes = std::fs::read(&path).unwrap();
    let header_len = super::image::header_len();
    let reseal = |b: &mut [u8]| {
        let crc = crate::wal::crc32(&b[..136]);
        b[136..140].copy_from_slice(&crc.to_le_bytes());
        let end = b.len() - 32;
        let digest = crate::sha256::digest(&b[..end]);
        b[end..].copy_from_slice(&digest);
    };
    assert_eq!(header_len, 192);
    for (tag, patch_off, patch_len, val, expect) in [
        ("bad-top", 68usize, 4usize, 1u64, "top=1"),
        ("bad-upper-nodes", 48, 8, 1, "upper_nodes=1"),
        ("bad-upper-cells", 56, 8, 17, "upper_cells=17"),
    ] {
        let mut bad = bytes.clone();
        if patch_len == 4 {
            bad[patch_off..patch_off + 4].copy_from_slice(&(val as u32).to_le_bytes());
        } else {
            bad[patch_off..patch_off + 8].copy_from_slice(&val.to_le_bytes());
        }
        reseal(&mut bad);
        let bad_path = dir.join(tag);
        std::fs::write(&bad_path, &bad).unwrap();
        for (how, res) in [
            ("load", HnswIndex::load(&bad_path)),
            ("map", HnswIndex::load_mapped(&bad_path)),
        ] {
            let err = res.err().unwrap_or_else(|| panic!("{how} accepted {tag}"));
            assert!(
                err.to_string().contains(expect),
                "{how} {tag}: {err} (expected {expect:?})"
            );
        }
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn segmented_provider_searches_linked_first_and_enumerates_unlinked_parts() {
    use crate::exact_vectors::ExactVectorStore;
    use crate::live_docs::LiveDocs;
    use crate::postings::{AnalyzedDoc, Bm25Store};
    use crate::segmented_vectors::SegmentedProvider;
    use crate::segments::{SegmentCatalog, SegmentSource, VectorLoad};

    let dim = 24;
    // Segment 0: 600 rows (>= MIN_GRAPH_ROWS, linked).
    // Segment 1: 100 rows (< MIN_GRAPH_ROWS, unlinked tiny segment).
    // Live tail: 150 rows (unlinked heap tail).
    let total = 850;
    let (vectors, queries) = corpus_with_queries(total, 10, dim, 0x9702);
    let cfg = config(BuildMode::FixedOrder);
    let dir = temp_dir("seg-linked-first");
    let catalog = SegmentCatalog::open_with(dir.join("catalog"), VectorLoad::Mapped).unwrap();

    let stage_segment = |seg_id: &str, gen: u64, base: usize, count: usize| {
        let stage = dir.join(seg_id);
        std::fs::create_dir_all(&stage).unwrap();
        let slice = &vectors[base * dim..(base + count) * dim];
        let mut idx = HnswIndex::new(dim, cfg).unwrap();
        idx.add(slice, dim).unwrap();
        let vec_path = stage.join("vector.index");
        idx.write(&vec_path).unwrap();
        let exact_path = stage.join("vectors.f32");
        ExactVectorStore::from_values(dim, slice.to_vec())
            .unwrap()
            .write(&exact_path)
            .unwrap();
        let mut bm25 = Bm25Store::new();
        for id in 0..count as u32 {
            bm25.add_document(id, String::new(), AnalyzedDoc::body(Vec::new(), 0));
        }
        let bm25_path = stage.join("bm25.bin");
        bm25.save(&bm25_path).unwrap();
        let live_path = stage.join("live.bin");
        LiveDocs::default().write(&live_path, count as u64).unwrap();
        catalog
            .append(SegmentSource {
                segment_id: seg_id,
                generation: gen,
                base_label: base as u64,
                backend_kind: HNSW,
                vector_path: Some(&vec_path),
                exact_vector_path: Some(&exact_path),
                bm25_path: &bm25_path,
                live_docs_path: &live_path,
                partition_column: None,
            })
            .unwrap()
    };

    let _ = stage_segment("seg-0001", 1, 0, 600);
    let set = stage_segment("seg-0002", 2, 600, 100);
    assert!(set.vector(0).unwrap().is_graph_linked());
    assert!(!set.vector(1).unwrap().is_graph_linked());

    let tail = HnswIndex::new(dim, cfg).unwrap().into_index();
    let mut provider = SegmentedProvider::open(set, tail).unwrap();
    provider.add(&vectors[700 * dim..total * dim], dim).unwrap();
    assert!(!provider.tail().is_graph_linked());

    let k = 10;
    let mut recall_hits = 0usize;
    for query in queries.chunks_exact(dim) {
        let truth = exact_top(&vectors, dim, query, k);
        let unary = provider
            .search(
                query,
                k,
                VectorSearchOptions::new().with_candidate_depth(64),
            )
            .unwrap();
        // Segment 1 (100 rows) + live tail (150 rows) = 250 unlinked rows.
        assert_eq!(unary.tail_enumerated_rows, 250);
        let slots = unary.slots_for_query(0);
        let scores = unary.scores_for_query(0);
        for (&slot, &score) in slots.iter().zip(scores) {
            let exact = crate::exact_vectors::dot(&vectors[slot as usize * dim..][..dim], query);
            assert_eq!(score.to_bits(), exact.to_bits());
            if truth.iter().any(|&id| i64::from(id) == slot) {
                recall_hits += 1;
            }
        }

        // Controlled streaming raises the floor from Segment 0's HNSW walk so
        // Segment 1 and the tail emit only rows that beat the running top-k.
        let mut streamed_top: Vec<(i64, f32)> = Vec::new();
        let mut emitted_unlinked = 0usize;
        let summary = provider
            .search_streaming_controlled(
                query,
                VectorSearchOptions::new().with_candidate_depth(64),
                &mut |batch| {
                    for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                        if slot >= 600 {
                            emitted_unlinked += 1;
                        }
                        streamed_top.push((slot, score));
                    }
                    streamed_top.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                    streamed_top.truncate(k);
                    if streamed_top.len() == k {
                        VectorStreamControl::RaiseFloor(streamed_top[k - 1].1)
                    } else {
                        VectorStreamControl::Continue
                    }
                },
                &mut || VectorStreamControl::Continue,
            )
            .unwrap();
        assert!(summary.completed);
        assert_eq!(summary.tail_enumerated_rows, 250);
        assert!(
            emitted_unlinked < 100,
            "linked-first floor should prune most of the 250 unlinked rows (emitted {emitted_unlinked})"
        );
        assert_eq!(
            streamed_top.iter().map(|r| r.0).collect::<Vec<_>>(),
            slots,
            "unary and floor-pruned streaming over linked + unlinked parts must agree"
        );
    }
    assert!(
        recall_hits >= 95,
        "segmented recall@10 {recall_hits}/100 across linked + tiny + tail"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn patience_off_reproduces_fixed_order_walk_bit_for_bit() {
    use crate::vector::{PatienceSpec, PatienceWindow};

    let dim = 32;
    let (vectors, queries) = corpus_with_queries(1_200, 12, dim, 0x9501);
    let mut cfg = config(BuildMode::FixedOrder);
    cfg.m = 12;
    cfg.ef_construction = 80;
    let mut index = HnswIndex::new(dim, cfg).unwrap();
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();

    let allow: Vec<bool> = (0..1_200).map(|i| i % 2 == 0).collect();
    for q in queries.chunks_exact(dim) {
        for base_opts in [
            VectorSearchOptions::new(),
            VectorSearchOptions::new()
                .with_mask(&allow)
                .with_filter_regime(FilterRegime::Plain),
            VectorSearchOptions::new()
                .with_mask(&allow)
                .with_filter_regime(FilterRegime::FilterAware),
        ] {
            let (base_hits, base_ans) = index.search_with_stats(q, 10, 96, base_opts).unwrap();
            assert!(!base_ans.patience_stopped);

            for off_spec in [
                PatienceSpec::OFF,
                PatienceSpec {
                    saturation: 0.0, // ignored when patience == 0
                    saturation_ppm: None,
                    patience: 0,
                    ascent_gate: 50,
                    window: PatienceWindow::TopK,
                },
            ] {
                let (off_hits, off_ans) = index
                    .search_with_stats(q, 10, 96, base_opts.with_patience(off_spec))
                    .unwrap();
                assert_eq!(off_ans, base_ans);
                assert_eq!(off_hits.len(), base_hits.len());
                for ((n0, s0), (n1, s1)) in base_hits.iter().zip(off_hits.iter()) {
                    assert_eq!(n0, n1);
                    assert_eq!(s0.to_bits(), s1.to_bits());
                }
            }
        }
    }
}

#[test]
fn patience_early_stop_and_ascent_gate_behave_exact() {
    use crate::vector::{PatienceSpec, PatienceWindow};

    let dim = 32;
    let (vectors, queries) = corpus_with_queries(3_000, 16, dim, 0x9502);
    let mut cfg = config(BuildMode::FixedOrder);
    cfg.m = 12;
    cfg.ef_construction = 96;
    let mut index = HnswIndex::new(dim, cfg).unwrap();
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();

    // Invalid saturation must be rejected when patience > 0.
    for bad in [0.0f32, -0.1, 1.01, f32::NAN, f32::INFINITY] {
        let err = index
            .search_with_stats(
                &queries[..dim],
                10,
                64,
                VectorSearchOptions::new().with_patience(PatienceSpec {
                    saturation: bad,
                    saturation_ppm: None,
                    patience: 7,
                    ascent_gate: 0,
                    window: PatienceWindow::Beam,
                }),
            )
            .unwrap_err();
        assert!(err.to_string().contains("patience saturation"), "{err}");
    }

    // Verify 0.99f32 * 100 binary rounding and exact 990_000 ppm both produce min_survived = 99.
    let p99 = super::graph::Patience {
        saturation: 0.99,
        saturation_ppm: None,
        hops: 7,
        top_k: 100,
        ascent_gate: 0,
        floor_active: false,
    };
    let p99_ppm = super::graph::Patience {
        saturation: 0.99,
        saturation_ppm: Some(990_000),
        hops: 7,
        top_k: 100,
        ascent_gate: 0,
        floor_active: false,
    };
    let p95_10 = super::graph::Patience {
        saturation: 0.95,
        saturation_ppm: None,
        hops: 7,
        top_k: 10,
        ascent_gate: 0,
        floor_active: false,
    };
    let p90_10 = super::graph::Patience {
        saturation: 0.90,
        saturation_ppm: None,
        hops: 7,
        top_k: 10,
        ascent_gate: 0,
        floor_active: false,
    };
    // Exercise both Beam and TopK windows at a wide beam (`ef = 256`) and verify
    // early stopping reduces hops/scored work and respects the ascent gate when
    // a finite floor is active.
    let _ = (p99, p99_ppm, p95_10, p90_10);
    let mut stopped_beam = 0usize;
    let mut stopped_topk = 0usize;
    let mut ascent_delayed = 0usize;
    for q in queries.chunks_exact(dim) {
        let (_, plain) = index
            .search_with_stats(q, 10, 256, VectorSearchOptions::new())
            .unwrap();

        let beam_spec = PatienceSpec::from_ppm(900_000, 5);
        let (_, beam_ans) = index
            .search_with_stats(
                q,
                10,
                256,
                VectorSearchOptions::new().with_patience(beam_spec),
            )
            .unwrap();
        if beam_ans.patience_stopped {
            stopped_beam += 1;
            assert!(beam_ans.stats.hops < plain.stats.hops);
            assert!(beam_ans.stats.scored < plain.stats.scored);
        } else {
            assert_eq!(beam_ans.stats.hops, plain.stats.hops);
        }

        let topk_spec = PatienceSpec {
            saturation: 0.95,
            saturation_ppm: None,
            patience: 5,
            ascent_gate: 0,
            window: PatienceWindow::TopK,
        };
        let (_, topk_ans) = index
            .search_with_stats(
                q,
                10,
                256,
                VectorSearchOptions::new().with_patience(topk_spec),
            )
            .unwrap();
        if topk_ans.patience_stopped {
            stopped_topk += 1;
            assert!(topk_ans.stats.hops < plain.stats.hops);
        }

        // Ascent gate: without a floor (`minimum_score = None`), a huge
        // `ascent_gate` is ignored; with a finite floor (`minimum_score = Some`),
        // `ascent_gate` suppresses patience until that many local allowed rows
        // have been scored.
        let gated_spec = PatienceSpec {
            saturation: 0.95,
            saturation_ppm: None,
            patience: 3,
            ascent_gate: 100_000,
            window: PatienceWindow::TopK,
        };
        let (_, no_floor_ans) = index
            .search_with_stats(
                q,
                10,
                256,
                VectorSearchOptions::new().with_patience(gated_spec),
            )
            .unwrap();
        let (_, with_floor_ans) = index
            .search_with_stats(
                q,
                10,
                256,
                VectorSearchOptions::new()
                    .with_minimum_score(-1.0e6)
                    .with_patience(gated_spec),
            )
            .unwrap();
        assert!(!with_floor_ans.patience_stopped);
        assert_eq!(with_floor_ans.stats.hops, plain.stats.hops);
        if no_floor_ans.patience_stopped {
            ascent_delayed += 1;
            assert!(no_floor_ans.stats.hops < with_floor_ans.stats.hops);
        }
    }
    assert!(
        stopped_beam > 0,
        "expected Beam patience to stop wide ef=256 walks"
    );
    assert!(
        stopped_topk > 0,
        "expected TopK patience to stop wide ef=256 walks"
    );
    assert!(
        ascent_delayed > 0,
        "expected floor-gated ascent_gate to suppress premature patience stops"
    );
}
