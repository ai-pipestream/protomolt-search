use pipestream_search::turbo_bridge::splade::SpladeFieldIndex;

#[test]
fn block_skip_must_include_unaligned_query_terms() {
    for scale in [1.0_f32, 0.01] {
        let mut idx = SpladeFieldIndex::default();
        idx.insert_doc(0, scale.to_bits(), &[("b".into(), 10)]);
        for doc in 1..=128 {
            let mut terms = vec![("a".into(), 1)];
            if doc == 2 {
                terms.push(("b".into(), 10));
            }
            idx.insert_doc(doc, scale.to_bits(), &terms);
        }
        idx.insert_doc(129, scale.to_bits(), &[("a".into(), 100)]);
        let query = vec![("a".into(), 1.0), ("b".into(), 1.0)];
        let (hits, stats) = idx.score_top_k(&query, 0.0, 1, |d| d != 129);
        assert_eq!(hits.first().map(|h| h.0), Some(2), "scale={scale}, hits={hits:?}, stats={stats:?}; doc 2 scores 11*scale, doc 0 scores 10*scale");
    }
}

#[test]
fn differing_document_scales_must_not_saturate_and_reverse_ranking() {
    let mut idx = SpladeFieldIndex::default();
    idx.insert_doc(0, 0.00001_f32.to_bits(), &[("x".into(), 65535)]);
    idx.insert_doc(1, 0.00002_f32.to_bits(), &[("x".into(), 65535)]);
    let (hits, _) = idx.score_top_k(&[("x".into(), 1.0)], 0.0, 1, |_| true);
    assert_eq!(
        hits[0].0, 1,
        "doc 1 has twice doc 0's calibrated impact; {hits:?}"
    );
}

#[test]
fn new_column_payloads_are_crc_covered_and_roundtrip() {
    use pipestream_search::postings::{
        AnalyzedDoc, AnalyzedField, Bm25Index, Bm25Reader, Bm25Store,
    };
    let dir = std::env::temp_dir().join(format!("review125-crc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("columns.bm25");
    let mut store = Bm25Store::with_fields(&["body"]).with_positions(&["body"]);
    store.add_document(
        0,
        "joined word".into(),
        AnalyzedDoc {
            fields: vec![AnalyzedField {
                terms: vec![
                    ("joined".into(), 1, vec![(0, 6)]),
                    ("word".into(), 1, vec![(7, 11)]),
                ],
                length: 3,
                positions: Some(vec![vec![0], vec![2]]),
                pos_lengths: Some(vec![vec![2], vec![1]]),
                splade: vec![("neural".into(), 200)],
                splade_scale_bits: 0.01_f32.to_bits(),
                ..Default::default()
            }],
            ..Default::default()
        },
    );
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    reader.verify_integrity().unwrap();
    assert_eq!(reader.posting_pos_lengths("joined", 0), Some(vec![2]));
    assert!(reader.has_splade());
    let original = std::fs::read(&path).unwrap();
    for (name, kind, payload_delta) in [
        ("pos_lengths:body", 18, 16usize),
        ("splade:body", 19, 8usize),
    ] {
        let entry = original
            .windows(name.len())
            .position(|w| w == name.as_bytes())
            .unwrap()
            + name.len();
        assert_eq!(original[entry], kind);
        let offset =
            u64::from_le_bytes(original[entry + 1..entry + 9].try_into().unwrap()) as usize;
        let mut corrupt = original.clone();
        corrupt[offset + payload_delta] ^= 1;
        let corrupt_path = dir.join(format!("bad-{kind}.bm25"));
        std::fs::write(&corrupt_path, corrupt).unwrap();
        if let Ok(r) = Bm25Reader::open(&corrupt_path) {
            assert!(
                r.verify_integrity().is_err(),
                "kind {kind} corruption missed"
            );
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn single_term_ties_and_admission_use_lower_live_doc_id() {
    let mut idx = SpladeFieldIndex::default();
    for d in 0..300 {
        idx.insert_doc(d, 1.0_f32.to_bits(), &[("x".into(), 10)]);
    }
    let (hits, _) = idx.score_top_k(&[("x".into(), 1.0)], 0.0, 3, |d| d >= 2);
    assert_eq!(hits, vec![(2, 10.0), (3, 10.0), (4, 10.0)]);
}

#[test]
fn lattice_slop_rejects_overlap_and_counts_only_gaps() {
    use pipestream_search::postings::{AnalyzedDoc, AnalyzedField, Bm25Store};
    use pipestream_search::proximity::phrase_matches;
    let mut store = Bm25Store::with_fields(&["body"]).with_positions(&["body"]);
    for (doc, court_pos) in [3, 4, 2].into_iter().enumerate() {
        store.add_document(
            doc as u32,
            "NYC court".into(),
            AnalyzedDoc {
                fields: vec![AnalyzedField {
                    terms: vec![
                        ("nyc".into(), 1, vec![(0, 3)]),
                        ("court".into(), 1, vec![(4, 9)]),
                    ],
                    length: 4,
                    positions: Some(vec![vec![0], vec![court_pos]]),
                    pos_lengths: Some(vec![vec![3], vec![1]]),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
    }
    let terms = vec!["nyc".into(), "court".into()];
    assert!(phrase_matches(&store.field(0), &terms, &[0, 1], 0, 0));
    assert!(!phrase_matches(&store.field(0), &terms, &[0, 1], 0, 1));
    assert!(phrase_matches(&store.field(0), &terms, &[0, 1], 1, 1));
    assert!(!phrase_matches(&store.field(0), &terms, &[0, 1], 100, 2));
}

#[test]
fn sparse_block_max_matches_exhaustive_scores_with_mixed_scales() {
    use pipestream_search::turbo_bridge::splade::{quantize_splade_impacts, SpladeSectionReader};
    // Sparse overlaps, >3 blocks, several quantization scales, and exclusions.
    let mut seed = 0x514aad83_u64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for bits in [8, 16] {
        for trial in 0..12 {
            let mut index = SpladeFieldIndex::default();
            let query: Vec<_> = (0..5)
                .map(|i| (format!("t{i}"), (i + 1) as f32 / 3.0))
                .collect();
            let mut all = Vec::new();
            for doc in 0..600 {
                let weights: Vec<_> = (0..5)
                    .filter_map(|i| {
                        let n = next();
                        (n % 3 != 0)
                            .then(|| (format!("t{i}"), ((n >> 16) % 2000 + 1) as f32 / 17.0))
                    })
                    .collect();
                let (scale, impacts, _) = quantize_splade_impacts(&weights, 5, bits);
                index.insert_doc(doc, scale.to_bits(), &impacts);
                let score = query.iter().fold(0.0_f32, |sum, (term, weight)| {
                    sum + impacts
                        .iter()
                        .find(|(t, _)| t == term)
                        .map_or(0.0, |(_, q)| weight * f32::from(*q) * scale)
                });
                if score > 0.0 && (doc + trial) % 7 != 0 {
                    all.push((doc, score));
                }
            }
            all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            let mut bytes = Vec::new();
            index.write(&mut bytes).unwrap();
            assert_eq!(bytes.len() as u64, index.encoded_len());
            let decoded = SpladeFieldIndex::decode(&bytes).unwrap();
            assert_eq!(decoded, index);
            let reader = SpladeSectionReader::open(&bytes).unwrap();
            for k in [1, 3, 20, 100] {
                for slack in [0.0, 0.01, 1.0] {
                    let (hits, _) = reader.score_top_k(&query, slack, k, |d| (d + trial) % 7 != 0);
                    assert_eq!(
                        hits,
                        all[..k],
                        "bits={bits} trial={trial} k={k} slack={slack}"
                    );
                }
            }
        }
    }
}

#[test]
fn uniform_splade_v1_bytes_remain_readable_and_mixed_scales_survive_transpose() {
    use pipestream_search::postings::{AnalyzedDoc, AnalyzedField, Bm25Reader, Bm25Store};
    let mut uniform = SpladeFieldIndex::default();
    uniform.insert_doc(3, 1.0_f32.to_bits(), &[("x".into(), 7)]);
    let mut bytes = Vec::new();
    uniform.write(&mut bytes).unwrap();
    let mut expected = b"TVSPLD01".to_vec();
    expected.extend_from_slice(&1.0_f32.to_le_bytes());
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.extend_from_slice(&1_u16.to_le_bytes());
    expected.push(b'x');
    for value in [1_u32, 1, 3] {
        expected.extend_from_slice(&value.to_le_bytes());
    }
    expected.extend_from_slice(&7_u16.to_le_bytes());
    expected.extend_from_slice(&1_u16.to_le_bytes());
    expected.extend_from_slice(&3_u32.to_le_bytes());
    expected.extend_from_slice(&7_u16.to_le_bytes());
    assert_eq!(bytes, expected);
    assert_eq!(SpladeFieldIndex::decode(&expected).unwrap(), uniform);

    let dir = std::env::temp_dir().join(format!("splade-scales-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut store = Bm25Store::with_fields(&["body"]);
    let scales = [0.00001_f32, 0.00002, 0.5];
    for (row, scale) in scales.iter().enumerate() {
        store.add_document(
            row as u32,
            "x".into(),
            AnalyzedDoc {
                fields: vec![AnalyzedField {
                    terms: vec![("x".into(), 1, vec![(0, 1)])],
                    length: 1,
                    splade: vec![("x".into(), 65535)],
                    splade_scale_bits: scale.to_bits(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
    }
    let path = dir.join("mixed.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    reader.verify_integrity().unwrap();
    let transposed = reader.field(0).transpose().unwrap();
    for (row, scale) in scales.iter().enumerate() {
        let field = transposed.field(row as u32).unwrap();
        assert_eq!(field.splade, vec![("x".into(), 65535)]);
        assert_eq!(field.splade_scale_bits, scale.to_bits());
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn query_lattice_enumerates_paths_and_rejects_excessive_expansion() {
    use pipestream_search::postings::AnalyzedField;
    use pipestream_search::proximity::query_paths;
    let field = AnalyzedField {
        terms: vec![
            ("joined".into(), 1, vec![(0, 6)]),
            ("join".into(), 1, vec![(0, 3)]),
            ("ed".into(), 1, vec![(3, 6)]),
            ("court".into(), 1, vec![(7, 12)]),
        ],
        length: 3,
        positions: Some(vec![vec![0], vec![0], vec![1], vec![2]]),
        pos_lengths: Some(vec![vec![2], vec![1], vec![1], vec![1]]),
        ..Default::default()
    };
    assert_eq!(
        query_paths(&field).unwrap(),
        vec![vec![0, 3], vec![1, 2, 3]]
    );
    let mut wide = AnalyzedField {
        length: 9,
        ..Default::default()
    };
    let mut positions = vec![];
    let mut lengths = vec![];
    for pos in 0..9 {
        for alternative in 0..2 {
            wide.terms
                .push((format!("t{pos}-{alternative}"), 1, vec![(pos, pos + 1)]));
            positions.push(vec![pos]);
            lengths.push(vec![1]);
        }
    }
    wide.positions = Some(positions);
    wide.pos_lengths = Some(lengths);
    assert!(query_paths(&wide).unwrap_err().contains("256 paths"));
}

#[test]
fn wand_pivot_bound_covers_different_float_accumulation_orders() {
    let mut index = SpladeFieldIndex::default();
    index.insert_doc(0, 1.0_f32.to_bits(), &[("z".into(), 1)]);
    for doc in 1..=8 {
        index.insert_doc(doc, 1.0_f32.to_bits(), &[(format!("t{doc}"), 1)]);
    }
    index.insert_doc(
        9,
        1.0_f32.to_bits(),
        &(0..=8).map(|i| (format!("t{i}"), 1)).collect::<Vec<_>>(),
    );
    let mut query = vec![("t0".into(), 1.0)];
    query.extend((1..=8).map(|i| (format!("t{i}"), 0.6 * f32::EPSILON)));
    query.push(("z".into(), 1.0 + 7.0 * f32::EPSILON));
    let (hits, _) = index.score_top_k(&query, 0.0, 1, |_| true);
    assert_eq!(hits, vec![(9, 1.0 + 8.0 * f32::EPSILON)]);
}
