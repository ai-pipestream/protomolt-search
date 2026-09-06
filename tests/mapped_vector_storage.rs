use pipestream_search::{
    mapping::derive_plan,
    pb::{MappedAnalysisColumn, MappedAnalysisContract},
    postings::{AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder, StoredBinding},
};
use prost::Message;

fn binding(explicit_analysis: bool) -> StoredBinding {
    let plan = derive_plan(
        include_bytes!("fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap();
    let mut binding = StoredBinding {
        plan_fingerprint: plan.fingerprint,
        body_path: "body".into(),
        vector_binding: plan.vector_binding.unwrap().encode_to_vec(),
        ..Default::default()
    };
    if explicit_analysis {
        binding.analysis_contract = MappedAnalysisContract {
            fields: vec![MappedAnalysisColumn {
                path: "body".into(),
                name: "body".into(),
                analysis: Some(pipestream_search::analyzer::body_spec()),
            }],
        }
        .encode_to_vec();
        let mut hasher = pipestream_search::sha256::Sha256::new();
        hasher.update(b"protomolt.search.mapped-analysis.v1\0");
        hasher.update(&binding.analysis_contract);
        binding.analysis_sha = pipestream_search::sha256::to_hex(&hasher.finalize());
    }
    binding
}

#[test]
fn named_vector_binding_survives_both_writers_and_empty_stores() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mapped_vector_storage_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for explicit_analysis in [false, true] {
        for rows in [0, 3] {
            let binding = binding(explicit_analysis);
            let mut heap = Bm25Store::new();
            let mut spill =
                SpillBuilder::create(&dir.join(format!("spill-{explicit_analysis}-{rows}")))
                    .unwrap();
            heap.set_binding(Some(binding.clone()));
            spill.set_binding(Some(binding.clone()));
            for row in 0..rows {
                let analyzed = AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1);
                heap.add_document(row, "word".into(), analyzed.clone());
                spill
                    .add_document_with_lineage(row, "word".into(), analyzed, None)
                    .unwrap();
            }
            let path = dir.join("heap.bm25");
            let spilled = dir.join("spill.bm25");
            heap.save(&path).unwrap();
            spill.finish(&spilled).unwrap();
            assert_eq!(
                std::fs::read(&path).unwrap(),
                std::fs::read(&spilled).unwrap()
            );
            assert_eq!(Bm25Store::load(&path).unwrap().binding(), Some(&binding));
            let mapped = Bm25Reader::open(&path).unwrap();
            assert_eq!(mapped.binding(), Some(&binding));
            mapped.verify_integrity().unwrap();
            drop(mapped);

            // Exercise structural checks without the v8 CRC envelope masking failures.
            let mut raw = Vec::new();
            heap.write_v6_to(&mut raw).unwrap();
            let kind = raw.windows(12).position(|v| v == b"plan-binding").unwrap() + 12;
            assert_eq!(raw[kind], 13);
            let mut cursor = kind + 1;
            for _ in 0..4 {
                let len = u16::from_le_bytes(raw[cursor..cursor + 2].try_into().unwrap()) as usize;
                cursor += 2 + len;
            }
            let analysis_len =
                u32::from_le_bytes(raw[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4 + analysis_len;
            let vector_len =
                u32::from_le_bytes(raw[cursor..cursor + 4].try_into().unwrap()) as usize;
            assert_eq!(
                &raw[cursor + 4..cursor + 4 + vector_len],
                binding.vector_binding
            );
            std::fs::write(&path, &raw).unwrap();
            assert_eq!(Bm25Reader::open(&path).unwrap().binding(), Some(&binding));
            for cut in cursor..cursor + 4 + vector_len {
                std::fs::write(&path, &raw[..cut]).unwrap();
                assert!(Bm25Store::load(&path).is_err());
                assert!(Bm25Reader::open(&path).is_err());
            }
            for old_kind in [6, 12] {
                let mut bad = raw.clone();
                bad[kind] = old_kind;
                std::fs::write(&path, bad).unwrap();
                assert!(Bm25Store::load(&path).is_err());
                assert!(Bm25Reader::open(&path).is_err());
            }
            for replacement in [vec![], vec![8, 2], {
                let mut bytes = binding.vector_binding.clone();
                bytes.extend([8, 1]); // duplicate version, noncanonical
                bytes
            }] {
                let mut bad = raw[..cursor].to_vec();
                bad.extend((replacement.len() as u32).to_le_bytes());
                bad.extend(replacement);
                bad.extend(&raw[cursor + 4 + vector_len..]);
                std::fs::write(&path, bad).unwrap();
                assert!(Bm25Store::load(&path).is_err());
                assert!(Bm25Reader::open(&path).is_err());
            }
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn writers_refuse_vector_declarations_from_another_plan() {
    let mut invalid = binding(false);
    invalid.plan_fingerprint = "0".repeat(64);
    let mut store = Bm25Store::new();
    store.set_binding(Some(invalid));
    assert!(store.write_v6_to(&mut Vec::new()).is_err());
}
