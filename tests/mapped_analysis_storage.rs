use pipestream_search::postings::{
    AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder, StoredBinding,
};
use prost::Message;

#[test]
fn explicit_binding_roundtrips_in_both_writers_and_refuses_corruption() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mapped_analysis_storage_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for explicit in [false, true] {
        for rows in [0, 3] {
            let contract = pipestream_search::pb::MappedAnalysisContract {
                fields: vec![pipestream_search::pb::MappedAnalysisColumn {
                    path: "body".into(),
                    name: "body".into(),
                    analysis: Some(pipestream_search::analyzer::body_spec()),
                }],
            }
            .encode_to_vec();
            let mut hasher = pipestream_search::sha256::Sha256::new();
            hasher.update(b"protomolt.search.mapped-analysis.v1\0");
            hasher.update(&contract);
            let binding = StoredBinding {
                plan_fingerprint: "plan".into(),
                body_path: "body".into(),
                materialize_sha: String::new(),
                analysis_sha: if explicit {
                    pipestream_search::sha256::to_hex(&hasher.finalize())
                } else {
                    String::new()
                },
                analysis_contract: if explicit { contract } else { Vec::new() },
                vector_binding: Vec::new(),
            };
            let mut heap = Bm25Store::new();
            let mut spill =
                SpillBuilder::create(&dir.join(format!("spill-{explicit}-{rows}"))).unwrap();
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
            // Non-CRC encoding exercises structural validation independently of CRC.
            let mut raw = Vec::new();
            heap.write_v6_to(&mut raw).unwrap();
            let name = raw
                .windows(12)
                .position(|window| window == b"plan-binding")
                .unwrap();
            let kind = name + 12;
            assert_eq!(raw[kind], if explicit { 12 } else { 6 });
            if explicit {
                let mut cursor = kind + 1;
                for _ in 0..3 {
                    let len =
                        u16::from_le_bytes(raw[cursor..cursor + 2].try_into().unwrap()) as usize;
                    cursor += 2 + len;
                }
                assert_eq!(&raw[cursor..cursor + 2], &64u16.to_le_bytes());
                for bad_byte in [b'G', b'A', 0xff] {
                    let mut bad = raw.clone();
                    bad[cursor + 2] = bad_byte;
                    std::fs::write(&path, bad).unwrap();
                    assert!(Bm25Store::load(&path).is_err());
                    assert!(Bm25Reader::open(&path).is_err());
                }
                for cut in cursor..cursor + 66 {
                    std::fs::write(&path, &raw[..cut]).unwrap();
                    assert!(Bm25Store::load(&path).is_err());
                    assert!(Bm25Reader::open(&path).is_err());
                }
                // An old-kind interpretation cannot silently eat the fourth string.
                raw[kind] = 6;
                std::fs::write(&path, &raw).unwrap();
                assert!(Bm25Reader::open(&path).is_err());
            }
        }
    }
    let mut heap = Bm25Store::new();
    heap.set_binding(Some(StoredBinding {
        analysis_sha: "bad".into(),
        ..Default::default()
    }));
    assert!(heap.save(&dir.join("invalid.bm25")).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}
