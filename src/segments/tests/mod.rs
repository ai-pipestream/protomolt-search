use super::*;

mod publication_tests;
mod retained_tests;
mod row_update_tests;

use crate::postings::{AnalyzedDoc, Bm25Store};
use crate::vector::{VectorIndex, EMBEDDED_TURBOVEC};

struct Fixture {
    root: PathBuf,
    vector: PathBuf,
    exact: PathBuf,
    bm25: PathBuf,
    live_docs: PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn fixture(name: &str, values: &[f32], terms: &[&str]) -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "protomolt-segments-{name}-{}-{}",
        std::process::id(),
        crate::wal::crc32(name.as_bytes())
    ));
    let _ = std::fs::remove_dir_all(&root);
    let source = root.join("source");
    std::fs::create_dir_all(&source).unwrap();
    let dim = 8;
    let config = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, dim, 4, values).unwrap();
    let mut vector = VectorIndex::from_backend_config(dim, &config).unwrap();
    vector.add(values, dim).unwrap();
    vector.prepare().unwrap();
    vector.write(&source.join("vector.index")).unwrap();
    ExactVectorStore::from_values(dim, values.to_vec())
        .unwrap()
        .write(&source.join("vectors.f32"))
        .unwrap();
    let mut bm25 = Bm25Store::new();
    for (row, term) in terms.iter().enumerate() {
        bm25.add_document(
            row as u32,
            format!("{term} document"),
            AnalyzedDoc::body(
                vec![
                    ((*term).to_string(), 1, vec![(0, term.len() as u32)]),
                    (
                        "document".into(),
                        1,
                        vec![(term.len() as u32 + 1, term.len() as u32 + 9)],
                    ),
                ],
                2,
            ),
        );
    }
    bm25.save(&source.join("documents.bm25")).unwrap();
    LiveDocs::default()
        .write(&source.join("live-docs.bin"), terms.len() as u64)
        .unwrap();
    Fixture {
        root,
        vector: source.join("vector.index"),
        exact: source.join("vectors.f32"),
        bm25: source.join("documents.bm25"),
        live_docs: source.join("live-docs.bin"),
    }
}

fn source<'a>(fixture: &'a Fixture, id: &'a str, base: u64) -> SegmentSource<'a> {
    source_generation(fixture, id, base, 1)
}

fn source_generation<'a>(
    fixture: &'a Fixture,
    id: &'a str,
    base: u64,
    generation: u64,
) -> SegmentSource<'a> {
    SegmentSource {
        multi_res_path: None,
        segment_id: id,
        generation,
        base_label: base,
        backend_kind: EMBEDDED_TURBOVEC,
        vector_path: Some(&fixture.vector),
        exact_vector_path: Some(&fixture.exact),
        bm25_path: &fixture.bm25,
        live_docs_path: &fixture.live_docs,
        partition_column: None,
        parents_colocated: false,
    }
}

fn publication_fixture(name: &str, terms: &[&str]) -> Fixture {
    fixture(
        name,
        &[
            1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ],
        terms,
    )
}

fn retire(id: &str, rows: &[u64]) -> SegmentRowRetirement {
    SegmentRowRetirement {
        segment_id: id.into(),
        rows: rows.to_vec(),
    }
}

fn ids(snapshot: &OpenedSegmentSet) -> Vec<u64> {
    let mut ids: Vec<_> = snapshot
        .search_bm25(&["document".into()], Bm25Params::default(), 20)
        .unwrap()
        .into_iter()
        .map(|hit| hit.doc_id)
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn source_owner_metadata_rejects_downgrade_corruption_and_noncanonical_protobuf() {
    let owner = crate::pb::storage::SourceIndexOwner {
        format_version: 1,
        history_id: vec![2; 16],
        index_key: b"index\0key".to_vec(),
        collection: String::new(),
    };
    let valid = SegmentSourceOwner::encode(&owner).unwrap();
    assert_eq!(valid.decode().unwrap(), owner);
    for case in 0..6 {
        let mut invalid = owner.clone();
        match case {
            0 => invalid.format_version = 2,
            1 => invalid.history_id = vec![0; 16],
            2 => invalid.history_id.pop().map(|_| ()).unwrap(),
            3 => invalid.index_key.clear(),
            4 => invalid.index_key = vec![1; 1025],
            _ => invalid.history_id.push(3),
        }
        assert!(SegmentSourceOwner::encode(&invalid).is_err());
    }
    let mut corrupted = valid.clone();
    corrupted.sha256 = "00".repeat(32);
    assert!(corrupted.decode().is_err());
    let mut noncanonical = valid.clone();
    noncanonical.protobuf.extend_from_slice(&[0x08, 0x01]);
    noncanonical.sha256 = turbo_sha256::sha256(&noncanonical.protobuf).to_hex();
    assert!(noncanonical.decode().is_err());
    let mut unknown = valid.clone();
    unknown.protobuf.extend_from_slice(&[0x78, 0x01]);
    unknown.sha256 = turbo_sha256::sha256(&unknown.protobuf).to_hex();
    assert!(unknown.decode().is_err());
    let root = std::env::temp_dir().join(format!("source-owner-format-{}", std::process::id()));
    let valid_manifest = SegmentSetManifest {
        format: 6,
        source_owner: Some(valid),
        ..Default::default()
    };
    OpenedSegmentSet::open_manifest(root.clone(), valid_manifest.clone(), Default::default())
        .unwrap();
    for case in 0..4 {
        let mut invalid = valid_manifest.clone();
        match case {
            0 => invalid.format = 1,
            1 => invalid.format = 2,
            2 => invalid.format = 5,
            _ => invalid.source_owner = Some(corrupted.clone()),
        }
        assert!(
            OpenedSegmentSet::open_manifest(root.clone(), invalid, Default::default()).is_err()
        );
    }
}

#[test]
fn raw_format5_manifest_refuses_by_version_not_missing_field() {
    let root =
        std::env::temp_dir().join(format!("protomolt-segments-format5-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    // A format-5 segments.json as the previous build wrote it: its one
    // segment record has no "parents_colocated" key at all.
    std::fs::write(
        root.join(SET_FILE),
        r#"{"format":5,"epoch":1,"segments":[{"segment_id":"s0","generation":1,"base_label":0,"rows":1,"live_rows":1}]}"#,
    )
    .unwrap();
    for error in [
        OpenedSegmentSet::open(root.clone()).unwrap_err(),
        SegmentCatalog::read_manifest(&root).unwrap_err(),
    ] {
        assert!(
            error.contains("reads only format 6"),
            "version refusal, got: {error}"
        );
        assert!(
            !error.contains("parents_colocated"),
            "must not surface as a serde field error: {error}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}
