use super::*;

fn manifest() -> RepositoryManifest {
    RepositoryManifest {
        format_version: FORMAT_VERSION,
        layout: LAYOUT_SINGLE_IMAGE.into(),
        backend_kind: "embedded-turbovec".into(),
        scoring_fingerprint: "fp".into(),
        dim: 4,
        slot_offset: 100,
        collection: String::new(),
        vector_rows: 2,
        document_rows: 2,
        live_rows: 2,
        analysis_fingerprints: vec![7],
        wal_generation: 1,
        wal_high_watermark: 9,
        wal_clocked: true,
        artifacts: vec![Artifact {
            file: "vector.index".into(),
            bytes: 3,
            sha256: crate::sha256::hex_digest(b"abc"),
        }],
    }
}

#[test]
fn encoding_is_deterministic_and_round_trips() {
    let m = manifest();
    let bytes = m.encode();
    assert_eq!(bytes, m.encode());
    let parsed = RepositoryManifest::parse(&bytes).unwrap();
    assert_eq!(parsed, m);
    assert_eq!(parsed.sha256(), crate::sha256::hex_digest(&bytes));
    let pb = m.to_pb();
    assert_eq!(RepositoryManifest::from_pb(&pb).unwrap(), m);
}

#[test]
fn refuses_escaping_paths_unknown_layouts_and_formats() {
    let mut escaping = manifest();
    escaping.artifacts[0].file = "../vector.index".into();
    assert!(escaping.validate().unwrap_err().contains("stay inside"));
    let mut absolute = manifest();
    absolute.artifacts[0].file = "/etc/passwd".into();
    assert!(absolute.validate().is_err());
    let mut layout = manifest();
    layout.layout = "tar".into();
    assert!(layout.validate().unwrap_err().contains("names layout"));
    let mut format = manifest();
    format.format_version = RETAINED_FORMAT_VERSION + 1;
    assert!(format.validate().unwrap_err().contains("format_version 3"));
    let mut twice = manifest();
    twice.artifacts.push(twice.artifacts[0].clone());
    assert!(twice.validate().unwrap_err().contains("listed twice"));
    let mut digest = manifest();
    digest.artifacts[0].sha256 = "ABC".into();
    assert!(digest.validate().unwrap_err().contains("malformed sha256"));
}

#[test]
fn copy_hash_and_verify_agree_and_name_mismatches() {
    let dir = std::env::temp_dir().join(format!("snap-repo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("src")).unwrap();
    let source = dir.join("src/vector.index");
    std::fs::write(&source, b"abc").unwrap();
    let repo = dir.join("repo");
    let artifact = copy_and_hash(&source, &repo.join("vector.index"), "vector.index").unwrap();
    assert_eq!(artifact, manifest().artifacts[0]);
    assert_eq!(
        hash_file(&repo.join("vector.index")).unwrap(),
        (3, crate::sha256::hex_digest(b"abc"))
    );
    let m = manifest();
    verify_artifacts(&repo, &m).unwrap();
    std::fs::write(repo.join("vector.index"), b"abd").unwrap();
    let error = verify_artifacts(&repo, &m).unwrap_err();
    assert!(
        error.contains("hashes to") && error.contains("vector.index"),
        "{error}"
    );
    std::fs::write(repo.join("vector.index"), b"ab").unwrap();
    let error = verify_artifacts(&repo, &m).unwrap_err();
    assert!(error.contains("has 2 bytes"), "{error}");
    std::fs::remove_file(repo.join("vector.index")).unwrap();
    assert!(verify_artifacts(&repo, &m)
        .unwrap_err()
        .contains("unreadable"));
    let (path, sha) = write_manifest(&repo, &m).unwrap();
    assert_eq!(path, repo.join(MANIFEST_FILE));
    assert_eq!(read_manifest(&repo).unwrap(), (m.clone(), sha.clone()));
    assert!(check_expected_sha("", &sha).is_ok());
    assert!(check_expected_sha(&sha.to_ascii_uppercase(), &sha).is_ok());
    assert!(check_expected_sha("00", &sha)
        .unwrap_err()
        .contains("expected 00"));
    let files = walk_files(&repo).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].0, MANIFEST_FILE);
    let _ = std::fs::remove_dir_all(&dir);
}
