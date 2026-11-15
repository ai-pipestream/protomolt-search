use super::*;

fn source_op(id: u64, source: &crate::pb::ProtobufSource) -> wal_record::Op {
    let wal_record::Op::AddDocuments(mut batch) = doc_op(id) else {
        unreachable!()
    };
    batch.documents[0].original_source = Some(source.clone());
    batch.documents[0].source_chunk_ordinal = Some(id as u32);
    wal_record::Op::AddDocuments(batch)
}

#[test]
fn explicit_analysis_bindings_need_a_valid_digest() {
    let dir = tempdir("mapped-analysis");
    let mut writer = WalWriter::create(&dir, manifest(2)).unwrap();
    let mut binding = crate::pb::wal::LoggedBinding {
        plan_fingerprint: "plan".into(),
        body_path: "body".into(),
        analysis_sha: String::new(),
        ..Default::default()
    };
    writer
        .append(wal_record::Op::Bind(binding.clone()))
        .unwrap();
    binding.analysis_sha = "invalid".into();
    assert!(writer
        .append(wal_record::Op::Bind(binding.clone()))
        .is_err());
    assert_eq!(writer.high_watermark(), 1);
    let contract = crate::pb::MappedAnalysisContract {
        fields: vec![crate::pb::MappedAnalysisColumn {
            path: "body".into(),
            name: "body".into(),
            analysis: Some(crate::analyzer::body_spec()),
        }],
    }
    .encode_to_vec();
    let mut hasher = crate::sha256::Sha256::new();
    hasher.update(b"protomolt.search.mapped-analysis.v1\0");
    hasher.update(&contract);
    binding.analysis_sha = crate::sha256::to_hex(&hasher.finalize());
    binding.analysis_contract = contract;
    writer
        .append(wal_record::Op::Bind(binding.clone()))
        .unwrap();
    writer.flush().unwrap();
    let generation = writer.dir().to_owned();
    drop(writer);
    let records = read_clocked_records(&generation, 0).unwrap();
    assert_eq!(records[1].op, Some(wal_record::Op::Bind(binding)));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn identified_rows_survive_replay() {
    let dir = tempdir("identity");
    let source = crate::pb::ProtobufSource {
        descriptor_set: b"original descriptor".to_vec(),
        message_type: "example.Record".into(),
        payload: vec![8, 0x81, 0],
    };
    let mut writer = WalWriter::create(&dir, manifest(2)).unwrap();
    let mut op = source_op(0, &source);
    let wal_record::Op::AddDocuments(batch) = &mut op else {
        unreachable!()
    };
    batch.documents[0].identity = Some(crate::pb::DocumentIdentity {
        document_key: b"key\0\xff".to_vec(),
        version: 7,
        chunk_ordinal: Some(1),
    });
    assert_eq!(
        writer.append(op.clone()).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(writer.high_watermark(), 0);
    let wal_record::Op::AddDocuments(batch) = &mut op else {
        unreachable!()
    };
    batch.documents[0].identity.as_mut().unwrap().chunk_ordinal = Some(0);
    writer.append(op.clone()).unwrap();
    writer.flush().unwrap();
    let generation = writer.dir().to_path_buf();
    drop(writer);
    let records = read_clocked_records(&generation, 0).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].op, Some(op));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn original_sources_are_interned_across_buckets_resume_and_replication() {
    let dir = tempdir("source-intern");
    let source = crate::pb::ProtobufSource {
        descriptor_set: b"exact producer descriptor".to_vec(),
        message_type: "example.Parent".into(),
        payload: vec![0xab; 128 * 1024],
    };
    let mut writer = WalWriter::create(&dir, manifest(8)).unwrap();
    writer.append(source_op(0, &source)).unwrap();
    writer.flush().unwrap();
    let generation = writer.dir().to_path_buf();
    let source_path = generation.join("sources.wal");
    let source_size = std::fs::metadata(&source_path).unwrap().len();
    for id in 1..100 {
        writer.append(source_op(id, &source)).unwrap();
    }
    writer.flush().unwrap();
    assert_eq!(std::fs::metadata(&source_path).unwrap().len(), source_size);
    drop(writer);
    let mut writer = WalWriter::resume(&generation, read_manifest(&generation).unwrap()).unwrap();
    writer.append(source_op(100, &source)).unwrap();
    writer.flush().unwrap();
    assert_eq!(std::fs::metadata(&source_path).unwrap().len(), source_size);
    let row_bytes: u64 = std::fs::read_dir(&generation)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| parse_bucket_name(&e.file_name().to_string_lossy()).is_some())
        .map(|e| e.metadata().unwrap().len())
        .sum();
    assert!(
        row_bytes < source.payload.len() as u64,
        "chunk records duplicated the original"
    );
    // A replica tail beginning after the original's first row remains
    // self-contained; it does not depend on that earlier row or bucket.
    let tail = read_clocked_records(&generation, 100).unwrap();
    assert_eq!(tail.len(), 1);
    let Some(wal_record::Op::AddDocuments(batch)) = &tail[0].op else {
        panic!()
    };
    assert!(batch.source_references.is_empty());
    assert_eq!(batch.documents[0].original_source.as_ref(), Some(&source));
    assert_eq!(batch.documents[0].source_chunk_ordinal, Some(100));
    let replica_dir = dir.join("replica");
    let mut replica = WalWriter::create(&replica_dir, manifest(8)).unwrap();
    replica.append(tail[0].op.clone().unwrap()).unwrap();
    replica.flush().unwrap();
    let replay = read_clocked_records(replica.dir(), 0).unwrap();
    assert_eq!(replay[0].op, tail[0].op);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn source_replay_refuses_missing_corrupt_and_truncated_referenced_bytes() {
    let dir = tempdir("source-corruption");
    let source = crate::pb::ProtobufSource {
        descriptor_set: vec![1, 2, 3],
        message_type: "x.Doc".into(),
        payload: vec![4, 5, 6],
    };
    let mut writer = WalWriter::create(&dir, manifest(2)).unwrap();
    writer.append(source_op(0, &source)).unwrap();
    writer.flush().unwrap();
    let generation = writer.dir().to_path_buf();
    drop(writer);
    let path = generation.join("sources.wal");
    let bytes = std::fs::read(&path).unwrap();
    for at in 0..bytes.len() {
        std::fs::write(&path, &bytes[..at]).unwrap();
        assert!(
            read_clocked_records(&generation, 0).is_err(),
            "truncation {at}"
        );
        let mut corrupt = bytes.clone();
        corrupt[at] ^= 1;
        std::fs::write(&path, corrupt).unwrap();
        assert!(
            read_clocked_records(&generation, 0).is_err(),
            "corruption {at}"
        );
    }
    std::fs::remove_file(&path).unwrap();
    assert!(read_clocked_records(&generation, 0).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn unused_torn_source_tail_is_removed_before_the_next_source_append() {
    let dir = tempdir("source-torn-tail");
    let mut source = crate::pb::ProtobufSource {
        descriptor_set: vec![1, 2, 3],
        message_type: "x.Doc".into(),
        payload: vec![4, 5, 6],
    };
    let mut writer = WalWriter::create(&dir, manifest(2)).unwrap();
    writer.append(source_op(0, &source)).unwrap();
    writer.flush().unwrap();
    let generation = writer.dir().to_path_buf();
    drop(writer);
    let path = generation.join("sources.wal");
    let valid_bytes = std::fs::read(&path).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[99, 0, 0, 0, 1, 2])
        .unwrap();
    let mut writer = WalWriter::resume(&generation, read_manifest(&generation).unwrap()).unwrap();
    source.payload.push(7);
    writer.append(source_op(1, &source)).unwrap();
    writer.flush().unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..valid_bytes.len()], valid_bytes);
    let records = read_clocked_records(&generation, 0).unwrap();
    let Some(wal_record::Op::AddDocuments(batch)) = &records[1].op else {
        panic!()
    };
    assert_eq!(batch.documents[0].original_source.as_ref(), Some(&source));
    std::fs::remove_dir_all(dir).unwrap();
}
