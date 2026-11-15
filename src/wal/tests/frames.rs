use super::*;

#[test]
fn crc32_known_vector() {
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    assert_eq!(crc32(b""), 0);
}

#[test]
fn manifest_roundtrip() {
    let dir = tempdir("manifest");
    let gen = gen_dir(&dir, 3);
    std::fs::create_dir_all(&gen).unwrap();
    let m = manifest(64);
    write_manifest(&gen, &m).unwrap();
    assert_eq!(read_manifest(&gen).unwrap(), m);
    // Bad geometry is rejected.
    let mut bad = m.clone();
    bad.bucket_bits = 5;
    write_manifest(&gen, &bad).unwrap();
    assert!(read_manifest(&gen).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn create_or_resume_adopts_an_existing_generation() {
    let dir = tempdir("create_or_resume");
    let mut first = WalWriter::create_or_resume(&dir, manifest(4)).unwrap();
    first.append(add_op(0)).unwrap();
    first.flush().unwrap();
    drop(first);
    // A second open of the same generation resumes it instead of
    // truncating the records the first writer logged.
    let second = WalWriter::create_or_resume(&dir, manifest(4)).unwrap();
    assert_eq!(second.generation(), 0);
    drop(second);
    let path = bucket_path(&gen_dir(&dir, 0), bucket_of(0, 4) as u32);
    assert_eq!(scan_records(&path).unwrap().last_seq, 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn roundtrip_buckets_and_resume() {
    let dir = tempdir("roundtrip");
    let mut writer = WalWriter::create(&dir, manifest(4)).unwrap();
    // Manifest completion is still possible before provider state locks.
    assert!(writer.update_manifest(|m| m.slot_offset = 42));
    for id in 0..20u64 {
        writer.append(add_op(id)).unwrap();
    }
    writer.append(doc_op(3)).unwrap();
    writer
        .append(wal_record::Op::Flush(FlushMarker {}))
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    // Records route to their id's bucket with per-file seq 1..m.
    let gen = gen_dir(&dir, 0);
    let mut total = 0;
    for bucket in 0..4u32 {
        let path = bucket_path(&gen, bucket);
        if !path.exists() {
            continue;
        }
        let records = read_all(&path).unwrap();
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.seq, i as u64 + 1);
            let first_id = match &record.op {
                Some(wal_record::Op::AddVectors(a)) => a.first_id,
                Some(wal_record::Op::AddDocuments(a)) => a.first_id,
                other => panic!("unexpected op in bucket file: {other:?}"),
            };
            assert_eq!(bucket_of(first_id, 4) as u32, bucket);
        }
        total += records.len();
    }
    assert_eq!(total, 21);
    // The flush marker is in markers.wal, seq 1.
    let markers = read_all(&markers_path(&gen)).unwrap();
    assert_eq!(markers.len(), 1);
    assert!(matches!(markers[0].op, Some(wal_record::Op::Flush(_))));

    // Resume: sequences continue, manifest is adopted (and locked).
    let m = read_manifest(&gen).unwrap();
    assert_eq!(m.slot_offset, 42);
    let mut writer = WalWriter::resume(&gen, m).unwrap();
    assert!(!writer.update_manifest(|m| m.slot_offset = 1));
    writer.append(add_op(100)).unwrap();
    writer.flush().unwrap();
    let bucket = bucket_of(100, 4) as u32;
    let records = read_all(&bucket_path(&gen, bucket)).unwrap();
    let bucket_records_before = (0..20u64)
            .filter(|&id| bucket_of(id, 4) as u32 == bucket)
            .count()
            // The document (id 3) rides its bucket too.
            + usize::from(bucket_of(3, 4) as u32 == bucket);
    assert_eq!(
        records.last().unwrap().seq,
        bucket_records_before as u64 + 1
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn torn_tail_on_one_bucket_leaves_others_intact() {
    let dir = tempdir("torn");
    let mut writer = WalWriter::create(&dir, manifest(4)).unwrap();
    for id in 0..20u64 {
        writer.append(add_op(id)).unwrap();
    }
    writer
        .append(wal_record::Op::Snapshot(SnapshotMarker {
            source_generation: 0,
        }))
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let gen = gen_dir(&dir, 0);
    // Tear the tail of bucket 0 (append garbage bytes, then cut mid-frame).
    let torn = bucket_path(&gen, 0);
    let len = std::fs::metadata(&torn).unwrap().len();
    let file = OpenOptions::new().write(true).open(&torn).unwrap();
    file.set_len(len - 3).unwrap();
    drop(file);

    // The torn bucket loses its tail frame but scans cleanly to the
    // valid prefix; resume continues its sequence.
    let scan = scan_records(&torn).unwrap();
    let intact: Vec<u64> = (0..20u64).filter(|&id| bucket_of(id, 4) == 0).collect();
    assert!(scan.last_seq <= intact.len() as u64);
    let m = read_manifest(&gen).unwrap();
    let mut writer = WalWriter::resume(&gen, m).unwrap();
    let extra_id = (20..).find(|&id| bucket_of(id, 4) == 0).unwrap();
    writer.append(add_op(extra_id)).unwrap();
    writer.flush().unwrap();
    drop(writer);
    let records = read_all(&torn).unwrap();
    assert_eq!(
        records.last().unwrap().seq,
        scan.last_seq + 1,
        "resume continues the truncated bucket's sequence"
    );

    // Every other bucket still replays fully.
    for bucket in 1..4u32 {
        let path = bucket_path(&gen, bucket);
        if !path.exists() {
            continue;
        }
        let records = read_all(&path).unwrap();
        let expected = (0..20u64)
            .filter(|&id| bucket_of(id, 4) as u32 == bucket)
            .count();
        assert_eq!(records.len(), expected);
    }
    // Markers file intact.
    assert_eq!(read_all(&markers_path(&gen)).unwrap().len(), 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_record_without_a_logical_clock_is_refused() {
    let dir = tempdir("unclocked");
    let path = dir.join("bucket-000.wal");
    let record = WalRecord {
        seq: 1,
        clock: 0,
        op: Some(add_op(0)),
    };
    let mut file = File::create(&path).unwrap();
    write_frame(&mut file, &record.encode_to_vec()).unwrap();
    drop(file);
    let error = read_all(&path).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("no logical clock"), "{error}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn corruption_is_an_error_with_offset() {
    let dir = tempdir("corrupt");
    let mut writer = WalWriter::create(&dir, manifest(2)).unwrap();
    for id in 0..10u64 {
        writer.append(add_op(id)).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);

    let gen = gen_dir(&dir, 0);
    let path = bucket_path(&gen, 1);
    let len = std::fs::metadata(&path).unwrap().len();
    let mut bytes = std::fs::read(&path).unwrap();
    // Flip a byte inside the LAST frame's payload (payloads here are
    // ~40 bytes, so the tail is payload, not a length prefix — a
    // corrupted length would read as a torn tail instead).
    bytes[len as usize - 3] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    let mut reader = RecordReader::open(&path).unwrap();
    let mut err = None;
    loop {
        match reader.next_record() {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    let err = err.expect("corruption must be detected");
    assert!(err.to_string().contains("byte offset"), "{err}");
    // Bucket 0 is unaffected.
    let other = read_all(&bucket_path(&gen, 0)).unwrap();
    assert_eq!(
        other.len(),
        (0..10u64).filter(|&id| bucket_of(id, 2) == 0).count()
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn truncate_cuts_records_at_or_above_the_applied_tip() {
    let dir = tempdir("truncate");
    let mut writer = WalWriter::create(&dir, manifest(4)).unwrap();
    // Ids 0..12 applied and flushed; 12..20 are the crash-surviving
    // tail the index never persisted.
    for id in 0..20u64 {
        writer.append(add_op(id)).unwrap();
    }
    writer.append(doc_op(20)).unwrap();
    writer
        .append(wal_record::Op::Flush(FlushMarker {}))
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let gen = gen_dir(&dir, 0);
    let dropped = truncate_records_at_or_above(&gen, 12).unwrap();
    assert_eq!(dropped, 9, "ids 12..20 plus the id-20 document");

    // Survivors are exactly ids < 12, each in its bucket, sequences
    // still gapless; the marker file is untouched; resume appends
    // continue cleanly at the cut.
    let mut survivors = 0;
    for bucket in 0..4u32 {
        let path = bucket_path(&gen, bucket);
        if !path.exists() {
            continue;
        }
        for record in read_all(&path).unwrap() {
            match record.op {
                Some(wal_record::Op::AddVectors(a)) => assert!(a.first_id < 12),
                other => panic!("unexpected survivor: {other:?}"),
            }
            survivors += 1;
        }
    }
    assert_eq!(survivors, 12);
    assert_eq!(read_all(&markers_path(&gen)).unwrap().len(), 1);
    let m = read_manifest(&gen).unwrap();
    let mut writer = WalWriter::resume(&gen, m).unwrap();
    writer.append(add_op(12)).unwrap();
    writer.flush().unwrap();
    assert_eq!(truncate_records_at_or_above(&gen, 13).unwrap(), 0);
    std::fs::remove_dir_all(&dir).ok();
}
