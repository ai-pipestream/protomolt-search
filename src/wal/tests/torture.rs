use super::*;

/// The crash contract, exhaustively: for EVERY prefix length of a
/// bucket file, recovery yields exactly the records whose frames
/// fit whole, `valid_len` lands on that frame boundary, and a
/// resumed writer appends cleanly from the cut. One targeted tear
/// (the test above) shows the mechanism works; only the exhaustive
/// sweep shows there is no byte where it does not.
#[test]
fn truncation_at_every_byte_recovers_the_whole_frame_prefix() {
    let dir = tempdir("everybyte");
    let mut writer = WalWriter::create(&dir, manifest(1)).unwrap();
    for id in 0..8u64 {
        writer.append(add_op(id)).unwrap();
        writer.append(doc_op(id)).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    let gen = gen_dir(&dir, 0);
    let intact_path = bucket_path(&gen, 0);
    let intact_bytes = std::fs::read(&intact_path).unwrap();
    let intact_records = read_all(&intact_path).unwrap();
    assert_eq!(intact_records.len(), 16);

    // Frame boundaries: byte length of the file after 0..=16 whole
    // records, from the reader's own offsets.
    let mut boundaries = vec![0u64];
    {
        let mut reader = RecordReader::open(&intact_path).unwrap();
        while reader.next_record().unwrap().is_some() {
            boundaries.push(reader.offset());
        }
    }
    assert_eq!(*boundaries.last().unwrap(), intact_bytes.len() as u64);

    // A second generation directory is the operating table: same
    // manifest, one bucket file we truncate to every length.
    let surgery = tempdir("everybyte_cut");
    let sgen = gen_dir(&surgery, 0);
    std::fs::create_dir_all(&sgen).unwrap();
    write_manifest(&sgen, &manifest(1)).unwrap();
    let cut = bucket_path(&sgen, 0);
    for len in 0..=intact_bytes.len() {
        std::fs::write(&cut, &intact_bytes[..len]).unwrap();
        let whole = boundaries.iter().filter(|&&b| b <= len as u64).count() - 1;

        let scan = scan_records(&cut).unwrap();
        assert_eq!(scan.last_seq, whole as u64, "cut at byte {len}");
        assert_eq!(scan.valid_len, boundaries[whole], "cut at byte {len}");

        // Resume truncates the torn tail and continues the
        // sequence; the recovered prefix is byte-for-byte the
        // intact file's.
        let m = read_manifest(&sgen).unwrap();
        let mut writer = WalWriter::resume(&sgen, m).unwrap();
        writer.append(add_op(1_000 + len as u64)).unwrap();
        writer.flush().unwrap();
        drop(writer);
        let recovered = read_all(&cut).unwrap();
        assert_eq!(recovered.len(), whole + 1, "cut at byte {len}");
        for (a, b) in recovered[..whole].iter().zip(&intact_records) {
            assert_eq!(a.seq, b.seq, "cut at byte {len}");
            assert_eq!(a.op, b.op, "cut at byte {len}");
        }
        assert_eq!(recovered[whole].seq, whole as u64 + 1);
        match &recovered[whole].op {
            Some(wal_record::Op::AddVectors(a)) => {
                assert_eq!(a.first_id, 1_000 + len as u64, "cut at byte {len}");
            }
            other => panic!("cut at byte {len}: unexpected resumed op {other:?}"),
        }
        let after = std::fs::read(&cut).unwrap();
        assert_eq!(
            &after[..boundaries[whole] as usize],
            &intact_bytes[..boundaries[whole] as usize],
            "cut at byte {len}: recovery rewrote committed bytes"
        );
    }
    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(&surgery).ok();
}

/// The other half of the crash contract: a bit flipped in ANY byte
/// of a committed file is detected, never served as a different
/// record. Every parse outcome must be an error or a STRICT prefix
/// of the intact records — crc32 catches all single-bit damage in
/// a frame body, a damaged length prefix reads as a torn or
/// misframed tail, and the per-file seq chain backstops the rest.
#[test]
fn bit_flip_in_every_byte_is_detected_never_reinterpreted() {
    let dir = tempdir("everyflip");
    let mut writer = WalWriter::create(&dir, manifest(1)).unwrap();
    for id in 0..6u64 {
        writer.append(add_op(id)).unwrap();
        writer.append(doc_op(id)).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    let gen = gen_dir(&dir, 0);
    let path = bucket_path(&gen, 0);
    let intact_bytes = std::fs::read(&path).unwrap();
    let intact_records = read_all(&path).unwrap();

    // Parse tolerantly: whatever records come out before the first
    // error or the end.
    let parsed = |p: &Path| -> Vec<WalRecord> {
        let mut out = Vec::new();
        let Ok(mut reader) = RecordReader::open(p) else {
            return out;
        };
        while let Ok(Some(record)) = reader.next_record() {
            out.push(record);
        }
        out
    };

    let flip = tempdir("everyflip_cut").join("bucket-000.wal");
    for i in 0..intact_bytes.len() {
        let mut bytes = intact_bytes.clone();
        bytes[i] ^= 0x01;
        std::fs::write(&flip, &bytes).unwrap();
        let got = parsed(&flip);
        assert!(
            got.len() < intact_records.len(),
            "flip at byte {i}: all {} records parsed despite damage",
            intact_records.len()
        );
        for (a, b) in got.iter().zip(&intact_records) {
            assert_eq!(a.seq, b.seq, "flip at byte {i}");
            assert_eq!(a.op, b.op, "flip at byte {i}: record reinterpreted");
        }
    }
    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(flip.parent().unwrap()).ok();
}

/// The append-only pin, byte-literal: after every append the file
/// is exactly the bytes it was plus one new frame. Nothing
/// committed is ever rewritten — the property every recovery
/// guarantee above rests on.
#[test]
fn committed_bytes_are_never_rewritten_by_later_appends() {
    let dir = tempdir("prefixpin");
    let mut writer = WalWriter::create(&dir, manifest(1)).unwrap();
    let path = bucket_path(&gen_dir(&dir, 0), 0);
    let mut prev = Vec::new();
    for id in 0..12u64 {
        writer
            .append(if id % 2 == 0 { add_op(id) } else { doc_op(id) })
            .unwrap();
        writer.flush().unwrap();
        let now = std::fs::read(&path).unwrap();
        assert!(now.len() > prev.len(), "append {id} wrote nothing");
        assert_eq!(
            &now[..prev.len()],
            &prev[..],
            "append {id} rewrote committed bytes"
        );
        // The manifest rewrite path leaves record bytes alone too.
        writer.update_manifest(|m| m.slot_offset = 42 + id);
        assert_eq!(std::fs::read(&path).unwrap(), now);
        prev = now;
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn seq_gap_is_an_error() {
    let dir = tempdir("seqgap");
    let path = bucket_path(&dir, 0);
    let mut bytes = Vec::new();
    let mut push = |payload: Vec<u8>| {
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);
    };
    push(
        WalRecord {
            seq: 1,
            clock: 1,
            op: Some(wal_record::Op::Flush(FlushMarker {})),
        }
        .encode_to_vec(),
    );
    push(
        WalRecord {
            seq: 3,
            clock: 2,
            op: Some(wal_record::Op::Flush(FlushMarker {})),
        }
        .encode_to_vec(),
    );
    std::fs::write(&path, &bytes).unwrap();

    let mut reader = RecordReader::open(&path).unwrap();
    assert_eq!(reader.next_record().unwrap().unwrap().seq, 1);
    let err = reader.next_record().unwrap_err();
    assert!(err.to_string().contains("seq gap"), "{err}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn retired_manifest_keys_are_refused() {
    for retired_line in [
        "bit_width = 4",
        "calibration_shift = [0.0, 0.0, 0.0, 0.0]",
        "calibration_scale = [1.0, 1.0, 1.0, 1.0]",
    ] {
        let dir = tempdir("retired_manifest_keys");
        write_manifest(&dir, &manifest(4)).unwrap();
        read_manifest(&dir).expect("current manifest reads cleanly");
        let path = manifest_path(&dir);
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, format!("{retired_line}\n{text}")).unwrap();
        let err =
            read_manifest(&dir).expect_err("manifest carrying a retired key must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("unknown field"),
            "unexpected error for `{retired_line}`: {err}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
