use std::sync::atomic::Ordering;

use prost::Message;

use super::codec::*;
use super::*;
use turbo_sha256::Sha256;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "multi-res-codec-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn row() -> RetainedVector {
    let full = vec![-0.0, f32::from_bits(1), 1.25, -2.5];
    let mut hash = Sha256::new();
    for value in &full {
        hash.update(&value.to_le_bytes());
    }
    RetainedVector {
        output: MultiResVectorOutput {
            full,
            prefixes: vec![(2, vec![-0.0, f32::from_bits(1)])],
            inv_norm: -0.0,
            quantized_codes: Some(vec![0, 255, 17]),
            sentence_vectors: vec![vec![f32::from_bits(0x80000001), 0.0]],
            sha256: hash.finish().0,
        },
    }
}

fn equal_bits(left: &RetainedVector, right: &RetainedVector) {
    let bits = |values: &[f32]| values.iter().map(|v| v.to_bits()).collect::<Vec<_>>();
    assert_eq!(bits(&left.output.full), bits(&right.output.full));
    assert_eq!(
        left.output.inv_norm.to_bits(),
        right.output.inv_norm.to_bits()
    );
    assert_eq!(left.output.quantized_codes, right.output.quantized_codes);
    assert_eq!(left.output.sha256, right.output.sha256);
    assert_eq!(
        left.output
            .prefixes
            .iter()
            .map(|(d, v)| (*d, bits(v)))
            .collect::<Vec<_>>(),
        right
            .output
            .prefixes
            .iter()
            .map(|(d, v)| (*d, bits(v)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        left.output
            .sentence_vectors
            .iter()
            .map(|v| bits(v))
            .collect::<Vec<_>>(),
        right
            .output
            .sentence_vectors
            .iter()
            .map(|v| bits(v))
            .collect::<Vec<_>>()
    );
}

fn framed(rows: &[Row], span: u64) -> Vec<u8> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&span.to_le_bytes());
    bytes.extend_from_slice(&(rows.len() as u64).to_le_bytes());
    for row in rows {
        let payload = row.encode_to_vec();
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&payload);
    }
    let footer = turbo_sha256::sha256(&bytes).0;
    bytes.extend_from_slice(&footer);
    bytes
}
fn stored(slot: u32) -> Row {
    Row {
        slot,
        embedding: Some(embedding(&row())),
    }
}
fn open_bytes(path: &Path, bytes: &[u8], span: u64) -> io::Result<MultiResVectors> {
    std::fs::write(path, bytes).unwrap();
    MultiResVectors::open(path, span, &turbo_sha256::sha256(bytes).to_hex())
}

#[test]
fn payload_and_sidecar_preserve_every_float_bit_and_optional_codes() {
    let directory = Directory::new();
    for codes in [None, Some(Vec::new()), Some(vec![0, 255])] {
        let mut original = row();
        original.output.quantized_codes = codes;
        equal_bits(&original, &decode(&encode(&original).unwrap()).unwrap());
        let mut rows = MultiResVectors::default();
        rows.insert(2, original.clone());
        rows.insert(9, original.clone());
        let path = directory.path("rows");
        let (size, hash) = rows.write(&path, 10).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(size, bytes.len() as u64);
        assert_eq!(hash, turbo_sha256::sha256(&bytes).to_hex());
        let opened = MultiResVectors::open(&path, 10, &hash).unwrap();
        assert_eq!(opened.row_count(), 2);
        assert!(opened.get(0).is_none());
        equal_bits(&original, opened.retained(2).unwrap());
        equal_bits(&original, opened.retained(9).unwrap());
    }
}

#[test]
fn envelope_boundary_is_identical_for_admission_and_sidecar_at_any_slot() {
    let directory = Directory::new();
    let original = row();
    let size = encode(&original).unwrap().len();
    let exact = Limits {
        record: envelope_size(size).unwrap(),
        decoded: MAX_DECODED_BYTES,
    };
    let short = Limits {
        record: exact.record - 1,
        ..exact
    };
    assert!(encode_with_limits(&original, short)
        .unwrap_err()
        .to_string()
        .contains("record exceeds"));
    assert_eq!(encode_with_limits(&original, exact).unwrap().len(), size);
    for slot in [0, u32::MAX] {
        let mut rows = MultiResVectors::default();
        rows.insert(slot, original.clone());
        let target = directory.path("rows");
        let tmp = directory.path("tmp");
        assert!(rows
            .write_at_temp(&target, u64::from(slot) + 1, &tmp, short, |_| Ok(()))
            .is_err());
        assert!(!tmp.exists());
        let (_, hash) = rows
            .write_at_temp(&target, u64::from(slot) + 1, &tmp, exact, |_| Ok(()))
            .unwrap();
        equal_bits(
            &original,
            MultiResVectors::open(&target, u64::from(slot) + 1, &hash)
                .unwrap()
                .retained(slot)
                .unwrap(),
        );
    }
    assert!(bounded_payload(MAX_RECORD_BYTES, LIMITS).is_err());
    assert!(envelope_size(usize::MAX).is_err());
}

#[test]
fn decoded_budget_rejects_before_encoding_or_publishing_an_oversized_row() {
    let directory = Directory::new();
    let original = row();
    let limits = Limits {
        record: MAX_RECORD_BYTES,
        decoded: 1,
    };
    assert!(encode_with_limits(&original, limits)
        .unwrap_err()
        .to_string()
        .contains("allocation exceeds"));
    assert!(
        scan_embedding(&embedding(&original).encode_to_vec(), limits)
            .unwrap_err()
            .to_string()
            .contains("allocation exceeds")
    );
    let mut rows = MultiResVectors::default();
    rows.insert(0, original);
    let target = directory.path("rows");
    let tmp = directory.path("tmp");
    std::fs::write(&target, b"accepted generation").unwrap();
    assert!(rows
        .write_at_temp(&target, 1, &tmp, limits, |_| Ok(()))
        .is_err());
    assert_eq!(std::fs::read(target).unwrap(), b"accepted generation");
    assert!(!tmp.exists());
}

#[test]
fn empty_and_dense_tier_amplification_is_rejected_by_allocation_free_scan() {
    let mut message = embedding(&row());
    message.prefixes = (0..4096)
        .map(|_| Tier {
            dim: 0,
            values: Vec::new(),
        })
        .collect();
    let bytes = message.encode_to_vec();
    assert!(scan_embedding(&bytes, LIMITS)
        .unwrap_err()
        .to_string()
        .contains("invalid multi-resolution vector tier"));
    assert!(decode(&bytes).is_err());
    message.prefixes = (0..4096)
        .map(|_| Tier {
            dim: 1,
            values: vec![1.0],
        })
        .collect();
    let limits = Limits {
        record: MAX_RECORD_BYTES,
        decoded: 4096,
    };
    assert!(scan_embedding(&message.encode_to_vec(), limits)
        .unwrap_err()
        .to_string()
        .contains("allocation exceeds"));
    let mut retained = row();
    retained.output.prefixes = vec![(1, vec![1.0]); 4096];
    assert!(encode_with_limits(&retained, limits).is_err());
}

#[test]
fn malformed_and_semantically_invalid_payloads_are_refused() {
    for change in 0..7 {
        let mut stored = embedding(&row());
        match change {
            0 => stored.full[0] = f32::INFINITY,
            1 => stored.inv_norm = None,
            2 => stored.full.clear(),
            3 => stored.sha256[0] ^= 1,
            4 => stored.prefixes[0].dim = 1,
            5 => stored.sentences[0].values[0] = f32::NAN,
            _ => stored.inv_norm = Some(-1.0),
        }
        assert!(decode(&stored.encode_to_vec()).is_err(), "change {change}");
    }
    let mut bad = encode(&row()).unwrap();
    bad.extend_from_slice(&[0x0a, 1, 0]);
    assert!(
        decode(&bad).is_err(),
        "packed float length must be divisible by four"
    );
    assert!(decode(&[0x80; 11]).is_err(), "oversized key varint");
}

#[test]
fn every_truncation_and_trailing_data_is_refused_even_with_its_own_artifact_hash() {
    let directory = Directory::new();
    let target = directory.path("rows");
    let bytes = framed(&[stored(0)], 1);
    for end in 0..bytes.len() {
        assert!(
            open_bytes(&target, &bytes[..end], 1).is_err(),
            "truncated at {end}"
        );
    }
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(open_bytes(&target, &extra, 1).is_err());
}

#[test]
fn artifact_binding_rejects_other_generation_and_bad_expected_digest() {
    let directory = Directory::new();
    let target = directory.path("rows");
    let first = framed(&[stored(0)], 1);
    let mut other = stored(0);
    other.embedding.as_mut().unwrap().inv_norm = Some(42.0);
    let second = framed(&[other], 1);
    std::fs::write(&target, &second).unwrap();
    assert!(MultiResVectors::open(&target, 1, &turbo_sha256::sha256(&first).to_hex()).is_err());
    assert!(MultiResVectors::open(&target, 1, "").is_err());
    assert!(MultiResVectors::open(&target, 1, &"G".repeat(64)).is_err());
    assert!(MultiResVectors::open(&target, 2, &turbo_sha256::sha256(&second).to_hex()).is_err());
}

#[test]
fn header_footer_slot_and_embedded_digest_corruption_are_refused() {
    let directory = Directory::new();
    let target = directory.path("rows");
    for bytes in [
        framed(&[stored(0), stored(0)], 2),
        framed(&[stored(1), stored(0)], 2),
        framed(&[stored(2)], 2),
        framed(
            &[Row {
                slot: 0,
                embedding: None,
            }],
            1,
        ),
    ] {
        assert!(open_bytes(&target, &bytes, 2).is_err());
    }
    let valid = framed(&[stored(0)], 1);
    for index in [0, 8, 16, valid.len() - 1] {
        let mut bad = valid.clone();
        bad[index] ^= 1;
        assert!(open_bytes(&target, &bad, 1).is_err(), "byte {index}");
    }
    let mut bad = stored(0);
    bad.embedding.as_mut().unwrap().sha256[0] ^= 1;
    assert!(open_bytes(&target, &framed(&[bad], 1), 1).is_err());
}

#[test]
fn oversized_record_length_refuses_before_reading_its_payload() {
    let directory = Directory::new();
    let target = directory.path("rows");
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&1u64.to_le_bytes());
    bytes.extend_from_slice(&1u64.to_le_bytes());
    bytes.extend_from_slice(&((MAX_RECORD_BYTES + 1) as u32).to_le_bytes());
    let error = open_bytes(&target, &bytes, 1).err().unwrap();
    assert!(error.to_string().contains("record exceeds size bound"));
}

#[test]
fn collision_preserves_unowned_temp_and_existing_destination() {
    let directory = Directory::new();
    let target = directory.path("rows");
    let tmp = directory.path("tmp");
    std::fs::write(&target, b"old generation").unwrap();
    std::fs::write(&tmp, b"someone else's temp").unwrap();
    let mut rows = MultiResVectors::default();
    rows.insert(0, row());
    assert_eq!(
        rows.write_at_temp(&target, 1, &tmp, LIMITS, |_| Ok(()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(std::fs::read(target).unwrap(), b"old generation");
    assert_eq!(std::fs::read(tmp).unwrap(), b"someone else's temp");
}

#[test]
fn pre_rename_failure_cleans_owned_temp_and_post_rename_error_reports_uncertainty() {
    let directory = Directory::new();
    let tmp = directory.path("tmp");
    let target = directory.path("rows");
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("keep"), b"old").unwrap();
    let mut rows = MultiResVectors::default();
    rows.insert(0, row());
    assert!(rows
        .write_at_temp(&target, 1, &tmp, LIMITS, |_| Ok(()))
        .is_err());
    assert!(!tmp.exists());
    assert_eq!(std::fs::read(target.join("keep")).unwrap(), b"old");
    std::fs::remove_dir_all(&target).unwrap();
    let error = rows
        .write_at_temp(&target, 1, &tmp, LIMITS, |_| {
            Err(io::Error::other("injected directory fsync failure"))
        })
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("publication uncertain after rename"));
    assert!(!tmp.exists());
    let bytes = std::fs::read(&target).unwrap();
    equal_bits(
        &row(),
        MultiResVectors::open(&target, 1, &turbo_sha256::sha256(&bytes).to_hex())
            .unwrap()
            .retained(0)
            .unwrap(),
    );
}

#[test]
fn sparse_range_append_and_last_u32_slot_preserve_rows_without_partial_overwrite() {
    let mut rows = MultiResVectors::default();
    rows.insert(2, row());
    rows.insert(9, row());
    let part = rows.range(2, 8).unwrap();
    assert_eq!(part.row_count(), 2);
    let mut joined = MultiResVectors::default();
    joined.append_at(10, &part).unwrap();
    assert!(joined.get(10).is_some());
    assert!(joined.get(17).is_some());
    assert!(joined.append_at(10, &part).is_err());
    assert_eq!(joined.row_count(), 2);
    rows.insert(u32::MAX, row());
    let last = rows.range(u32::MAX, 1).unwrap();
    equal_bits(&row(), last.retained(0).unwrap());
    assert!(rows.range(u32::MAX, 2).is_err());
    assert!(joined.append_at(u32::MAX, &part).is_err());
    assert_eq!(joined.row_count(), 2);
    assert!(rows.validate_rows(u64::from(u32::MAX)).is_err());
    rows.validate_rows(u64::from(u32::MAX) + 1).unwrap();
}

#[test]
fn empty_generation_round_trips_and_parent_creation_is_not_claimed_durable() {
    let directory = Directory::new();
    let target = directory.path("rows");
    let rows = MultiResVectors::default();
    assert!(rows.is_empty());
    let (bytes, hash) = rows.write(&target, 0).unwrap();
    assert_eq!(bytes, 56);
    assert!(MultiResVectors::open(&target, 0, &hash).unwrap().is_empty());
    let missing = directory.path("missing/rows");
    assert!(rows.write(&missing, 0).is_err());
    assert!(!directory.path("missing").exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(target).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
