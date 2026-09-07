//! Read the short-lived main-branch derived tag without reinterpreting maps.
//!
//! Called only for generations whose manifest proves the legacy declaration.
//! Unknown fields are copied byte for byte; traversal follows only the two
//! envelope fields containing documents. CRC verification precedes this step.

use prost::encoding::{
    decode_key, decode_varint, encode_key, encode_varint, skip_field, DecodeContext, WireType,
};
use std::io;

fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

pub(super) fn upgrade_record(bytes: &[u8], fingerprint: &str) -> io::Result<Vec<u8>> {
    upgrade(bytes, fingerprint.as_bytes(), 0)
}

fn upgrade(bytes: &[u8], fingerprint: &[u8], level: u8) -> io::Result<Vec<u8>> {
    let mut remaining = bytes;
    let mut output = Vec::with_capacity(bytes.len());
    let mut legacy = false;
    let mut current = false;
    while !remaining.is_empty() {
        let start = remaining;
        let (tag, wire) = decode_key(&mut remaining).map_err(invalid)?;
        let nested = (level == 0 && tag == 3) || (level == 1 && tag == 2);
        let document_field = level == 2 && matches!(tag, 27 | 29);
        if wire == WireType::LengthDelimited && (nested || document_field) {
            let len = usize::try_from(decode_varint(&mut remaining).map_err(invalid)?)
                .map_err(|_| invalid("legacy derived WAL length overflows"))?;
            let payload = remaining
                .get(..len)
                .ok_or_else(|| invalid("legacy derived WAL field is truncated"))?;
            remaining = &remaining[len..];
            if nested {
                let inner = upgrade(payload, fingerprint, level + 1)?;
                encode_key(tag, wire, &mut output);
                encode_varint(inner.len() as u64, &mut output);
                output.extend_from_slice(&inner);
                continue;
            }
            if tag == 29 {
                current = true;
            } else if payload.len() == 64
                && payload
                    .iter()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                if legacy || payload != fingerprint {
                    return Err(invalid(
                        "legacy derived WAL fingerprint is duplicated or differs from its manifest",
                    ));
                }
                legacy = true;
                encode_key(29, wire, &mut output);
                encode_varint(payload.len() as u64, &mut output);
                output.extend_from_slice(payload);
                continue;
            }
        } else {
            skip_field(wire, tag, &mut remaining, DecodeContext::default()).map_err(invalid)?;
        }
        output.extend_from_slice(&start[..start.len() - remaining.len()]);
    }
    if legacy && current {
        return Err(invalid(
            "legacy derived WAL document carries both fingerprint tags 27 and 29",
        ));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pb,
        wal::{self, RecordReader, WalManifest, WalWriter},
    };
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    struct LegacyDocument {
        #[prost(string, tag = "1")]
        text: String,
        #[prost(string, tag = "27")]
        derived_fingerprint: String,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Envelope {
        #[prost(uint64, tag = "1")]
        seq: u64,
        #[prost(bytes = "vec", tag = "3")]
        documents: Vec<u8>,
        #[prost(uint64, tag = "6")]
        clock: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Batch {
        #[prost(bytes = "vec", repeated, tag = "2")]
        documents: Vec<Vec<u8>>,
    }

    fn manifest() -> WalManifest {
        let mut manifest: WalManifest = toml::from_str("dim=0\nbit_width=4\nslot_offset=0\ngeneration=0\nbucket_bits=0\nbucket_count=1\nformat_version=6\n").unwrap();
        let decl = crate::derived::Declaration::compile(&pb::DerivedColumns {
            columns: vec![pb::DerivedColumn {
                name: "constant".into(),
                expression: "7".into(),
                kind: pb::MaterializeKind::I64 as i32,
                disclosure: pb::DerivedDisclosure::Inputs as i32,
            }],
        })
        .unwrap();
        manifest.derived_fingerprint = decl.fingerprint().to_string();
        manifest.derived = decl.config();
        manifest
    }
    fn record(document: Vec<u8>) -> Vec<u8> {
        Envelope {
            seq: 1,
            clock: 1,
            documents: Batch {
                documents: vec![document],
            }
            .encode_to_vec(),
        }
        .encode_to_vec()
    }
    fn old_record(manifest: &WalManifest) -> Vec<u8> {
        record(
            LegacyDocument {
                text: "old".into(),
                derived_fingerprint: manifest.derived_fingerprint.clone(),
            }
            .encode_to_vec(),
        )
    }
    fn create_old(tag: &str) -> (std::path::PathBuf, std::path::PathBuf, WalManifest) {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "target/test-tmp/derived-wal-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let gen = wal::gen_dir(&root, 0);
        std::fs::create_dir_all(&gen).unwrap();
        let manifest = manifest();
        wal::write_manifest(&gen, &manifest).unwrap();
        let mut file = std::fs::File::create(wal::bucket_path(&gen, 0)).unwrap();
        wal::write_frame(&mut file, &old_record(&manifest)).unwrap();
        file.sync_all().unwrap();
        std::fs::write(wal::markers_path(&gen), []).unwrap();
        (root, gen, manifest)
    }

    #[test]
    fn legacy_derived_wal_reopens_after_appending_current_derived_and_map_records() {
        let (root, gen, old) = create_old("mixed");
        let mut reader = RecordReader::open(&wal::bucket_path(&gen, 0)).unwrap();
        let first = reader.next_record().unwrap().unwrap();
        let Some(pb::wal::wal_record::Op::AddDocuments(batch)) = first.op else {
            panic!()
        };
        assert_eq!(
            batch.documents[0].derived_fingerprint,
            old.derived_fingerprint
        );
        assert!(batch.documents[0].map_integers.is_empty());
        let mut writer = WalWriter::resume(&gen, wal::read_manifest(&gen).unwrap()).unwrap();
        let upgraded = wal::read_manifest(&gen).unwrap();
        assert_eq!(upgraded.format_version, 8);
        assert!(upgraded.legacy_derived_tag27);
        let document = pb::AddDocumentsRequest {
            text: "new".into(),
            derived_fingerprint: old.derived_fingerprint.clone(),
            map_integers: vec![pb::MapIntegerEntry {
                field: "signed".into(),
                key: "".into(),
                value: i64::MIN,
            }],
            map_unsigned_integers: vec![pb::MapUnsignedIntegerEntry {
                field: "unsigned".into(),
                key: "".into(),
                value: u64::MAX,
            }],
            ..Default::default()
        };
        writer
            .append(pb::wal::wal_record::Op::AddDocuments(
                pb::wal::LoggedAddDocuments {
                    first_id: 1,
                    documents: vec![document.clone()],
                    ..Default::default()
                },
            ))
            .unwrap();
        writer.flush().unwrap();
        drop(writer);
        let records = wal::read_clocked_records(&gen, 0).unwrap();
        assert_eq!(records.len(), 2);
        let Some(pb::wal::wal_record::Op::AddDocuments(batch)) = &records[1].op else {
            panic!()
        };
        assert_eq!(batch.documents, [document]);
        let writer = WalWriter::resume(&gen, wal::read_manifest(&gen).unwrap()).unwrap();
        assert!(writer.manifest().legacy_derived_tag27);
        drop(writer);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_profile_and_declaration_fail_before_truncating_rows() {
        let (root, gen, mut fresh) = create_old("refuse");
        let path = wal::bucket_path(&gen, 0);
        let original = std::fs::read(&path).unwrap();
        fresh.derived.clear();
        fresh.derived_fingerprint.clear();
        let result = wal::open_or_create(&root, 0, fresh);
        assert!(result.err().unwrap().to_string().contains("derived"));
        assert_eq!(std::fs::read(&path).unwrap(), original);
        let mut bad = wal::read_manifest(&gen).unwrap();
        bad.derived_fingerprint = "0".repeat(64);
        wal::write_manifest(&gen, &bad).unwrap();
        assert!(RecordReader::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn wire_adapter_refuses_conflicting_tags_and_preserves_unknown_bytes() {
        let manifest = manifest();
        let old = old_record(&manifest);
        for end in 1..old.len() {
            // A truncated top-level envelope may end on a complete field; every
            // successful decode must still be a valid protobuf record.
            if let Ok(bytes) = upgrade_record(&old[..end], &manifest.derived_fingerprint) {
                pb::wal::WalRecord::decode(bytes.as_slice()).unwrap();
            }
        }
        assert!(upgrade_record(&old, &"0".repeat(64)).is_err());
        let mut document = LegacyDocument {
            text: "old".into(),
            derived_fingerprint: manifest.derived_fingerprint.clone(),
        }
        .encode_to_vec();
        document.extend(
            pb::AddDocumentsRequest {
                derived_fingerprint: manifest.derived_fingerprint.clone(),
                ..Default::default()
            }
            .encode_to_vec(),
        );
        assert!(upgrade_record(&record(document), &manifest.derived_fingerprint).is_err());
        let mut opaque = old.clone();
        encode_key(100, WireType::LengthDelimited, &mut opaque);
        encode_varint(3, &mut opaque);
        opaque.extend([0, 255, 0]);
        let upgraded = upgrade_record(&opaque, &manifest.derived_fingerprint).unwrap();
        assert!(upgraded.ends_with(&opaque[old.len()..]));
    }

    #[test]
    fn complete_column_tables_require_version8_even_without_a_single_map_value() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "target/test-tmp/derived-empty-table-{}",
            std::process::id()
        ));
        let mut plain = manifest();
        plain.derived.clear();
        plain.derived_fingerprint.clear();
        plain.columns = Some(wal::ColumnTable {
            map_integers: vec!["absent_signed".into()],
            map_unsigned_integers: vec!["absent_unsigned".into()],
            ..Default::default()
        });
        let mut writer = WalWriter::create(&root, plain.clone()).unwrap();
        assert_eq!(writer.manifest().format_version, 8);
        writer
            .append(pb::wal::wal_record::Op::AddDocuments(
                pb::wal::LoggedAddDocuments {
                    documents: vec![pb::AddDocumentsRequest {
                        text: "no map values".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ))
            .unwrap();
        writer.flush().unwrap();
        assert_eq!(
            wal::read_manifest(writer.dir()).unwrap().columns,
            plain.columns
        );
        drop(writer);
        let columns = plain.columns.take();
        plain.generation = 1;
        let mut writer = WalWriter::create(&root, plain).unwrap();
        assert_eq!(writer.manifest().format_version, 6);
        assert!(writer.update_manifest(|manifest| manifest.columns = columns.clone()));
        assert_eq!(wal::read_manifest(writer.dir()).unwrap().format_version, 8);
        drop(writer);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn mismatched_document_declarations_never_advance_the_wal() {
        let (root, gen, old) = create_old("append-refusal");
        let mut writer = WalWriter::resume(&gen, wal::read_manifest(&gen).unwrap()).unwrap();
        let clock = writer.high_watermark();
        let path = wal::bucket_path(&gen, 0);
        let bytes = std::fs::read(&path).unwrap();
        for fingerprint in [String::new(), "0".repeat(64)] {
            assert_ne!(fingerprint, old.derived_fingerprint);
            let error = writer
                .append(pb::wal::wal_record::Op::AddDocuments(
                    pb::wal::LoggedAddDocuments {
                        first_id: 1,
                        documents: vec![pb::AddDocumentsRequest {
                            derived_fingerprint: fingerprint,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ))
                .unwrap_err();
            assert!(error.to_string().contains("differs from its manifest"));
            assert_eq!(writer.high_watermark(), clock);
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        drop(writer);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fresh_derived_generations_and_peer_acknowledgements_require_the_new_contract() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
            "target/test-tmp/derived-wal-new-{}",
            std::process::id()
        ));
        let writer = WalWriter::create(&root, manifest()).unwrap();
        assert_eq!(writer.manifest().format_version, 8);
        assert!(!writer.manifest().legacy_derived_tag27);
        let doc = pb::AddDocumentsRequest {
            derived_fingerprint: manifest().derived_fingerprint,
            ..Default::default()
        };
        let required = crate::document_contract::required_version(&doc);
        assert_eq!(required, 2);
        assert!(crate::document_contract::require_supported(1, required).is_err());
        assert!(crate::document_contract::require_supported(2, required).is_ok());
        drop(writer);
        std::fs::remove_dir_all(root).unwrap();
    }
}
