use super::frames::*;
use super::layout::*;
use super::manifest::*;
use super::tail::*;
use super::writer::*;
use super::*;
use crate::pb::wal::{wal_record, WalRecord};
use crate::pb::wal::{FlushMarker, LoggedAddDocuments, LoggedAddVectors, SnapshotMarker};
use crate::pb::{AddDocumentsRequest, AddVectorsRequest};
use crate::reshard::bucket_of;
use prost::Message;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

fn tempdir(tag: &str) -> PathBuf {
    // Under target/ (a real disk), not the tmpfs /tmp — same
    // convention as the integration tests' CARGO_TARGET_TMPDIR,
    // which cargo does not set for unit tests.
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
        "target/test-tmp/wal_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn manifest(bucket_count: u32) -> WalManifest {
    WalManifest {
        collection: String::new(),
        dim: 8,
        vector_backend: String::new(),
        vector_config_format: String::new(),
        vector_config_payload: Vec::new(),
        slot_offset: 25_000_000,
        generation: 0,
        bucket_bits: bucket_count.trailing_zeros(),
        bucket_count,
        preexisting_vectors: 0,
        preexisting_documents: 0,
        format_version: FORMAT_VERSION,
        derived_fingerprint: String::new(),
        derived: Vec::new(),
        columns: None,
    }
}

#[test]
fn manifest_round_trips_opaque_vector_backend_state() {
    let mut manifest = manifest(4);
    let config = crate::vector::embedded_turbovec_config(4, &[0.25; 8], &[1.5; 8]).unwrap();
    manifest.set_backend_config(config.clone());
    let encoded = toml::to_string(&manifest).unwrap();
    let decoded: WalManifest = toml::from_str(&encoded).unwrap();
    assert_eq!(decoded.backend_config().unwrap(), config);
}

#[test]
fn a_manifest_without_provider_state_has_no_backend_config() {
    assert!(manifest(4).backend_config().is_err());
}

fn add_op(id: u64) -> wal_record::Op {
    wal_record::Op::AddVectors(LoggedAddVectors {
        first_id: id,
        batch: Some(AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: vec![1.0; 8],
            dim: 8,
        }),
        stable_routing_keys: Vec::new(),
    })
}

fn doc_op(id: u64) -> wal_record::Op {
    wal_record::Op::AddDocuments(LoggedAddDocuments {
        first_id: id,
        documents: vec![AddDocumentsRequest {
            nlp_materialization: String::new(),
            cased_field: String::new(),
            collection: String::new(),
            sentence_fields: Vec::new(),
            materialize: None,
            map_numerics: Vec::new(),
            map_facets: Vec::new(),
            numerics: Vec::new(),
            facets: Vec::new(),
            text: format!("doc {id}"),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
            integers: Vec::new(),
            unsigned_integers: Vec::new(),
            map_integers: Vec::new(),
            map_unsigned_integers: Vec::new(),
            derived_fingerprint: String::new(),
            timestamps: Vec::new(),
            geo_points: Vec::new(),
            quality: None,
            geography: None,
            phrases: vec![crate::pb::PhrasePosting {
                term: protomolt_analyzer::phrase_posting_term("example"),
                concept_id: "example".to_string(),
                token_count: 2,
                offsets: vec![crate::pb::OffsetSpan { start: 0, end: 3 }],
                field: "phrases".to_string(),
            }],
            phrase_fingerprint: 0x1234_5678,
            phrase_field: "phrases".to_string(),
            position_fields: Vec::new(),
            bigram_fields: Vec::new(),
            original_source: None,
            source_chunk_ordinal: None,
            identity: None,
        }],
        stable_routing_keys: Vec::new(),
        source_references: Vec::new(),
    })
}

/// All records in one file, in log order.
fn read_all(path: &Path) -> io::Result<Vec<WalRecord>> {
    let mut reader = RecordReader::open(path)?;
    let mut out = Vec::new();
    while let Some(record) = reader.next_record()? {
        out.push(record);
    }
    Ok(out)
}

#[test]
fn generation_clock_merges_bucket_and_marker_files() {
    let dir = tempdir("clock_merge");
    let mut writer = WalWriter::create(&dir, manifest(4)).unwrap();
    writer.append(add_op(1)).unwrap();
    writer
        .append(wal_record::Op::Flush(FlushMarker {}))
        .unwrap();
    writer.append(add_op(2)).unwrap();
    writer.flush().unwrap();
    let generation = writer.dir().to_path_buf();
    assert_eq!(writer.high_watermark(), 3);
    drop(writer);

    let records = read_clocked_records(&generation, 0).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.clock)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(matches!(records[1].op, Some(wal_record::Op::Flush(_))));
    let tail = read_clocked_records(&generation, 2).unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].clock, 3);

    let resumed = WalWriter::resume(&generation, read_manifest(&generation).unwrap()).unwrap();
    assert_eq!(resumed.high_watermark(), 3);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn invalid_vector_bindings_leave_no_record() {
    let dir = tempdir("mapped-vector");
    let mut writer = WalWriter::create(&dir, manifest(2)).unwrap();
    let plan = crate::mapping::derive_plan(
        include_bytes!("../../../tests/fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap();
    let binding = crate::pb::wal::LoggedBinding {
        plan_fingerprint: plan.fingerprint,
        body_path: "body".into(),
        vector_binding: plan.vector_binding.unwrap().encode_to_vec(),
        ..Default::default()
    };
    for case in 0..3 {
        let mut invalid = binding.clone();
        match case {
            0 => invalid.plan_fingerprint = "0".repeat(64),
            1 => invalid.vector_binding.extend([8, 1]),
            _ => invalid.vector_binding = vec![8, 2],
        }
        assert!(writer.append(wal_record::Op::Bind(invalid)).is_err());
        assert_eq!(writer.high_watermark(), 0);
    }
    writer
        .append(wal_record::Op::Bind(binding.clone()))
        .unwrap();
    writer.flush().unwrap();
    let generation = writer.dir().to_owned();
    drop(writer);
    let records = read_clocked_records(&generation, 0).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].op, Some(wal_record::Op::Bind(binding)));
    let current = read_manifest(&generation).unwrap();
    for version in [FORMAT_VERSION - 1, FORMAT_VERSION + 1] {
        let mut other = current.clone();
        other.format_version = version;
        write_manifest(&generation, &other).unwrap();
        assert!(read_manifest(&generation).is_err());
    }
    std::fs::remove_dir_all(dir).unwrap();
}

mod frames;
mod sources;
mod torture;
