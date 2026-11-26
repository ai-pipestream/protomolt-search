//! Tests for the backend-neutral contract and the embedded adapter.

use std::ops::Range;
use std::path::Path;

use super::*;

struct FakeProvider;

impl VectorProvider for FakeProvider {
    fn descriptor(&self) -> VectorBackendDescriptor {
        VectorBackendDescriptor {
            backend_kind: "fake".into(),
            backend_version: "test".into(),
            dimension: Some(2),
            bits_per_dimension: None,
            metric: "inner_product".into(),
            score_direction: ScoreDirection::HigherIsBetter,
            scoring_fingerprint: "fake-score-v1".into(),
            quality_contract: QualityContract::ConfiguredAnn,
            capabilities: vec![VectorCapability::BatchQuery],
        }
    }

    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        Ok(VectorBackendConfig {
            backend_kind: "fake".into(),
            config_format: "test/fake".into(),
            payload: vec![1, 2, 3],
        })
    }

    fn len(&self) -> usize {
        1
    }

    fn dimension(&self) -> Option<usize> {
        Some(2)
    }

    fn add(&mut self, _vectors: &[f32], _dimension: usize) -> Result<(), VectorError> {
        Ok(())
    }

    fn prepare(&mut self) -> Result<(), VectorError> {
        Ok(())
    }

    fn write(&self, _path: &Path) -> Result<(), VectorError> {
        Ok(())
    }

    fn search(
        &self,
        _queries: &[f32],
        k: usize,
        _options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        Ok(VectorSearchResults {
            scores: vec![0.75; k],
            slots: vec![0; k],
            query_count: 1,
            result_count: k,
            parts: PartCounts::default(),
            regimes: FilterRegimeCounts::default(),
            tail_enumerated_rows: 0,
            patience_stopped: 0,
            floor_pruned: 0,
        })
    }

    fn search_streaming_controlled(
        &self,
        _queries: &[f32],
        _options: VectorSearchOptions<'_>,
        _sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        _control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        Ok(VectorStreamSummary {
            query_count: 1,
            emitted: 0,
            units_scanned: 0,
            completed: true,
            end_reason: VectorStreamEndReason::Exhausted,
            parts: PartCounts::default(),
            regimes: FilterRegimeCounts::default(),
            tail_enumerated_rows: 0,
            patience_stopped: 0,
            floor_pruned: 0,
        })
    }
}

fn scripted_transcripts(
    rows: Range<usize>,
    emitted: &[usize],
) -> (Result<(), VectorError>, Vec<usize>) {
    let requested = rows.clone();
    let mut forwarded = Vec::new();
    let result = checked_row_transcripts(rows, &mut |row, _| forwarded.push(row), |seen, sink| {
        assert_eq!(seen, requested);
        for &row in emitted {
            sink(row, &[row as u8]);
        }
        Ok(())
    });
    (result, forwarded)
}

#[test]
#[allow(clippy::reversed_empty_ranges)]
fn checked_transcripts_require_each_requested_row_once_in_order() {
    let (result, forwarded) = scripted_transcripts(3..6, &[3, 4, 5]);
    result.unwrap();
    assert_eq!(forwarded, [3, 4, 5]);

    for (emitted, expected_forwarded) in [
        (&[][..], &[][..]),
        (&[3, 4][..], &[3, 4][..]),
        (&[3, 3, 4, 5][..], &[3][..]),
        (&[3, 5, 4][..], &[3][..]),
        (&[2, 3, 4, 5][..], &[][..]),
        (&[3, 4, 5, 6][..], &[3, 4, 5][..]),
    ] {
        let (result, forwarded) = scripted_transcripts(3..6, emitted);
        assert!(result.is_err(), "accepted {emitted:?}");
        assert_eq!(
            forwarded, expected_forwarded,
            "forwarded invalid callback from {emitted:?}"
        );
    }

    let (result, forwarded) = scripted_transcripts(4..4, &[]);
    result.unwrap();
    assert!(forwarded.is_empty());
    assert!(scripted_transcripts(6..3, &[]).0.is_err());

    let mut forwarded = Vec::new();
    let error = checked_row_transcripts(0..1, &mut |row, _| forwarded.push(row), |_, _| {
        Err(VectorError::new("scripted provider failure"))
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("scripted provider failure"),
        "{error}"
    );
    assert!(forwarded.is_empty());
}

#[test]
fn one_row_transcript_refuses_usize_max_without_wrapping() {
    let error = VectorIndex::from_provider(FakeProvider)
        .row_transcript(usize::MAX)
        .unwrap_err();
    assert!(error.to_string().contains("overflow"), "{error}");
}

#[test]
fn unknown_backend_refuses() {
    let err = VectorIndex::create("surprise", 64, 4).unwrap_err();
    assert!(err.to_string().contains("unknown vector backend"));
}

#[test]
fn product_handle_accepts_a_non_turbovec_provider() {
    let index = VectorIndex::from_provider(FakeProvider);
    assert_eq!(index.descriptor().backend_kind, "fake");
    assert_eq!(
        index.descriptor().quality_contract,
        QualityContract::ConfiguredAnn
    );
    assert_eq!(
        index.search_unfiltered(&[1.0, 0.0], 2).scores,
        vec![0.75; 2]
    );
}

#[test]
fn embedded_config_round_trips_without_exposing_native_types() {
    let index = VectorIndex::create(EMBEDDED_TURBOVEC, 64, 4).unwrap();
    let config = index.backend_config().unwrap();
    let rebuilt = VectorIndex::from_backend_config(64, &config).unwrap();
    assert_eq!(rebuilt.descriptor(), index.descriptor());
    assert_eq!(rebuilt.len(), 0);
}

#[test]
fn descriptor_and_search_contract_are_backend_neutral() {
    let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, 64, 4).unwrap();
    let vectors: Vec<f32> = (0..8 * 64)
        .map(|i| ((i % 31) as f32 - 15.0) / 31.0)
        .collect();
    index.add(&vectors, 64).unwrap();
    let descriptor = index.descriptor();
    assert_eq!(
        descriptor.quality_contract,
        QualityContract::ExhaustiveQuantized
    );
    assert!(descriptor
        .capabilities
        .contains(&VectorCapability::DenseMask));

    let result = index.search(&vectors[..64], 3, VectorSearchOptions::new());
    assert_eq!(result.query_count, 1);
    assert_eq!(result.result_count, 3);
    assert_eq!(result.slots_for_query(0).len(), 3);
}

/// The embedded adapter's row transcript is the stored encoding: it
/// follows a row through reordering, differs when the stored codes do
/// even with equal source rows, and is the same whether the image is
/// heap-loaded or mapped. A provider without a representation refuses
/// by name instead of certifying nothing.
#[test]
fn row_transcript_is_the_stored_encoding_and_refuses_without_one() {
    let dim = 64;
    let rows: Vec<Vec<f32>> = (0..4)
        .map(|r| {
            (0..dim)
                .map(|i| ((i * (r + 3)) % 17) as f32 / 17.0)
                .collect()
        })
        .collect();
    let build = |order: &[usize]| {
        let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, dim, 4).unwrap();
        let flat: Vec<f32> = order.iter().flat_map(|&r| rows[r].clone()).collect();
        index.add(&flat, dim).unwrap();
        index.prepare().unwrap();
        index
    };
    let forward = build(&[0, 1, 2, 3]);
    let reversed = build(&[3, 2, 1, 0]);
    for r in 0..4 {
        let transcript = forward.row_transcript(r).unwrap();
        assert_eq!(transcript.len(), 16 + 4 * dim / 8);
        assert_eq!(transcript, reversed.row_transcript(3 - r).unwrap());
    }
    assert_ne!(
        forward.row_transcript(0).unwrap(),
        forward.row_transcript(1).unwrap()
    );
    assert!(forward.row_transcript(4).is_err());

    // A different calibration stores different codes for equal rows.
    let sample: Vec<f32> = (0..8 * dim)
        .map(|i| ((i % 13) as f32 - 6.0) / 6.0)
        .collect();
    let calibrated = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, dim, 4, &sample).unwrap();
    let mut other = VectorIndex::from_backend_config(dim, &calibrated).unwrap();
    other.add(&rows.concat(), dim).unwrap();
    other.prepare().unwrap();
    assert_ne!(
        other.row_transcript(0).unwrap(),
        forward.row_transcript(0).unwrap()
    );

    let dir = std::env::temp_dir().join(format!("row-transcript-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("image");
    forward.write(&path).unwrap();
    let loaded = VectorIndex::load(EMBEDDED_TURBOVEC, &path).unwrap();
    let mapped = VectorIndex::load_mapped(EMBEDDED_TURBOVEC, &path).unwrap();
    for r in 0..4 {
        let expected = forward.row_transcript(r).unwrap();
        assert_eq!(loaded.row_transcript(r).unwrap(), expected);
        assert_eq!(mapped.row_transcript(r).unwrap(), expected);
    }
    std::fs::remove_dir_all(&dir).unwrap();

    let error = VectorIndex::from_provider(FakeProvider)
        .row_transcript(0)
        .unwrap_err();
    assert!(error.to_string().contains("cannot certify"), "{error}");
}

/// A sealed image of 32,768+ rows: on hosts whose kernels read
/// turbovec's bit-plane layout (aarch64, x86 with AVX-512 VBMI + VNNI)
/// it is written as a planes v8 file, which `load_mapped` must still
/// serve, since sealed segments map by default (`vector_mmap`).
#[test]
fn a_sealed_image_past_the_planes_threshold_serves_mapped() {
    let dim = 64;
    let n = 40_000;
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let rows: Vec<f32> = (0..n * dim)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((state >> 33) as f32 / (1u64 << 30) as f32) - 1.0
        })
        .collect();
    let dir = std::env::temp_dir().join(format!("planes-mapped-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for bits in [2, 4] {
        let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, dim, bits).unwrap();
        index.add(&rows, dim).unwrap();
        index.prepare().unwrap();
        let path = dir.join(format!("image-{bits}"));
        index.write(&path).unwrap();
        let loaded = VectorIndex::load(EMBEDDED_TURBOVEC, &path).unwrap();
        let mut mapped = VectorIndex::load_mapped(EMBEDDED_TURBOVEC, &path)
            .unwrap_or_else(|e| panic!("{bits}-bit image did not map: {e}"));
        mapped.prepare().unwrap();
        assert!(mapped.is_mapped());
        assert_eq!(mapped.len(), n);
        for r in [0, 31, 32, 20_000, n - 1] {
            assert_eq!(
                mapped.row_transcript(r).unwrap(),
                loaded.row_transcript(r).unwrap(),
                "{bits}-bit row {r}"
            );
        }
        let hits = mapped.search_unfiltered(&rows[..dim], 10);
        assert_eq!(hits.slots_for_query(0).len(), 10);
        assert_eq!(hits.slots_for_query(0)[0], 0, "{bits}-bit self match");
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
