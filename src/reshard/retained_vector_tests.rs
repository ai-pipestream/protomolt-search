use super::*;
use crate::multi_res::RetainedVector;
use crate::turbo_bridge::MultiResVectorOutput;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "reshard-retained-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn retained(seed: f32) -> RetainedVector {
    let mut raw_full = (0..16).map(|i| seed + i as f32).collect::<Vec<_>>();
    raw_full[0] = -0.0;
    RetainedVector {
        output: MultiResVectorOutput::from_full_and_sentences(
            &raw_full,
            &[8],
            Some(vec![0, 255]),
            vec![vec![-0.0, f32::from_bits(1)]],
        ),
    }
}
fn replay() -> Replay {
    let mut replay = Replay::default();
    for (id, seed) in [(11, 1.0), (40, 2.0), (70, 3.0)] {
        let row = retained(seed);
        replay
            .vectors
            .insert(id, row.output.tier(8).unwrap().to_vec());
        replay
            .retained
            .insert(id, crate::multi_res::encode(&row).unwrap());
    }
    replay.deleted.insert(40);
    replay
}

#[test]
fn deleted_retention_is_removed_and_reordered_row_sink_preserves_original_payloads() {
    let mut replay = replay();
    let expected = [replay.retained[&70].clone(), replay.retained[&11].clone()];
    replay.compact();
    assert!(!replay.retained.contains_key(&40));
    let mut observed = Vec::new();
    let mut sink = |row: CompactedRow<'_>| {
        observed.push((
            row.old_id,
            row.new_local,
            row.materialized_row.unwrap().to_vec(),
        ));
        Ok(())
    };
    let mut remap = BTreeMap::new();
    assert_eq!(
        emit_rows_in(&replay, &[70, 11], 5, &mut remap, &mut sink).unwrap(),
        2
    );
    assert_eq!(
        observed,
        vec![(70, 5, expected[0].clone()), (11, 6, expected[1].clone())]
    );
    assert_eq!(remap, BTreeMap::from([(11, 6), (70, 5)]));
}

#[test]
fn child_reordering_publishes_full_resolution_bits_and_clears_reused_legacy_paths() {
    let directory = Directory::new();
    let replay = replay();
    let expected = [replay.retained[&70].clone(), replay.retained[&11].clone()];
    let index = VectorIndex::create(crate::vector::EMBEDDED_TURBOVEC, 8, 4).unwrap();
    let manifest =
        crate::node::wal_manifest(Some(&index), &crate::node::NodeConfig::default(), 1, (0, 0));
    let mut analyze =
        |_: &[(&str, Option<&AnalysisSpec>, SessionLayers)]| -> Result<Vec<AnalyzedDoc>, String> {
            Err("vector-only reshard must not analyze".into())
        };
    let image = finish_child_ordered_with_sources(
        &manifest,
        replay,
        &[70, 11],
        0,
        &directory.0,
        100,
        0,
        u64::MAX,
        None,
        None,
        None,
        None,
        None,
        None,
        &mut analyze,
    )
    .unwrap();
    assert_eq!(image.parent_ids, vec![70, 11]);
    assert_eq!(image.row_parent_ids, vec![70, 11]);
    let payload = image.multi_res_path.as_ref().unwrap();
    assert!(crate::multi_res_storage::declaration_path(payload).is_file());
    let rows = crate::multi_res_storage::open(payload, 2).unwrap();
    assert_eq!(rows.row_count(), 2);
    for (slot, bytes) in expected.iter().enumerate() {
        assert_eq!(
            &crate::multi_res::encode(rows.retained(slot as u32).unwrap()).unwrap(),
            bytes
        );
    }
    // Reusing the same output path with legacy rows must not adopt old
    // higher tiers merely because its new physical row count is equal.
    let mut legacy = Replay::default();
    legacy.vectors.insert(11, vec![0.0; 8]);
    legacy.vectors.insert(70, vec![0.0; 8]);
    let replaced = finish_child(
        &manifest,
        legacy,
        0,
        &directory.0,
        100,
        0,
        u64::MAX,
        None,
        None,
        &mut analyze,
    )
    .unwrap();
    assert!(replaced.multi_res_path.is_none());
    assert!(crate::multi_res_storage::open(payload, 2)
        .unwrap()
        .is_empty());
}

#[test]
fn materialized_row_without_vector_is_not_silently_discarded_by_child_build() {
    let directory = Directory::new();
    let mut replay = Replay::default();
    replay
        .retained
        .insert(99, crate::multi_res::encode(&retained(1.0)).unwrap());
    let index = VectorIndex::create(crate::vector::EMBEDDED_TURBOVEC, 8, 4).unwrap();
    let manifest =
        crate::node::wal_manifest(Some(&index), &crate::node::NodeConfig::default(), 1, (0, 0));
    let mut analyze =
        |_: &[(&str, Option<&AnalysisSpec>, SessionLayers)]| -> Result<Vec<AnalyzedDoc>, String> {
            Ok(Vec::new())
        };
    assert!(finish_child(
        &manifest,
        replay,
        0,
        &directory.0,
        0,
        0,
        u64::MAX,
        None,
        None,
        &mut analyze
    )
    .unwrap_err()
    .contains("no matching child vectors"));
}
