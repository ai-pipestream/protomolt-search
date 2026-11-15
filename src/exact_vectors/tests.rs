use super::*;

/// The disk builder appends straight to its file, reads rows back
/// bit for bit, scores like the heap builder, finalizes in place
/// (one header write, one rename) with the payload digest it kept
/// while appending, and leaves nothing behind when dropped
/// unfinalized.
#[test]
fn spilled_builder_matches_the_heap_builder_and_finalizes_in_place() {
    let dir = std::env::temp_dir().join(format!("pm-exact-spill-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let target = dir.join("gen").join("vectors.f32");
    let dim = 5;
    let rows: Vec<f32> = (0..dim * 37).map(|i| (i as f32 * 0.37).sin()).collect();

    let mut heap = ExactVectorStore::empty(Some(dim));
    let mut spilled = ExactVectorStore::spilling(&target, None).unwrap();
    assert!(spilled.is_spilled());
    assert_eq!(spilled.len(), 0);
    for batch in rows.chunks(dim * 7) {
        heap.append(batch, dim).unwrap();
        spilled.append(batch, dim).unwrap();
    }
    assert_eq!(spilled.len(), 37);
    assert_eq!(spilled.dim(), Some(dim));
    assert_eq!(
        spilled.row_values(3, 11).unwrap(),
        heap.row_values(3, 11).unwrap()
    );
    assert_eq!(spilled.row_values(0, 37).unwrap(), rows);
    let query: Vec<f32> = (0..dim).map(|i| 0.5 - i as f32 * 0.1).collect();
    let slots = [36usize, 0, 17, 5];
    assert_eq!(
        spilled.score_slots(&query, &slots).unwrap(),
        heap.score_slots(&query, &slots).unwrap()
    );
    let spill_file = spill_path(&target);
    assert!(spill_file.exists());
    assert!(!target.exists());

    let mapped = spilled.write(&target).unwrap();
    assert!(mapped.is_mapped());
    assert!(target.exists(), "finalized in place");
    assert!(!spill_file.exists(), "renamed onto the target");
    mapped.verify_payload().unwrap();
    assert_eq!(mapped.row_values(0, 37).unwrap(), rows);
    let expected = heap.write(&dir.join("heap.f32")).unwrap();
    assert_eq!(
        std::fs::read(&target).unwrap(),
        std::fs::read(dir.join("heap.f32")).unwrap(),
        "the same bytes either way"
    );
    assert_eq!(expected.len(), mapped.len());
    drop(spilled);
    assert!(
        target.exists(),
        "dropping a finalized builder keeps the store"
    );

    // Appending to the mapped store builds on disk again beside it.
    let mut reopened = ExactVectorStore::open(&target).unwrap();
    reopened.append(&rows[..dim], dim).unwrap();
    assert!(reopened.is_spilled());
    assert_eq!(reopened.len(), 38);
    assert_eq!(reopened.row_values(37, 38).unwrap(), &rows[..dim]);
    assert_eq!(reopened.row_values(0, 37).unwrap(), rows);
    let grown = reopened.write(&target).unwrap();
    assert_eq!(grown.len(), 38);
    grown.verify_payload().unwrap();

    // Writing a spilled store to ANOTHER path copies it there and
    // leaves the builder in place.
    let mut other = ExactVectorStore::spilling(&dir.join("other.f32"), Some(dim)).unwrap();
    other.append(&rows, dim).unwrap();
    let elsewhere = other.write(&dir.join("copy.f32")).unwrap();
    elsewhere.verify_payload().unwrap();
    assert_eq!(elsewhere.row_values(0, 37).unwrap(), rows);
    assert!(spill_path(&dir.join("other.f32")).exists());
    drop(other);
    assert!(
        !spill_path(&dir.join("other.f32")).exists(),
        "an unfinalized builder removes its file"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn test_directory(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "exact-{name}-{}-{}",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

#[test]
fn mapped_append_writes_only_new_rows() {
    let directory = test_directory("append-delta");
    let path = directory.join("vectors.f32");
    let mut store = ExactVectorStore::from_values(4, vec![0.5; 8192])
        .unwrap()
        .write(&path)
        .unwrap();
    let original = std::fs::read(&path).unwrap();
    store.append(&[1.0, 2.0, 3.0, 4.0], 4).unwrap();
    let scratch_bytes: u64 = std::fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "building")
        })
        .map(|entry| entry.metadata().unwrap().len())
        .sum();
    assert_eq!(
        scratch_bytes,
        (HEADER_BYTES + 16) as u64,
        "append must not copy existing FP32 rows"
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn short_spill_reads_refuse_scoring_instead_of_inventing_zeroes() {
    let directory = test_directory("short-read");
    let path = directory.join("vectors.f32");
    let mut store = ExactVectorStore::spilling(&path, Some(2)).unwrap();
    store.append(&vec![1.0; 2048], 2).unwrap();
    OpenOptions::new()
        .write(true)
        .open(spill_path(&path))
        .unwrap()
        .set_len(HEADER_BYTES as u64 + 4)
        .unwrap();
    assert!(store.score_slots(&[1.0, 2.0], &[0]).is_err());
    assert!(store.row_values(0, 1).is_err());
    assert!(store
        .score_slots_profiled(&[1.0, 2.0], &(0..1024).collect::<Vec<_>>(), 4)
        .is_err());
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn finalized_builder_cannot_mutate_the_published_image() {
    let directory = test_directory("finalized");
    let path = directory.join("vectors.f32");
    let mut store = ExactVectorStore::spilling(&path, Some(2)).unwrap();
    store.append(&[1.0, 2.0], 2).unwrap();
    let image = store.write(&path).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert!(store.append(&[3.0, 4.0], 2).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    image.verify_payload().unwrap();
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn uncertain_finalization_fences_the_builder_after_rename() {
    let directory = test_directory("uncertain-finalization");
    let path = directory.join("vectors.f32");
    let mut store = ExactVectorStore::spilling(&path, Some(2)).unwrap();
    store.append(&[1.0, 2.0], 2).unwrap();
    FAIL_FINALIZE_SYNC.with(|fail| fail.set(true));
    assert!(store
        .write(&path)
        .unwrap_err()
        .to_string()
        .contains("after rename"));
    let committed = std::fs::read(&path).unwrap();
    assert!(store.append(&[3.0, 4.0], 2).is_err());
    assert!(store.write(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), committed);
    let recovered = ExactVectorStore::open(&path).unwrap();
    recovered.verify_payload().unwrap();
    assert_eq!(recovered.row_values(0, 1).unwrap(), [1.0, 2.0]);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn a_replaced_path_cannot_acknowledge_a_different_snapshot() {
    let directory = test_directory("replaced-path");
    let path = directory.join("vectors.f32");
    let old = ExactVectorStore::from_values(2, vec![1.0, 2.0])
        .unwrap()
        .write(&path)
        .unwrap();
    let new = ExactVectorStore::from_values(2, vec![3.0, 4.0])
        .unwrap()
        .write(&path)
        .unwrap();
    assert!(old.write(&path).is_err());
    assert_eq!(
        ExactVectorStore::open(&path)
            .unwrap()
            .row_values(0, 1)
            .unwrap(),
        [3.0, 4.0]
    );
    assert_eq!(
        new.write(&path).unwrap().row_values(0, 1).unwrap(),
        [3.0, 4.0]
    );
    assert_eq!(
        old.write(&directory.join("old.f32"))
            .unwrap()
            .row_values(0, 1)
            .unwrap(),
        [1.0, 2.0]
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn delta_reads_scores_and_snapshots_preserve_one_row_space() {
    let directory = test_directory("delta-snapshots");
    let path = directory.join("vectors.f32");
    let dim = 8;
    let values: Vec<f32> = (0..dim * 1100).map(|i| (i as f32 * 0.013).sin()).collect();
    let mut view = ExactVectorStore::from_values(dim, values[..dim * 700].to_vec())
        .unwrap()
        .write(&path)
        .unwrap();
    let old = ExactVectorStore::open(&path).unwrap();
    assert!(view.append(&[f32::NAN; 8], dim).is_err());
    assert!(view.is_mapped());
    view.append(&values[dim * 700..dim * 1000], dim).unwrap();
    let first = view.write(&path).unwrap();
    // Continue through the same append view after a snapshot. The snapshot
    // and original mapping must stay immutable while the private delta grows.
    view.append(&values[dim * 1000..], dim).unwrap();
    assert_eq!(old.len(), 700);
    assert_eq!(first.len(), 1000);
    assert_eq!(view.len(), 1100);
    assert_eq!(old.row_values(0, 700).unwrap(), values[..dim * 700]);
    assert_eq!(first.row_values(0, 1000).unwrap(), values[..dim * 1000]);
    for (from, to) in [(0, 3), (699, 702), (700, 706), (1097, 1100)] {
        assert_eq!(
            view.row_values(from, to).unwrap(),
            values[from * dim..to * dim]
        );
    }
    assert!(view.row_values(0, 1101).is_err());
    assert!(view.row_values(5, 4).is_err());
    let query: Vec<f32> = (0..dim).map(|i| i as f32 * 0.11).collect();
    let slots: Vec<usize> = (0..1100).rev().chain([699, 700, 1099]).collect();
    let expected = ExactVectorStore::from_values(dim, values.clone())
        .unwrap()
        .score_slots(&query, &slots)
        .unwrap();
    let scored = view.score_slots_profiled(&query, &slots, 4).unwrap();
    assert_eq!(scored.rows, expected);
    assert!(scored.pages_touched > 0);
    assert!(scored.tasks > 1);
    view.verify_payload().unwrap();
    let latest = view.write(&path).unwrap();
    let canonical = directory.join("canonical.f32");
    ExactVectorStore::from_values(dim, values)
        .unwrap()
        .write(&canonical)
        .unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        std::fs::read(&canonical).unwrap()
    );
    latest.verify_payload().unwrap();
    old.verify_payload().unwrap();
    first.verify_payload().unwrap();
    drop(view);
    assert!(std::fs::read_dir(&directory).unwrap().all(|entry| entry
        .unwrap()
        .path()
        .extension()
        .is_none_or(|ext| ext != "building")));
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn broken_delta_refuses_complete_reads_and_keeps_the_published_base() {
    let directory = test_directory("delta-failure");
    let path = directory.join("vectors.f32");
    let mut view = ExactVectorStore::from_values(2, vec![1.0, 2.0])
        .unwrap()
        .write(&path)
        .unwrap();
    let old = std::fs::read(&path).unwrap();
    view.append(&[3.0, 4.0], 2).unwrap();
    let Storage::Appended { delta, .. } = &view.storage else {
        unreachable!()
    };
    let Storage::Spilled { file, .. } = &delta.storage else {
        unreachable!()
    };
    file.set_len(HEADER_BYTES as u64).unwrap();
    assert_eq!(view.row_values(0, 1).unwrap(), [1.0, 2.0]);
    assert!(view.row_values(0, 2).is_err());
    assert!(view.score_slots(&[1.0, 1.0], &[0, 1]).is_err());
    assert!(view.verify_payload().is_err());
    assert!(view.write(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), old);
    drop(view);
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn round_trip_is_mapped_and_scores_fp32() {
    let dir = std::env::temp_dir().join(format!(
        "protomolt-exact-vectors-{}-{}",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("vectors.exact");
    let store = ExactVectorStore::from_values(3, vec![1.0, 2.0, 3.0, -1.0, 0.5, 2.0])
        .unwrap()
        .write(&path)
        .unwrap();
    assert!(store.is_mapped());
    assert_eq!(store.dim(), Some(3));
    assert_eq!(store.len(), 2);
    store.verify_payload().unwrap();
    assert_eq!(
        store.score_slots(&[0.5, 1.0, -1.0], &[1, 0]).unwrap(),
        vec![(1, -2.0), (0, -0.5)]
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn header_and_payload_corruption_are_distinct() {
    let dir = std::env::temp_dir().join(format!(
        "protomolt-exact-vectors-corrupt-{}-{}",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("vectors.exact");
    ExactVectorStore::from_values(2, vec![1.0, 2.0])
        .unwrap()
        .write(&path)
        .unwrap();

    let mut bytes = std::fs::read(&path).unwrap();
    bytes[HEADER_BYTES] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    let store = ExactVectorStore::open(&path).unwrap();
    assert!(store.verify_payload().is_err());

    bytes[16] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    assert!(ExactVectorStore::open(&path).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn page_ordered_parallel_scoring_is_bit_identical_and_restores_order() {
    let dir = std::env::temp_dir().join(format!(
        "protomolt-exact-vectors-parallel-{}-{}",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("vectors.exact");
    let dim = 32usize;
    let rows = 1_024usize;
    let values: Vec<f32> = (0..rows * dim)
        .map(|i| ((i * 17 % 251) as f32 - 125.0) / 127.0)
        .collect();
    let query: Vec<f32> = (0..dim).map(|i| (i as f32 - 15.0) / 31.0).collect();
    let store = ExactVectorStore::from_values(dim, values)
        .unwrap()
        .write(&path)
        .unwrap();
    let slots: Vec<usize> = (0..rows).rev().step_by(3).collect();
    let serial = store.score_slots_profiled(&query, &slots, 1).unwrap();
    let parallel = store.score_slots_profiled(&query, &slots, 4).unwrap();
    assert_eq!(parallel.rows, serial.rows);
    assert_eq!(
        parallel.rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        slots
    );
    assert_eq!(parallel.logical_bytes, (slots.len() * dim * 4) as u64);
    assert!(parallel.pages_touched > 1);
    assert!(parallel.tasks > 1 && parallel.tasks <= 4);
    std::fs::remove_dir_all(dir).unwrap();
}
