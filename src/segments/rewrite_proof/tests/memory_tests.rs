use super::*;

fn materialized(set: &OpenedSegmentSet) -> Vec<Option<bool>> {
    (0..set.len())
        .map(|i| set.vector(i).and_then(|v| v.representation_materialized()))
        .collect()
}

/// A proof over mapped images (the default open mode) reads them in
/// bounded pieces: afterwards no segment of either set holds a
/// materialized packed-row copy, and the certificate equals the one the
/// heap-loaded form produces.
#[test]
fn mapped_proof_leaves_packed_codes_unmaterialized() {
    let fixture = Fixture::new();
    let before = fixture.set("before", &[0, 1, 2, 3], &[0], "", 2);
    let after = fixture.set("after", &[3, 1, 2], &[], "", 3);
    assert_eq!(materialized(&before), vec![Some(false); 2]);
    assert_eq!(materialized(&after), vec![Some(false); 1]);
    let proof = before
        .verify_source_rewrite(&after, &fixture.0.join("proof"), 2)
        .unwrap();
    assert_eq!(proof.live_rows, 3);
    assert_eq!(
        materialized(&before),
        vec![Some(false); 2],
        "before materialized"
    );
    assert_eq!(
        materialized(&after),
        vec![Some(false); 1],
        "after materialized"
    );
    let heap = OpenedSegmentSet::open_manifest(
        after.root().to_path_buf(),
        after.published_manifest(),
        VectorLoad::Heap,
    )
    .unwrap();
    assert_eq!(
        before
            .verify_source_rewrite(&heap, &fixture.0.join("heap-proof"), 2)
            .unwrap(),
        proof
    );
    assert_eq!(
        materialized(&heap),
        vec![Some(false); 1],
        "heap image materialized"
    );
}

/// Proof memory at a fixed batch does not grow with the vector image:
/// the same rows at 8 and at 512 dimensions, a 64x larger image, allocate
/// within one transcript piece of each other on the proof's thread. A
/// materialized image at 512 dimensions would be 16,384 rows x 256 bytes
/// = 4 MiB of packed rows per set, plus a transient of the same size.
#[test]
fn proof_allocation_is_independent_of_vector_image_size() {
    let rows: Vec<u32> = (0..16_384).collect();
    let mut peaks = Vec::new();
    for dim in [8usize, 512] {
        let mut fixture = Fixture::new();
        fixture.1 = dim;
        let before = fixture.set("before", &rows, &[], "", 4096);
        let after = fixture.set("after", &rows, &[], "", 4096);
        allocation::reset();
        let proof = before
            .verify_source_rewrite(&after, &fixture.0.join("proof"), 1024)
            .unwrap();
        let peak = allocation::peak();
        assert_eq!(proof.live_rows, rows.len() as u64);
        assert_eq!(materialized(&before), vec![Some(false); 4]);
        assert_eq!(materialized(&after), vec![Some(false); 4]);
        eprintln!("proof peak allocation at {dim} dimensions, batch 1024: {peak} bytes");
        peaks.push(peak);
    }
    assert!(
        peaks[1] - peaks[0] < 1 << 20,
        "proof memory grew with the image: {peaks:?}"
    );
}
