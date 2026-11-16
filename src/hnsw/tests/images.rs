//! The persisted image: round trips, the empty image, and refusal of damaged or hostile files.

use std::path::Path;

use super::*;

#[test]
fn image_round_trips_and_mapped_load_equals_heap_load() {
    let dim = 16;
    let (vectors, queries) = corpus_with_queries(2000, 20, dim, 0x99);
    let index = built(&vectors, dim, BuildMode::Parallel);
    let dir = temp_dir("round-trip");
    let path = dir.join("image");
    index.write(&path).unwrap();
    let heap = HnswIndex::load(&path).unwrap();
    let mapped = HnswIndex::load_mapped(&path).unwrap();
    assert!(!heap.is_mapped() && mapped.is_mapped());
    assert_eq!(heap.len(), 2000);
    assert_eq!(mapped.len(), 2000);
    assert_eq!(heap.descriptor(), index.descriptor());
    assert_eq!(mapped.descriptor(), index.descriptor());
    assert_eq!(heap.config(), index.config());
    let bytes = std::fs::metadata(&path).unwrap().len() as f64 / 2000.0;
    assert!((mapped.image_bytes_per_row().unwrap() - bytes).abs() < 1.0e-9);
    // Rows: 64 B, inv_norm 4, level 1, upper_slot 4, layer 0 (2M+1)*4 = 132 B.
    assert!(bytes > 200.0 && bytes < 260.0, "{bytes} bytes per row");

    let search = |index: &HnswIndex| {
        let results = index
            .search(&queries, 10, VectorSearchOptions::new())
            .unwrap();
        results
            .scores
            .iter()
            .map(|s| s.to_bits())
            .zip(results.slots.iter().copied())
            .collect::<Vec<_>>()
    };
    let expected = search(&index);
    assert_eq!(search(&heap), expected);
    assert_eq!(search(&mapped), expected);

    let transcript = |index: &HnswIndex, row: usize| {
        let mut out = None;
        index
            .row_transcripts(row..row + 1, &mut |_, t| out = Some(t.to_vec()))
            .unwrap();
        out.unwrap()
    };
    for row in [0, 1, 777, 1999] {
        let expected = transcript(&index, row);
        assert_eq!(expected.len(), 12 + 4 * dim);
        assert_eq!(&expected[..4], b"HNSW");
        let stored: Vec<f32> = expected[12..]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect();
        assert_eq!(stored, &vectors[row * dim..(row + 1) * dim]);
        assert_eq!(transcript(&heap, row), expected);
        assert_eq!(transcript(&mapped, row), expected);
    }
    assert!(index.row_transcripts(1999..2001, &mut |_, _| {}).is_err());

    // A mapped image is sealed.
    let mut mapped = mapped;
    assert!(mapped
        .add(&vectors[..dim], dim)
        .unwrap_err()
        .to_string()
        .contains("sealed"));
    assert!(mapped.write(&dir.join("copy")).is_err());

    // A loaded heap image keeps growing like the original.
    let mut heap = heap;
    heap.add(&corpus(50, dim, 0x97), dim).unwrap();
    assert_eq!(heap.len(), 2050);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_empty_image_round_trips() {
    let dim = 8;
    let index = HnswIndex::new(dim, config(BuildMode::FixedOrder)).unwrap();
    let results = index
        .search(&[1.0; 16], 3, VectorSearchOptions::new())
        .unwrap();
    assert_eq!(results.slots, vec![-1; 6]);
    assert_eq!(results.query_count, 2);
    let dir = temp_dir("empty");
    let path = dir.join("image");
    index.write(&path).unwrap();
    for loaded in [
        HnswIndex::load(&path).unwrap(),
        HnswIndex::load_mapped(&path).unwrap(),
    ] {
        assert_eq!(loaded.len(), 0);
        assert_eq!(loaded.dimension(), Some(dim));
        assert!(loaded.image_bytes_per_row().is_none());
        let results = loaded
            .search(&[1.0; 8], 2, VectorSearchOptions::new())
            .unwrap();
        assert_eq!(results.slots, vec![-1, -1]);
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

/// Both loaders must refuse `body` with an error naming `expect`.
fn refuse(dir: &Path, tag: &str, body: &[u8], expect: &str) {
    let path = dir.join(tag);
    std::fs::write(&path, body).unwrap();
    for (how, result) in [
        ("load", HnswIndex::load(&path)),
        ("map", HnswIndex::load_mapped(&path)),
    ] {
        let error = result
            .err()
            .unwrap_or_else(|| panic!("{how} accepted {tag}"));
        assert!(
            error.to_string().contains(expect),
            "{how} {tag}: {error} (expected {expect:?})"
        );
    }
}

/// Recompute the header checksum after a header edit.
fn recrc(b: &mut [u8]) {
    let crc = crate::wal::crc32(&b[..136]);
    b[136..140].copy_from_slice(&crc.to_le_bytes());
}

/// Recompute the trailer after a body edit.
fn reseal(b: &mut [u8]) {
    let n = b.len() - 32;
    let digest = turbo_sha256::sha256(&b[..n]).0;
    b[n..].copy_from_slice(&digest);
}

fn damaged_headers_and_bodies_are_refused(dir: &Path, bytes: &[u8]) {
    let header_len = super::super::image::header_len();
    let refuse = |tag: &str, body: &[u8], expect: &str| refuse(dir, tag, body, expect);
    refuse("empty", &[], "truncated");
    refuse("short", &bytes[..header_len - 1], "truncated");
    refuse("header-only", &bytes[..header_len], "declares");
    refuse("truncated", &bytes[..bytes.len() - 1], "declares");
    let mut magic = bytes.to_vec();
    magic[0] ^= 1;
    refuse("magic", &magic, "unknown magic");
    let mut format = bytes.to_vec();
    format[8] = 9;
    recrc(&mut format);
    refuse("format", &format, "format 9");
    let mut flipped = bytes.to_vec();
    flipped[20] ^= 1; // m, without fixing the CRC
    refuse("header-bit", &flipped, "header checksum");
    let mut body = bytes.to_vec();
    let rows_off = u64_at(bytes, 72) as usize;
    body[rows_off + 5] ^= 0x80;
    refuse("body-bit", &body, "trailer");
    let mut trailer = bytes.to_vec();
    let last = trailer.len() - 1;
    trailer[last] ^= 1;
    refuse("trailer-bit", &trailer, "trailer");
    // A header that claims a huge row count lays out past the file: refused
    // from the header alone.
    let mut huge = bytes.to_vec();
    huge[40..48].copy_from_slice(&(1u64 << 30).to_le_bytes());
    recrc(&mut huge);
    refuse("huge-rows", &huge, "section offsets");
    let mut too_many = bytes.to_vec();
    too_many[40..48].copy_from_slice(&(u64::from(u32::MAX)).to_le_bytes());
    recrc(&mut too_many);
    refuse("too-many-rows", &too_many, "limit");
    let mut bad_m = bytes.to_vec();
    bad_m[20..24].copy_from_slice(&1u32.to_le_bytes());
    recrc(&mut bad_m);
    refuse("bad-m", &bad_m, "m=1");
    let mut bad_mode = bytes.to_vec();
    bad_mode[28..32].copy_from_slice(&0u32.to_le_bytes());
    recrc(&mut bad_mode);
    refuse("bad-mode", &bad_mode, "unspecified");
    let mut bad_entry = bytes.to_vec();
    bad_entry[64..68].copy_from_slice(&5000u32.to_le_bytes());
    recrc(&mut bad_entry);
    refuse("bad-entry", &bad_entry, "entry 5000");
}

fn hostile_structures_are_refused(dir: &Path, bytes: &[u8]) {
    let refuse = |tag: &str, body: &[u8], expect: &str| refuse(dir, tag, body, expect);
    // Structurally hostile with a valid header and trailer: a neighbor id
    // past the rows, a count past the cap, a self link, a level past the
    // header's top.
    let links0_off = u64_at(bytes, 112) as usize;
    let mut wild = bytes.to_vec();
    wild[links0_off + 4..links0_off + 8].copy_from_slice(&40_000u32.to_le_bytes());
    reseal(&mut wild);
    refuse("wild-neighbor", &wild, "names neighbor 40000");
    let mut count = bytes.to_vec();
    count[links0_off..links0_off + 4].copy_from_slice(&33u32.to_le_bytes());
    reseal(&mut count);
    refuse("wild-count", &count, "lists 33 neighbors");
    let mut selfie = bytes.to_vec();
    selfie[links0_off + 4..links0_off + 8].copy_from_slice(&0u32.to_le_bytes());
    reseal(&mut selfie);
    refuse("self-link", &selfie, "names neighbor 0");
    let level_off = u64_at(bytes, 88) as usize;
    let mut tall = bytes.to_vec();
    tall[level_off] = 9;
    reseal(&mut tall);
    refuse("tall-row", &tall, "upper slot");
}

#[test]
fn corrupted_and_hostile_images_are_refused() {
    let dim = 8;
    let vectors = corpus(300, dim, 0x77);
    let index = built(&vectors, dim, BuildMode::FixedOrder);
    let dir = temp_dir("hostile");
    let good = dir.join("good");
    index.write(&good).unwrap();
    let bytes = std::fs::read(&good).unwrap();
    damaged_headers_and_bodies_are_refused(&dir, &bytes);
    hostile_structures_are_refused(&dir, &bytes);
    std::fs::remove_dir_all(&dir).unwrap();
}
