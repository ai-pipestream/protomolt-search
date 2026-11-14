use std::io::{self, Write};
use std::path::Path;

use super::*;

/// Build a store whose v5 file exercises every validated structure:
/// one term with >4096 postings (2 level-1 groups), other terms,
/// gap slots, lineages, offsets.
fn malformed_corpus_store() -> Bm25Store {
    let mut store = Bm25Store::new();
    for i in 0..4300u32 {
        let id = i * 2; // gap slots
        let lineage = (i % 3 == 0).then_some(DocLineage {
            parent_id: u64::from(i),
            group_id: u64::from(i) * 7,
            span_start: i,
            span_end: i + 10,
        });
        let mut terms: DocTerms = vec![
            ("hot".to_string(), 1 + i % 3, vec![(i, i + 2)]),
            (format!("t{}", i % 7), 1, vec![(0, 1)]),
        ];
        if i % 11 == 0 {
            terms.push(("cold".to_string(), 2, vec![(3, 9)]));
        }
        let length = terms.iter().map(|t| t.1).sum();
        store.add_document_with_lineage(
            id,
            format!("d{i}"),
            AnalyzedDoc::body(terms, length),
            lineage,
        );
    }
    store
}

fn write_v4(store: &Bm25Store, path: &Path) {
    let mut w = io::BufWriter::new(std::fs::File::create(path).unwrap());
    store.write_v4_for_bench(&mut w).unwrap();
    w.flush().unwrap();
}

/// A small store (~few KB on disk) for the every-byte truncation
/// sweep: gaps, lineages, one multi-block term, offsets.
fn small_corpus_store() -> Bm25Store {
    let mut store = Bm25Store::new();
    for i in 0..140u32 {
        let id = i * 2;
        let lineage = (i % 3 == 0).then_some(DocLineage {
            parent_id: u64::from(i),
            group_id: u64::from(i) * 7,
            span_start: i,
            span_end: i + 10,
        });
        let terms: DocTerms = vec![
            ("hot".to_string(), 1 + i % 3, vec![(i, i + 2)]),
            (format!("t{}", i % 3), 1, vec![(0, 1)]),
        ];
        let length = terms.iter().map(|t| t.1).sum();
        store.add_document_with_lineage(
            id,
            format!("d{i}"),
            AnalyzedDoc::body(terms, length),
            lineage,
        );
    }
    store
}

/// Truncation at EVERY byte length must error, never panic — on a
/// small file so the sweep stays fast. v6, v5, and v4.
#[test]
fn truncated_open_errors_never_panics() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/tmp")
        .join(format!("truncate_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = small_corpus_store();
    let v6_path = dir.join("small.v6.bm25");
    let v5_path = dir.join("small.v5.bm25");
    let v4_path = dir.join("small.v4.bm25");
    store.save(&v6_path).unwrap();
    store.save_v5(&v5_path).unwrap();
    write_v4(&store, &v4_path);
    for (tag, src) in [("v6", &v6_path), ("v5", &v5_path), ("v4", &v4_path)] {
        let bytes = std::fs::read(src).unwrap();
        Bm25Reader::open(src).unwrap();
        let trunc = dir.join(format!("trunc.{tag}"));
        for len in 0..bytes.len() {
            std::fs::write(&trunc, &bytes[..len]).unwrap();
            assert!(
                Bm25Reader::open(&trunc).is_err(),
                "{tag}: truncation to {len} bytes opened successfully"
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn corrupted_open_errors_never_panics() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/tmp")
        .join(format!("malformed_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = malformed_corpus_store();
    let v6_path = dir.join("corpus.v6.bm25");
    let v5_path = dir.join("corpus.v5.bm25");
    let v4_path = dir.join("corpus.v4.bm25");
    store.save(&v6_path).unwrap();
    store.save_v5(&v5_path).unwrap();
    write_v4(&store, &v4_path);

    for (tag, src) in [("v6", &v6_path), ("v5", &v5_path), ("v4", &v4_path)] {
        let bytes = std::fs::read(src).unwrap();
        // Sanity: the intact file opens.
        Bm25Reader::open(src).unwrap();

        // Random single-byte flips in the header, directory, and
        // skip-run regions: error or valid open, never a panic.
        let mut s = 0xC0FFEEu64;
        let mut next = move || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (s >> 33) as usize
        };
        // v6: flips anywhere — the explicit section table makes
        // the whole file validated structure. v5/v4: header,
        // directory, and (v5) skip-run regions from the fixed
        // header: magic(8) total_length(8) texts(8) lineages(8)
        // postings(8) directory(8) n_slots(4).
        let regions: Vec<(usize, usize)> = if tag == "v6" {
            vec![(0, bytes.len())]
        } else {
            let directory_off = u64::from_le_bytes(bytes[40..48].try_into().unwrap()) as usize;
            let n_terms =
                u32::from_le_bytes(bytes[directory_off..directory_off + 4].try_into().unwrap())
                    as usize;
            let mut regions: Vec<(usize, usize)> = vec![(0, 52), (directory_off, bytes.len())];
            if tag == "v5" {
                for i in 0..n_terms {
                    let e = directory_off + 4 + 34 * i;
                    let skip_off =
                        u64::from_le_bytes(bytes[e + 8..e + 16].try_into().unwrap()) as usize;
                    let end = if i + 1 < n_terms {
                        u64::from_le_bytes(bytes[e + 34..e + 42].try_into().unwrap()) as usize
                    } else {
                        directory_off
                    };
                    regions.push((skip_off, end));
                }
            }
            regions
        };
        // v3/v4 flips stay in the header and directory: postings
        // record payloads are data (like occurrence bytes and
        // frontier pairs), not validated structure.
        let flipped = dir.join(format!("flip.{tag}"));
        let (mut errors, mut opens) = (0usize, 0usize);
        for _ in 0..2000 {
            let &(lo, hi) = &regions[next() % regions.len()];
            let pos = lo + next() % (hi - lo).max(1);
            let mut bad = bytes.clone();
            bad[pos] ^= 1 + (next() % 255) as u8;
            std::fs::write(&flipped, &bad).unwrap();
            match Bm25Reader::open(&flipped) {
                Ok(reader) => {
                    opens += 1;
                    // A valid open must stay usable on the read
                    // paths the structure guards (no panics).
                    let _ = reader.df("hot");
                    reader.for_each_doc_tf("hot", &mut |_, _| {});
                    let _ = reader.posting_offsets("hot", 0);
                }
                Err(_) => errors += 1,
            }
        }
        assert!(errors > 0, "{tag}: no flip was ever rejected");
        eprintln!("{tag}: flips: {errors} rejected, {opens} opened-valid");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
