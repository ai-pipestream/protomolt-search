//! The v4-to-v5 transcode: rewrite one legacy file through the v5 three-run
//! layout (`docs/block-max.md`), validating structure before and after.

use super::*;

use std::io::{self, Write};
use std::path::Path;

/// Statistics from one [`transcode_to_v5`] run.
#[derive(Debug, Clone, Copy)]
pub struct TranscodeStats {
    /// Terms transcoded.
    pub n_terms: u32,
    /// Document slots carried over.
    pub n_slots: u32,
    /// Postings walked.
    pub postings: u64,
    /// Source file size in bytes.
    pub bytes_in: u64,
    /// Output file size in bytes.
    pub bytes_out: u64,
}

/// Rewrite a v3/v4 `.bm25` file as v5 (`TVBM2505`) without re-analysis:
/// the shared sections (doc_lengths, texts, text_index, lineages) are
/// byte-copied from the source map, and each term's interleaved v3/v4
/// postings are re-run into the v5 doc/occurrence/skip runs in two
/// map walks (doc run + skip state first, then the occurrence bytes,
/// which ARE the v5 occurrence-run encoding, copied map to file, never
/// staged). Heap per term is the skip builder's state plus its level-0
/// record bytes (~70 B per 128 postings, single-digit MB even for a
/// df-in-the-millions term), NOT the occurrence data — a hot term in a
/// 50 GB shard carries gigabytes of occurrence pairs, and staging
/// those was an OOM on real shards.
///
/// The output is byte-identical to what [`Bm25Store::save_v5`] would
/// write for the same corpus (pinned by test), so everything proven of
/// written-v5 files (dual-writer identity, pruned == exhaustive) holds
/// of transcoded ones. The source must be v3 or v4; the write is
/// atomic (tmp + rename) and the source is not modified.
pub fn transcode_to_v5(src: &Path, dst: &Path) -> io::Result<TranscodeStats> {
    use std::io::Seek;
    let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
    let reader = Bm25Reader::open(src)?;
    if reader.v5_runs {
        return Err(invalid("source is already v5 or v6"));
    }
    let map: &[u8] = &reader.map;
    let u32_at = |off: usize| u32::from_le_bytes(map[off..off + 4].try_into().unwrap());
    let total_length = u64::from_le_bytes(map[8..16].try_into().unwrap());
    let texts_off = u64::from_le_bytes(map[16..24].try_into().unwrap());
    let lineages_off = u64::from_le_bytes(map[24..32].try_into().unwrap());
    let postings_off = u64::from_le_bytes(map[32..40].try_into().unwrap());
    let n_slots = u32_at(48);
    let field0 = &reader.fields[0];
    let n_terms = field0.n_terms;

    let tmp = dst.with_extension("bm25tmp");
    let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
    w.write_all(MAGIC_V5)?;
    write_u64(&mut w, total_length)?;
    write_u64(&mut w, texts_off)?;
    write_u64(&mut w, lineages_off)?;
    write_u64(&mut w, postings_off)?;
    write_u64(&mut w, 0)?; // directory_off, patched after the postings pass
    write_u32(&mut w, n_slots)?;
    // Shared sections: identical bytes in v3/v4/v5, at identical
    // offsets (the header is the same 52 bytes), so the text index's
    // absolute offsets stay valid verbatim.
    w.write_all(&map[52..postings_off as usize])?;

    write_u32(&mut w, n_terms)?;
    let mut directory: Vec<(u64, u64, u64, u32)> = Vec::with_capacity(n_terms as usize);
    let mut cursor = postings_off + 4;
    let mut postings_walked = 0u64;
    for i in 0..n_terms {
        let (_, term_off, df) = reader.directory_entry(field0, i);
        // Step over the term's inline header: u32 len, term, u32 count.
        let term_len = u32_at(term_off as usize) as usize;
        let postings_start = term_off as usize + 4 + term_len + 4;
        let doc_run_off = cursor;
        // Pass 1: the doc run (doc_id, tf, occ_start) plus skip-run
        // state, straight off the map.
        let mut skip_l0: Vec<u8> = Vec::new();
        let mut skip = SkipRunBuilder::new();
        let mut occ_start = 0u32;
        let mut occ_bytes = 0u64;
        let mut p = postings_start;
        for _ in 0..df {
            let doc_id = u32_at(p);
            let tf = u32_at(p + 4);
            let n_offsets = u32_at(p + 8) as usize;
            write_u32(&mut w, doc_id)?;
            write_u32(&mut w, tf)?;
            write_u32(&mut w, occ_start)?;
            skip.push(tf, reader.doc_length(doc_id), doc_id, &mut skip_l0)?;
            occ_start = occ_start
                .checked_add(n_offsets as u32)
                .ok_or_else(|| invalid("occurrence run exceeds u32 pairs"))?;
            occ_bytes += 8 * n_offsets as u64;
            p += 12 + 8 * n_offsets;
        }
        write_u32(&mut w, occ_start)?; // sentinel
                                       // Pass 2: occurrence bytes, copied map to file per posting —
                                       // no staging, so the term's occurrence volume never touches
                                       // the heap. The re-walk revisits pages just read; the doc-run
                                       // fields it steps over are cheaper than staging gigabytes.
        let mut p = postings_start;
        for _ in 0..df {
            let n_offsets = u32_at(p + 8) as usize;
            w.write_all(&map[p + 12..p + 12 + 8 * n_offsets])?;
            p += 12 + 8 * n_offsets;
        }
        let (l0_bytes, l1) = skip.finish(&mut skip_l0)?;
        debug_assert_eq!(l0_bytes, skip_l0.len() as u64);
        let occ_run_off = doc_run_off + 12 * u64::from(df) + 4;
        let skip_run_off = occ_run_off + occ_bytes;
        write_skip_run(&mut w, &skip_l0, &l1)?;
        directory.push((doc_run_off, skip_run_off, occ_run_off, df));
        cursor = skip_run_off + skip_run_size(l0_bytes, &l1);
        postings_walked += u64::from(df);
    }
    let directory_off = cursor;
    write_u32(&mut w, n_terms)?;
    let mut blob_off = 0u64; // relative to the term blob start
    for (i, &(doc_off, skip_off, occ_off, df)) in directory.iter().enumerate() {
        let (term, _, _) = reader.directory_entry(field0, i as u32);
        write_u64(&mut w, doc_off)?;
        write_u64(&mut w, skip_off)?;
        write_u64(&mut w, occ_off)?;
        write_u32(&mut w, df)?;
        write_u32(
            &mut w,
            u32::try_from(blob_off).expect("term blob exceeds u32"),
        )?;
        write_u16(&mut w, term.len() as u16)?;
        blob_off += term.len() as u64;
    }
    for i in 0..n_terms {
        let (term, _, _) = reader.directory_entry(field0, i);
        w.write_all(term)?;
    }
    w.flush()?;
    let mut f = w
        .into_inner()
        .map_err(|e| io::Error::other(e.to_string()))?;
    let bytes_out = f.stream_position()?;
    f.seek(io::SeekFrom::Start(40))?;
    f.write_all(&directory_off.to_le_bytes())?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, dst)?;
    fsync_parent(dst)?;
    Ok(TranscodeStats {
        n_terms,
        n_slots,
        postings: postings_walked,
        bytes_in: map.len() as u64,
        bytes_out,
    })
}
