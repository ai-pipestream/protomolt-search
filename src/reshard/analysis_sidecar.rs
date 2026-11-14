//! The on-disk analysis sidecar a build spills analyzed fields to.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::postings::AnalyzedField;

/// One row's analyzed fields in the spill's analysis sidecar.
pub(super) fn write_analysis_entry(
    out: &mut impl std::io::Write,
    id: u64,
    fields: &[AnalyzedField],
) -> std::io::Result<()> {
    out.write_all(&id.to_le_bytes())?;
    out.write_all(&(fields.len() as u32).to_le_bytes())?;
    for field in fields {
        field
            .check_positions()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        out.write_all(&field.length.to_le_bytes())?;
        out.write_all(&[
            0x80 | u8::from(field.positions.is_some())
                | (u8::from(field.pos_lengths.is_some()) << 1),
            u8::from(field.sentences.is_some()),
        ])?;
        out.write_all(&(field.terms.len() as u32).to_le_bytes())?;
        for (i, (term, tf, spans)) in field.terms.iter().enumerate() {
            let bytes = term.as_bytes();
            let len = u16::try_from(bytes.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "term longer than 65535 bytes",
                )
            })?;
            out.write_all(&len.to_le_bytes())?;
            out.write_all(bytes)?;
            out.write_all(&tf.to_le_bytes())?;
            out.write_all(&(spans.len() as u32).to_le_bytes())?;
            for (start, end) in spans {
                out.write_all(&start.to_le_bytes())?;
                out.write_all(&end.to_le_bytes())?;
            }
            if let Some(positions) = &field.positions {
                let ordinals = positions.get(i).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "positions table shorter than the term table",
                    )
                })?;
                if ordinals.len() != spans.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "positions count differs from occurrence count",
                    ));
                }
                for ordinal in ordinals {
                    out.write_all(&ordinal.to_le_bytes())?;
                }
            }
            if let Some(lengths) = &field.pos_lengths {
                for len in &lengths[i] {
                    out.write_all(&len.to_le_bytes())?;
                }
            }
        }
        if let Some(sentences) = &field.sentences {
            out.write_all(&(sentences.len() as u32).to_le_bytes())?;
            for (start, end) in sentences {
                out.write_all(&start.to_le_bytes())?;
                out.write_all(&end.to_le_bytes())?;
            }
        }
        out.write_all(&field.splade_scale_bits.to_le_bytes())?;
        out.write_all(&(field.splade.len() as u32).to_le_bytes())?;
        for (term, impact) in &field.splade {
            let len = u16::try_from(term.len()).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "SPLADE term too long")
            })?;
            out.write_all(&len.to_le_bytes())?;
            out.write_all(term.as_bytes())?;
            out.write_all(&impact.to_le_bytes())?;
        }
    }
    Ok(())
}

/// The sidecar of one spill bucket, keyed by source id.
pub(super) fn read_analysis_file(path: &Path) -> Result<BTreeMap<u64, Vec<AnalyzedField>>, String> {
    use std::io::Read;
    let mut bytes = Vec::new();
    match std::fs::File::open(path) {
        Ok(mut file) => file
            .read_to_end(&mut bytes)
            .map_err(|e| format!("read {}: {e}", path.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(e) => return Err(format!("open {}: {e}", path.display())),
    };
    let mut at = 0usize;
    let take = |at: &mut usize, n: usize| -> Result<&[u8], String> {
        let end = at
            .checked_add(n)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| format!("{}: truncated analysis sidecar", path.display()))?;
        let out = &bytes[*at..end];
        *at = end;
        Ok(out)
    };
    let u32_at = |at: &mut usize| -> Result<u32, String> {
        Ok(u32::from_le_bytes(
            take(at, 4)?.try_into().expect("four bytes"),
        ))
    };
    let mut out = BTreeMap::new();
    while at < bytes.len() {
        let id = u64::from_le_bytes(take(&mut at, 8)?.try_into().expect("eight bytes"));
        let n_fields = u32_at(&mut at)? as usize;
        let mut fields = Vec::with_capacity(n_fields);
        for _ in 0..n_fields {
            let length = u32_at(&mut at)?;
            let flags = take(&mut at, 2)?;
            let extended = flags[0] & 0x80 != 0;
            let has_lengths = extended && flags[0] & 2 != 0;
            if flags[0] & !0x83 != 0
                || (!extended && flags[0] > 1)
                || flags[1] > 1
                || (has_lengths && flags[0] & 1 == 0)
            {
                return Err(format!("{}: invalid analysis flags", path.display()));
            }
            let (has_positions, has_sentences) = (flags[0] & 1 != 0, flags[1] != 0);
            let n_terms = u32_at(&mut at)? as usize;
            let mut terms = Vec::with_capacity(n_terms);
            let mut positions = has_positions.then(|| Vec::with_capacity(n_terms));
            let mut pos_lengths = has_lengths.then(|| Vec::with_capacity(n_terms));
            for _ in 0..n_terms {
                let len =
                    u16::from_le_bytes(take(&mut at, 2)?.try_into().expect("two bytes")) as usize;
                let term = std::str::from_utf8(take(&mut at, len)?)
                    .map_err(|e| format!("{}: term is not UTF-8: {e}", path.display()))?
                    .to_string();
                let tf = u32_at(&mut at)?;
                let n_occ = u32_at(&mut at)? as usize;
                let mut spans = Vec::with_capacity(n_occ);
                for _ in 0..n_occ {
                    let start = u32_at(&mut at)?;
                    let end = u32_at(&mut at)?;
                    spans.push((start, end));
                }
                if let Some(positions) = positions.as_mut() {
                    let mut ordinals = Vec::with_capacity(n_occ);
                    for _ in 0..n_occ {
                        ordinals.push(u32_at(&mut at)?);
                    }
                    positions.push(ordinals);
                }
                if let Some(lengths) = &mut pos_lengths {
                    let mut values = Vec::with_capacity(n_occ);
                    for _ in 0..n_occ {
                        values.push(u32_at(&mut at)?);
                    }
                    lengths.push(values);
                }
                terms.push((term, tf, spans));
            }
            let sentences = if has_sentences {
                let n = u32_at(&mut at)? as usize;
                let mut table = Vec::with_capacity(n);
                for _ in 0..n {
                    let start = u32_at(&mut at)?;
                    let end = u32_at(&mut at)?;
                    table.push((start, end));
                }
                Some(table)
            } else {
                None
            };
            let mut splade = Vec::new();
            let splade_scale_bits = if extended {
                let scale = u32_at(&mut at)?;
                let count = u32_at(&mut at)?;
                for _ in 0..count {
                    let len = u16::from_le_bytes(take(&mut at, 2)?.try_into().unwrap()) as usize;
                    let term = std::str::from_utf8(take(&mut at, len)?)
                        .map_err(|e| e.to_string())?
                        .to_owned();
                    let impact = u16::from_le_bytes(take(&mut at, 2)?.try_into().unwrap());
                    splade.push((term, impact));
                }
                scale
            } else {
                0
            };
            let field = AnalyzedField {
                terms,
                length,
                positions,
                sentences,
                pos_lengths,
                splade,
                splade_scale_bits,
            };
            field.check_positions()?;
            fields.push(field);
        }
        if out.insert(id, fields).is_some() {
            return Err(format!(
                "{}: source id {id} has two analysis entries",
                path.display()
            ));
        }
    }
    Ok(out)
}

/// The sidecar file of spill bucket `bucket` under a child's spill dir.
pub(super) fn analysis_sidecar_path(spill_dir: &Path, bucket: u32) -> PathBuf {
    spill_dir.join(format!("analysis-{bucket:05}.bin"))
}

/// Ask the kernel to drop the page cache of a file this pass is done
/// with (`posix_fadvise` DONTNEED). A split reads hundreds of gigabytes
/// of segments once and writes as much spill and output; without this
/// the run's cache footprint is that whole volume, taken from the nodes
/// serving beside it, and its resident set with it. Advice only: dirty
/// pages stay until written back, and the run's result does not depend
/// on it. Off the network build (no libc) it does nothing.
#[cfg(all(feature = "net", target_os = "linux"))]
pub(super) fn drop_page_cache(path: &Path) {
    use std::os::unix::io::AsRawFd;
    if let Ok(file) = std::fs::File::open(path) {
        // SAFETY: a plain advisory call on an open descriptor.
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }
}

#[cfg(not(all(feature = "net", target_os = "linux")))]
pub(super) fn drop_page_cache(_path: &Path) {}

/// [`drop_page_cache`] over every file of a sealed segment's directory.
pub(super) fn drop_segment_cache(root: &Path, segment_id: &str) {
    let dir = crate::segments::SegmentCatalog::segment_dir(root, segment_id);
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            drop_page_cache(&entry.path());
        }
    }
}

#[cfg(test)]
mod analysis_payload_tests {
    use super::*;
    #[test]
    fn reshard_sidecar_preserves_lattice_and_splade() {
        let field = AnalyzedField {
            terms: vec![("joined".into(), 1, vec![(0, 6)])],
            length: 2,
            positions: Some(vec![vec![0]]),
            pos_lengths: Some(vec![vec![2]]),
            splade: vec![("neural".into(), 200)],
            splade_scale_bits: 0.01_f32.to_bits(),
            ..Default::default()
        };
        let path = std::env::temp_dir().join(format!("review125-analysis-{}", std::process::id()));
        let mut bytes = vec![];
        write_analysis_entry(&mut bytes, 0, std::slice::from_ref(&field)).unwrap();
        std::fs::write(&path, bytes).unwrap();
        let rows = read_analysis_file(&path).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(
            rows[&0][0], field,
            "reshard sidecar silently dropped new payloads"
        );
    }
}

#[cfg(test)]
mod legacy_analysis_tests {
    use super::*;
    #[test]
    fn old_analysis_sidecars_still_decode_and_truncated_new_ones_fail() {
        let field = AnalyzedField {
            terms: vec![("x".into(), 1, vec![(0, 1)])],
            length: 1,
            positions: Some(vec![vec![0]]),
            ..Default::default()
        };
        let mut bytes = vec![];
        write_analysis_entry(&mut bytes, 9, std::slice::from_ref(&field)).unwrap();
        let path = std::env::temp_dir().join(format!("legacy-analysis-{}", std::process::id()));
        for end in [bytes.len() - 1, bytes.len() - 4, bytes.len() - 7] {
            std::fs::write(&path, &bytes[..end]).unwrap();
            assert!(read_analysis_file(&path).is_err());
        }
        // Legacy fields use only the low positions flag, without the new
        // trailing scale/count fields. This is the original writer's layout.
        bytes[16] &= 1;
        bytes.truncate(bytes.len() - 8);
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(read_analysis_file(&path).unwrap()[&9], vec![field]);
        std::fs::remove_file(path).unwrap();
    }
}
