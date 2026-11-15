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

/// The sidecar of one spill bucket, keyed by source id. A sidecar with no
/// entry is right only beside an empty log bucket: a populated bucket
/// that lost its sidecar is refused, so the build never leaves transplant
/// mode for rows whose fields were spilled.
pub(super) fn read_analysis_file(path: &Path) -> Result<BTreeMap<u64, Vec<AnalyzedField>>, String> {
    use std::io::Read;
    let mut bytes = Vec::new();
    match std::fs::File::open(path) {
        Ok(mut file) => file
            .read_to_end(&mut bytes)
            .map_err(|e| format!("read {}: {e}", path.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => return Err(format!("open {}: {e}", path.display())),
    };
    let fields = parse_analysis(path, &bytes)?;
    if fields.is_empty() && bucket_log_holds_records(path)? {
        return Err(format!(
            "{}: the spill's analysis sidecar is missing or truncated beside a populated log \
             bucket; the child is not built",
            path.display()
        ));
    }
    Ok(fields)
}

/// Whether the log bucket a sidecar describes holds any record: bucket
/// `n` of the spill generation for `analysis-n.bin`, or the generation's
/// only bucket when it has one.
fn bucket_log_holds_records(sidecar: &Path) -> Result<bool, String> {
    let spill_dir = sidecar.parent().unwrap_or(Path::new("."));
    let named = sidecar
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.strip_prefix("analysis-"))
        .and_then(|digits| digits.parse::<u32>().ok())
        .ok_or_else(|| format!("{}: not an analysis sidecar name", sidecar.display()))?;
    let manifest = crate::wal::read_manifest(spill_dir)
        .map_err(|e| format!("read spill manifest in {}: {e}", spill_dir.display()))?;
    let bucket = if manifest.bucket_count == 1 { 0 } else { named };
    match std::fs::metadata(crate::wal::bucket_path(spill_dir, bucket)) {
        Ok(metadata) => Ok(metadata.len() > 0),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(format!("stat spill bucket in {}: {e}", spill_dir.display())),
    }
}

/// A read position over a sidecar's bytes; every read is bounds-checked.
struct Cursor<'a> {
    path: &'a Path,
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self
            .at
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| format!("{}: truncated analysis sidecar", self.path.display()))?;
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn u32s(&mut self, n: usize) -> Result<Vec<u32>, String> {
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            values.push(self.u32()?);
        }
        Ok(values)
    }

    fn spans(&mut self, n: usize) -> Result<Vec<(u32, u32)>, String> {
        let mut spans = Vec::with_capacity(n);
        for _ in 0..n {
            let start = self.u32()?;
            let end = self.u32()?;
            spans.push((start, end));
        }
        Ok(spans)
    }

    fn term(&mut self) -> Result<String, String> {
        let len = self.u16()? as usize;
        std::str::from_utf8(self.take(len)?)
            .map(str::to_string)
            .map_err(|e| format!("{}: term is not UTF-8: {e}", self.path.display()))
    }
}

/// Every entry of a sidecar's bytes, keyed by source id.
fn parse_analysis(path: &Path, bytes: &[u8]) -> Result<BTreeMap<u64, Vec<AnalyzedField>>, String> {
    let mut cursor = Cursor { path, bytes, at: 0 };
    let mut out = BTreeMap::new();
    while cursor.at < bytes.len() {
        let id = u64::from_le_bytes(cursor.take(8)?.try_into().expect("eight bytes"));
        let n_fields = cursor.u32()? as usize;
        let mut fields = Vec::with_capacity(n_fields);
        for _ in 0..n_fields {
            fields.push(parse_field(&mut cursor)?);
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

/// One analyzed field: its terms with spans, then the optional position,
/// arc-length, sentence, and SPLADE payloads its flags name.
fn parse_field(cursor: &mut Cursor<'_>) -> Result<AnalyzedField, String> {
    let length = cursor.u32()?;
    let flags = cursor.take(2)?;
    let extended = flags[0] & 0x80 != 0;
    let has_lengths = extended && flags[0] & 2 != 0;
    if flags[0] & !0x83 != 0
        || (!extended && flags[0] > 1)
        || flags[1] > 1
        || (has_lengths && flags[0] & 1 == 0)
    {
        return Err(format!("{}: invalid analysis flags", cursor.path.display()));
    }
    let (has_positions, has_sentences) = (flags[0] & 1 != 0, flags[1] != 0);
    let n_terms = cursor.u32()? as usize;
    let mut terms = Vec::with_capacity(n_terms);
    let mut positions = has_positions.then(|| Vec::with_capacity(n_terms));
    let mut pos_lengths = has_lengths.then(|| Vec::with_capacity(n_terms));
    for _ in 0..n_terms {
        let term = cursor.term()?;
        let tf = cursor.u32()?;
        let n_occ = cursor.u32()? as usize;
        let spans = cursor.spans(n_occ)?;
        if let Some(positions) = positions.as_mut() {
            positions.push(cursor.u32s(n_occ)?);
        }
        if let Some(lengths) = pos_lengths.as_mut() {
            lengths.push(cursor.u32s(n_occ)?);
        }
        terms.push((term, tf, spans));
    }
    let sentences = if has_sentences {
        let n = cursor.u32()? as usize;
        Some(cursor.spans(n)?)
    } else {
        None
    };
    let mut splade = Vec::new();
    let splade_scale_bits = if extended {
        let scale = cursor.u32()?;
        let count = cursor.u32()?;
        for _ in 0..count {
            let term = cursor.term()?;
            splade.push((term, cursor.u16()?));
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
    Ok(field)
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
