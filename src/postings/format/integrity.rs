//! The v8 integrity envelope: trailer, section table, and CRC partition checks.

use std::io;

use super::{TRAILER_LEN, TRAILER_MAGIC};

/// One v8 integrity-table entry: a named payload byte range and the
/// CRC32 its build recorded for it.
#[derive(Debug, Clone)]
pub(in crate::postings) struct IntegrityEntry {
    pub name: String,
    pub off: u64,
    pub len: u64,
    pub crc: u32,
}

/// The parsed v8 integrity envelope.
#[derive(Debug)]
pub(in crate::postings) struct IntegrityTable {
    pub entries: Vec<IntegrityEntry>,
    /// Where the payload ends and the integrity section begins.
    pub payload_len: u64,
    /// Whether the payload is v7-shaped (column table) or v6-shaped.
    pub base_v7: bool,
}

/// The big lazily-paged blobs are the only sections open does not
/// CRC-verify; everything else is read at open anyway, so checking it
/// there is nearly free. `verify_integrity` checks these too.
pub(in crate::postings) fn integrity_eager(name: &str) -> bool {
    name != "texts" && !name.ends_with(":postings")
}

/// Parse and check the v8 envelope of `full` (a whole file, leading
/// magic already matched): trailer, table, table CRC, and that the
/// entries partition the payload exactly. Section CRCs are NOT
/// verified here — callers decide eager vs deep.
pub(in crate::postings) fn parse_integrity(full: &[u8]) -> io::Result<IntegrityTable> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let (trailer, integrity_off, base_v7) = parse_integrity_trailer(full)?;
    let u64_at = |off: u64| -> u64 {
        u64::from_le_bytes(
            full[off as usize..off as usize + 8]
                .try_into()
                .expect("8 bytes"),
        )
    };
    let need = |cur: u64, n: u64| -> io::Result<()> {
        if cur + n > trailer {
            return Err(invalid(format!(
                "v8 integrity table runs past its section at {cur}"
            )));
        }
        Ok(())
    };
    let mut cur = integrity_off;
    need(cur, 4)?;
    let n_entries = u32::from_le_bytes(full[cur as usize..cur as usize + 4].try_into().unwrap());
    cur += 4;
    if n_entries == 0 {
        return Err(invalid("v8 integrity table with zero entries".into()));
    }
    let max_entries = (trailer - integrity_off).saturating_sub(8) / 23;
    if u64::from(n_entries) > max_entries {
        return Err(invalid(format!(
            "v8 integrity table declares {n_entries} entries, at most {max_entries} fit its section"
        )));
    }
    let mut entries = Vec::with_capacity(n_entries as usize);
    for i in 0..n_entries {
        need(cur, 2)?;
        let name_len = u64::from(u16::from_le_bytes(
            full[cur as usize..cur as usize + 2].try_into().unwrap(),
        ));
        need(cur + 2, name_len + 20)?;
        if name_len == 0 {
            return Err(invalid(format!("v8 integrity entry {i}: empty name")));
        }
        let name = std::str::from_utf8(&full[(cur + 2) as usize..(cur + 2 + name_len) as usize])
            .map_err(|_| invalid(format!("v8 integrity entry {i}: name is not UTF-8")))?
            .to_string();
        let base = cur + 2 + name_len;
        let crc = u32::from_le_bytes(
            full[(base + 16) as usize..(base + 20) as usize]
                .try_into()
                .unwrap(),
        );
        entries.push(IntegrityEntry {
            name,
            off: u64_at(base),
            len: u64_at(base + 8),
            crc,
        });
        cur = base + 20;
    }
    need(cur, 4)?;
    verify_integrity_crc_and_partition(full, &entries, integrity_off, cur, trailer)?;
    Ok(IntegrityTable {
        entries,
        payload_len: integrity_off,
        base_v7,
    })
}

fn parse_integrity_trailer(full: &[u8]) -> io::Result<(u64, u64, bool)> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let file_len = full.len() as u64;
    if file_len < 52 + TRAILER_LEN {
        return Err(invalid("v8 file shorter than header plus trailer".into()));
    }
    let trailer = file_len - TRAILER_LEN;
    if &full[(trailer + 16) as usize..] != TRAILER_MAGIC {
        return Err(invalid(
            "v8 trailer magic missing: truncated or overwritten tail".into(),
        ));
    }
    let u64_at = |off: u64| -> u64 {
        u64::from_le_bytes(
            full[off as usize..off as usize + 8]
                .try_into()
                .expect("8 bytes"),
        )
    };
    let integrity_off = u64_at(trailer);
    let base_v7 = match u64_at(trailer + 8) {
        6 => false,
        7 => true,
        v => {
            return Err(invalid(format!(
                "v8 trailer names base version {v}, not 6 or 7"
            )))
        }
    };
    if integrity_off < 52 || integrity_off > trailer {
        return Err(invalid(format!(
            "v8 integrity section offset {integrity_off} outside [52, {trailer}]"
        )));
    }
    Ok((trailer, integrity_off, base_v7))
}

fn verify_integrity_crc_and_partition(
    full: &[u8],
    entries: &[IntegrityEntry],
    integrity_off: u64,
    crc_off: u64,
    trailer: u64,
) -> io::Result<()> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let stored_table_crc = u32::from_le_bytes(
        full[crc_off as usize..crc_off as usize + 4]
            .try_into()
            .unwrap(),
    );
    let computed = crate::wal::crc32(&full[integrity_off as usize..crc_off as usize]);
    if stored_table_crc != computed {
        return Err(invalid(format!(
            "v8 integrity table CRC mismatch: stored {stored_table_crc:08x}, computed {computed:08x}"
        )));
    }
    if crc_off + 4 != trailer {
        return Err(invalid(format!(
            "v8 integrity section ends at {} but the trailer starts at {trailer}",
            crc_off + 4
        )));
    }
    let mut expected = 0u64;
    for e in entries {
        if e.off != expected {
            return Err(invalid(format!(
                "v8 integrity entry {} starts at {} but the previous section ended at {expected}",
                e.name, e.off
            )));
        }
        expected = e
            .off
            .checked_add(e.len)
            .ok_or_else(|| invalid(format!("v8 integrity entry {}: length overflows", e.name)))?;
    }
    if expected != integrity_off {
        return Err(invalid(format!(
            "v8 integrity entries cover [0, {expected}) but the payload is [0, {integrity_off})"
        )));
    }
    Ok(())
}
