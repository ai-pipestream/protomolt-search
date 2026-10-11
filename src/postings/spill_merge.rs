use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// The merge-time stage of one positional field's kind-7 section:
/// ordinals stream to a file in directory order as the merge emits
/// occurrences, and the per-term bases accumulate in heap (4 B per
/// term, never the memory ceiling the spill exists for).
pub(crate) struct PositionsStage {
    pub(crate) writer: io::BufWriter<std::fs::File>,
    pub(crate) path: PathBuf,
    pub(crate) bases: Vec<u32>,
    pub(crate) total: u64,
}

/// One run's read cursor for the merge: the current term group's header
/// plus a reader positioned at its postings.
pub(crate) struct RunHead {
    pub(crate) reader: io::BufReader<std::fs::File>,
    pub(crate) term: String,
    pub(crate) n_postings: u32,
    /// Whether each posting is followed by one ordinal per occurrence
    /// (a positional field's run encoding).
    pub(crate) positional: bool,
}

impl RunHead {
    pub(crate) fn open(path: &Path, positional: bool) -> io::Result<Option<Self>> {
        let reader = io::BufReader::new(std::fs::File::open(path)?);
        let mut head = Self {
            reader,
            term: String::new(),
            n_postings: 0,
            positional,
        };
        Ok(if head.advance()? { Some(head) } else { None })
    }

    /// Read the next term group header; false at end of run.
    pub(crate) fn advance(&mut self) -> io::Result<bool> {
        let mut len_buf = [0u8; 2];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e),
        }
        let term_len = u16::from_le_bytes(len_buf) as usize;
        let mut term = vec![0u8; term_len];
        self.reader.read_exact(&mut term)?;
        self.term = String::from_utf8(term)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf-8 in run"))?;
        let mut n = [0u8; 4];
        self.reader.read_exact(&mut n)?;
        self.n_postings = u32::from_le_bytes(n);
        Ok(true)
    }

    /// Copy this group's postings to `out` in the final v3 encoding
    /// (which is the run encoding), returning bytes written.
    pub(crate) fn copy_postings<W: Write>(&mut self, out: &mut W) -> io::Result<u64> {
        if self.positional {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the v4 encoding cannot copy a positional run",
            ));
        }
        let mut written = 0u64;
        for _ in 0..self.n_postings {
            let mut fixed = [0u8; 12];
            self.reader.read_exact(&mut fixed)?;
            out.write_all(&fixed)?;
            let n_offsets = u32::from_le_bytes(fixed[8..12].try_into().expect("4 bytes"));
            let mut offsets = vec![0u8; 8 * n_offsets as usize];
            self.reader.read_exact(&mut offsets)?;
            out.write_all(&offsets)?;
            written += 12 + 8 * u64::from(n_offsets);
        }
        Ok(written)
    }

    /// Read one posting of the current group as `(doc_id, tf, raw offset
    /// bytes, raw ordinal bytes)` — the v5 merge splits the runs, and
    /// the run encoding's offset bytes are already the v5 occurrence-run
    /// pairs, its ordinal bytes already the kind-7 entries (empty on a
    /// field without positions).
    pub(crate) fn next_posting_raw(&mut self) -> io::Result<(u32, u32, Vec<u8>, Vec<u8>)> {
        let mut fixed = [0u8; 12];
        self.reader.read_exact(&mut fixed)?;
        let doc_id = u32::from_le_bytes(fixed[0..4].try_into().expect("4 bytes"));
        let tf = u32::from_le_bytes(fixed[4..8].try_into().expect("4 bytes"));
        let n_offsets = u32::from_le_bytes(fixed[8..12].try_into().expect("4 bytes")) as usize;
        let mut offsets = vec![0u8; 8 * n_offsets];
        self.reader.read_exact(&mut offsets)?;
        let mut ordinals = Vec::new();
        if self.positional {
            ordinals = vec![0u8; 4 * n_offsets];
            self.reader.read_exact(&mut ordinals)?;
        }
        Ok((doc_id, tf, offsets, ordinals))
    }
}
