//! Read cursor over one sorted spill run during the merge.

use std::io::{self, Read};
use std::path::Path;

pub(super) type RawSpillPosting = (u32, u32, Vec<u8>, Vec<u8>, Vec<u8>);

/// One run's read cursor for the merge: the current term group's header
/// plus a reader positioned at its postings.
pub(super) struct RunHead {
    reader: io::BufReader<std::fs::File>,
    pub(super) term: String,
    pub(super) n_postings: u32,
    positional: bool,
}

impl RunHead {
    pub(super) fn open(path: &Path, positional: bool) -> io::Result<Option<Self>> {
        let reader = io::BufReader::new(std::fs::File::open(path)?);
        let mut head = Self {
            reader,
            term: String::new(),
            n_postings: 0,
            positional,
        };
        Ok(if head.advance()? { Some(head) } else { None })
    }

    pub(super) fn advance(&mut self) -> io::Result<bool> {
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

    pub(super) fn next_posting_raw(&mut self) -> io::Result<RawSpillPosting> {
        let mut fixed = [0u8; 12];
        self.reader.read_exact(&mut fixed)?;
        let doc_id = u32::from_le_bytes(fixed[0..4].try_into().expect("4 bytes"));
        let tf = u32::from_le_bytes(fixed[4..8].try_into().expect("4 bytes"));
        let n_offsets = u32::from_le_bytes(fixed[8..12].try_into().expect("4 bytes")) as usize;
        let mut offsets = vec![0u8; 8 * n_offsets];
        self.reader.read_exact(&mut offsets)?;
        let mut ordinals = Vec::new();
        let mut pos_lengths = Vec::new();
        if self.positional {
            ordinals = vec![0u8; 4 * n_offsets];
            self.reader.read_exact(&mut ordinals)?;
            let mut flag = [0u8; 1];
            self.reader.read_exact(&mut flag)?;
            if flag[0] != 0 {
                pos_lengths = vec![0u8; 4 * n_offsets];
                self.reader.read_exact(&mut pos_lengths)?;
            }
        }
        Ok((doc_id, tf, offsets, ordinals, pos_lengths))
    }
}
