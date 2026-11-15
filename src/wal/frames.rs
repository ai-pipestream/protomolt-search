//! Frame codec, the per-file record reader, and the recovery scan.

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use prost::Message;

use super::crc::crc32;
use super::sources;
use crate::pb::wal::{wal_record, WalRecord};

/// The three ways a frame read can end: a complete frame, a clean
/// end-of-log, or a torn tail (crash mid-append) at `offset`.
pub(super) enum FrameRead {
    Frame(Vec<u8>),
    End,
    Torn { offset: u64 },
}

/// Read until `buf` is full or EOF; returns how many bytes were read.
pub(super) fn read_up_to(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match reader.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(m) => n += m,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

pub(super) fn read_frame(reader: &mut impl Read, offset: &mut u64) -> io::Result<FrameRead> {
    read_frame_limited(reader, offset, u32::MAX as u64)
}

pub(super) fn read_frame_limited(
    reader: &mut impl Read,
    offset: &mut u64,
    max_payload: u64,
) -> io::Result<FrameRead> {
    let start = *offset;
    let mut len_buf = [0u8; 4];
    match read_up_to(reader, &mut len_buf)? {
        0 => return Ok(FrameRead::End), // clean end-of-log
        4 => {}
        _ => return Ok(FrameRead::Torn { offset: start }),
    }
    *offset += 4;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len as u64 > max_payload {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL frame exceeds its source address",
        ));
    }
    let mut crc_buf = [0u8; 4];
    if read_up_to(reader, &mut crc_buf)? < 4 {
        return Ok(FrameRead::Torn { offset: start });
    }
    *offset += 4;
    let mut payload = vec![0u8; len];
    if read_up_to(reader, &mut payload)? < len {
        return Ok(FrameRead::Torn { offset: start });
    }
    *offset += len as u64;
    let expected = u32::from_le_bytes(crc_buf);
    if crc32(&payload) != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wal: crc mismatch in frame at byte offset {start}"),
        ));
    }
    Ok(FrameRead::Frame(payload))
}

pub(super) fn write_frame(file: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.write_all(&crc32(payload).to_le_bytes())?;
    file.write_all(payload)
}

/// Sequential replay over one bucket or markers file. Validates per-frame
/// CRC and seq continuity; a torn tail frame ends the file with a warning
/// (see the module docs).
pub struct RecordReader {
    pub(super) reader: BufReader<File>,
    pub(super) path: PathBuf,
    pub(super) offset: u64,
    pub(super) next_seq: u64,
    pub(super) done: bool,
    sources: Option<sources::Reader>,
}

impl RecordReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
            path: path.to_path_buf(),
            offset: 0,
            next_seq: 1,
            done: false,
            sources: None,
        })
    }

    /// Resume a sequential read at `offset`, where the next frame carries
    /// seq `next_seq` — the position a previous reader stopped at
    /// ([`ClockedTail`]). Validation continues from there exactly as a
    /// read from the start would.
    pub fn open_at(path: &Path, offset: u64, next_seq: u64) -> io::Result<Self> {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        Ok(Self {
            reader: BufReader::new(file),
            path: path.to_path_buf(),
            offset,
            next_seq,
            done: false,
            sources: None,
        })
    }

    /// Byte offset one past the last frame consumed — the length of the
    /// valid prefix so far. Inspection tooling uses it to detect a torn
    /// tail (`offset() < file length` after the reader ends).
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The next record, or `None` at the end of the file (including a
    /// torn tail, which is reported on stderr and treated as the end).
    pub fn next_record(&mut self) -> io::Result<Option<WalRecord>> {
        if self.done {
            return Ok(None);
        }
        let frame_start = self.offset;
        let frame = match read_frame(&mut self.reader, &mut self.offset)? {
            FrameRead::Frame(payload) => payload,
            FrameRead::End => {
                self.done = true;
                return Ok(None);
            }
            FrameRead::Torn { offset } => {
                eprintln!(
                    "wal: ignoring torn tail frame at byte offset {offset} in {}",
                    self.path.display()
                );
                self.done = true;
                return Ok(None);
            }
        };
        let mut record = WalRecord::decode(frame.as_slice()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "wal: undecodable record at byte offset {frame_start} in {}: {e}",
                    self.path.display()
                ),
            )
        })?;
        if record.seq != self.next_seq {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "wal: seq gap at byte offset {frame_start} in {}: expected {}, got {}",
                    self.path.display(),
                    self.next_seq,
                    record.seq
                ),
            ));
        }
        if record.clock == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "wal: record at byte offset {frame_start} in {} carries no logical clock",
                    self.path.display()
                ),
            ));
        }
        if let Some(wal_record::Op::AddDocuments(batch)) = record.op.as_mut() {
            if !batch.source_references.is_empty() {
                if self.sources.is_none() {
                    self.sources = Some(sources::Reader::open(
                        self.path.parent().unwrap_or(Path::new(".")),
                    )?);
                }
                self.sources.as_mut().expect("opened").restore(batch)?;
            }
        }
        self.next_seq += 1;
        Ok(Some(record))
    }
}

/// The recovery summary of one file after a crash: the seq of the last
/// intact record and the byte length of the valid prefix (append resumes
/// there; the torn tail is truncated).
pub struct RecordScan {
    pub last_seq: u64,
    pub valid_len: u64,
    /// Highest generation-wide logical clock in this file.
    pub max_clock: u64,
}

/// Scan one file tolerantly for reopen-after-crash. Like [`RecordReader`]
/// but never fails on an empty file or torn tail; CRC/seq errors inside
/// complete frames are still hard errors.
pub fn scan_records(path: &Path) -> io::Result<RecordScan> {
    let mut reader = RecordReader::open(path)?;
    let mut scan = RecordScan {
        last_seq: 0,
        valid_len: 0,
        max_clock: 0,
    };
    loop {
        let frame_start = reader.offset;
        match reader.next_record()? {
            Some(record) => {
                scan.last_seq = record.seq;
                scan.max_clock = scan.max_clock.max(record.clock);
                scan.valid_len = reader.offset;
            }
            None => {
                if reader.offset > frame_start {
                    // Torn tail: the valid prefix ends where it began.
                    scan.valid_len = frame_start;
                }
                return Ok(scan);
            }
        }
    }
}
