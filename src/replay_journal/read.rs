//! Bounded pagination over the accepted prefix.

use prost::Message;
use redb::ReadableDatabase;
use tonic::Status;

use super::{storage, ReplayJournal, FRAMES, MAX_FRAME_BYTES, META};
use crate::pb::{ReadReplayJournalRequest, ReadReplayJournalResponse};

impl ReplayJournal {
    /// Bounded snapshot pagination. No record can exceed the caller's byte
    /// budget, including the first record; failure never advances the cursor.
    pub fn read(
        &self,
        request: &ReadReplayJournalRequest,
    ) -> Result<ReadReplayJournalResponse, Status> {
        if !(1..=1000).contains(&request.limit)
            || request.max_bytes == 0
            || request.max_bytes > MAX_FRAME_BYTES as u64
        {
            return Err(Status::invalid_argument(
                "replay reads require limit 1..1000 and max_bytes 1..64 MiB",
            ));
        }
        let tx = self.database.begin_read().map_err(storage)?;
        let meta = tx.open_table(META).map_err(storage)?;
        let frames = tx.open_table(FRAMES).map_err(storage)?;
        let header = self.checked_header(
            meta.get("header")
                .map_err(storage)?
                .ok_or_else(|| Status::data_loss("replay journal header is missing"))?
                .value(),
            &frames,
        )?;
        self.read_policy(&header, &meta)?;
        let through = request.through_sequence.unwrap_or(header.accepted_sequence);
        if request.after_sequence > through || through > header.accepted_sequence {
            return Err(Status::invalid_argument(
                "replay read cursor or fence exceeds the accepted prefix",
            ));
        }
        let (mut previous, mut clock) = if request.after_sequence == 0 {
            (self.digest.clone(), self.binding.baseline_clock)
        } else {
            let prior = self.read_frame(&frames, request.after_sequence)?;
            (prior.sha256, prior.source_clock)
        };
        let mut out = Vec::new();
        let mut bytes = 0u64;
        let mut next = request.after_sequence;
        while next < through && out.len() < request.limit as usize {
            let frame = self.read_frame(&frames, next + 1)?;
            if frame.previous_sha256 != previous || frame.source_clock <= clock {
                return Err(Status::data_loss(
                    "replay journal prefix has a divergent chain or source clock",
                ));
            }
            let size = frame.encoded_len() as u64;
            if size > request.max_bytes - bytes {
                if out.is_empty() {
                    return Err(Status::resource_exhausted(
                        "first replay frame exceeds the requested byte budget",
                    ));
                }
                break;
            }
            bytes += size;
            previous = frame.sha256.clone();
            clock = frame.source_clock;
            next = frame.sequence;
            out.push(frame);
        }
        Ok(ReadReplayJournalResponse {
            frames: out,
            through_sequence: through,
            next_sequence: next,
            complete: next == through,
        })
    }
}
