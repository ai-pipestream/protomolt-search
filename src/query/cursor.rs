//! Search-after cursors: the token a page mints and the next page
//! resumes from.

use tonic::Status;

use super::refuse;
use crate::pb::{QueryHit, QueryRequest};

/// A search-after boundary: the last returned hit's absolute rank,
/// exact score bits, and doc id. The score-bits equality on resume is
/// the corpus-state check — search here is bitwise deterministic, so a
/// moved score means the corpus changed under the cursor.
pub(super) struct Cursor {
    pub(super) rank: u32,
    pub(super) score_bits: u32,
    /// Sorted boundary: the last hit's sort keys in merge form, one per
    /// sort column. `Some` exactly on tokens minted by a sorted query;
    /// a cursor from one query shape refuses on another.
    pub(super) keys: Option<Vec<crate::sortkeys::Key>>,
    pub(super) doc_id: u64,
}

const CURSOR_PREFIX: &str = "tvq1";
const SORTED_CURSOR_PREFIX: &str = "tvqs2";

impl Cursor {
    pub(super) fn encode(&self) -> String {
        match &self.keys {
            Some(keys) => format!(
                "{SORTED_CURSOR_PREFIX}:{}:{}:{}",
                self.rank,
                crate::sortkeys::encode_keys(keys),
                self.doc_id
            ),
            None => format!(
                "{CURSOR_PREFIX}:{}:{:08x}:{}",
                self.rank, self.score_bits, self.doc_id
            ),
        }
    }

    fn parse(token: &str) -> Result<Self, Status> {
        let bad = || refuse(format!("malformed cursor {token:?}"));
        let mut parts = token.split(':');
        let sorted = match parts.next() {
            Some(p) if p == CURSOR_PREFIX => false,
            Some(p) if p == SORTED_CURSOR_PREFIX => true,
            _ => return Err(bad()),
        };
        let rank: u32 = parts.next().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
        let hex = parts.next().ok_or_else(bad)?;
        let doc_id: u64 = parts.next().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
        if parts.next().is_some() || rank == 0 {
            return Err(bad());
        }
        let (score_bits, keys) = if sorted {
            (0, Some(crate::sortkeys::decode_keys(hex).ok_or_else(bad)?))
        } else {
            (u32::from_str_radix(hex, 16).map_err(|_| bad())?, None)
        };
        Ok(Cursor {
            rank,
            score_bits,
            keys,
            doc_id,
        })
    }

    /// The request's cursor, when it names one. A cursor counts pages of
    /// `k`, so it needs the caller's own `k`.
    pub(super) fn from_request(req: &QueryRequest) -> Result<Option<Self>, Status> {
        if req.cursor.is_empty() {
            return Ok(None);
        }
        if req.k == 0 {
            return Err(refuse("a cursor requires an explicit k"));
        }
        Self::parse(&req.cursor).map(Some)
    }
}

/// Where the page after `cursor` starts in the full fetched list. The
/// boundary hit must still be there with the score the cursor recorded.
fn resume_index(hits: &[QueryHit], cursor: &Cursor) -> Result<usize, Status> {
    if cursor.keys.is_some() {
        return Err(refuse(
            "this cursor came from a sorted query; the rest of the request must \
             repeat the query that minted it",
        ));
    }
    let Some(i) = hits.iter().position(|h| h.doc_id == cursor.doc_id) else {
        return Err(Status::failed_precondition(format!(
            "cursor boundary doc {} is no longer in the result set; the corpus changed under the cursor — restart from the first page",
            cursor.doc_id
        )));
    };
    if hits[i].score.to_bits() != cursor.score_bits {
        return Err(Status::failed_precondition(format!(
            "cursor boundary doc {}'s score moved; the corpus changed under the cursor — restart from the first page",
            cursor.doc_id
        )));
    }
    Ok(i + 1)
}

/// Cut the k-page after the cursor out of the full fetched list, with
/// absolute ranks, and mint the next cursor (empty when the page came
/// up short — nothing provably follows at the served depth).
pub(super) fn page(
    mut hits: Vec<QueryHit>,
    k: u32,
    cursor: Option<&Cursor>,
) -> Result<(Vec<QueryHit>, String), Status> {
    for (i, hit) in hits.iter_mut().enumerate() {
        hit.rank = (i + 1) as u32;
    }
    let start = match cursor {
        None => 0,
        Some(c) => resume_index(&hits, c)?,
    };
    let end = hits.len().min(start + k as usize);
    let page: Vec<QueryHit> = hits.drain(..end).skip(start).collect();
    let next = if page.len() == k as usize {
        page.last()
            .map(|h| {
                Cursor {
                    rank: h.rank,
                    score_bits: h.score.to_bits(),
                    keys: None,
                    doc_id: h.doc_id,
                }
                .encode()
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    Ok((page, next))
}
