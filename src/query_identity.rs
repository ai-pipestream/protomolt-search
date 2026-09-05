//! Identity resolution against the metadata and eligibility of one scan.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use prost::Message;
use tonic::Status;

use crate::pb::{
    stream_search_response, ResolveStreamIdentities, StreamIdentities, StreamIdentity,
    StreamIdentityLimits, StreamSearchResponse,
};
use crate::source_archive::IdentitySnapshot;

pub(crate) fn validate_limits(limits: &StreamIdentityLimits) -> Result<Duration, Status> {
    if !(1..=1_000_000).contains(&limits.max_rows)
        || !(1..=64 * 1024 * 1024).contains(&limits.max_response_bytes)
        || !(1..=60_000).contains(&limits.timeout_ms)
    {
        return Err(Status::invalid_argument(
            "identity limits require max_rows=1..1000000, max_response_bytes=1..67108864, \
             timeout_ms=1..60000",
        ));
    }
    Ok(Duration::from_millis(u64::from(limits.timeout_ms)))
}

pub(crate) struct ScanIdentities {
    base: u64,
    rows: usize,
    identities: IdentitySnapshot,
    admitted: Option<Vec<bool>>,
}

impl ScanIdentities {
    pub(crate) fn range(&self) -> Option<crate::pb::StreamIdentityRange> {
        (self.rows > 0).then(|| crate::pb::StreamIdentityRange {
            first_id: self.base,
            last_id: self.base + (self.rows - 1) as u64,
        })
    }

    /// All arguments must come from the read guard that scored the scan.
    pub(crate) fn new(
        base: u64,
        rows: usize,
        identities: IdentitySnapshot,
        admitted: Option<Vec<bool>>,
    ) -> Result<Self, Status> {
        if rows as u128 > u128::from(u32::MAX) + 1
            || (rows > 0 && base.checked_add((rows - 1) as u64).is_none())
            || admitted.as_ref().is_some_and(|mask| mask.len() != rows)
        {
            return Err(Status::failed_precondition(
                "scan identity range or eligibility mask is invalid",
            ));
        }
        Ok(Self {
            base,
            rows,
            identities,
            admitted,
        })
    }

    #[cfg(test)]
    fn resolve(
        &self,
        request: ResolveStreamIdentities,
        limits: &StreamIdentityLimits,
    ) -> Result<StreamSearchResponse, Status> {
        self.resolve_until(
            request,
            limits,
            Instant::now() + Duration::from_secs(60),
            &AtomicBool::new(false),
        )
    }

    pub(crate) fn resolve_until(
        &self,
        request: ResolveStreamIdentities,
        limits: &StreamIdentityLimits,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<StreamSearchResponse, Status> {
        if request.vector_ids.len() > limits.max_rows as usize {
            return Err(Status::resource_exhausted(
                "identity selection exceeds max_rows",
            ));
        }
        let mut seen = HashSet::with_capacity(request.vector_ids.len());
        let mut rows = Vec::with_capacity(request.vector_ids.len());
        let mut payload_bytes = 0usize;
        for id in request.vector_ids {
            if cancelled.load(Ordering::Acquire) {
                return Err(Status::cancelled("identity selection cancelled"));
            }
            if Instant::now() >= deadline {
                return Err(Status::deadline_exceeded("identity selection timed out"));
            }
            if !seen.insert(id) {
                return Err(Status::invalid_argument("identity selection repeats an ID"));
            }
            let local = id
                .checked_sub(self.base)
                .filter(|local| *local < self.rows as u64)
                .ok_or_else(|| {
                    Status::invalid_argument("identity ID is outside the captured scan")
                })? as u32;
            if self
                .admitted
                .as_ref()
                .is_some_and(|mask| !mask[local as usize])
            {
                return Err(Status::permission_denied(
                    "identity ID was not admitted by the scan",
                ));
            }
            let row = StreamIdentity {
                vector_id: id,
                identity: self.identities.identity(local),
            };
            let len = row.encoded_len();
            payload_bytes = payload_bytes
                .checked_add(1 + prost::encoding::encoded_len_varint(len as u64) + len)
                .ok_or_else(|| Status::resource_exhausted("identity response length overflow"))?;
            let response_bytes =
                1 + prost::encoding::encoded_len_varint(payload_bytes as u64) + payload_bytes;
            if response_bytes > limits.max_response_bytes as usize {
                return Err(Status::resource_exhausted(
                    "identity selection exceeds max_response_bytes",
                ));
            }
            rows.push(row);
        }
        let response = StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Identities(
                StreamIdentities { rows },
            )),
        };
        if response.encoded_len() > limits.max_response_bytes as usize {
            return Err(Status::resource_exhausted(
                "identity selection exceeds max_response_bytes",
            ));
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captured_eligibility_and_range_apply_even_without_identity_metadata() {
        let view = ScanIdentities::new(
            100,
            3,
            IdentitySnapshot::default(),
            Some(vec![true, false, true]),
        )
        .unwrap();
        let limits = StreamIdentityLimits {
            max_rows: 3,
            max_response_bytes: 1024,
            timeout_ms: 1000,
        };
        let resolve = |ids| view.resolve(ResolveStreamIdentities { vector_ids: ids }, &limits);
        let response = resolve(vec![102, 100]).unwrap();
        let Some(stream_search_response::Payload::Identities(found)) = response.payload else {
            panic!("identities")
        };
        assert_eq!(
            found
                .rows
                .iter()
                .map(|row| row.vector_id)
                .collect::<Vec<_>>(),
            [102, 100]
        );
        assert!(found.rows.iter().all(|row| row.identity.is_none()));
        assert_eq!(
            resolve(vec![101]).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            resolve(vec![99]).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            resolve(vec![103]).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            resolve(vec![100, 100]).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            resolve(vec![100; 4]).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        assert!(ScanIdentities::new(u64::MAX, 2, IdentitySnapshot::default(), None).is_err());
        assert!(ScanIdentities::new(0, 2, IdentitySnapshot::default(), Some(vec![true])).is_err());
    }

    #[test]
    fn response_budget_includes_the_outer_message_and_empty_selection() {
        let view = ScanIdentities::new(0, 2, IdentitySnapshot::default(), None).unwrap();
        let mut limits = StreamIdentityLimits {
            max_rows: 2,
            max_response_bytes: 1024,
            timeout_ms: 1000,
        };
        let request = ResolveStreamIdentities {
            vector_ids: vec![0, 1],
        };
        let exact = view
            .resolve(request.clone(), &limits)
            .unwrap()
            .encoded_len() as u32;
        limits.max_response_bytes = exact;
        assert!(view.resolve(request.clone(), &limits).is_ok());
        limits.max_response_bytes -= 1;
        assert_eq!(
            view.resolve(request, &limits).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        limits.max_response_bytes = 1;
        assert!(view
            .resolve(ResolveStreamIdentities::default(), &limits)
            .is_err());
        assert!(validate_limits(&StreamIdentityLimits::default()).is_err());
        assert!(validate_limits(&limits).is_ok());
    }
}
