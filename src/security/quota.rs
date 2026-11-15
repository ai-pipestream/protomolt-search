//! A principal's live quota state: the `k` cap, in-flight slots, and the
//! ingest token bucket.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tonic::Status;

use super::principals::PrincipalConfig;

/// A principal's live quota state.
#[derive(Debug)]
pub struct Principal {
    pub name: String,
    pub max_k: u32,
    pub concurrency: u32,
    pub admin: bool,
    in_flight: AtomicU64,
    ingest: Option<Mutex<TokenBucket>>,
}

impl Principal {
    pub(super) fn new(config: &PrincipalConfig) -> Self {
        Principal {
            name: config.name.clone(),
            max_k: config.max_k,
            concurrency: config.concurrency,
            admin: config.admin,
            in_flight: AtomicU64::new(0),
            ingest: (config.ingest_docs_per_sec > 0)
                .then(|| Mutex::new(TokenBucket::new(f64::from(config.ingest_docs_per_sec)))),
        }
    }

    /// Check a requested `k` against this principal's cap.
    pub fn admit_k(&self, k: u32) -> Result<(), Status> {
        if self.max_k != 0 && k > self.max_k {
            return Err(Status::resource_exhausted(format!(
                "principal {:?}: k={k} exceeds its max_k={}; lower k",
                self.name, self.max_k
            )));
        }
        Ok(())
    }

    /// The rule for an unset `k`: it means the coordinator's default,
    /// `default_k`, and that resolved value is what the cap judges. The
    /// request keeps its unset `k`; nothing rewrites it to the cap.
    pub fn admit_default_k(&self, default_k: u32) -> Result<(), Status> {
        if self.max_k != 0 && default_k > self.max_k {
            return Err(Status::resource_exhausted(format!(
                "principal {:?}: k is unset, which means the coordinator default k={default_k}, \
                 above its max_k={}; send an explicit k",
                self.name, self.max_k
            )));
        }
        Ok(())
    }

    /// Take one in-flight slot, or refuse by name when the principal is
    /// at its concurrency limit. The permit releases on drop.
    pub fn admit_request(self: &Arc<Self>) -> Result<Permit, Status> {
        if self.concurrency == 0 {
            return Ok(Permit { principal: None });
        }
        let limit = u64::from(self.concurrency);
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            if current >= limit {
                return Err(Status::resource_exhausted(format!(
                    "principal {:?}: {} requests in flight, at its concurrency limit of {}",
                    self.name, current, self.concurrency
                )));
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Permit {
                        principal: Some(Arc::clone(self)),
                    })
                }
                Err(seen) => current = seen,
            }
        }
    }

    /// Charge `documents` against the ingest rate, or refuse by name
    /// when the bucket cannot cover them: the request is not trimmed.
    pub fn admit_ingest(&self, documents: u32) -> Result<(), Status> {
        let Some(bucket) = &self.ingest else {
            return Ok(());
        };
        let mut bucket = bucket.lock().expect("token bucket poisoned");
        if bucket.take(f64::from(documents), std::time::Instant::now()) {
            Ok(())
        } else {
            Err(Status::resource_exhausted(format!(
                "principal {:?}: ingest of {documents} document(s) exceeds its rate of {} per \
                 second; slow the stream",
                self.name, bucket.rate
            )))
        }
    }

    /// Requests in flight right now.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Acquire)
    }
}

/// An in-flight slot; dropping it releases the slot.
#[derive(Debug)]
pub struct Permit {
    principal: Option<Arc<Principal>>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(p) = &self.principal {
            p.in_flight.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// A token bucket: `rate` tokens per second, one second of burst.
#[derive(Debug)]
struct TokenBucket {
    rate: f64,
    tokens: f64,
    last: std::time::Instant,
}

impl TokenBucket {
    fn new(rate: f64) -> Self {
        TokenBucket {
            rate,
            tokens: rate,
            last: std::time::Instant::now(),
        }
    }

    fn take(&mut self, n: f64, now: std::time::Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.rate);
        if self.tokens + 1e-9 >= n {
            self.tokens -= n;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_buckets_refill_and_refuse() {
        let start = std::time::Instant::now();
        let mut bucket = TokenBucket::new(4.0);
        bucket.last = start;
        assert!(bucket.take(4.0, start));
        assert!(!bucket.take(1.0, start));
        assert!(bucket.take(2.0, start + std::time::Duration::from_millis(500)));
        assert!(!bucket.take(1.0, start + std::time::Duration::from_millis(500)));
        assert!(bucket.take(4.0, start + std::time::Duration::from_secs(5)));
    }
}
