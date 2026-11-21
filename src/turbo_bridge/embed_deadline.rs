//! One caller budget across embedding setup, admission, and execution.

use std::{future::Future, time::Duration};

use tokio::time::Instant;
use tonic::Status;

#[derive(Clone, Copy)]
pub(crate) struct EmbedDeadline(Option<Instant>);

impl EmbedDeadline {
    pub(crate) fn new(timeout: Option<Duration>) -> Result<Self, Status> {
        timeout
            .map(|duration| {
                Instant::now()
                    .checked_add(duration)
                    .ok_or_else(|| Status::invalid_argument("embedding timeout is too large"))
            })
            .transpose()
            .map(Self)
    }

    pub(crate) fn remaining(self) -> Result<Option<Duration>, Status> {
        self.0
            .map(|end| {
                end.checked_duration_since(Instant::now())
                    .filter(|left| !left.is_zero())
                    .ok_or_else(expired)
            })
            .transpose()
    }

    pub(crate) async fn wait<T>(
        self,
        future: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        self.remaining()?;
        let result = match self.0 {
            Some(end) => tokio::time::timeout_at(end, future)
                .await
                .map_err(|_| expired())?,
            None => future.await,
        };
        // Tonic can return Cancelled at the same instant its deadline expires.
        self.remaining()?;
        result
    }
}

fn expired() -> Status {
    Status::deadline_exceeded("embedding caller deadline expired")
}
