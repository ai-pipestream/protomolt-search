//! Mapping of `turbo-client` errors onto gRPC statuses, and the eviction of
//! a pooled channel when a remote status says the connection is unhealthy.

use tonic::Status;

#[cfg(feature = "net")]
use super::pool::evict_unhealthy_turbo_channel;

pub(super) fn client_err_to_status(err: turbo_client::ClientError) -> Status {
    match err {
        turbo_client::ClientError::InvalidArgument(m) => Status::invalid_argument(m),
        turbo_client::ClientError::FailedPrecondition(m) => Status::failed_precondition(m),
        turbo_client::ClientError::NotFound(m) => Status::not_found(m),
        turbo_client::ClientError::PermissionDenied(m) => Status::permission_denied(m),
        turbo_client::ClientError::DeadlineExceeded(m) => Status::deadline_exceeded(m),
        turbo_client::ClientError::Unauthenticated(m) => Status::unauthenticated(m),
        turbo_client::ClientError::Unavailable(m) => Status::unavailable(m),
        turbo_client::ClientError::Internal(m) => Status::internal(m),
    }
}

#[cfg(feature = "net")]
pub(crate) fn inspect_remote_status(addr: &str, status: Status) -> Status {
    if matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::DeadlineExceeded
    ) {
        evict_unhealthy_turbo_channel(addr);
    }
    status
}

#[cfg(feature = "net")]
pub(super) fn inspect_remote_client_err(addr: &str, err: turbo_client::ClientError) -> Status {
    let status = client_err_to_status(err);
    inspect_remote_status(addr, status)
}

#[cfg(feature = "net")]
pub(crate) fn remote_client_err_to_status(
    client: &turbo_client::RemoteClient,
    err: turbo_client::ClientError,
) -> Status {
    inspect_remote_client_err(&client.config().endpoint, err)
}

#[cfg(not(feature = "net"))]
pub(super) fn sidecar_needs_net(addr: &str) -> Status {
    Status::failed_precondition(format!(
        "analysis service {addr:?} is unreachable from this build: the `net` feature is off, so \
         nothing dials; use --analysis-addr=native or --analysis-addr=turbo"
    ))
}
