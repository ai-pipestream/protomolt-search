//! Bounded local work whose permits follow the blocking job's lifetime.

use std::sync::{Arc, OnceLock};

use tokio::{sync::Semaphore, task::AbortHandle};
use tonic::Status;

use super::embed_deadline::EmbedDeadline;

pub(crate) fn local_limit() -> Arc<Semaphore> {
    static LIMIT: OnceLock<Arc<Semaphore>> = OnceLock::new();
    Arc::clone(LIMIT.get_or_init(|| Arc::new(Semaphore::new(1))))
}

struct AbortQueued(AbortHandle);

impl Drop for AbortQueued {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) async fn run<T: Send + 'static, F: FnOnce() -> Result<T, Status> + Send + 'static>(
    limit: Arc<Semaphore>,
    deadline: EmbedDeadline,
    prepare: impl FnOnce() -> F,
) -> Result<T, Status> {
    deadline
        .wait(async {
            let permit = limit
                .acquire_owned()
                .await
                .map_err(|_| Status::unavailable("local embedding executor closed"))?;
            deadline.remaining()?;
            let job = prepare();
            let task = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                // A job can also queue in Tokio after admission to our executor.
                deadline.remaining()?;
                job()
            });
            let _cancel = AbortQueued(task.abort_handle());
            task.await
                .map_err(|e| Status::internal(format!("local embedding join error: {e}")))?
        })
        .await
}

#[cfg(test)]
#[path = "embed_executor_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "embed_load_tests.rs"]
mod load_tests;
