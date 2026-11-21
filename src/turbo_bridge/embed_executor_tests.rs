use super::*;
use std::time::Duration;
use tokio::time::{advance, Instant};

fn budget(ms: u64) -> EmbedDeadline {
    EmbedDeadline::new(Some(Duration::from_millis(ms))).unwrap()
}

#[tokio::test(start_paused = true)]
async fn cumulative_setup_and_admission_do_not_restart_budget() {
    let limit = Arc::new(Semaphore::new(1));
    let occupied = limit.clone().acquire_owned().await.unwrap();
    let deadline = budget(100);
    let start = Instant::now();
    advance(Duration::from_millis(60)).await;
    let result = run(limit.clone(), deadline, || {
        || panic!("expired admission must never execute")
    })
    .await as Result<(), Status>;
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    assert_eq!(start.elapsed(), Duration::from_millis(100));
    drop(occupied);
    assert_eq!(limit.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn cancelled_queued_request_never_prepares_or_executes() {
    let limit = Arc::new(Semaphore::new(1));
    let occupied = limit.clone().acquire_owned().await.unwrap();
    let caller_limit = limit.clone();
    let caller = tokio::spawn(async move {
        run(
            caller_limit,
            budget(100),
            || -> fn() -> Result<(), Status> { panic!("cancelled waiter must not prepare") },
        )
        .await
    });
    tokio::task::yield_now().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(occupied);
    let recovered = run(limit.clone(), EmbedDeadline::new(None).unwrap(), || {
        || Ok(7)
    })
    .await
    .unwrap();
    assert_eq!(recovered, 7);
    assert_eq!(limit.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn expired_running_encoder_keeps_capacity_until_completion() {
    let limit = Arc::new(Semaphore::new(1));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let caller_limit = limit.clone();
    let caller = tokio::spawn(async move {
        run(caller_limit, budget(100), || {
            move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            }
        })
        .await
    });
    // Poll explicitly: paused-time auto-advance must not outrun the OS worker.
    let mut started_rx = started_rx;
    while started_rx.try_recv().is_err() {
        tokio::task::yield_now().await;
    }
    advance(Duration::from_millis(100)).await;
    assert_eq!(
        caller.await.unwrap().unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    assert_eq!(limit.available_permits(), 0);
    let waiting_limit = limit.clone();
    let queued = tokio::spawn(async move {
        run(waiting_limit, budget(20), || {
            || panic!("running encoder still owns the permit")
        })
        .await as Result<(), Status>
    });
    tokio::task::yield_now().await;
    advance(Duration::from_millis(20)).await;
    assert_eq!(
        queued.await.unwrap().unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    release_tx.send(()).unwrap();
    assert_eq!(
        run(limit.clone(), EmbedDeadline::new(None).unwrap(), || || Ok(
            9
        ))
        .await
        .unwrap(),
        9
    );
    assert_eq!(limit.available_permits(), 1);
}

#[tokio::test]
async fn cancellation_of_running_encoder_keeps_capacity() {
    let limit = Arc::new(Semaphore::new(1));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let caller_limit = limit.clone();
    let caller = tokio::spawn(async move {
        run(caller_limit, EmbedDeadline::new(None).unwrap(), || {
            move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            }
        })
        .await
    });
    started_rx.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(limit.available_permits(), 0);
    release_tx.send(()).unwrap();
    run(limit.clone(), EmbedDeadline::new(None).unwrap(), || {
        || Ok(())
    })
    .await
    .unwrap();
    assert_eq!(limit.available_permits(), 1);
}

#[test]
fn cancelled_tokio_queued_job_never_enters_encoder() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    rt.block_on(async {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();
        let limit = Arc::new(Semaphore::new(1));
        let caller_limit = limit.clone();
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let entered_job = entered.clone();
        let caller = tokio::spawn(async move {
            run(caller_limit, EmbedDeadline::new(None).unwrap(), || {
                move || {
                    entered_job.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            })
            .await as Result<(), Status>
        });
        while limit.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        run(limit.clone(), EmbedDeadline::new(None).unwrap(), || {
            || Ok(())
        })
        .await
        .unwrap();
        assert_eq!(limit.available_permits(), 1);
        assert!(!entered.load(std::sync::atomic::Ordering::SeqCst));
    });
}

#[tokio::test(start_paused = true)]
async fn expired_budget_does_not_poll_ready_work_and_early_cancellation_is_preserved() {
    let deadline = budget(0);
    let result: Result<(), Status> = deadline
        .wait(async { panic!("expired before polling") })
        .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    let error = budget(100)
        .wait(async { Err::<(), _>(Status::cancelled("peer cancelled")) })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Cancelled);
    assert!(EmbedDeadline::new(Some(Duration::MAX)).is_err());
}
