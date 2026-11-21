use super::*;
use pipestream_search::analyzer::embed_texts_with_options;
use pipestream_search::turbo_bridge::{BatchEmbedder, EmbedOptions, LocalBatchEmbedder};
use std::{future::Future, pin::Pin, task::Poll, time::Duration};

async fn poll_pending<F: Future>(mut future: Pin<&mut F>) {
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
            .await
            .is_pending()
    );
}

async fn started(mut receiver: tokio::sync::oneshot::Receiver<()>) {
    loop {
        match receiver.try_recv() {
            Ok(()) => return,
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => tokio::task::yield_now().await,
            Err(error) => panic!("worker failed: {error}"),
        }
    }
}

fn block_worker() -> (
    std::sync::mpsc::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
    tokio::task::JoinHandle<()>,
) {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let task = tokio::task::spawn_blocking(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    (release_tx, started_rx, task)
}

async fn cold_then_warm() {
    let (release_setup, setup_started, setup_blocker) = block_worker();
    started(setup_started).await;
    let mut options = EmbedOptions::default();
    options.call.timeout = Some(Duration::from_millis(100));
    let begin = tokio::time::Instant::now();
    let mut request = Box::pin(embed_texts_with_options("turbo", &["fox"], &options));
    poll_pending(request.as_mut()).await;
    // The one-thread pool now holds the bundle load between these two blockers.
    let (release_inference, inference_started, inference_blocker) = block_worker();
    tokio::time::advance(Duration::from_millis(60)).await;
    release_setup.send(()).unwrap();
    started(inference_started).await;
    setup_blocker.await.unwrap();
    poll_pending(request.as_mut()).await;
    tokio::time::advance(Duration::from_millis(40)).await;
    assert_eq!(
        request.await.unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    assert_eq!(begin.elapsed(), Duration::from_millis(100));
    release_inference.send(()).unwrap();
    inference_blocker.await.unwrap();
    warm_and_direct().await;
}

async fn warm_and_direct() {
    let mut options = EmbedOptions::default();
    options.call.timeout = Some(Duration::from_secs(10));
    let first = embed_texts_with_options("turbo", &["fox"], &options)
        .await
        .unwrap();
    let warm = embed_texts_with_options("turbo", &["fox"], &options)
        .await
        .unwrap();
    assert_eq!(first, warm);
    options.call.timeout = Some(Duration::ZERO);
    assert_eq!(
        embed_texts_with_options("turbo", &["fox"], &options)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    let engine = LocalBatchEmbedder::open(
        &PathBuf::from(std::env::var_os("TURBO_EMBED_BUNDLE").unwrap()),
        None,
    )
    .unwrap();
    assert_eq!(
        engine
            .embed_batch(&["fox"], &options)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    options.call.timeout = None;
    assert_eq!(engine.embed_batch(&["fox"], &options).await.unwrap(), first);
}

#[test]
fn cold_initialization_timeout_recovers_and_warm_bundle_obeys_deadline() {
    if std::env::var_os("PSEARCH_EMBED_DEADLINE_CHILD").is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .max_blocking_threads(1)
            .build()
            .unwrap()
            .block_on(cold_then_warm());
        return;
    }
    let fixture = create_static_fixture("deadline", "NORMALIZE_L2");
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            std::thread::current().name().unwrap(),
            "--test-threads=2",
        ])
        .env("PSEARCH_EMBED_DEADLINE_CHILD", "1")
        .env("TURBO_EMBED_BUNDLE", fixture.bundle_dir.as_os_str())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        result.status.success() && stdout.contains("1 passed"),
        "{stdout} {}",
        String::from_utf8_lossy(&result.stderr)
    );
}
