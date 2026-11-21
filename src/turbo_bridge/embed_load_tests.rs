//! Controlled blocking workloads measure admission policy, not model throughput.
use super::{run, EmbedDeadline};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tonic::Status;

#[derive(Clone, Copy)]
struct Sample {
    interactive: bool,
    latency_ms: f64,
    expired: bool,
}

#[derive(Clone, Copy)]
struct Execution {
    interactive: bool,
    queue_ms: f64,
    execution_ms: f64,
}

async fn baseline<T: Send + 'static>(
    limit: Arc<Semaphore>,
    job: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    let permit = tokio::time::timeout(Duration::from_millis(40), limit.acquire_owned())
        .await
        .map_err(|_| Status::deadline_exceeded("admission expired"))?
        .map_err(|_| Status::unavailable("closed"))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        job()
    })
    .await
    .unwrap()
}

async fn workload(candidate: bool, permits: usize) -> (Vec<Sample>, Vec<Execution>) {
    let limit = Arc::new(Semaphore::new(permits));
    let executions = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..8 {
        let limit = limit.clone();
        let executions = executions.clone();
        let barrier = barrier.clone();
        workers.spawn(async move {
            barrier.wait().await;
            let mut samples = Vec::new();
            for _ in 0..32 {
                let start = Instant::now();
                let interactive = worker < 4;
                let executions = executions.clone();
                let job = move || {
                    let entered = Instant::now();
                    std::thread::sleep(Duration::from_millis(if interactive { 2 } else { 60 }));
                    executions.lock().unwrap().push(Execution {
                        interactive,
                        queue_ms: entered.duration_since(start).as_secs_f64() * 1000.0,
                        execution_ms: entered.elapsed().as_secs_f64() * 1000.0,
                    });
                    Ok(())
                };
                let result = if candidate {
                    run(
                        limit.clone(),
                        EmbedDeadline::new(Some(Duration::from_millis(40))).unwrap(),
                        || job,
                    )
                    .await
                } else {
                    baseline(limit.clone(), job).await
                };
                samples.push(Sample {
                    interactive,
                    latency_ms: start.elapsed().as_secs_f64() * 1000.0,
                    expired: result.is_err_and(|e| e.code() == tonic::Code::DeadlineExceeded),
                });
            }
            samples
        });
    }
    let mut samples = Vec::new();
    while let Some(result) = workers.join_next().await {
        samples.extend(result.unwrap());
    }
    let _drained = limit.acquire_many(permits as u32).await.unwrap();
    let executions = executions.lock().unwrap().clone();
    (samples, executions)
}

fn percentile(values: &mut [f64], fraction: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len() as f64 * fraction).ceil() as usize).saturating_sub(1)]
}

fn report(candidate: bool, permits: usize, samples: &[Sample], jobs: &[Execution]) {
    for interactive in [true, false] {
        let calls: Vec<_> = samples
            .iter()
            .filter(|s| s.interactive == interactive)
            .collect();
        let executions: Vec<_> = jobs
            .iter()
            .filter(|s| s.interactive == interactive)
            .collect();
        let mut latency: Vec<_> = calls.iter().map(|s| s.latency_ms).collect();
        let mean = |values: Vec<f64>| values.iter().sum::<f64>() / values.len().max(1) as f64;
        println!(
            "{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3}",
            if candidate {
                "candidate"
            } else {
                "baseline-policy"
            },
            permits,
            if interactive { "interactive" } else { "bulk" },
            calls.len(),
            executions.len(),
            calls.iter().filter(|s| s.expired).count(),
            mean(executions.iter().map(|s| s.queue_ms).collect()),
            mean(executions.iter().map(|s| s.execution_ms).collect()),
            percentile(&mut latency, 0.99),
            mean(
                calls
                    .iter()
                    .filter(|s| s.expired)
                    .map(|s| s.latency_ms)
                    .collect()
            )
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "controlled mixed-load benchmark; run with --ignored --nocapture"]
async fn mixed_embedding_load() {
    println!("policy,permits,lane,calls,executed,expired,queue_mean_ms,execution_mean_ms,caller_p99_ms,expired_caller_mean_ms");
    for _ in 0..3 {
        for (candidate, permits) in [(false, 1), (true, 1), (true, 2)] {
            let (samples, jobs) = workload(candidate, permits).await;
            assert_eq!(samples.len(), 256);
            report(candidate, permits, &samples, &jobs);
        }
    }
}
