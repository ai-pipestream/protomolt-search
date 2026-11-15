use std::sync::atomic::Ordering;

use tokio_stream::StreamExt;
use tonic::{Code, Request, Response, Status};

use super::registry::{Histogram, COMPLETE, ERRORS, FIRST_RESPONSE, IN_FLIGHT, N_CODES, REQUESTS};
use super::registry_tests::sample;
use super::routes::REQUEST_ROUTES;
use super::{inc_request, nested, render, timed, timed_stream, Route};
use crate::test_support::ForkGuarded;

/// Runs the named test alone in a child process, where the process-wide
/// registry sees no other test's traffic, and returns true. In that child,
/// which `marker` identifies, returns false so the test body runs. The
/// child must report exactly one test run: a name that matched nothing
/// would otherwise pass without testing anything.
fn reexecuted_alone(marker: &str, test: &str, what: &str) -> bool {
    if std::env::var_os(marker).is_some() {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env(marker, "1")
        .output_guarded()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "isolated {what} test failed:\n{}\n{}",
        stdout,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("1 passed"),
        "isolated {what} test did not run {test}:\n{stdout}"
    );
    true
}

/// Every route increments its own row and only its own row.
#[test]
fn routes_count_independently() {
    // Other library tests serve real requests concurrently. Verify exact
    // deltas in a process that runs only this test, without resetting or
    // weakening assertions about the production registry.
    if reexecuted_alone(
        "PSEARCH_TEST_ISOLATED_ROUTE_COUNTERS",
        "metrics::seam_tests::routes_count_independently",
        "route counter",
    ) {
        return;
    }

    let before = render(&[]);
    let count = |page: &str, rpc: &str| -> u64 {
        sample(page, &format!("turbovec_requests_total{{rpc=\"{rpc}\"}} "))
    };
    inc_request(Route::Bm25Query);
    let after = render(&[]);
    for (route, name, _) in REQUEST_ROUTES {
        let increment = u64::from(route as usize == Route::Bm25Query as usize);
        assert_eq!(
            count(&after, name),
            count(&before, name) + increment,
            "{name}"
        );
    }
}

fn snapshot(route: Route) -> (u64, u64, u64, u64, [u64; N_CODES]) {
    let i = route as usize;
    let count = |h: &Histogram| h.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum();
    let mut errors = [0u64; N_CODES];
    for (c, slot) in errors.iter_mut().enumerate() {
        *slot = ERRORS[i][c].load(Ordering::Relaxed);
    }
    (
        REQUESTS[i].load(Ordering::Relaxed),
        IN_FLIGHT[i].load(Ordering::Relaxed),
        count(&COMPLETE[i]),
        count(&FIRST_RESPONSE[i]),
        errors,
    )
}

/// `timed` counts the arrival, holds the in-flight slot for the
/// body's duration, records the outcome by code, and lets the
/// gauge return to zero — on success, on refusal, and when the
/// handler future is dropped before it answers.
#[tokio::test]
async fn timed_records_outcomes_and_in_flight_returns_to_zero() {
    let route = Route::RollbackCluster;
    let (req0, inf0, done0, _, err0) = snapshot(route);
    assert_eq!(inf0, 0);

    let ok: Result<u32, Status> = timed(route, Request::new(()), |request| async move {
        assert_eq!(IN_FLIGHT[route as usize].load(Ordering::Relaxed), 1);
        let () = request.into_inner();
        Ok(7)
    })
    .await;
    assert_eq!(ok.unwrap(), 7);
    let (req1, inf1, done1, _, err1) = snapshot(route);
    assert_eq!((req1, inf1, done1, err1), (req0 + 1, 0, done0 + 1, err0));

    let refused: Result<u32, Status> = timed(route, Request::new(()), |_| async move {
        Err(Status::failed_precondition("no"))
    })
    .await;
    assert_eq!(refused.unwrap_err().code(), Code::FailedPrecondition);
    let (req2, inf2, done2, _, err2) = snapshot(route);
    assert_eq!((req2, inf2, done2), (req0 + 2, 0, done0 + 2));
    assert_eq!(err2[1], err1[1] + 1);

    // The client goes away: the handler future is dropped mid-body.
    let abandoned = tokio::time::timeout(
        std::time::Duration::from_millis(5),
        timed(route, Request::new(()), |_| async move {
            std::future::pending::<Result<u32, Status>>().await
        }),
    )
    .await;
    assert!(abandoned.is_err());
    let (req3, inf3, done3, _, err3) = snapshot(route);
    assert_eq!((req3, inf3, done3), (req0 + 3, 0, done0 + 3));
    assert_eq!(err3[9], err2[9] + 1, "a dropped handler counts as other");

    // A nested re-dispatch is not a second arrival.
    let nested_ok: Result<u32, Status> =
        timed(route, nested(Request::new(())), |_| async move { Ok(1) }).await;
    assert_eq!(nested_ok.unwrap(), 1);
    assert_eq!(snapshot(route).0, req0 + 3);
}

/// A streaming route reports two phases: `first_response` when the
/// first message is polled out and `complete` at the terminal
/// event; a stream that ends in `Err` records that code; a stream
/// dropped unread records `other`; a refusal before the stream
/// opens records both phases at once.
#[tokio::test]
async fn streaming_phases_record_first_response_and_completion() {
    // Other library tests stream real queries through this global registry.
    // Keep exact phase and gauge assertions in an isolated process, without
    // resetting production metrics.
    if reexecuted_alone(
        "PSEARCH_TEST_ISOLATED_STREAM_PHASES",
        "metrics::seam_tests::streaming_phases_record_first_response_and_completion",
        "streaming metrics",
    ) {
        return;
    }

    let route = Route::QueryStream;
    let (req0, _, done0, first0, err0) = snapshot(route);

    let items: Vec<Result<u32, Status>> = vec![Ok(1), Ok(2)];
    let response = timed_stream(route, Request::new(()), |_| async move {
        Ok(Response::new(tokio_stream::iter(items)))
    })
    .await
    .unwrap();
    let mut stream = response.into_inner();
    assert_eq!(stream.next().await.unwrap().unwrap(), 1);
    let (_, inf, done, first, _) = snapshot(route);
    assert_eq!((inf, done, first), (1, done0, first0 + 1));
    assert_eq!(stream.next().await.unwrap().unwrap(), 2);
    assert_eq!(snapshot(route).3, first0 + 1, "first response is once");
    assert!(stream.next().await.is_none());
    let (req1, inf1, done1, first1, err1) = snapshot(route);
    assert_eq!(
        (req1, inf1, done1, first1, err1),
        (req0 + 1, 0, done0 + 1, first0 + 1, err0)
    );

    let items: Vec<Result<u32, Status>> = vec![Ok(1), Err(Status::unavailable("gone"))];
    let mut stream = timed_stream(route, Request::new(()), |_| async move {
        Ok(Response::new(tokio_stream::iter(items)))
    })
    .await
    .unwrap()
    .into_inner();
    assert_eq!(stream.next().await.unwrap().unwrap(), 1);
    assert_eq!(
        stream.next().await.unwrap().unwrap_err().code(),
        Code::Unavailable
    );
    let (_, inf2, done2, _, err2) = snapshot(route);
    assert_eq!((inf2, done2), (0, done0 + 2));
    assert_eq!(err2[4], err1[4] + 1);

    let items: Vec<Result<u32, Status>> = vec![Ok(1)];
    let stream = timed_stream(route, Request::new(()), |_| async move {
        Ok(Response::new(tokio_stream::iter(items)))
    })
    .await
    .unwrap()
    .into_inner();
    drop(stream);
    let (_, inf3, done3, first3, err3) = snapshot(route);
    assert_eq!((inf3, done3, first3), (0, done0 + 3, first0 + 3));
    assert_eq!(
        err3[9],
        err2[9] + 1,
        "a stream dropped unread counts as other"
    );

    let refused = timed_stream(route, Request::new(()), |_| async move {
        Err::<Response<tokio_stream::Iter<std::vec::IntoIter<Result<u32, Status>>>>, _>(
            Status::invalid_argument("bad"),
        )
    })
    .await;
    assert_eq!(refused.err().unwrap().code(), Code::InvalidArgument);
    let (req4, inf4, done4, first4, err4) = snapshot(route);
    assert_eq!(
        (req4, inf4, done4, first4),
        (req0 + 4, 0, done0 + 4, first0 + 4)
    );
    assert_eq!(err4[0], err3[0] + 1);
}
