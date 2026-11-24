//! The fleet-readiness wait, and `--ready-only`: the wait alone, with an
//! exit status an orchestrator can branch on.
//!
//! | status | meaning |
//! |---|---|
//! | 0 | the coordinator lists the expected primaries, all answer, and they score in one space |
//! | 3 | not ready yet: a primary has not answered, or the coordinator is not serving; another attempt can succeed |
//! | 4 | cannot become ready as configured: the coordinator lists another number of primaries than expected (or none), a primary answers for another collection, or the primaries do not score in one space |
//! | other | this invocation cannot succeed: a bad flag, an unreadable token or certificate, credentials the coordinator refuses, a TLS failure |

use std::error::Error;
use std::time::{Duration, Instant};

use pipestream_search::coordinator::FleetReadiness;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::ClusterHealthRequest;
use pipestream_search::security::{Bearer, ToolClient};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

/// The verifier's client: one channel, with the bearer header on each call.
pub type Client = SearchServiceClient<InterceptedService<Channel, Bearer>>;

/// `--ready-only` exit status for a fleet that is not ready yet.
const NOT_READY_YET: i32 = 3;

/// `--ready-only` exit status for a fleet that waiting cannot make ready.
const CANNOT_BECOME_READY: i32 = 4;

/// Poll `ClusterHealth` until the fleet is ready, an obstacle appears that
/// waiting cannot clear, or `wait_ready` seconds have passed. Print which
/// it was, with every blocker when the fleet is not ready.
pub async fn wait(
    client: &mut Client,
    expect_shards: usize,
    wait_ready: u64,
) -> Result<FleetReadiness, tonic::Status> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(wait_ready);
    loop {
        let health = client
            .cluster_health(ClusterHealthRequest {
                collection: String::new(),
            })
            .await?
            .into_inner();
        let state = FleetReadiness::of(&health, expect_shards);
        let (up, want) = (state.reachable, state.expected);
        if state.is_ready() {
            println!(
                "  ready   {up}/{want} shards after {:.0}s",
                started.elapsed().as_secs_f64()
            );
            return Ok(state);
        }
        let gave_up = if !state.can_become_ready() {
            println!("  ready   NOT POSSIBLE at {up}/{want} shards: waiting cannot clear this");
            true
        } else if Instant::now() >= deadline {
            println!("  ready   TIMED OUT at {up}/{want} shards after {wait_ready}s");
            true
        } else {
            false
        };
        if gave_up {
            for blocker in state.blockers() {
                println!("  not ready  {blocker}");
            }
            return Ok(state);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Run `--ready-only` against `coord` and return its exit status.
pub async fn ready_only(
    security: &ToolClient,
    coord: &str,
    expect_shards: usize,
    wait_ready: u64,
) -> Result<i32, Box<dyn Error>> {
    if wait_ready == 0 {
        eprintln!("--ready-only needs --wait-ready=<seconds>");
        return Ok(2);
    }
    let channel = match security.endpoint(coord)?.connect().await {
        Ok(channel) => channel,
        Err(e) => return not_serving_yet_or_fatal(Box::new(e)),
    };
    let mut client = SearchServiceClient::with_interceptor(channel, security.bearer());
    match wait(&mut client, expect_shards, wait_ready).await {
        Ok(state) if state.is_ready() => Ok(0),
        Ok(state) if state.can_become_ready() => Ok(NOT_READY_YET),
        Ok(_) => Ok(CANNOT_BECOME_READY),
        Err(status) => not_serving_yet_or_fatal(Box::new(status)),
    }
}

fn not_serving_yet_or_fatal(error: Box<dyn Error>) -> Result<i32, Box<dyn Error>> {
    if !FleetReadiness::coordinator_not_serving_yet(error.as_ref()) {
        return Err(error);
    }
    println!("  not ready  the coordinator is not serving yet: {error}");
    Ok(NOT_READY_YET)
}
