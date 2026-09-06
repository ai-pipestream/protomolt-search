use std::sync::Arc;
use std::time::Duration;

use pipestream_search::authorization::PolicyAuthority;
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::diagnostics::{CoordinatorDiagnostics, RecentRing};
use pipestream_search::pb::diagnostics_service_server::DiagnosticsService;
use pipestream_search::pb::*;
use pipestream_search::security::{PrincipalConfig, Principals};
use tokio_stream::StreamExt;
use tonic::{Code, Request};

fn policy(revision: u64, collections: &[&str]) -> AccessPolicy {
    AccessPolicy {
        format_version: 1,
        revision,
        resources: ["a", "b"]
            .into_iter()
            .map(|name| CollectionResource {
                workspace: format!("workspace-{name}"),
                collection: name.into(),
            })
            .collect(),
        grants: collections
            .iter()
            .map(|name| CollectionGrant {
                document_visibility: None,
                principal: "operator".into(),
                workspace: format!("workspace-{name}"),
                collection: (*name).into(),
                actions: vec![AccessAction::Admin as i32],
            })
            .collect(),
    }
}

fn principals() -> Principals {
    Principals::from_configs(&[PrincipalConfig {
        name: "operator".into(),
        token: "diagnostics-operator-token".into(),
        admin: true,
        ..Default::default()
    }])
    .unwrap()
}

fn request<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request.metadata_mut().insert(
        "authorization",
        "Bearer diagnostics-operator-token".parse().unwrap(),
    );
    request
}

fn service(principals: Principals) -> (CoordinatorDiagnostics, Vec<CoordinatorServiceImpl>) {
    let members: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|name| {
            (
                name.to_string(),
                CoordinatorServiceImpl::new(Vec::new())
                    .with_collection(name.into())
                    .with_max_k(50),
            )
        })
        .collect();
    let observers = members.iter().map(|(_, member)| member.clone()).collect();
    let ring = Arc::new(RecentRing::default());
    ring.push(RecentQuery {
        principal: "private-workspace-b-reader".into(),
        collection: "b".into(),
        hits: 42,
        ..Default::default()
    });
    (
        CoordinatorDiagnostics::new(members, Some(Arc::new(principals)), ring),
        observers,
    )
}

async fn assert_all_routes_denied(service: &CoordinatorDiagnostics) {
    let outcomes = [
        service
            .get_runtime_knobs(request(GetRuntimeKnobsRequest {}))
            .await
            .map(|_| ()),
        service
            .set_runtime_knob(request(SetRuntimeKnobRequest {
                name: "max_k".into(),
                value: "7".into(),
            }))
            .await
            .map(|_| ()),
        service
            .get_metrics_snapshot(request(MetricsSnapshotRequest {}))
            .await
            .map(|_| ()),
        service
            .stream_metrics(request(StreamMetricsRequest { interval_ms: 0 }))
            .await
            .map(|_| ()),
        service
            .get_shard_diagnostics(request(ShardDiagnosticsRequest { shard: None }))
            .await
            .map(|_| ()),
        service
            .recent_queries(request(RecentQueriesRequest { limit: 0 }))
            .await
            .map(|_| ()),
    ];
    let codes: Vec<_> = outcomes
        .into_iter()
        .map(|result| result.err().map(|error| error.code()))
        .collect();
    assert_eq!(codes, vec![Some(Code::PermissionDenied); 6]);
}

#[tokio::test]
async fn diagnostics_require_admin_grants_for_every_served_workspace() {
    let (service, observers) = service(principals().with_policy(policy(1, &["a"])).unwrap());
    assert_all_routes_denied(&service).await;
    assert!(observers.iter().all(|member| member.max_k() == 50));
}

#[tokio::test]
async fn an_operator_flag_without_an_authority_grants_no_diagnostics_access() {
    let (service, observers) = service(principals());
    assert_all_routes_denied(&service).await;
    assert!(observers.iter().all(|member| member.max_k() == 50));
}

#[tokio::test]
async fn complete_admin_grants_admit_all_six_routes() {
    let (service, observers) = service(principals().with_policy(policy(1, &["a", "b"])).unwrap());
    service
        .get_runtime_knobs(request(GetRuntimeKnobsRequest {}))
        .await
        .unwrap();
    service
        .set_runtime_knob(request(SetRuntimeKnobRequest {
            name: "max_k".into(),
            value: "7".into(),
        }))
        .await
        .unwrap();
    assert!(observers.iter().all(|member| member.max_k() == 7));
    service
        .get_metrics_snapshot(request(MetricsSnapshotRequest {}))
        .await
        .unwrap();
    let mut stream = service
        .stream_metrics(request(StreamMetricsRequest {
            interval_ms: 60_000,
        }))
        .await
        .unwrap()
        .into_inner();
    stream.next().await.unwrap().unwrap();
    service
        .get_shard_diagnostics(request(ShardDiagnosticsRequest { shard: None }))
        .await
        .unwrap();
    let recent = service
        .recent_queries(request(RecentQueriesRequest { limit: 0 }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(recent.total_seen, 1);
    assert_eq!(recent.queries[0].collection, "b");
}

#[tokio::test]
async fn idle_metrics_stream_wakes_on_either_revocation_or_policy_replacement() {
    for retained in [&["a"][..], &["a", "b"][..]] {
        let authority = Arc::new(PolicyAuthority::new(policy(1, &["a", "b"])).unwrap());
        let (service, _) = service(principals().with_authorizer(authority.clone()));
        let mut stream = service
            .stream_metrics(request(StreamMetricsRequest {
                interval_ms: 60_000,
            }))
            .await
            .unwrap()
            .into_inner();
        stream.next().await.unwrap().unwrap();
        authority.replace(policy(2, retained)).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("policy replacement must wake an idle metrics stream")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied);
        assert!(stream.next().await.is_none());
        if retained.len() == 1 {
            assert_all_routes_denied(&service).await;
        } else {
            service
                .get_metrics_snapshot(request(MetricsSnapshotRequest {}))
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn revocation_while_collecting_metrics_suppresses_the_snapshot() {
    let authority = Arc::new(PolicyAuthority::new(policy(1, &["a", "b"])).unwrap());
    let (service, _) = service(principals().with_authorizer(authority.clone()));
    let service = service.with_gauges(vec![Box::new(move || {
        // A provider observation spans a policy change, after admission and
        // before the response. Its private values must not leave the handler.
        authority.replace(policy(2, &["a"])).unwrap();
        pipestream_search::metrics::ShardGauges {
            slot_offset: 900,
            vectors: 42,
            documents: 42,
            stats_epoch: 3,
        }
    })]);
    let error = service
        .get_metrics_snapshot(request(MetricsSnapshotRequest {}))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn an_empty_membership_cannot_admit_an_operator_by_vacuous_grants() {
    let service = CoordinatorDiagnostics::new(
        Vec::new(),
        Some(Arc::new(principals().with_policy(policy(1, &[])).unwrap())),
        Arc::new(RecentRing::default()),
    );
    assert_all_routes_denied(&service).await;
}
