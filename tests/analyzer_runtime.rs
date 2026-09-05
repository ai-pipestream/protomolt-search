use pipestream_search::{analyzer, harness::mock_analysis::start_mock_analysis};
use std::time::Duration;

/// Keep the sidecar alive on its own runtime while replacing only the client
/// runtime. No server restart, port reuse, or transport retry is involved.
#[test]
fn analysis_reconnects_after_client_runtime_replacement() {
    let server_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (address, server) = server_runtime.block_on(start_mock_analysis());
    for round in 0..4 {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            for _ in 0..2 {
                let result = tokio::time::timeout(
                    Duration::from_secs(3),
                    analyzer::analyze_document(&address, "court opinion", None),
                )
                .await
                .expect("a healthy sidecar must respond within the deadline")
                .unwrap_or_else(|error| panic!("client runtime {round}: {error}"));
                assert!(!result.into_body().terms.is_empty());
            }
        });
    }
    server.abort();
    let _ = server_runtime.block_on(server);
}

#[test]
fn closing_one_client_runtime_does_not_break_another_runtime() {
    let build = || {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    };
    let server_runtime = build();
    let (address, server) = server_runtime.block_on(start_mock_analysis());
    let first = build();
    let second = build();
    let call = |runtime: &tokio::runtime::Runtime| {
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(3),
                analyzer::analyze_document(&address, "court", None),
            )
            .await
            .unwrap()
            .unwrap();
        });
    };
    call(&first);
    call(&second);
    drop(first);
    call(&second);
    drop(second);
    server.abort();
    let _ = server_runtime.block_on(server);
}
