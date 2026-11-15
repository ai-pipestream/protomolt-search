use std::sync::Arc;

use super::pool::runtime_channel_pool;
use super::shared_channel;

#[test]
fn runtime_shutdown_releases_even_an_unpolled_pool_owner() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let weak = runtime.block_on(async {
        let first = runtime_channel_pool().unwrap();
        let second = runtime_channel_pool().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let _channel = shared_channel("http://127.0.0.1:12345").unwrap();
        assert_eq!(first.lock().unwrap().len(), 1);
        Arc::downgrade(&first)
    });
    assert!(weak.upgrade().is_some(), "the runtime owns the idle pool");
    drop(runtime);
    assert!(
        weak.upgrade().is_none(),
        "shutdown must release cached channels"
    );
}

#[test]
fn channels_outside_a_runtime_refuse_instead_of_panicking() {
    let error = shared_channel("http://127.0.0.1:12345").unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("Tokio runtime"));
}
