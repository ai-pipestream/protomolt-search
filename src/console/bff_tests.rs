//! The BFF surface without a cluster: the method table against the
//! descriptor, the request guard, Connect framing and errors, and the
//! server-sent pump's keep-alive comments.

use tokio_stream::StreamExt as _;

use super::*;

fn request(host: Option<&str>, origin: Option<&str>, query: &[(&str, &str)]) -> HttpRequest {
    HttpRequest {
        method: "POST".into(),
        path: format!("/bff/v1/{BFF_SERVICE}/Query"),
        query: query
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        body: Vec::new(),
        keep_alive: true,
        headers: Headers {
            host: host.map(str::to_string),
            origin: origin.map(str::to_string),
            content_type: "application/json".into(),
        },
    }
}

#[test]
fn the_table_and_the_descriptor_name_the_same_methods() {
    let service = bff_pool()
        .get_service_by_name(BFF_SERVICE)
        .expect("the BFF service");
    let declared: Vec<String> = service.methods().map(|m| m.name().to_string()).collect();
    let served: Vec<String> = METHODS.iter().map(|(n, _)| n.to_string()).collect();
    assert_eq!(declared.len(), served.len(), "{declared:?} vs {served:?}");
    for name in &declared {
        assert!(served.contains(name), "{name} is declared but not served");
    }
    let search = descriptor_pool()
        .get_service_by_name(&format!("{PACKAGE}.SearchService"))
        .expect("SearchService");
    for (name, op) in METHODS.iter().filter(|(_, op)| *op == Op::Upstream) {
        let (bff, _) = bff_method(name).unwrap();
        let upstream = search
            .methods()
            .find(|m| m.name() == *name)
            .expect("an upstream method");
        assert_eq!(
            bff.input().full_name(),
            upstream.input().full_name(),
            "{name} {op:?}"
        );
        assert_eq!(
            bff.output().full_name(),
            upstream.output().full_name(),
            "{name}"
        );
        assert_eq!(
            bff.is_server_streaming(),
            upstream.is_server_streaming(),
            "{name}"
        );
        assert!(!upstream.is_client_streaming(), "{name}");
    }
    for refused in [
        "IngestDocuments",
        "RoutedIngestMapped",
        "PublishTopology",
        "SetRuntimeKnob",
    ] {
        assert_eq!(
            bff_method(refused).unwrap_err().code,
            Code::NotFound,
            "{refused}"
        );
    }
}

#[test]
fn loopback_hosts_are_names_of_this_machine() {
    for host in [
        "localhost:8600",
        "127.0.0.1:1",
        "127.9.9.9",
        "[::1]:80",
        "[::1]",
        "localhost",
    ] {
        assert!(is_loopback_host(host), "{host}");
    }
    for host in [
        "evil.example:8600",
        "10.0.0.1:1",
        "localhost.evil.example",
        "",
        "[::2]:1",
    ] {
        assert!(!is_loopback_host(host), "{host}");
    }
}

#[tokio::test]
async fn the_guard_refuses_targets_rebound_hosts_and_foreign_origins() {
    let console = Console::bind(ConsoleConfig {
        listen: "127.0.0.1:0".into(),
        coordinator: "http://127.0.0.1:1".into(),
        nodes: vec!["http://127.0.0.1:2".into()],
        analysis: None,
        security: ToolClient::default(),
        allow_remote: false,
    })
    .await
    .unwrap();
    let ctx = &console.ctx;
    let local = Some("127.0.0.1:8600");
    assert!(guard(ctx, &request(local, None, &[])).is_ok());
    assert!(guard(ctx, &request(local, Some("http://127.0.0.1:8600"), &[])).is_ok());
    for (host, origin, query) in [
        (local, None, &[("target", "node0")][..]),
        (Some("rebound.example:8600"), None, &[][..]),
        (None, None, &[][..]),
        (local, Some("http://evil.example"), &[][..]),
        (local, Some("null"), &[][..]),
    ] {
        let refused = guard(ctx, &request(host, origin, query)).unwrap_err();
        assert_eq!(
            refused.code,
            Code::PermissionDenied,
            "{host:?} {origin:?}: {}",
            refused.message
        );
        assert_eq!(refused.reply().status, 403);
    }
}

#[test]
fn connect_errors_carry_the_protocol_codes_and_hide_cluster_detail() {
    let refused = BffError::new(Code::InvalidArgument, "bad k");
    assert_eq!(refused.reply().status, 400);
    assert_eq!(
        refused.json(),
        json!({ "code": "invalid_argument", "message": "bad k" })
    );
    let hidden = BffError::upstream(&Status::unavailable("tcp connect 10.1.2.3:50051 refused"));
    assert_eq!(hidden.reply().status, 503);
    assert!(!hidden.message.contains("10.1.2.3"), "{}", hidden.message);
    assert_eq!(
        BffError::upstream(&Status::internal("panic at node7"))
            .reply()
            .status,
        500
    );
    let kept = BffError::upstream(&Status::not_found("collection \"x\" is unknown"));
    assert_eq!(
        (kept.reply().status, kept.message.as_str()),
        (404, "collection \"x\" is unknown")
    );
    assert_eq!(connect_code(Code::Cancelled), "canceled");
}

#[test]
fn a_stream_request_is_one_uncompressed_envelope() {
    let body = envelope(0, b"{\"k\":1}");
    assert_eq!(&body[..5], &[0, 0, 0, 0, 7]);
    assert_eq!(request_envelope(&body).unwrap(), b"{\"k\":1}");
    let mut long = body.clone();
    long.push(b' ');
    let compressed = envelope(1, b"{}");
    for (bad, code) in [
        (&b""[..], Code::InvalidArgument),
        (&[0u8, 0, 0][..], Code::InvalidArgument),
        (&long[..], Code::InvalidArgument),
        (&compressed[..], Code::Unimplemented),
        (&envelope(END_STREAM, b"{}")[..], Code::InvalidArgument),
    ] {
        assert_eq!(request_envelope(bad).unwrap_err().code, code, "{bad:?}");
    }
    let end = end_stream(Some(&BffError::new(Code::NotFound, "x")));
    assert_eq!(end[0], END_STREAM);
    let json: Value = serde_json::from_slice(&end[5..]).unwrap();
    assert_eq!(
        json,
        json!({ "error": { "code": "not_found", "message": "x" } })
    );
    assert_eq!(&end_stream(None)[5..], b"{}");
}

#[test]
fn snippets_cut_on_characters_and_say_so() {
    assert_eq!(snippet("héllo wörld", 4), ("héll", true));
    assert_eq!(snippet("short", 5), ("short", false));
    assert_eq!(snippet("", 0), ("", false));
}

#[tokio::test]
async fn an_idle_event_stream_carries_keepalive_comments() {
    let (mut writer, mut reader) = tokio::io::duplex(4096);
    let pump = tokio::spawn(async move {
        let idle = tokio_stream::pending::<Result<Vec<u8>, BffError>>();
        sse_pump(&mut writer, Duration::from_millis(5), idle).await
    });
    let mut seen = Vec::new();
    let read = async {
        while seen
            .windows(KEEPALIVE.len())
            .filter(|w| *w == KEEPALIVE)
            .count()
            < 3
        {
            let mut chunk = [0u8; 64];
            let n = reader.read(&mut chunk).await.unwrap();
            assert!(n > 0, "the pump ended");
            seen.extend_from_slice(&chunk[..n]);
        }
    };
    tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .expect("three keep-alive comments");
    assert!(
        seen.starts_with(KEEPALIVE),
        "the first frame is a keep-alive"
    );
    pump.abort();
}

#[tokio::test]
async fn an_event_stream_ends_with_end_or_with_its_error() {
    let mut out = Vec::new();
    let messages = tokio_stream::iter(vec![Ok(b"{\"a\":1}".to_vec())]);
    sse_pump(&mut out, Duration::from_secs(60), messages)
        .await
        .unwrap();
    assert_eq!(
        out,
        b": keepalive\n\nevent: message\ndata: {\"a\":1}\n\nevent: end\ndata: {}\n\n"
    );
    let mut out = Vec::new();
    let failing = tokio_stream::iter(vec![Err(BffError::new(Code::OutOfRange, "k"))])
        .chain(tokio_stream::pending());
    sse_pump(&mut out, Duration::from_secs(60), failing)
        .await
        .unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.ends_with("event: error\ndata: {\"code\":\"out_of_range\",\"message\":\"k\"}\n\n"),
        "{text}"
    );
}

#[test]
fn the_raw_explorer_flag_is_refused_with_a_remote_bind() {
    let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    assert_eq!(raw_explorer_arg(&args(&[])), Ok(false));
    assert_eq!(
        raw_explorer_arg(&args(&["--enable-raw-explorer"])),
        Ok(true)
    );
    let refused =
        raw_explorer_arg(&args(&["--allow-remote", "--enable-raw-explorer"])).unwrap_err();
    assert!(refused.contains("--allow-remote"), "{refused}");
    assert_eq!(raw_explorer_arg(&args(&["--allow-remote"])), Ok(false));
}
