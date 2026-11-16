//! The HTTP/1.1 connection: framing, persistence, deadlines, the
//! connection cap, and the UI files served from `--ui-dir`.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::ReadBuf;

use super::*;

/// Yields its bytes one per read, so every parse resumes mid-token.
struct Trickle(Vec<u8>);

impl AsyncRead for Trickle {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.0.is_empty() {
            buf.put_slice(&[self.0.remove(0)]);
        }
        Poll::Ready(Ok(()))
    }
}

const QUICK: Limits = Limits {
    idle: Duration::from_millis(100),
    header: Duration::from_millis(100),
    body: Duration::from_millis(100),
    max_head: 256,
    max_body: 16,
};

async fn next(conn: &mut Conn<impl AsyncRead + Unpin>) -> Next {
    read_request(conn, &QUICK).await.expect("reads succeed")
}

fn request(next: Next) -> HttpRequest {
    match next {
        Next::Request(request) => request,
        Next::Reject(reply) => panic!("rejected: {reply:?}"),
        Next::Closed => panic!("closed"),
    }
}

/// The refusal for `wire`, as `(status, body)`.
async fn refusal(wire: &[u8]) -> (u16, String) {
    match next(&mut Conn::new(wire)).await {
        Next::Reject(reply) => (reply.status, String::from_utf8(reply.body).unwrap()),
        Next::Request(r) => panic!("accepted {} {}", r.method, r.path),
        Next::Closed => panic!("closed"),
    }
}

#[tokio::test]
async fn pipelined_requests_are_read_in_order_one_byte_at_a_time() {
    let wire = b"\r\nGET /a HTTP/1.1\r\nHost: x\r\n\r\nPOST /b?k=1 HTTP/1.1\r\ncontent-length: 3\r\n\r\nabcGET /c HTTP/1.0\r\nConnection: Keep-Alive, Upgrade\r\n\r\n";
    let mut conn = Conn::new(Trickle(wire.to_vec()));
    let a = request(next(&mut conn).await);
    assert_eq!(
        (a.method.as_str(), a.path.as_str(), a.keep_alive),
        ("GET", "/a", true)
    );
    let b = request(next(&mut conn).await);
    assert_eq!((b.path.as_str(), b.body.as_slice()), ("/b", &b"abc"[..]));
    assert_eq!(b.query, vec![("k".to_string(), "1".to_string())]);
    assert!(
        request(next(&mut conn).await).keep_alive,
        "HTTP/1.0 asked to keep it"
    );
    assert!(matches!(next(&mut conn).await, Next::Closed));
}

#[tokio::test]
async fn connection_persistence_follows_the_version_and_the_header() {
    let cases: [(&[u8], bool); 3] = [
        (b"GET / HTTP/1.1\r\nConnection: close\r\n\r\n", false),
        (b"GET / HTTP/1.0\r\n\r\n", false),
        (b"GET / HTTP/1.1\r\nconnection: keep-alive\r\n\r\n", true),
    ];
    for (wire, keep) in cases {
        assert_eq!(request(next(&mut Conn::new(wire)).await).keep_alive, keep);
    }
}

#[tokio::test]
async fn chunked_bodies_decode_with_extensions_and_trailers() {
    let wire = b"POST /x HTTP/1.1\r\nTransfer-Encoding: Chunked\r\n\r\n4;name=v\r\nWiki\r\nA\r\npedia, fre\r\n0\r\nX-Sum: 1\r\n\r\nGET /after HTTP/1.1\r\n\r\n";
    let mut conn = Conn::new(Trickle(wire.to_vec()));
    assert_eq!(request(next(&mut conn).await).body, b"Wikipedia, fre");
    assert_eq!(request(next(&mut conn).await).path, "/after");
    let empty = b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
    assert!(request(next(&mut Conn::new(&empty[..])).await)
        .body
        .is_empty());
}

#[tokio::test]
async fn malformed_and_oversized_requests_are_refused() {
    let cases: &[(&[u8], u16, &str)] = &[
        (
            b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
            400,
            "chunk size",
        ),
        (
            b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nabX\r\n",
            400,
            "CRLF",
        ),
        (
            b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n11\r\n",
            413,
            "too large",
        ),
        (
            b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n9\r\n123456789\r\n9\r\n",
            413,
            "too large",
        ),
        (
            b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nab",
            400,
            "closed",
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n",
            400,
            "both",
        ),
        (
            b"POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
            501,
            "chunked",
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\n",
            400,
            "Content-Length",
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: -1\r\n\r\n",
            400,
            "Content-Length",
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 17\r\n\r\n",
            413,
            "too large",
        ),
        (
            b"POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nab",
            400,
            "closed",
        ),
        (b"GET / HTTP/2.0\r\n\r\n", 505, "HTTP/1.1"),
        (b"GET /\r\n\r\n", 400, "request line"),
        (b"GET / HTTP/1.1\r\nHost: x", 400, "closed inside the head"),
    ];
    for (wire, status, needle) in cases {
        let (got, body) = refusal(wire).await;
        assert_eq!(got, *status, "{}: {body}", String::from_utf8_lossy(wire));
        assert!(body.contains(needle), "{body}");
    }
    let long = format!("GET / HTTP/1.1\r\nX: {}\r\n\r\n", "a".repeat(300));
    assert_eq!(refusal(long.as_bytes()).await.0, 431);
    assert!(matches!(next(&mut Conn::new(&b""[..])).await, Next::Closed));
}

#[tokio::test]
async fn reads_past_their_deadlines_close_or_answer_408() {
    let (client, server) = tokio::io::duplex(1024);
    let mut conn = Conn::new(server);
    assert!(matches!(next(&mut conn).await, Next::Closed), "idle");
    let (mut client2, server2) = tokio::io::duplex(1024);
    let mut conn2 = Conn::new(server2);
    client2.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
    assert!(matches!(next(&mut conn2).await, Next::Reject(r) if r.status == 408));
    let (mut client3, server3) = tokio::io::duplex(1024);
    let mut conn3 = Conn::new(server3);
    client3
        .write_all(b"POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nab")
        .await
        .unwrap();
    match next(&mut conn3).await {
        Next::Reject(r) => assert_eq!(
            (r.status, &r.body[..]),
            (408, &b"request body timed out"[..])
        ),
        _ => panic!("the body stalled"),
    }
    drop(client);
}

#[test]
fn the_ui_dir_flag_takes_either_form() {
    let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    assert_eq!(
        ui_dir_arg(&args(&["--ui-dir=/a"])),
        Some(PathBuf::from("/a"))
    );
    assert_eq!(
        ui_dir_arg(&args(&["--x", "--ui-dir", "b"])),
        Some(PathBuf::from("b"))
    );
    assert_eq!(ui_dir_arg(&args(&["--ui-dir"])), None);
    assert_eq!(ui_dir_arg(&args(&[])), None);
}

#[test]
fn asset_names_are_single_typed_segments() {
    assert_eq!(asset_name("/"), Some("index.html"));
    assert_eq!(asset_name("/dashboard"), Some("dashboard.html"));
    assert_eq!(asset_name("/search-run.js"), Some("search-run.js"));
    for refused in [
        "/../x.js",
        "/a/b.js",
        "/..%2fx.js",
        "/.hidden.js",
        "/x.txt",
        "/api",
        "/a\\b.js",
    ] {
        assert_eq!(asset_name(refused), None, "{refused}");
    }
    for (name, _) in EMBEDDED {
        assert_eq!(asset_name(&format!("/{name}")), Some(*name));
    }
}

#[test]
fn embedding_calls_carry_the_bearer_token_on_the_interactive_lane() {
    let security = ToolClient::from_args(["--bearer-token=0123456789abcdef".to_string()]).unwrap();
    let options = embed_options(&security);
    assert_eq!(options.bearer_token.as_deref(), Some("0123456789abcdef"));
    assert!(options.interactive && options.tls.is_none());
    assert_eq!(options.timeout, Some(EMBED_DEADLINE));
    let plain = embed_options(&ToolClient::default());
    assert!(plain.bearer_token.is_none() && plain.tls.is_none());
}
