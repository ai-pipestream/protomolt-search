//! A served console over real sockets: connection reuse, chunked bodies,
//! the connection cap, and `--ui-dir`.

use super::*;

async fn console(max_connections: usize, ui_dir: Option<&Path>) -> SocketAddr {
    let console = Console::bind(ConsoleConfig {
        listen: "127.0.0.1:0".into(),
        coordinator: "http://127.0.0.1:1".into(),
        nodes: Vec::new(),
        analysis: None,
        security: ToolClient::default(),
        allow_remote: false,
    })
    .await
    .unwrap()
    .with_max_connections(max_connections);
    let console = match ui_dir {
        Some(dir) => console.with_ui_dir(dir).unwrap(),
        None => console,
    };
    let addr = console.local_addr();
    tokio::spawn(console.serve());
    addr
}

/// One response off `stream`: the head, then exactly `Content-Length`
/// body bytes, so the connection is left at the next response.
async fn reply(stream: &mut TcpStream) -> (String, String) {
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    while !raw.ends_with(b"\r\n\r\n") {
        assert_eq!(
            stream.read(&mut byte).await.unwrap(),
            1,
            "closed in the head"
        );
        raw.push(byte[0]);
    }
    let head = String::from_utf8(raw).unwrap();
    let length: usize = head
        .lines()
        .find_map(|l| l.strip_prefix("Content-Length: "))
        .expect("a length")
        .parse()
        .unwrap();
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await.unwrap();
    (head, String::from_utf8(body).unwrap())
}

async fn get(stream: &mut TcpStream, path: &str) -> (String, String) {
    let request = format!("GET {path} HTTP/1.1\r\nHost: c\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    reply(stream).await
}

#[tokio::test]
async fn one_connection_carries_several_requests_until_close() {
    let addr = console(4, None).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let (head, css) = get(&mut stream, "/app.css").await;
    assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
    assert!(
        head.contains("Connection: keep-alive\r\nKeep-Alive: timeout=30\r\n"),
        "{head}"
    );
    assert_eq!(
        css,
        EMBEDDED.iter().find(|(n, _)| *n == "app.css").unwrap().1
    );
    let (head, body) = get(&mut stream, "/api/describe/SearchService/Query").await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}: {body}");
    let chunked =
        "POST /api/documents HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
                   8\r\n{\"doc_id\r\na\r\ns\": [\"x\"]}\r\n0\r\n\r\n";
    stream.write_all(chunked.as_bytes()).await.unwrap();
    let (head, body) = reply(&mut stream).await;
    assert!(
        head.starts_with("HTTP/1.1 400") && head.contains("Connection: close"),
        "{head}"
    );
    assert!(body.contains("unsigned integers, got \\\"x\\\""), "{body}");
    let mut rest = Vec::new();
    assert_eq!(
        stream.read_to_end(&mut rest).await.unwrap(),
        0,
        "closed after the reply"
    );
}

#[tokio::test]
async fn connections_past_the_cap_wait_for_a_slot() {
    let addr = console(1, None).await;
    let mut first = TcpStream::connect(addr).await.unwrap();
    assert!(get(&mut first, "/app.css")
        .await
        .0
        .starts_with("HTTP/1.1 200"));
    let mut second = TcpStream::connect(addr).await.unwrap();
    second
        .write_all(b"GET /app.css HTTP/1.1\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut byte = [0u8; 1];
    let waited = tokio::time::timeout(Duration::from_millis(300), second.read(&mut byte)).await;
    assert!(
        waited.is_err(),
        "the second connection was served while the first held the slot"
    );
    drop(first);
    let served = tokio::time::timeout(Duration::from_secs(5), reply(&mut second)).await;
    assert!(served
        .expect("served once the slot frees")
        .0
        .starts_with("HTTP/1.1 200"));
}

#[tokio::test]
async fn the_ui_dir_serves_live_files_and_falls_back_to_the_compiled_copy() {
    let root = std::env::temp_dir().join(format!("console-ui-dir-{}", std::process::id()));
    let dir = root.join("ui");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(root.join("secret.js"), "secret").unwrap();
    std::fs::write(dir.join("search.js"), "// one").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(root.join("secret.js"), dir.join("leak.js")).unwrap();
    let addr = console(4, Some(&dir)).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    assert_eq!(get(&mut stream, "/search.js").await.1, "// one");
    std::fs::write(dir.join("search.js"), "// two").unwrap();
    std::fs::write(dir.join("extra.js"), "// new").unwrap();
    assert_eq!(
        get(&mut stream, "/search.js").await.1,
        "// two",
        "read per request"
    );
    assert_eq!(get(&mut stream, "/extra.js").await.1, "// new");
    let common = EMBEDDED.iter().find(|(n, _)| *n == "common.js").unwrap().1;
    assert_eq!(get(&mut stream, "/common.js").await.1, common, "fallback");
    for refused in ["/../secret.js", "/..%2fsecret.js", "/missing.js"] {
        assert!(
            get(&mut stream, refused)
                .await
                .0
                .starts_with("HTTP/1.1 404"),
            "{refused}"
        );
    }
    #[cfg(unix)]
    assert!(get(&mut stream, "/leak.js")
        .await
        .0
        .starts_with("HTTP/1.1 403"));
    let file = Console::bind(ConsoleConfig {
        listen: "127.0.0.1:0".into(),
        coordinator: "http://127.0.0.1:1".into(),
        nodes: Vec::new(),
        analysis: None,
        security: ToolClient::default(),
        allow_remote: false,
    })
    .await
    .unwrap()
    .with_ui_dir(&dir.join("extra.js"));
    assert!(file.err().unwrap().contains("not a directory"));
    std::fs::remove_dir_all(&root).unwrap();
}
