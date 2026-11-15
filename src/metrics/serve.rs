//! The scrape endpoint.

use super::gauges::{GaugeProvider, MemberGaugeProvider};
use super::render::render_with_member;

/// Serve `render` over HTTP on `listener`, forever. The server answers
/// every request on the socket with the metrics page — it parses
/// nothing beyond draining the request head, because the only client
/// is a scraper and the only resource is the page. The page is plain
/// HTTP with no auth (TLS and bearer principals cover the gRPC
/// listeners, not this one): bind the listener to a trusted interface.
pub async fn serve(listener: tokio::net::TcpListener, gauges: Vec<GaugeProvider>) {
    serve_with_member(listener, gauges, Vec::new()).await
}

/// Serve [`render_with_member`] over HTTP; see [`serve`].
pub async fn serve_with_member(
    listener: tokio::net::TcpListener,
    gauges: Vec<GaugeProvider>,
    members: Vec<MemberGaugeProvider>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let gauges = std::sync::Arc::new(gauges);
    let members = std::sync::Arc::new(members);
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            continue;
        };
        let gauges = gauges.clone();
        let members = members.clone();
        tokio::spawn(async move {
            // Drain the request head (up to a bound; a scraper's GET is
            // tiny) so the peer never sees a reset before our response.
            let mut buf = [0u8; 4096];
            let mut head = Vec::new();
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        head.extend_from_slice(&buf[..n]);
                        if head.windows(4).any(|w| w == b"\r\n\r\n") || head.len() > 16 * 1024 {
                            break;
                        }
                    }
                }
            }
            let body = render_with_member(&gauges, &members);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; \
                 charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}
