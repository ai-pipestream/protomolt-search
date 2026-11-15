//! Floor signals a relay forwards between its parent and its children.

#[cfg(feature = "net")]
use std::collections::HashMap;
#[cfg(feature = "net")]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "net")]
use std::sync::{Arc, Mutex};

use tokio::sync::{watch, Notify};

/// The relay's parent-facing stream signals: the parent's floor raises
/// (gRPC or UDP) fold into one watch cell, its cancellation into one
/// flag, and the forwarding loop wakes on either.
pub(super) struct RelaySignals {
    pub(super) floor: watch::Sender<f32>,
    pub(super) cancelled: AtomicBool,
    pub(super) cancel: Notify,
    finish_state: std::sync::atomic::AtomicU8,
    pub(super) finish: Notify,
    /// The newest signed sequence applied; a datagram at or behind it
    /// is a replay and is ignored.
    #[cfg(feature = "net")]
    last_seq: AtomicU32,
}

impl RelaySignals {
    pub(super) fn new(initial_floor: f32) -> Self {
        let (floor, _) = watch::channel(initial_floor);
        Self {
            floor,
            cancelled: AtomicBool::new(false),
            cancel: Notify::new(),
            finish_state: std::sync::atomic::AtomicU8::new(0),
            finish: Notify::new(),
            #[cfg(feature = "net")]
            last_seq: AtomicU32::new(0),
        }
    }

    /// Monotone: a raise below the current floor is ignored, NaN too.
    pub(super) fn raise(&self, floor: f32) {
        if floor.is_nan() {
            return;
        }
        self.floor.send_if_modified(|current| {
            if floor > *current {
                *current = floor;
                true
            } else {
                false
            }
        });
    }

    pub(super) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.cancel.notify_one();
    }

    pub(super) async fn wait_cancelled(&self) {
        while !self.cancelled.load(Ordering::Acquire) {
            self.cancel.notified().await;
        }
    }

    pub(super) async fn wait_finish(&self) {
        while self.finish_state_raw() == 0 {
            self.finish.notified().await;
        }
    }

    pub(super) fn request_finish(&self, deadline: bool) {
        let code = if deadline { 2 } else { 1 };
        let prev = self.finish_state.fetch_max(code, Ordering::AcqRel);
        if code > prev {
            self.finish.notify_one();
        }
    }

    pub(super) fn finish_state_raw(&self) -> u8 {
        self.finish_state.load(Ordering::Acquire)
    }

    // fetch_update is deprecated in favor of try_update from Rust 1.99, but the
    // Pi binaries are built on 1.97, which lacks try_update.
    #[allow(deprecated)]
    #[cfg(feature = "net")]
    fn accept_seq(&self, seq: u32) -> bool {
        self.last_seq
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                (seq > last).then_some(seq)
            })
            .is_ok()
    }
}

/// Apply one parent-facing datagram to the relay's stream registry.
#[cfg(feature = "net")]
pub(super) fn apply_datagram(
    signals: &Mutex<HashMap<u64, Arc<RelaySignals>>>,
    key: Option<&crate::security::UdpKey>,
    datagram: &[u8],
) {
    let (signal, seq) = match key {
        Some(key) => match crate::stream_signal::decode_signed(key, datagram) {
            Some((signal, seq)) => (signal, Some(seq)),
            None => return,
        },
        None => match crate::stream_signal::decode(datagram) {
            Some(signal) => (signal, None),
            None => return,
        },
    };
    let token = match signal {
        crate::stream_signal::StreamSignal::RaiseFloor { token, .. }
        | crate::stream_signal::StreamSignal::Cancel { token }
        | crate::stream_signal::StreamSignal::Finish { token, .. } => token,
    };
    let state = signals
        .lock()
        .expect("relay stream signal registry poisoned")
        .get(&token)
        .cloned();
    let Some(state) = state else {
        return;
    };
    if let Some(seq) = seq {
        if !state.accept_seq(seq) {
            return;
        }
    }
    match signal {
        crate::stream_signal::StreamSignal::RaiseFloor { floor, .. } => state.raise(floor),
        crate::stream_signal::StreamSignal::Cancel { .. } => state.cancel(),
        crate::stream_signal::StreamSignal::Finish { deadline, .. } => {
            state.request_finish(deadline);
        }
    }
}

impl super::RelayService {
    /// Bind the relay's parent-facing UDP signal lane on `addr` (the
    /// same host:port as its gRPC listener) and fold typed floor and
    /// cancel datagrams into the matching stream. The same acceptance
    /// rule as a node's lane: signed when a key is configured, unsigned
    /// on loopback only.
    #[cfg(feature = "net")]
    pub fn spawn_floor_listener(&self, addr: std::net::SocketAddr) {
        let signals = Arc::clone(&self.signals);
        let key = self.base().udp_key().cloned();
        if key.is_none() && !crate::security::is_loopback(&addr) {
            eprintln!(
                "relay stream-signal UDP lane on {addr} stays closed: no --udp-hmac-key, and \
                 unsigned datagrams are accepted on loopback only; signals ride the gRPC streams"
            );
            return;
        }
        tokio::spawn(async move {
            let socket = match tokio::net::UdpSocket::bind(addr).await {
                Ok(socket) => socket,
                Err(e) => {
                    eprintln!(
                        "relay stream-signal UDP bind {addr}: {e}; signals ride the gRPC streams \
                         only"
                    );
                    return;
                }
            };
            let mut buf = [0u8; 64];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, _peer)) => apply_datagram(&signals, key.as_ref(), &buf[..n]),
                    Err(_) => continue,
                }
            }
        });
    }
}
