//! UDP floor-lane datagram folding and signed-lane replay protection.

use super::*;
use std::sync::atomic::Ordering;

fn state_of(
    signals: &std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>,
    token: u64,
) -> Arc<StreamSignals> {
    Arc::clone(&signals.lock().unwrap()[&token])
}

fn floor_of(signals: &std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>, token: u64) -> f32 {
    f32::from_bits(state_of(signals, token).floor.load(Ordering::Acquire))
}

/// Typed UDP raises remain monotone, cancellation is sticky, and garbage
/// cannot mutate either signal or panic the listener.
#[test]
fn stream_datagrams_fold_typed_signals_and_ignore_garbage() {
    let signals: std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>> =
        std::sync::Mutex::new(HashMap::new());
    signals
        .lock()
        .unwrap()
        .insert(7, Arc::new(StreamSignals::new(f32::NEG_INFINITY)));

    apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.25));
    assert_eq!(floor_of(&signals, 7), 0.25);
    // Lower and equal floors are ignored.
    apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.10));
    apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.25));
    assert_eq!(floor_of(&signals, 7), 0.25);
    // Duplicated and reordered raises: max wins regardless.
    apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.75));
    apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.50));
    apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.75));
    assert_eq!(floor_of(&signals, 7), 0.75);

    let state = state_of(&signals, 7);
    assert_eq!(state.finish_state(), None);
    apply_stream_datagram(
        &signals,
        None,
        &crate::stream_signal::encode_finish(7, false),
    );
    assert_eq!(state.finish_state(), Some(false));
    apply_stream_datagram(
        &signals,
        None,
        &crate::stream_signal::encode_finish(7, true),
    );
    assert_eq!(state.finish_state(), Some(true));
    // A later non-deadline finish does not downgrade a deadline finish.
    apply_stream_datagram(
        &signals,
        None,
        &crate::stream_signal::encode_finish(7, false),
    );
    assert_eq!(state.finish_state(), Some(true));

    assert!(!state.cancelled.load(Ordering::Acquire));
    apply_stream_datagram(&signals, None, &crate::stream_signal::encode_cancel(7));
    assert!(state.cancelled.load(Ordering::Acquire));
    assert_eq!(
        floor_of(&signals, 7),
        0.75,
        "cancel is not a score sentinel"
    );

    // Unknown tokens, malformed frames, and empty datagrams are dropped.
    apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(8, 9.0));
    apply_stream_datagram(
        &signals,
        None,
        &crate::stream_signal::encode_floor(7, 9.0)[..crate::stream_signal::FRAME_LEN - 1],
    );
    apply_stream_datagram(&signals, None, &[0u8; crate::stream_signal::FRAME_LEN + 1]);
    apply_stream_datagram(&signals, None, &[]);
    assert_eq!(floor_of(&signals, 7), 0.75);
    assert!(state.cancelled.load(Ordering::Acquire));
    assert_eq!(signals.lock().unwrap().len(), 1);
}

/// When a key is configured, only validly signed datagrams with strictly
/// advancing sequence numbers update the floor.
#[test]
fn signed_floor_lane_ignores_forgeries_and_replays() {
    use crate::security::UdpKey;
    use crate::stream_signal::{encode_cancel, encode_floor, sign};
    let key = UdpKey::from_bytes(&[1u8; 32]).unwrap();
    let other = UdpKey::from_bytes(&[2u8; 32]).unwrap();
    let signals: std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>> =
        std::sync::Mutex::new(HashMap::new());
    signals
        .lock()
        .unwrap()
        .insert(7, Arc::new(StreamSignals::new(f32::NEG_INFINITY)));
    let state = state_of(&signals, 7);

    // An authentic raise applies.
    apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(7, 0.25)));
    assert_eq!(floor_of(&signals, 7), 0.25);
    // A plain frame is not read when a key is configured.
    apply_stream_datagram(&signals, Some(&key), &encode_floor(7, 9.0));
    // A foreign key does not verify.
    apply_stream_datagram(
        &signals,
        Some(&key),
        &sign(&other, 2, &encode_floor(7, 9.0)),
    );
    // A damaged tag does not verify.
    let mut damaged = sign(&key, 2, &encode_floor(7, 9.0));
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    apply_stream_datagram(&signals, Some(&key), &damaged);
    // A replay of the authentic datagram, and a stale sequence with a
    // higher floor, are both behind the newest applied sequence.
    apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(7, 0.25)));
    apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(7, 9.0)));
    apply_stream_datagram(&signals, Some(&key), &sign(&key, 0, &encode_floor(7, 9.0)));
    assert_eq!(floor_of(&signals, 7), 0.25, "no forgery moved the floor");
    assert!(!state.cancelled.load(Ordering::Acquire));
    // A forged cancel is ignored the same way; an authentic one lands.
    apply_stream_datagram(&signals, Some(&key), &encode_cancel(7));
    apply_stream_datagram(&signals, Some(&key), &sign(&other, 3, &encode_cancel(7)));
    assert!(!state.cancelled.load(Ordering::Acquire));
    apply_stream_datagram(&signals, Some(&key), &sign(&key, 2, &encode_floor(7, 0.5)));
    assert_eq!(floor_of(&signals, 7), 0.5);
    apply_stream_datagram(&signals, Some(&key), &sign(&key, 3, &encode_cancel(7)));
    assert!(state.cancelled.load(Ordering::Acquire));
    // Sequences are per stream: a fresh token starts at zero, and signed
    // FINISH datagrams obey the same HMAC + replay check.
    signals
        .lock()
        .unwrap()
        .insert(8, Arc::new(StreamSignals::new(f32::NEG_INFINITY)));
    let state8 = state_of(&signals, 8);
    apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(8, 0.1)));
    assert_eq!(floor_of(&signals, 8), 0.1);
    apply_stream_datagram(
        &signals,
        Some(&key),
        &crate::stream_signal::encode_finish(8, false),
    );
    apply_stream_datagram(
        &signals,
        Some(&key),
        &sign(&other, 2, &crate::stream_signal::encode_finish(8, false)),
    );
    assert_eq!(state8.finish_state(), None);
    apply_stream_datagram(
        &signals,
        Some(&key),
        &sign(&key, 2, &crate::stream_signal::encode_finish(8, true)),
    );
    assert_eq!(state8.finish_state(), Some(true));
}
