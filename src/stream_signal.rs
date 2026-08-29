//! Typed datagrams for the optional streaming-search UDP fast lane.
//!
//! Every signal is duplicated on the authoritative gRPC request stream. UDP
//! only shortens the time until a node observes a floor raise or cancellation;
//! loss, duplication, reordering, and malformed packets cannot turn an
//! incomplete scan into a completed one.

/// `TVS1`, the frozen identifier for the first streaming-signal frame.
const MAGIC: [u8; 4] = *b"TVS1";
const RAISE_FLOOR: u8 = 1;
const CANCEL: u8 = 2;

/// Four magic bytes, one opcode, three reserved bytes, a token, and an f32.
pub(crate) const FRAME_LEN: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum StreamSignal {
    RaiseFloor { token: u64, floor: f32 },
    Cancel { token: u64 },
}

pub(crate) fn encode_floor(token: u64, floor: f32) -> [u8; FRAME_LEN] {
    debug_assert!(!floor.is_nan());
    encode(token, RAISE_FLOOR, floor)
}

pub(crate) fn encode_cancel(token: u64) -> [u8; FRAME_LEN] {
    encode(token, CANCEL, 0.0)
}

fn encode(token: u64, opcode: u8, value: f32) -> [u8; FRAME_LEN] {
    let mut frame = [0u8; FRAME_LEN];
    frame[..4].copy_from_slice(&MAGIC);
    frame[4] = opcode;
    frame[8..16].copy_from_slice(&token.to_le_bytes());
    frame[16..].copy_from_slice(&value.to_le_bytes());
    frame
}

pub(crate) fn decode(frame: &[u8]) -> Option<StreamSignal> {
    if frame.len() != FRAME_LEN || frame[..4] != MAGIC || frame[5..8] != [0; 3] {
        return None;
    }
    let token = u64::from_le_bytes(frame[8..16].try_into().expect("8-byte token"));
    if token == 0 {
        return None;
    }
    match frame[4] {
        RAISE_FLOOR => {
            let floor = f32::from_le_bytes(frame[16..20].try_into().expect("4-byte floor"));
            (!floor.is_nan()).then_some(StreamSignal::RaiseFloor { token, floor })
        }
        CANCEL if frame[16..20] == 0.0f32.to_le_bytes() => Some(StreamSignal::Cancel { token }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_frames_round_trip_and_reject_ambiguous_inputs() {
        assert_eq!(
            decode(&encode_floor(7, -1.0)),
            Some(StreamSignal::RaiseFloor {
                token: 7,
                floor: -1.0,
            })
        );
        assert_eq!(
            decode(&encode_cancel(7)),
            Some(StreamSignal::Cancel { token: 7 })
        );

        let mut bad = encode_cancel(7);
        bad[5] = 1;
        assert_eq!(decode(&bad), None, "reserved bytes must remain zero");
        assert_eq!(
            decode(&encode_floor(0, 1.0)),
            None,
            "token zero is disabled"
        );
        let mut nan = encode_floor(7, 1.0);
        nan[16..20].copy_from_slice(&f32::NAN.to_le_bytes());
        assert_eq!(decode(&nan), None);
        assert_eq!(decode(&[0; FRAME_LEN]), None);
        assert_eq!(decode(&encode_cancel(7)[..FRAME_LEN - 1]), None);
    }
}
