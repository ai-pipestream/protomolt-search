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

/// A signed datagram (docs/security.md): the plain frame, a 4-byte
/// sequence number, and a 16-byte HMAC-SHA256 tag over both. The
/// receiver ignores a tag that does not verify and a sequence at or
/// behind the newest it accepted for the stream, so a forged, damaged,
/// or replayed datagram changes nothing; the gRPC twin still governs.
pub(crate) const SIGNED_FRAME_LEN: usize = FRAME_LEN + 4 + 16;

pub(crate) fn sign(
    key: &crate::security::UdpKey,
    seq: u32,
    frame: &[u8; FRAME_LEN],
) -> [u8; SIGNED_FRAME_LEN] {
    let mut out = [0u8; SIGNED_FRAME_LEN];
    out[..FRAME_LEN].copy_from_slice(frame);
    out[FRAME_LEN..FRAME_LEN + 4].copy_from_slice(&seq.to_le_bytes());
    let tag = key.tag(&out[..FRAME_LEN + 4]);
    out[FRAME_LEN + 4..].copy_from_slice(&tag);
    out
}

/// Verify and decode a signed datagram: `(signal, sequence)` when the
/// tag matches and the frame is well formed, `None` otherwise.
pub(crate) fn decode_signed(
    key: &crate::security::UdpKey,
    datagram: &[u8],
) -> Option<(StreamSignal, u32)> {
    if datagram.len() != SIGNED_FRAME_LEN {
        return None;
    }
    let (body, tag) = datagram.split_at(FRAME_LEN + 4);
    if !key.verify(body, tag) {
        return None;
    }
    let seq = u32::from_le_bytes(body[FRAME_LEN..].try_into().expect("4-byte sequence"));
    decode(&body[..FRAME_LEN]).map(|signal| (signal, seq))
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

    #[test]
    fn signed_frames_verify_and_reject_forgeries() {
        let key = crate::security::UdpKey::from_bytes(&[9u8; 32]).unwrap();
        let other = crate::security::UdpKey::from_bytes(&[8u8; 32]).unwrap();
        let signed = sign(&key, 42, &encode_floor(7, 0.5));
        assert_eq!(
            decode_signed(&key, &signed),
            Some((
                StreamSignal::RaiseFloor {
                    token: 7,
                    floor: 0.5
                },
                42
            ))
        );
        assert_eq!(decode_signed(&other, &signed), None, "wrong key");
        assert_eq!(
            decode_signed(&key, &signed[..SIGNED_FRAME_LEN - 1]),
            None,
            "truncated"
        );
        let mut flipped = signed;
        flipped[17] ^= 1;
        assert_eq!(decode_signed(&key, &flipped), None, "damaged floor");
        let mut reseq = signed;
        reseq[FRAME_LEN] ^= 1;
        assert_eq!(decode_signed(&key, &reseq), None, "damaged sequence");
        assert_eq!(decode(&signed), None, "a signed frame is not a plain frame");
        assert_eq!(
            decode_signed(&key, &encode_floor(7, 0.5)),
            None,
            "a plain frame is not signed"
        );
    }
}
