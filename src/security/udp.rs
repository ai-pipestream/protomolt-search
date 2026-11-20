//! Constant-time comparison and the HMAC-SHA256 key that authenticates
//! UDP floor and cancel datagrams.

use std::path::Path;

/// Byte equality whose running time depends on the lengths only.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // The accumulator is a usize so a length difference survives whole:
    // narrowing it to a byte would let lengths 256 apart compare as one.
    let mut diff: usize = a.len() ^ b.len();
    let n = a.len().min(b.len());
    for i in 0..n {
        diff |= usize::from(a[i] ^ b[i]);
    }
    // Walk the rest of the longer input so length differences cost
    // the same as content differences.
    for &x in a.iter().skip(n).chain(b.iter().skip(n)) {
        std::hint::black_box(x);
    }
    std::hint::black_box(diff) == 0
}

/// HMAC-SHA256 (RFC 2104).
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    turbo_sha256::hmac_sha256(key, message).0
}

/// The shared key that authenticates UDP floor and cancel datagrams.
#[derive(Clone)]
pub struct UdpKey(Vec<u8>);

impl std::fmt::Debug for UdpKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UdpKey({} bytes)", self.0.len())
    }
}

impl UdpKey {
    /// Read the key from a file: raw bytes, or a hex string (whitespace
    /// ignored). At least 16 bytes of key material.
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes =
            std::fs::read(path).map_err(|e| format!("read UDP key {}: {e}", path.display()))?;
        Self::from_bytes(&bytes).map_err(|e| format!("UDP key {}: {e}", path.display()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let text: Vec<u8> = bytes
            .iter()
            .copied()
            .filter(|b| !b.is_ascii_whitespace())
            .collect();
        let key = if !text.is_empty()
            && text.len().is_multiple_of(2)
            && text.iter().all(u8::is_ascii_hexdigit)
        {
            text.chunks(2)
                .map(|pair| {
                    let hi = (pair[0] as char).to_digit(16).expect("hex digit") as u8;
                    let lo = (pair[1] as char).to_digit(16).expect("hex digit") as u8;
                    (hi << 4) | lo
                })
                .collect::<Vec<u8>>()
        } else {
            bytes.to_vec()
        };
        if key.len() < 16 {
            return Err(format!(
                "key material is {} bytes; at least 16 are required",
                key.len()
            ));
        }
        Ok(UdpKey(key))
    }

    /// The 16-byte tag over `message`: HMAC-SHA256 truncated.
    pub fn tag(&self, message: &[u8]) -> [u8; 16] {
        let full = hmac_sha256(&self.0, message);
        let mut tag = [0u8; 16];
        tag.copy_from_slice(&full[..16]);
        tag
    }

    pub fn verify(&self, message: &[u8], tag: &[u8]) -> bool {
        tag.len() == 16 && constant_time_eq(&self.tag(message), tag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_sha256_matches_rfc_4231_vectors() {
        // Test case 2 of RFC 4231.
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            turbo_sha256::to_hex(mac),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // Test case 1.
        let mac = hmac_sha256(&[0x0b; 20], b"Hi There");
        assert_eq!(
            turbo_sha256::to_hex(mac),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // Test case 6: a key longer than the block.
        let mac = hmac_sha256(
            &[0xaa; 131],
            b"Test Using Larger Than Block-Size Key - Hash Key First",
        );
        assert_eq!(
            turbo_sha256::to_hex(mac),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn udp_keys_load_hex_or_raw_and_tags_verify() {
        let key = UdpKey::from_bytes(b"000102030405060708090a0b0c0d0e0f\n").unwrap();
        assert_eq!(key.0, (0u8..16).collect::<Vec<_>>());
        let raw = UdpKey::from_bytes(&[7u8; 32]).unwrap();
        assert_eq!(raw.0.len(), 32);
        assert!(UdpKey::from_bytes(b"short").is_err());
        let tag = key.tag(b"frame");
        assert!(key.verify(b"frame", &tag));
        assert!(!key.verify(b"framf", &tag));
        assert!(!raw.verify(b"frame", &tag));
        assert!(!key.verify(b"frame", &tag[..15]));
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        // Lengths 256 apart, in both directions, with equal prefixes.
        let short = [7u8; 16];
        let long = [7u8; 272];
        assert!(!constant_time_eq(&short, &long));
        assert!(!constant_time_eq(&long, &short));
        assert!(!constant_time_eq(&[], &[0u8; 256]));
        assert!(constant_time_eq(&long, &long[..]));
    }
}
