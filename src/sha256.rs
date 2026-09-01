//! Hand-rolled SHA-256 (FIPS 180-4), the digest behind mapping-plan
//! fingerprints and descriptor-set content addressing
//! (`docs/descriptor-mappings.md`).
//!
//! Hand-rolled for the same reason the CEL front-end and the Prometheus
//! exporter are: the algorithm is small, frozen since 2002, and pinned
//! here by the NIST test vectors, while a hashing crate is a dependency
//! the serving binary does not need. This is content addressing, not
//! authentication — no key handling, no constant-time obligations; a
//! digest either matches the registered bytes or it does not.

/// Streaming SHA-256 state.
pub struct Sha256 {
    state: [u32; 8],
    /// Total message length in bytes.
    len: u64,
    /// Partial block awaiting 64 bytes.
    block: [u8; 64],
    fill: usize,
}

/// Round constants: the first 32 bits of the fractional parts of the
/// cube roots of the first 64 primes (FIPS 180-4 section 4.2.2).
const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256 {
    /// A fresh hash state (FIPS 180-4 section 5.3.3 initial values: the
    /// first 32 bits of the fractional parts of the square roots of the
    /// first 8 primes).
    pub fn new() -> Self {
        Self {
            state: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
                0x5be0cd19,
            ],
            len: 0,
            block: [0; 64],
            fill: 0,
        }
    }

    /// Absorb `data`. Chunk boundaries do not affect the digest.
    pub fn update(&mut self, data: &[u8]) {
        self.len = self
            .len
            .checked_add(data.len() as u64)
            .expect("SHA-256 input under 2^61 bytes");
        let mut data = data;
        if self.fill > 0 {
            let take = (64 - self.fill).min(data.len());
            self.block[self.fill..self.fill + take].copy_from_slice(&data[..take]);
            self.fill += take;
            data = &data[take..];
            if self.fill < 64 {
                // All input fit in the partial block; the tail below
                // must not clobber `fill` with the empty remainder.
                return;
            }
            let block = self.block;
            self.compress(&block);
            self.fill = 0;
        }
        while data.len() >= 64 {
            let (block, rest) = data.split_at(64);
            self.compress(block.try_into().expect("split_at(64)"));
            data = rest;
        }
        self.block[..data.len()].copy_from_slice(data);
        self.fill = data.len();
    }

    /// Pad, finish, and return the 32-byte digest.
    pub fn finalize(mut self) -> [u8; 32] {
        let bit_len = self.len * 8;
        self.update(&[0x80]);
        while self.fill != 56 {
            self.update(&[0]);
        }
        // bit_len was captured before padding, so the padding updates
        // above cannot leak into the encoded length.
        let block_start = &mut self.block[56..];
        block_start.copy_from_slice(&bit_len.to_be_bytes());
        let block = self.block;
        self.compress(&block);
        let mut out = [0u8; 32];
        for (chunk, word) in out.as_chunks_mut::<4>().0.iter_mut().zip(self.state) {
            chunk.copy_from_slice(&word.to_be_bytes());
        }
        out
    }

    fn compress(&mut self, block: &[u8; 64]) {
        let mut w = [0u32; 64];
        for (i, chunk) in block.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(*chunk);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = self.state;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        self.state[0] = self.state[0].wrapping_add(a);
        self.state[1] = self.state[1].wrapping_add(b);
        self.state[2] = self.state[2].wrapping_add(c);
        self.state[3] = self.state[3].wrapping_add(d);
        self.state[4] = self.state[4].wrapping_add(e);
        self.state[5] = self.state[5].wrapping_add(f);
        self.state[6] = self.state[6].wrapping_add(g);
        self.state[7] = self.state[7].wrapping_add(h);
    }
}

/// One-shot digest.
pub fn digest(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize()
}

/// One-shot digest rendered as lowercase hex, the form fingerprints and
/// content addresses travel in.
pub fn hex_digest(data: &[u8]) -> String {
    to_hex(&digest(data))
}

/// Lowercase hex rendering of a digest.
pub fn to_hex(digest: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FIPS 180-4 / NIST CAVP vectors. A digest function that passes
    /// these on empty, one-block, and two-block inputs is SHA-256.
    #[test]
    fn nist_vectors() {
        assert_eq!(
            hex_digest(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex_digest(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex_digest(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    /// One million 'a's, the classic long-input vector, exercises the
    /// streaming path across many blocks.
    #[test]
    fn million_a() {
        let mut h = Sha256::new();
        // Deliberately awkward chunk size so partial blocks span calls.
        let chunk = [b'a'; 977];
        let mut left = 1_000_000;
        while left > 0 {
            let take = left.min(chunk.len());
            h.update(&chunk[..take]);
            left -= take;
        }
        assert_eq!(
            to_hex(&h.finalize()),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    /// Chunk boundaries never change the digest.
    #[test]
    fn chunking_is_invisible() {
        let data: Vec<u8> = (0u8..=255).cycle().take(1000).collect();
        let whole = digest(&data);
        for split in [1, 63, 64, 65, 128, 999] {
            let mut h = Sha256::new();
            let (a, b) = data.split_at(split);
            h.update(a);
            h.update(b);
            assert_eq!(h.finalize(), whole, "split at {split} changed the digest");
        }
    }
}
