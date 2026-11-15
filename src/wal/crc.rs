//! CRC32 (IEEE, table-based; hand-rolled to avoid a crate dependency).

const CRC_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
};

/// Eight derived tables for slice-by-8: `SLICE_TABLE[k][b]` advances a
/// CRC eight bytes at a time instead of one. Same polynomial, same
/// answers as the byte-at-a-time loop — `crc32_known_vector` pins it —
/// but ~5x the throughput, which is what makes CRC-verifying a 50 GB
/// postings section an explicit-stage cost instead of a prohibitive
/// one.
const SLICE_TABLE: [[u32; 256]; 8] = {
    let mut t = [[0u32; 256]; 8];
    t[0] = CRC_TABLE;
    let mut i = 0;
    while i < 256 {
        let mut c = t[0][i];
        let mut k = 1;
        while k < 8 {
            c = t[0][(c & 0xFF) as usize] ^ (c >> 8);
            t[k][i] = c;
            k += 1;
        }
        i += 1;
    }
    t
};

/// CRC32 (IEEE 802.3 polynomial, reflected) of `data`.
pub fn crc32(data: &[u8]) -> u32 {
    let mut c = !0u32;
    let (chunks, remainder) = data.as_chunks::<8>();
    for w in chunks {
        let lo = u32::from_le_bytes(w[..4].try_into().expect("4 bytes")) ^ c;
        let hi = u32::from_le_bytes(w[4..].try_into().expect("4 bytes"));
        c = SLICE_TABLE[7][(lo & 0xFF) as usize]
            ^ SLICE_TABLE[6][((lo >> 8) & 0xFF) as usize]
            ^ SLICE_TABLE[5][((lo >> 16) & 0xFF) as usize]
            ^ SLICE_TABLE[4][(lo >> 24) as usize]
            ^ SLICE_TABLE[3][(hi & 0xFF) as usize]
            ^ SLICE_TABLE[2][((hi >> 8) & 0xFF) as usize]
            ^ SLICE_TABLE[1][((hi >> 16) & 0xFF) as usize]
            ^ SLICE_TABLE[0][(hi >> 24) as usize];
    }
    for &b in remainder {
        c = CRC_TABLE[((c ^ u32::from(b)) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}
