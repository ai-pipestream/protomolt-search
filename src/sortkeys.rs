//! Sort keys (`docs/query-api.md`, "Sorting"): the one comparison a
//! sorted row is ordered by on the node's heap, in the coordinator's
//! merge, and at a cursor boundary.
//!
//! A key is either order-preserving u64 bits (an i64 or f64 column
//! through the node's offset-binary / sign-flip mapping, or a lineage
//! id as is) or a facet term's text. Bits are complemented on the node
//! for a descending key, so they always compare ascending; text cannot
//! be complemented, so its comparison is reversed per key instead. Rows
//! compare key by key, most significant first, then by doc id.

use std::cmp::Ordering;

use crate::pb;

/// One key of one row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Key {
    Bits(u64),
    Text(String),
}

/// A borrowed key, for comparing a candidate against a stored row
/// without allocating.
#[derive(Clone, Copy, Debug)]
pub enum KeyRef<'a> {
    Bits(u64),
    Text(&'a str),
}

impl Key {
    pub fn as_ref(&self) -> KeyRef<'_> {
        match self {
            Key::Bits(b) => KeyRef::Bits(*b),
            Key::Text(t) => KeyRef::Text(t),
        }
    }

    pub fn to_pb(&self) -> pb::SortKey {
        pb::SortKey {
            key: Some(match self {
                Key::Bits(b) => pb::sort_key::Key::Bits(*b),
                Key::Text(t) => pb::sort_key::Key::Text(t.clone()),
            }),
        }
    }

    pub fn from_pb(key: &pb::SortKey) -> Option<Key> {
        match key.key.as_ref()? {
            pb::sort_key::Key::Bits(b) => Some(Key::Bits(*b)),
            pb::sort_key::Key::Text(t) => Some(Key::Text(t.clone())),
        }
    }
}

impl<'a> KeyRef<'a> {
    pub fn to_owned(self) -> Key {
        match self {
            KeyRef::Bits(b) => Key::Bits(b),
            KeyRef::Text(t) => Key::Text(t.to_string()),
        }
    }
}

/// Compare two keys of the same position under the key's direction:
/// bits compare ascending as they are (a descending key's bits were
/// complemented at the source); text compares by bytes, reversed for a
/// descending key. Two keys of different kinds never meet (a column has
/// one kind); should a shard disagree, bits order before text so the
/// comparison stays total.
pub fn cmp_key(a: KeyRef<'_>, b: KeyRef<'_>, descending: bool) -> Ordering {
    match (a, b) {
        (KeyRef::Bits(x), KeyRef::Bits(y)) => x.cmp(&y),
        (KeyRef::Text(x), KeyRef::Text(y)) => {
            let o = x.as_bytes().cmp(y.as_bytes());
            if descending {
                o.reverse()
            } else {
                o
            }
        }
        (KeyRef::Bits(_), KeyRef::Text(_)) => Ordering::Less,
        (KeyRef::Text(_), KeyRef::Bits(_)) => Ordering::Greater,
    }
}

/// Compare two rows: key by key under `descending`, then by doc id.
pub fn cmp_rows(a: &[Key], a_id: u64, b: &[Key], b_id: u64, descending: &[bool]) -> Ordering {
    for (i, (x, y)) in a.iter().zip(b).enumerate() {
        let o = cmp_key(
            x.as_ref(),
            y.as_ref(),
            descending.get(i).copied().unwrap_or(false),
        );
        if o != Ordering::Equal {
            return o;
        }
    }
    a_id.cmp(&b_id)
}

/// Compare a borrowed candidate row against a stored row.
pub fn cmp_candidate(
    a: &[KeyRef<'_>],
    a_id: u64,
    b: &[Key],
    b_id: u64,
    descending: &[bool],
) -> Ordering {
    for (i, (x, y)) in a.iter().zip(b).enumerate() {
        let o = cmp_key(*x, y.as_ref(), descending.get(i).copied().unwrap_or(false));
        if o != Ordering::Equal {
            return o;
        }
    }
    a_id.cmp(&b_id)
}

/// The cursor form of a key list: `b<16 hex>` for bits, `t<hex of the
/// UTF-8>` for text, joined by `,`. Hex keeps the token free of the
/// cursor's own separators whatever the term holds.
pub fn encode_keys(keys: &[Key]) -> String {
    keys.iter()
        .map(|k| match k {
            Key::Bits(b) => format!("b{b:016x}"),
            Key::Text(t) => {
                let mut out = String::with_capacity(1 + t.len() * 2);
                out.push('t');
                for byte in t.as_bytes() {
                    out.push_str(&format!("{byte:02x}"));
                }
                out
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Inverse of [`encode_keys`]; `None` on any malformed part.
pub fn decode_keys(text: &str) -> Option<Vec<Key>> {
    if text.is_empty() {
        return Some(Vec::new());
    }
    text.split(',')
        .map(|part| {
            let (tag, body) =
                part.split_at(part.char_indices().nth(1).map_or(part.len(), |(i, _)| i));
            match tag {
                "b" => u64::from_str_radix(body, 16).ok().map(Key::Bits),
                "t" => {
                    if !body.len().is_multiple_of(2) {
                        return None;
                    }
                    let bytes: Option<Vec<u8>> = (0..body.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&body[i..i + 2], 16).ok())
                        .collect();
                    String::from_utf8(bytes?).ok().map(Key::Text)
                }
                _ => None,
            }
        })
        .collect()
}

/// The public value of a key (`SortValue`).
pub fn value_from_pb(value: &pb::SortValue) -> Option<Value> {
    match value.value.as_ref()? {
        pb::sort_value::Value::Number(n) => Some(Value::Number(*n)),
        pb::sort_value::Value::Integer(i) => Some(Value::Integer(*i)),
        pb::sort_value::Value::Text(t) => Some(Value::Text(t.clone())),
    }
}

/// A key's reported value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Number(f64),
    Integer(i64),
    Text(String),
}

impl Value {
    pub fn to_pb(&self) -> pb::SortValue {
        pb::SortValue {
            value: Some(match self {
                Value::Number(n) => pb::sort_value::Value::Number(*n),
                Value::Integer(i) => pb::sort_value::Value::Integer(*i),
                Value::Text(t) => pb::sort_value::Value::Text(t.clone()),
            }),
        }
    }

    /// The numeric view `QueryHit.sort_key` keeps: the number, the
    /// integer through the monotone f64 cast, 0 for text.
    pub fn as_f64(&self) -> f64 {
        match self {
            Value::Number(n) => *n,
            Value::Integer(i) => *i as f64,
            Value::Text(_) => 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_compare_key_by_key_then_by_id_and_text_reverses_when_descending() {
        let desc = [false, true];
        let a = [Key::Bits(1), Key::Text("m".into())];
        let b = [Key::Bits(1), Key::Text("z".into())];
        // Second key descending: "z" orders before "m".
        assert_eq!(cmp_rows(&a, 1, &b, 2, &desc), Ordering::Greater);
        assert_eq!(cmp_rows(&b, 2, &a, 1, &desc), Ordering::Less);
        // Equal keys: the id decides.
        assert_eq!(cmp_rows(&a, 1, &a, 2, &desc), Ordering::Less);
        // First key decides before the second is looked at.
        let c = [Key::Bits(0), Key::Text("a".into())];
        assert_eq!(cmp_rows(&c, 9, &a, 1, &desc), Ordering::Less);
        let borrowed = [KeyRef::Bits(1), KeyRef::Text("m")];
        assert_eq!(cmp_candidate(&borrowed, 1, &a, 1, &desc), Ordering::Equal);
    }

    #[test]
    fn cursor_keys_round_trip_through_hex() {
        let keys = vec![
            Key::Bits(u64::MAX - 3),
            Key::Text("Smith, v. Jones:ünï".into()),
        ];
        let text = encode_keys(&keys);
        assert!(!text.contains(':'), "{text}");
        assert_eq!(decode_keys(&text).unwrap(), keys);
        assert_eq!(decode_keys("").unwrap(), Vec::<Key>::new());
        assert!(decode_keys("x00").is_none());
        assert!(decode_keys("t0").is_none());
        assert!(decode_keys("tzz").is_none());
    }
}
