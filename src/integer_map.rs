//! Exact integer map sections. Keys are UTF-8; entry presence has no sentinel.

use std::{
    collections::HashMap,
    fmt::Debug,
    io::{self, Write},
};

pub(crate) trait Integer: Copy + Ord + Debug {
    fn bits(self) -> u64;
    fn from_bits(bits: u64) -> Self;
}
impl Integer for i64 {
    fn bits(self) -> u64 {
        self as u64
    }
    fn from_bits(bits: u64) -> Self {
        bits as i64
    }
}
impl Integer for u64 {
    fn bits(self) -> u64 {
        self
    }
    fn from_bits(bits: u64) -> Self {
        bits
    }
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("integer map: {message}"),
    )
}
fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| invalid("section size overflow"))
}
fn mul(a: usize, b: usize) -> io::Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("section size overflow"))
}
fn bytes(data: &[u8], at: usize, len: usize) -> io::Result<&[u8]> {
    data.get(at..add(at, len)?)
        .ok_or_else(|| invalid("section truncated"))
}
fn u32_at(data: &[u8], at: usize) -> io::Result<u32> {
    Ok(u32::from_le_bytes(bytes(data, at, 4)?.try_into().unwrap()))
}
fn u64_at(data: &[u8], at: usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(bytes(data, at, 8)?.try_into().unwrap()))
}
fn include<T: Integer>(bounds: &mut Option<(T, T)>, value: T) {
    *bounds = Some(match *bounds {
        Some((min, max)) => (min.min(value), max.max(value)),
        None => (value, value),
    });
}

#[derive(Debug)]
pub(crate) struct Store<T: Integer> {
    pub name: String,
    pub keys: Vec<String>,
    index: HashMap<String, u32>,
    bounds: Vec<Option<(T, T)>>,
    rows: Vec<Vec<(u32, T)>>,
}
impl<T: Integer> Store<T> {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            keys: Vec::new(),
            index: HashMap::new(),
            bounds: Vec::new(),
            rows: Vec::new(),
        }
    }
    pub fn key_ord(&self, key: &str) -> Option<u32> {
        self.index.get(key).copied()
    }
    pub fn value(&self, key: u32, row: u32) -> Option<T> {
        let entries = self.rows.get(row as usize)?;
        entries
            .binary_search_by_key(&key, |&(key, _)| key)
            .ok()
            .map(|i| entries[i].1)
    }
    pub fn set(&mut self, row: u32, key: &str, value: T) -> io::Result<()> {
        if key.len() > u32::MAX as usize {
            return Err(invalid("key exceeds u32 length"));
        }
        let ordinal = match self.key_ord(key) {
            Some(ordinal) => ordinal,
            None => {
                let ordinal =
                    u32::try_from(self.keys.len()).map_err(|_| invalid("key count exceeds u32"))?;
                self.keys.push(key.into());
                self.bounds.push(None);
                self.index.insert(key.into(), ordinal);
                ordinal
            }
        };
        let needed = (row as usize)
            .checked_add(1)
            .ok_or_else(|| invalid("row count overflow"))?;
        if self.rows.len() < needed {
            self.rows.resize_with(needed, Vec::new);
        }
        let entries = &mut self.rows[row as usize];
        match entries.binary_search_by_key(&ordinal, |&(key, _)| key) {
            Ok(_) => Err(invalid("duplicate key in one document")),
            Err(at) => {
                entries.insert(at, (ordinal, value));
                include(&mut self.bounds[ordinal as usize], value);
                Ok(())
            }
        }
    }
    pub fn bounds(&self, key: u32) -> Option<(T, T)> {
        self.bounds.get(key as usize).copied().flatten()
    }
    pub fn encoded_len(&self, rows: u32) -> io::Result<u64> {
        if self.rows.len() > rows as usize {
            return Err(invalid("entries exceed document slots"));
        }
        let mut size = 12u64;
        for key in &self.keys {
            size = size
                .checked_add(21 + key.len() as u64)
                .ok_or_else(|| invalid("section size overflow"))?;
        }
        size = size
            .checked_add(8 * (u64::from(rows) + 1))
            .ok_or_else(|| invalid("section size overflow"))?;
        for entries in &self.rows {
            size = size
                .checked_add(12 * entries.len() as u64)
                .ok_or_else(|| invalid("section size overflow"))?;
        }
        Ok(size)
    }
    pub fn write(&self, w: &mut impl Write, rows: u32) -> io::Result<()> {
        self.encoded_len(rows)?;
        w.write_all(&1u32.to_le_bytes())?;
        w.write_all(&rows.to_le_bytes())?;
        w.write_all(
            &u32::try_from(self.keys.len())
                .map_err(|_| invalid("key count exceeds u32"))?
                .to_le_bytes(),
        )?;
        let mut order: Vec<_> = (0..self.keys.len()).collect();
        order.sort_by(|&a, &b| self.keys[a].cmp(&self.keys[b]));
        let mut remap = vec![0u32; order.len()];
        for (ordinal, &old) in order.iter().enumerate() {
            remap[old] = ordinal as u32;
            let key = &self.keys[old];
            w.write_all(
                &u32::try_from(key.len())
                    .map_err(|_| invalid("key exceeds u32 length"))?
                    .to_le_bytes(),
            )?;
            w.write_all(key.as_bytes())?;
            let (present, min, max) =
                self.bounds[old].map_or((0, 0, 0), |(min, max)| (1, min.bits(), max.bits()));
            w.write_all(&[present])?;
            w.write_all(&min.to_le_bytes())?;
            w.write_all(&max.to_le_bytes())?;
        }
        let mut total = 0u64;
        w.write_all(&total.to_le_bytes())?;
        for row in 0..rows as usize {
            total = total
                .checked_add(self.rows.get(row).map_or(0, |entries| entries.len() as u64))
                .ok_or_else(|| invalid("pair count overflow"))?;
            w.write_all(&total.to_le_bytes())?;
        }
        // Only one document's pairs are remapped at once, even for a large shard.
        let mut remapped = Vec::new();
        for entries in &self.rows {
            remapped.clear();
            remapped.extend(
                entries
                    .iter()
                    .map(|&(key, value)| (remap[key as usize], value)),
            );
            remapped.sort_by_key(|&(key, _)| key);
            for &(key, value) in &remapped {
                w.write_all(&key.to_le_bytes())?;
                w.write_all(&value.bits().to_le_bytes())?;
            }
        }
        Ok(())
    }
    pub fn load(name: &str, data: &[u8], rows: u32) -> io::Result<Self> {
        let reader = Reader::<T>::open(data, rows)?;
        let mut store = Self::new(name);
        store.keys = reader.keys.clone();
        store.bounds = reader.bounds.clone();
        store.index = store
            .keys
            .iter()
            .enumerate()
            .map(|(i, key)| (key.clone(), i as u32))
            .collect();
        for row in 0..rows {
            let (start, end) = reader.pair_range(data, row).expect("validated rows");
            let mut entries = Vec::with_capacity(end - start);
            for pair in start..end {
                entries.push(reader.pair(data, pair));
            }
            store.rows.push(entries);
        }
        Ok(store)
    }
}

pub(crate) struct Reader<T: Integer> {
    pub keys: Vec<String>,
    bounds: Vec<Option<(T, T)>>,
    rows: u32,
    offsets: usize,
    pairs: usize,
}
impl<T: Integer> Reader<T> {
    pub fn open(data: &[u8], rows: u32) -> io::Result<Self> {
        if u32_at(data, 0)? != 1 {
            return Err(invalid("unsupported section version"));
        }
        if u32_at(data, 4)? != rows {
            return Err(invalid("document slot count differs from file"));
        }
        let count = u32_at(data, 8)? as usize;
        if count > data.len().saturating_sub(12) / 21 {
            return Err(invalid("key count exceeds section size"));
        }
        let mut keys = Vec::<String>::with_capacity(count);
        let mut bounds = Vec::with_capacity(count);
        let mut at = 12;
        for _ in 0..count {
            let len = u32_at(data, at)? as usize;
            at = add(at, 4)?;
            let key = std::str::from_utf8(bytes(data, at, len)?)
                .map_err(|_| invalid("key is not UTF-8"))?;
            if keys.last().is_some_and(|last| last.as_str() >= key) {
                return Err(invalid("keys are not strictly ordered"));
            }
            keys.push(key.into());
            at = add(at, len)?;
            let present = bytes(data, at, 1)?[0];
            at = add(at, 1)?;
            let min = u64_at(data, at)?;
            let max = u64_at(data, add(at, 8)?)?;
            at = add(at, 16)?;
            bounds.push(match present {
                0 if min == 0 && max == 0 => None,
                1 if T::from_bits(min) <= T::from_bits(max) => {
                    Some((T::from_bits(min), T::from_bits(max)))
                }
                _ => return Err(invalid("invalid bound presence or range")),
            });
        }
        let offsets = at;
        let pairs = add(offsets, mul(add(rows as usize, 1)?, 8)?)?;
        bytes(data, offsets, pairs - offsets)?;
        if u64_at(data, offsets)? != 0 {
            return Err(invalid("first pair offset is not zero"));
        }
        let total = usize::try_from(u64_at(data, pairs - 8)?)
            .map_err(|_| invalid("pair count exceeds address space"))?;
        if add(pairs, mul(total, 12)?)? != data.len() {
            return Err(invalid("pairs do not fill section"));
        }
        let reader = Self {
            keys,
            bounds,
            rows,
            offsets,
            pairs,
        };
        let mut scanned = vec![None; count];
        let mut start = 0;
        for row in 0..rows as usize {
            let end = usize::try_from(u64_at(data, offsets + 8 * (row + 1))?)
                .map_err(|_| invalid("pair offset exceeds address space"))?;
            if end < start || end > total {
                return Err(invalid("pair offsets are not monotone"));
            }
            let mut previous = None;
            for pair in start..end {
                let (key, value) = reader.pair(data, pair);
                if key as usize >= count || previous.is_some_and(|previous| key <= previous) {
                    return Err(invalid("pair keys are invalid or not strictly ordered"));
                }
                include(&mut scanned[key as usize], value);
                previous = Some(key);
            }
            start = end;
        }
        if scanned != reader.bounds {
            return Err(invalid("bounds disagree with entry values"));
        }
        Ok(reader)
    }
    fn pair_range(&self, data: &[u8], row: u32) -> Option<(usize, usize)> {
        if row >= self.rows {
            return None;
        }
        Some((
            u64_at(data, self.offsets + 8 * row as usize).ok()? as usize,
            u64_at(data, self.offsets + 8 * (row as usize + 1)).ok()? as usize,
        ))
    }
    fn pair(&self, data: &[u8], pair: usize) -> (u32, T) {
        let at = self.pairs + 12 * pair;
        (
            u32::from_le_bytes(data[at..at + 4].try_into().unwrap()),
            T::from_bits(u64::from_le_bytes(
                data[at + 4..at + 12].try_into().unwrap(),
            )),
        )
    }
    pub fn key_ord(&self, key: &str) -> Option<u32> {
        self.keys
            .binary_search_by(|stored| stored.as_str().cmp(key))
            .ok()
            .map(|i| i as u32)
    }
    pub fn bounds(&self, key: u32) -> Option<(T, T)> {
        self.bounds.get(key as usize).copied().flatten()
    }
    pub fn value(&self, data: &[u8], key: u32, row: u32) -> Option<T> {
        if key as usize >= self.keys.len() {
            return None;
        }
        let (mut start, mut end) = self.pair_range(data, row)?;
        while start < end {
            let mid = start + (end - start) / 2;
            let (found, value) = self.pair(data, mid);
            match found.cmp(&key) {
                std::cmp::Ordering::Equal => return Some(value),
                std::cmp::Ordering::Less => start = mid + 1,
                std::cmp::Ordering::Greater => end = mid,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corrupt_sections_fail_without_panicking() {
        let mut store = Store::<i64>::new("m");
        store.set(0, "b", i64::MIN).unwrap();
        store.set(0, "a", i64::MAX).unwrap();
        store.set(2, "a", 0).unwrap();
        let mut bytes = Vec::new();
        store.write(&mut bytes, 3).unwrap();
        let reader = Reader::<i64>::open(&bytes, 3).unwrap();
        for end in 0..bytes.len() {
            assert!(
                Reader::<i64>::open(&bytes[..end], 3).is_err(),
                "truncation {end}"
            );
        }
        let mut corruptions = vec![
            (0, 2u32.to_le_bytes().to_vec()),
            (4, 4u32.to_le_bytes().to_vec()),
            (8, u32::MAX.to_le_bytes().to_vec()),
            (12, u32::MAX.to_le_bytes().to_vec()),
            (16, vec![0xff]),
            (17, vec![2]),
            (18, 1u64.to_le_bytes().to_vec()),
            (38, vec![b'a']),
            (reader.offsets, 1u64.to_le_bytes().to_vec()),
            (reader.offsets + 8, u64::MAX.to_le_bytes().to_vec()),
            (reader.offsets + 16, 1u64.to_le_bytes().to_vec()),
            (reader.pairs, 99u32.to_le_bytes().to_vec()),
            (reader.pairs + 12, 0u32.to_le_bytes().to_vec()),
            (reader.pairs + 4, 8u64.to_le_bytes().to_vec()),
        ];
        // A zero presence flag cannot conceal nonzero bounds.
        corruptions.push((17, vec![0]));
        for (at, replacement) in corruptions {
            let mut corrupt = bytes.clone();
            corrupt[at..at + replacement.len()].copy_from_slice(&replacement);
            assert!(
                Reader::<i64>::open(&corrupt, 3).is_err(),
                "corruption at {at}"
            );
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(Reader::<i64>::open(&extra, 3).is_err());
        assert!(store.encoded_len(2).is_err());
    }
}
