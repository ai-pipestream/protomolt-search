//! Little-endian primitives shared by the session and graph codecs.

use tonic::Status;

use super::{WireSpan, WireTermVector};

pub(super) fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(super) fn put_i32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(super) fn put_f64(out: &mut Vec<u8>, v: f64) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(super) fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}

pub(super) fn put_span(out: &mut Vec<u8>, span: Option<WireSpan>) {
    match span {
        None => out.push(0),
        Some(s) => {
            out.push(1);
            put_i32(out, s.start);
            put_i32(out, s.end);
        }
    }
}

pub(super) fn encode_term_vectors(out: &mut Vec<u8>, tvs: &[WireTermVector]) {
    put_u32(out, tvs.len() as u32);
    for tv in tvs {
        put_str(out, &tv.term);
        put_i32(out, tv.frequency);
        put_u32(out, tv.occurrences.len() as u32);
        for occ in &tv.occurrences {
            put_i32(out, occ.start);
            put_i32(out, occ.end);
        }
    }
}

pub(super) fn decode_term_vectors(r: &mut Reader<'_>) -> Result<Vec<WireTermVector>, Status> {
    let n = r.u32()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let term = r.string()?;
        let frequency = r.i32()?;
        let n_occ = r.u32()? as usize;
        let mut occurrences = Vec::with_capacity(n_occ);
        for _ in 0..n_occ {
            let start = r.i32()?;
            let end = r.i32()?;
            occurrences.push(WireSpan { start, end });
        }
        out.push(WireTermVector {
            term,
            frequency,
            occurrences,
        });
    }
    Ok(out)
}

pub(super) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(super) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub(super) fn take(&mut self, len: usize) -> Result<&'a [u8], Status> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| Status::internal("wire decode overflow"))?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| Status::internal("truncated wire payload"))?;
        self.pos = end;
        Ok(slice)
    }

    pub(super) fn u8(&mut self) -> Result<u8, Status> {
        Ok(self.take(1)?[0])
    }

    pub(super) fn u32(&mut self) -> Result<u32, Status> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub(super) fn i32(&mut self) -> Result<i32, Status> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub(super) fn f64(&mut self) -> Result<f64, Status> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub(super) fn string(&mut self) -> Result<String, Status> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|e| Status::internal(format!("invalid UTF-8 in wire payload: {e}")))
    }

    pub(super) fn span(&mut self) -> Result<Option<WireSpan>, Status> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let start = self.i32()?;
                let end = self.i32()?;
                Ok(Some(WireSpan { start, end }))
            }
            tag => Err(Status::internal(format!("invalid wire span tag {tag}"))),
        }
    }
}
