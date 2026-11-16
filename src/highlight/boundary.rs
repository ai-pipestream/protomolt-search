//! Character boundaries of a stored text and the whitespace-snapped
//! window cut over them.

use super::HighlightError;

/// UTF-8 character-boundary map for one text: the boundaries of every
/// scalar, so a persisted UTF-8 byte offset is validated on a character
/// boundary and window edges snap cleanly across characters.
pub(super) struct CharBoundaryMap {
    /// `(byte_u32, byte)` at every character start, plus the end.
    boundaries: Vec<(u32, usize)>,
}

impl CharBoundaryMap {
    pub(super) fn new(text: &str) -> CharBoundaryMap {
        let mut boundaries = Vec::with_capacity(text.len() + 1);
        for (byte, _) in text.char_indices() {
            boundaries.push((byte as u32, byte));
        }
        boundaries.push((text.len() as u32, text.len()));
        CharBoundaryMap { boundaries }
    }

    pub(super) fn len_bytes(&self) -> u32 {
        self.boundaries.last().map_or(0, |b| b.0)
    }

    pub(super) fn byte_at(&self, offset: u32) -> Result<usize, HighlightError> {
        match self.boundaries.binary_search_by_key(&offset, |b| b.0) {
            Ok(i) => Ok(self.boundaries[i].1),
            Err(_) => Err(HighlightError::internal(format!(
                "highlight: UTF-8 byte offset {offset} is not a character boundary of the stored \
                 text (a multi-byte scalar is split, or the span outruns the text)"
            ))),
        }
    }

    /// The character-boundary index of `offset`, when it is one.
    fn index_of(&self, offset: u32) -> Option<usize> {
        self.boundaries.binary_search_by_key(&offset, |b| b.0).ok()
    }

    /// The first character-boundary index at or after `offset`.
    fn ceil_index_of(&self, offset: u32) -> Option<usize> {
        match self.boundaries.binary_search_by_key(&offset, |b| b.0) {
            Ok(i) => Some(i),
            Err(i) if i < self.boundaries.len() => Some(i),
            Err(_) => None,
        }
    }

    /// The last character-boundary index at or before `offset`.
    fn floor_index_of(&self, offset: u32) -> Option<usize> {
        match self.boundaries.binary_search_by_key(&offset, |b| b.0) {
            Ok(i) => Some(i),
            Err(i) if i > 0 => Some(i - 1),
            Err(_) => None,
        }
    }
}

/// Whether the character starting at boundary `i` is whitespace.
fn is_space_at(text: &str, map: &CharBoundaryMap, i: usize) -> bool {
    let byte = map.boundaries[i].1;
    text[byte..].chars().next().is_some_and(char::is_whitespace)
}

/// A window of at most `max_chars` UTF-8 bytes inside `bounds` around
/// `anchor` (the first highlight), edges snapped outward-to-inward onto
/// whitespace so no token is cut. When the snap would exclude the
/// anchor, the window becomes the anchor's whitespace-delimited run.
pub(super) fn window(
    text: &str,
    map: &CharBoundaryMap,
    bounds: (u32, u32),
    anchor: (u32, u32),
    max_chars: u32,
) -> Result<(u32, u32), HighlightError> {
    let (lo, hi) = bounds;
    // Lead with a third of the budget before the anchor, the rest after.
    let lead = max_chars / 3;
    let mut start = anchor.0.saturating_sub(lead).max(lo);
    let mut end = start.saturating_add(max_chars).min(hi);
    if end - start < max_chars {
        start = end.saturating_sub(max_chars).max(lo);
    }
    // Snap inward: the start moves forward to the first character after
    // a whitespace (or stays at the bound); the end moves back to the
    // last whitespace (or stays at the bound).
    let mut si = map.ceil_index_of(start).ok_or_else(|| {
        HighlightError::internal("highlight: window start off a character boundary")
    })?;
    if start != lo {
        while si + 1 < map.boundaries.len()
            && map.boundaries[si].0 < hi
            && !is_space_at(text, map, si)
        {
            si += 1;
        }
        // `si` sits on a whitespace character: the token starts after it.
        while si + 1 < map.boundaries.len()
            && map.boundaries[si].0 < hi
            && is_space_at(text, map, si)
        {
            si += 1;
        }
    }
    let mut ei = map.floor_index_of(end).ok_or_else(|| {
        HighlightError::internal("highlight: window end off a character boundary")
    })?;
    if end != hi {
        // If the end lands mid-token (the character before it and the
        // character at it are both non-space), back up to whitespace.
        while ei > si && !is_space_at(text, map, ei) && ei > 0 && !is_space_at(text, map, ei - 1) {
            ei -= 1;
        }
        // Trim trailing whitespace inside the window.
        while ei > si && is_space_at(text, map, ei - 1) {
            ei -= 1;
        }
    }
    start = map.boundaries[si].0;
    end = map.boundaries[ei].0;
    if start <= anchor.0 && anchor.1 <= end {
        return Ok((start, end));
    }
    // The anchor did not survive the snap: take its own run instead.
    let mut ai = map
        .index_of(anchor.0)
        .ok_or_else(|| HighlightError::internal("highlight: anchor off a character boundary"))?;
    while ai > 0 && map.boundaries[ai - 1].0 >= lo && !is_space_at(text, map, ai - 1) {
        ai -= 1;
    }
    let mut bi = map.index_of(anchor.1).ok_or_else(|| {
        HighlightError::internal("highlight: anchor end off a character boundary")
    })?;
    while bi + 1 < map.boundaries.len() && map.boundaries[bi].0 < hi && !is_space_at(text, map, bi)
    {
        bi += 1;
    }
    Ok((map.boundaries[ai].0, map.boundaries[bi].0))
}
