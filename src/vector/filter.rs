//! Filter routing, filter regimes, and the row masks a search runs under.

use std::borrow::Cow;
use std::ops::ControlFlow;

use crate::boolean_bits::Bits;

/// What a provider that chooses the filter regime itself optimizes for.
/// A provider with one way to filter ignores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FilterRouting {
    /// The fastest regime at the request's beam: a mask is enumerated only
    /// where that is cheaper than any walk, so a mask that could have been
    /// enumerated may get an approximate answer.
    #[default]
    Speed,
    /// Every mask of at most the enumerate threshold is enumerated, so it
    /// gets the exact answer; larger masks are routed as under `Speed`.
    Exact,
}

impl FilterRouting {
    pub fn name(self) -> &'static str {
        match self {
            FilterRouting::Speed => "speed",
            FilterRouting::Exact => "exact",
        }
    }
}

/// How a provider that can steer its search by the mask answers a filtered
/// query. A provider with one way to filter ignores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterRegime {
    /// Score every allowed row: exact over the mask, linear in the allowed
    /// rows.
    Enumerate,
    /// Walk the graph through the allowed rows only.
    FilterAware,
    /// Walk the graph as if unfiltered; the mask gates results.
    Plain,
}

impl FilterRegime {
    pub fn name(self) -> &'static str {
        match self {
            FilterRegime::Enumerate => "enumerate",
            FilterRegime::FilterAware => "filter_aware",
            FilterRegime::Plain => "plain",
        }
    }
}

/// The rows a search may return, one flag per row of the index, in the
/// shape the caller holds. The scan kernel masks with one byte per row; a
/// graph walk tests single rows and counts and lists the set ones, which
/// a bitmap does a word at a time. A provider takes its own shape as it
/// is and converts the other once per search; the chunked scan reads a
/// bitmap one chunk at a time ([`Mask::any_in`], [`Mask::for_each_in`])
/// and never expands it to a byte per row of the index.
#[derive(Debug, Clone, Copy)]
pub enum Mask<'a> {
    Bools(&'a [bool]),
    Bits(&'a Bits),
    /// Every row in `0..len` is allowed without materializing a flag buffer.
    All(usize),
}

impl<'a> Mask<'a> {
    /// Rows covered, allowed or not.
    pub fn len(&self) -> usize {
        match self {
            Mask::Bools(allow) => allow.len(),
            Mask::Bits(bits) => bits.len(),
            Mask::All(len) => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn test(&self, row: usize) -> bool {
        match self {
            Mask::Bools(allow) => allow[row],
            Mask::Bits(bits) => bits.test(row),
            Mask::All(len) => {
                assert!(row < *len, "mask row {row} out of bounds for len {len}");
                true
            }
        }
    }

    /// Allowed rows.
    pub fn count(&self) -> usize {
        match self {
            Mask::Bools(allow) => allow.iter().filter(|&&on| on).count(),
            Mask::Bits(bits) => bits.count() as usize,
            Mask::All(len) => *len,
        }
    }

    /// The allowed rows ascending, until `each` breaks.
    pub fn try_for_each<B>(&self, mut each: impl FnMut(usize) -> ControlFlow<B>) -> ControlFlow<B> {
        match self {
            Mask::Bools(allow) => {
                for (row, &on) in allow.iter().enumerate() {
                    if on {
                        each(row)?;
                    }
                }
                ControlFlow::Continue(())
            }
            Mask::Bits(bits) => bits.iter().try_for_each(each),
            Mask::All(len) => {
                for row in 0..*len {
                    each(row)?;
                }
                ControlFlow::Continue(())
            }
        }
    }

    /// Whether any row of `[start, end)` is allowed. A bitmap answers a
    /// word at a time, so a span the mask leaves empty costs its words.
    pub fn any_in(&self, start: usize, end: usize) -> bool {
        let end = end.min(self.len());
        let start = start.min(end);
        match self {
            Mask::Bools(allow) => allow[start..end].iter().any(|&on| on),
            Mask::Bits(bits) => bits.count_range(start, end) > 0,
            Mask::All(_) => start < end,
        }
    }

    /// The allowed rows of `[start, end)` ascending, read from the mask in
    /// the shape it is held in: no flag buffer is built for the span.
    pub fn for_each_in(&self, start: usize, end: usize, mut each: impl FnMut(usize)) {
        let end = end.min(self.len());
        let start = start.min(end);
        match self {
            Mask::Bools(allow) => {
                for (offset, &on) in allow[start..end].iter().enumerate() {
                    if on {
                        each(start + offset);
                    }
                }
            }
            Mask::Bits(bits) => bits.iter_range(start, end).for_each(each),
            Mask::All(_) => (start..end).for_each(each),
        }
    }

    /// The rows of `[start, end)` as their own mask, row `start` at 0, or
    /// `None` when the span allows nothing.
    pub fn slice(&self, start: usize, end: usize) -> Option<OwnedMask> {
        let end = end.min(self.len());
        let start = start.min(end);
        match self {
            Mask::Bools(allow) => {
                let part = &allow[start..end];
                part.iter()
                    .any(|&on| on)
                    .then(|| OwnedMask::Bools(part.to_vec()))
            }
            Mask::Bits(bits) => {
                let part = bits.slice(start, end);
                (part.count() > 0).then_some(OwnedMask::Bits(part))
            }
            Mask::All(_) => (start < end).then_some(OwnedMask::All(end - start)),
        }
    }

    /// One byte per row, the shape the scan kernel masks with.
    pub fn bools(&self) -> Cow<'a, [bool]> {
        match self {
            Mask::Bools(allow) => Cow::Borrowed(allow),
            Mask::Bits(bits) => Cow::Owned(bits.to_bools()),
            Mask::All(len) => Cow::Owned(vec![true; *len]),
        }
    }
}

/// A mask that owns its flags (one part of a segmented search).
#[derive(Debug, Clone)]
pub enum OwnedMask {
    Bools(Vec<bool>),
    Bits(Bits),
    All(usize),
}

impl OwnedMask {
    pub fn as_mask(&self) -> Mask<'_> {
        match self {
            OwnedMask::Bools(allow) => Mask::Bools(allow),
            OwnedMask::Bits(bits) => Mask::Bits(bits),
            OwnedMask::All(len) => Mask::All(*len),
        }
    }
}
