//! Unicode 16 character answers for persisted identity, on ICU4X 2.3.
//!
//! Term identity (protomolt-analyzer) and vector identity
//! (protomolt-embedder) are persisted, and both were built on ICU4X 2.0.0:
//! Unicode 16 general categories, scripts, Extended_Pictographic and
//! normalization, the tables JDK 25 gives OpenNLP, together with the
//! Unicode 17 case folds OpenNLP bundles. ICU4X 2.3.0 carries Unicode 17
//! data. This crate answers from ICU4X 2.3.0 and restores the Unicode 16
//! answer wherever the two releases differ, so both crates keep their
//! identities on the newer code. It is the only path by which they reach
//! ICU4X.
//!
//! `tools/unicode16-freeze` compares every scalar value's answers under
//! both releases and writes `tables.rs`. The differences are:
//!
//! - scalars unassigned in Unicode 16 and assigned in 17
//!   ([`is_new_in_unicode_17`]): Unassigned, script Unknown and outside
//!   normalization here, as Unicode 16 had them. Some are combining marks
//!   with a nonzero combining class in 17, which is why normalization
//!   passes them through instead of reordering marks around them;
//! - scalars whose Extended_Pictographic flag Unicode 17 removed;
//! - scalars assigned in Unicode 16 whose general category changed;
//! - the case folds of new scalars, which are Unicode 17 CaseFolding.txt
//!   and so the folds OpenNLP bundles: [`case_fold`] takes them as ICU4X
//!   2.3.0 has them.
//!
//! Decomposition mappings, canonical composition, combining classes and
//! scripts of every scalar assigned in Unicode 16 are the same in both
//! releases; the tool refuses to write tables if that stops being true.
//! The digest tests in protomolt-analyzer and protomolt-embedder compare
//! the complete result with ICU4X 2.0.0 for every scalar value.

use std::cmp::Ordering;

use icu_casemap::CaseMapper;
use icu_normalizer::{ComposingNormalizerBorrowed, DecomposingNormalizerBorrowed};
use icu_properties::props::ExtendedPictographic;
use icu_properties::{CodePointMapData, CodePointSetData};

pub use icu_properties::props::{GeneralCategory, Script};

mod tables;

fn in_ranges(ranges: &[(u32, u32)], ch: char) -> bool {
    let cp = ch as u32;
    ranges
        .binary_search_by(|&(start, end)| {
            if end < cp {
                Ordering::Less
            } else if start > cp {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        })
        .is_ok()
}

/// Whether `ch` was unassigned in Unicode 16 and is assigned in Unicode 17.
pub fn is_new_in_unicode_17(ch: char) -> bool {
    in_ranges(tables::NEW_IN_UNICODE_17, ch)
}

/// The Unicode 16 general category of `ch`.
pub fn general_category(ch: char) -> GeneralCategory {
    if is_new_in_unicode_17(ch) {
        return GeneralCategory::Unassigned;
    }
    if let Ok(at) =
        tables::GENERAL_CATEGORY_IN_UNICODE_16.binary_search_by_key(&(ch as u32), |&(cp, _)| cp)
    {
        return tables::GENERAL_CATEGORY_IN_UNICODE_16[at].1;
    }
    CodePointMapData::<GeneralCategory>::new().get(ch)
}

/// The Unicode 16 script of `ch`.
pub fn script(ch: char) -> Script {
    if is_new_in_unicode_17(ch) {
        return Script::Unknown;
    }
    CodePointMapData::<Script>::new().get(ch)
}

/// Whether `ch` has Extended_Pictographic in Unicode 16.
pub fn is_extended_pictographic(ch: char) -> bool {
    in_ranges(tables::EXTENDED_PICTOGRAPHIC_REMOVED_IN_UNICODE_17, ch)
        || CodePointSetData::new::<ExtendedPictographic>().contains(ch)
}

/// Normalize the text between scalars new in Unicode 17 and copy those
/// scalars through. In Unicode 16 each of them is an unassigned starter
/// with no decomposition and no composition, so it bounds mark reordering
/// and composition exactly as a segment boundary does.
fn by_unicode_16_segments(text: &str, normalize: impl Fn(&str) -> String) -> String {
    if !text.chars().any(is_new_in_unicode_17) {
        return normalize(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut start = 0;
    for (at, ch) in text.char_indices() {
        if is_new_in_unicode_17(ch) {
            out.push_str(&normalize(&text[start..at]));
            out.push(ch);
            start = at + ch.len_utf8();
        }
    }
    out.push_str(&normalize(&text[start..]));
    out
}

/// Unicode 16 canonical decomposition (NFD).
pub fn nfd(text: &str) -> String {
    let normalizer = DecomposingNormalizerBorrowed::new_nfd();
    by_unicode_16_segments(text, |segment| normalizer.normalize(segment).into_owned())
}

/// Unicode 16 canonical composition (NFC).
pub fn nfc(text: &str) -> String {
    let normalizer = ComposingNormalizerBorrowed::new_nfc();
    by_unicode_16_segments(text, |segment| normalizer.normalize(segment).into_owned())
}

/// Unicode full, non-Turkic case folding with the Unicode 17 CaseFolding.txt
/// mappings OpenNLP bundles.
pub fn case_fold(text: &str) -> String {
    CaseMapper::new().fold_string(text).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalars(ranges: &[(u32, u32)]) -> usize {
        ranges
            .iter()
            .map(|&(start, end)| (end - start + 1) as usize)
            .sum()
    }

    fn sorted_and_disjoint(ranges: &[(u32, u32)]) -> bool {
        ranges.iter().all(|&(start, end)| start <= end)
            && ranges.windows(2).all(|pair| pair[0].1 + 1 < pair[1].0)
    }

    #[test]
    fn the_tables_are_sorted_disjoint_and_the_sizes_the_comparison_found() {
        assert!(sorted_and_disjoint(tables::NEW_IN_UNICODE_17));
        assert!(sorted_and_disjoint(
            tables::EXTENDED_PICTOGRAPHIC_REMOVED_IN_UNICODE_17
        ));
        assert!(tables::GENERAL_CATEGORY_IN_UNICODE_16
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0));
        assert_eq!(scalars(tables::NEW_IN_UNICODE_17), 4_803);
        assert_eq!(
            scalars(tables::EXTENDED_PICTOGRAPHIC_REMOVED_IN_UNICODE_17),
            689
        );
        assert_eq!(
            tables::GENERAL_CATEGORY_IN_UNICODE_16,
            &[(0x0295, GeneralCategory::LowercaseLetter)]
        );
    }

    /// The tables describe the release this crate links: each entry is a
    /// difference the linked ICU4X data really has. A different release
    /// fails here and needs `tools/unicode16-freeze/run.sh`.
    #[test]
    fn every_table_entry_is_a_difference_in_the_linked_icu4x_data() {
        let categories = CodePointMapData::<GeneralCategory>::new();
        let pictographic = CodePointSetData::new::<ExtendedPictographic>();
        for &(start, end) in tables::NEW_IN_UNICODE_17 {
            for ch in (start..=end).filter_map(char::from_u32) {
                assert_ne!(categories.get(ch), GeneralCategory::Unassigned, "{ch:?}");
            }
        }
        for &(start, end) in tables::EXTENDED_PICTOGRAPHIC_REMOVED_IN_UNICODE_17 {
            for ch in (start..=end).filter_map(char::from_u32) {
                assert!(!pictographic.contains(ch), "{ch:?}");
            }
        }
        for &(cp, category) in tables::GENERAL_CATEGORY_IN_UNICODE_16 {
            let ch = char::from_u32(cp).unwrap();
            assert_ne!(categories.get(ch), category, "{ch:?}");
        }
    }

    #[test]
    fn a_black_star_and_a_ballot_box_stay_pictographic() {
        assert!(is_extended_pictographic('\u{2605}'));
        assert!(is_extended_pictographic('\u{2610}'));
        assert!(is_extended_pictographic('\u{1F600}'));
        assert!(!is_extended_pictographic('a'));
    }

    #[test]
    fn a_letter_whose_category_changed_keeps_its_unicode_16_category() {
        assert_eq!(
            general_category('\u{0295}'),
            GeneralCategory::LowercaseLetter
        );
        assert_eq!(general_category('a'), GeneralCategory::LowercaseLetter);
    }

    #[test]
    fn a_scalar_new_in_unicode_17_is_unassigned_with_an_unknown_script() {
        // U+A7CE LATIN CAPITAL LETTER PINWHEEL S, new in Unicode 17.
        assert!(is_new_in_unicode_17('\u{A7CE}'));
        assert_eq!(general_category('\u{A7CE}'), GeneralCategory::Unassigned);
        assert_eq!(script('\u{A7CE}'), Script::Unknown);
        assert_eq!(script('a'), Script::Latin);
    }

    #[test]
    fn a_mark_new_in_unicode_17_bounds_reordering_as_an_unassigned_starter() {
        // U+1ACF is a class 230 mark in Unicode 17 and unassigned in 16;
        // U+0323 is class 220. Unicode 17 would reorder the two marks.
        let text = "a\u{1ACF}\u{0323}";
        assert_eq!(nfd(text), text);
        assert_eq!(nfc(text), text);
        // Without the new scalar the marks are reordered and composed.
        assert_eq!(nfd("a\u{0301}\u{0323}"), "a\u{0323}\u{0301}");
        assert_eq!(nfc("a\u{0323}"), "\u{1EA1}");
        assert_eq!(nfc("\u{1EA1}\u{1ACF}a\u{0301}"), "\u{1EA1}\u{1ACF}\u{00E1}");
    }

    #[test]
    fn case_folding_includes_the_unicode_17_folds_opennlp_bundles() {
        assert_eq!(case_fold("\u{A7CE}"), "\u{A7CF}");
        assert_eq!(case_fold("\u{16EA0}"), "\u{16EBB}");
        assert_eq!(case_fold("Stra\u{00DF}e"), "strasse");
    }
}
