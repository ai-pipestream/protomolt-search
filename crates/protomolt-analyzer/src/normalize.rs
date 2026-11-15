//! Normalizer steps: invisible stripping, whitespace, accents, case folding.

use super::*;

/// OpenNLP's builder applies a fixed semantic order independent of the order
/// in which enum values appear on the wire. Duplicate values are idempotent.
pub(crate) fn normalize(token: &str, steps: &[NormalizerStep]) -> String {
    let has = |step| steps.contains(&step);
    let mut value = token.to_string();
    if has(NormalizerStep::StripInvisible) {
        value = strip_invisible(&value);
    }
    if has(NormalizerStep::Whitespace) {
        value = normalize_whitespace(&value);
    }
    if has(NormalizerStep::AccentFold) {
        value = accent_fold(&value);
    }
    if has(NormalizerStep::FullCaseFold) {
        value = full_case_fold(&value);
    }
    value
}

pub(crate) fn full_case_fold(text: &str) -> String {
    CaseMapper::new().fold_string(text).into_owned()
}

/// Unicode full, non-Turkic case folding used by persisted product identities.
pub fn unicode_case_fold(text: &str) -> String {
    full_case_fold(text)
}

/// Run the normalizer chain alone over one token: what a term prefix
/// must go through to be compared against the dictionary of a field
/// whose identity is normalized tokens or normalized stems. Stemming is
/// deliberately not applied — a prefix of a stem is what the dictionary
/// holds, and stemming a fragment would change the fragment.
pub fn normalize_term(token: &str, steps: &[NormalizerStep]) -> String {
    normalize(token, steps)
}

pub(crate) fn is_unicode_whitespace(ch: char) -> bool {
    matches!(
        ch as u32,
        0x0009..=0x000D
            | 0x0020
            | 0x0085
            | 0x00A0
            | 0x1680
            | 0x2000..=0x200A
            | 0x2028
            | 0x2029
            | 0x202F
            | 0x205F
            | 0x3000
    )
}

pub(crate) fn strip_invisible(text: &str) -> String {
    text.chars()
        .filter(|ch| {
            !matches!(
                *ch as u32,
                0x00AD
                    | 0x061C
                    | 0x200B
                    | 0x200E
                    | 0x200F
                    | 0x202A..=0x202E
                    | 0x2060..=0x2064
                    | 0x2066..=0x2069
                    | 0xFEFF
            )
        })
        .collect()
}

pub(crate) fn normalize_whitespace(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_space = false;
    for ch in text.chars() {
        if is_unicode_whitespace(ch) {
            pending_space = !out.is_empty();
        } else {
            if pending_space {
                out.push(' ');
                pending_space = false;
            }
            out.push(ch);
        }
    }
    out
}

pub(crate) fn accent_fold(text: &str) -> String {
    let decomposed = DecomposingNormalizerBorrowed::new_nfd()
        .normalize(text)
        .into_owned();
    let categories = CodePointMapData::<GeneralCategory>::new();
    let scripts = CodePointMapData::<Script>::new();
    let mut out = String::with_capacity(decomposed.len());
    let mut base_script = None;

    for ch in decomposed.chars() {
        if categories.get(ch) == GeneralCategory::NonspacingMark {
            if !base_script.is_some_and(is_folded_script) {
                out.push(ch);
            }
            continue;
        }
        if let Some(mapped) = stroke_letter(ch) {
            out.push_str(mapped);
            base_script = Some(Script::Latin);
        } else {
            out.push(ch);
            base_script = Some(scripts.get(ch));
        }
    }
    ComposingNormalizerBorrowed::new_nfc()
        .normalize(&out)
        .into_owned()
}

pub(crate) fn is_folded_script(script: Script) -> bool {
    matches!(script, Script::Latin | Script::Greek | Script::Cyrillic)
}

pub(crate) fn stroke_letter(ch: char) -> Option<&'static str> {
    Some(match ch {
        '\u{00F8}' => "o",
        '\u{00D8}' => "O",
        '\u{00E6}' => "ae",
        '\u{00C6}' => "AE",
        '\u{0153}' => "oe",
        '\u{0152}' => "OE",
        '\u{00DF}' => "ss",
        '\u{1E9E}' => "SS",
        '\u{00FE}' => "th",
        '\u{00DE}' => "TH",
        '\u{00F0}' => "d",
        '\u{00D0}' => "D",
        '\u{0111}' => "d",
        '\u{0110}' => "D",
        '\u{0142}' => "l",
        '\u{0141}' => "L",
        '\u{0127}' => "h",
        '\u{0126}' => "H",
        '\u{0131}' => "i",
        _ => return None,
    })
}
