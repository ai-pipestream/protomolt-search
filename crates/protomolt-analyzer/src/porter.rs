// SPDX-License-Identifier: Apache-2.0
//
// This implementation follows Apache OpenNLP's classic PorterStemmer,
// derived from Martin Porter's Release 3 algorithm. The OpenNLP source is
// licensed under Apache License 2.0. See the repository NOTICE.

/// Classic, case-sensitive Porter stemming over one token.
pub(crate) fn stem(input: &str) -> String {
    // OpenNLP's Java implementation operates on `char[]`, so supplementary
    // scalars occupy two consonant code units inside the state machine.
    let mut word: Vec<u16> = input.encode_utf16().collect();
    if word.len() <= 2 {
        return input.to_string();
    }
    step1(&mut word);
    step2(&mut word);
    step3(&mut word);
    step4(&mut word);
    step5(&mut word);
    step6(&mut word);
    String::from_utf16(&word).expect("Porter suffix edits preserve valid UTF-16")
}

fn consonant(word: &[u16], index: usize) -> bool {
    match word[index] {
        0x0061 | 0x0065 | 0x0069 | 0x006F | 0x0075 => false,
        0x0079 => index == 0 || !consonant(word, index - 1),
        _ => true,
    }
}

fn measure(word: &[u16]) -> usize {
    let mut count = 0;
    let mut index = 0;
    while index < word.len() && consonant(word, index) {
        index += 1;
    }
    while index < word.len() {
        while index < word.len() && !consonant(word, index) {
            index += 1;
        }
        if index == word.len() {
            break;
        }
        count += 1;
        while index < word.len() && consonant(word, index) {
            index += 1;
        }
    }
    count
}

fn contains_vowel(word: &[u16]) -> bool {
    (0..word.len()).any(|index| !consonant(word, index))
}

fn double_consonant(word: &[u16]) -> bool {
    word.len() >= 2
        && word[word.len() - 1] == word[word.len() - 2]
        && consonant(word, word.len() - 1)
}

fn cvc(word: &[u16]) -> bool {
    if word.len() < 3 {
        return false;
    }
    let end = word.len() - 1;
    consonant(word, end)
        && !consonant(word, end - 1)
        && consonant(word, end - 2)
        && !matches!(word[end], 0x0077..=0x0079)
}

fn suffix_start(word: &[u16], suffix: &str) -> Option<usize> {
    let suffix: Vec<u16> = suffix.encode_utf16().collect();
    word.ends_with(&suffix).then(|| word.len() - suffix.len())
}

fn replace_suffix(word: &mut Vec<u16>, start: usize, replacement: &str) {
    word.truncate(start);
    word.extend(replacement.encode_utf16());
}

fn replace_if_measure(
    word: &mut Vec<u16>,
    suffix: &str,
    replacement: &str,
    minimum: usize,
) -> bool {
    let Some(start) = suffix_start(word, suffix) else {
        return false;
    };
    if measure(&word[..start]) >= minimum {
        replace_suffix(word, start, replacement);
    }
    true
}

fn step1(word: &mut Vec<u16>) {
    if let Some(start) = suffix_start(word, "sses") {
        replace_suffix(word, start, "ss");
    } else if let Some(start) = suffix_start(word, "ies") {
        replace_suffix(word, start, "i");
    } else if word.ends_with(&[b's' as u16]) && word.get(word.len() - 2) != Some(&(b's' as u16)) {
        word.pop();
    }

    if let Some(start) = suffix_start(word, "eed") {
        if measure(&word[..start]) > 0 {
            word.pop();
        }
        return;
    }

    let removable = suffix_start(word, "ed").or_else(|| suffix_start(word, "ing"));
    let Some(start) = removable else { return };
    if !contains_vowel(&word[..start]) {
        return;
    }
    word.truncate(start);
    if word.ends_with(&[b'a' as u16, b't' as u16])
        || word.ends_with(&[b'b' as u16, b'l' as u16])
        || word.ends_with(&[b'i' as u16, b'z' as u16])
    {
        word.push(b'e' as u16);
    } else if double_consonant(word) {
        if !matches!(word.last(), Some(0x006C | 0x0073 | 0x007A)) {
            word.pop();
        }
    } else if measure(word) == 1 && cvc(word) {
        word.push(b'e' as u16);
    }
}

fn step2(word: &mut [u16]) {
    if word.last() == Some(&(b'y' as u16)) && contains_vowel(&word[..word.len() - 1]) {
        let end = word.len() - 1;
        word[end] = b'i' as u16;
    }
}

fn step3(word: &mut Vec<u16>) {
    const RULES: &[(&str, &str)] = &[
        ("ational", "ate"),
        ("tional", "tion"),
        ("enci", "ence"),
        ("anci", "ance"),
        ("izer", "ize"),
        ("bli", "ble"),
        ("alli", "al"),
        ("entli", "ent"),
        ("eli", "e"),
        ("ousli", "ous"),
        ("ization", "ize"),
        ("ation", "ate"),
        ("ator", "ate"),
        ("alism", "al"),
        ("iveness", "ive"),
        ("fulness", "ful"),
        ("ousness", "ous"),
        ("aliti", "al"),
        ("iviti", "ive"),
        ("biliti", "ble"),
        ("logi", "log"),
    ];
    for &(suffix, replacement) in RULES {
        if replace_if_measure(word, suffix, replacement, 1) {
            return;
        }
    }
}

fn step4(word: &mut Vec<u16>) {
    const RULES: &[(&str, &str)] = &[
        ("icate", "ic"),
        ("ative", ""),
        ("alize", "al"),
        ("iciti", "ic"),
        ("ical", "ic"),
        ("ful", ""),
        ("ness", ""),
    ];
    for &(suffix, replacement) in RULES {
        if replace_if_measure(word, suffix, replacement, 1) {
            return;
        }
    }
}

fn step5(word: &mut Vec<u16>) {
    const SUFFIXES: &[&str] = &[
        "al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent", "ou",
        "ism", "ate", "iti", "ous", "ive", "ize",
    ];
    for &suffix in SUFFIXES {
        let Some(start) = suffix_start(word, suffix) else {
            continue;
        };
        if measure(&word[..start]) > 1 {
            word.truncate(start);
        }
        return;
    }
    if let Some(start) = suffix_start(word, "ion") {
        if start > 0 && matches!(word[start - 1], 0x0073 | 0x0074) && measure(&word[..start]) > 1 {
            word.truncate(start);
        }
    }
}

fn step6(word: &mut Vec<u16>) {
    let before_final_e = word.clone();
    if word.last() == Some(&(b'e' as u16)) {
        let stem = &word[..word.len() - 1];
        let m = measure(stem);
        if m > 1 || (m == 1 && !cvc(stem)) {
            word.pop();
        }
    }
    if word.last() == Some(&(b'l' as u16)) && double_consonant(word) {
        // OpenNLP's Release 3 implementation leaves its measure cursor at
        // the pre-e word. Preserve that edge case when an e was removed.
        let measured = if before_final_e.len() != word.len() {
            &before_final_e[..]
        } else {
            &word[..]
        };
        if measure(measured) > 1 {
            word.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::stem;

    #[test]
    fn matches_opennlp_examples() {
        for (input, expected) in [
            ("deny", "deni"),
            ("declining", "declin"),
            ("diversity", "divers"),
            ("divers", "diver"),
            ("dental", "dental"),
            ("likes", "like"),
            ("liked", "like"),
            ("likely", "like"),
            ("liking", "like"),
            ("this", "thi"),
        ] {
            assert_eq!(stem(input), expected, "{input}");
        }
    }

    #[test]
    fn porter_reference_examples() {
        for (input, expected) in [
            ("caresses", "caress"),
            ("ponies", "poni"),
            ("ties", "ti"),
            ("cats", "cat"),
            ("feed", "feed"),
            ("agreed", "agre"),
            ("disabled", "disabl"),
            ("matting", "mat"),
            ("mating", "mate"),
            ("meeting", "meet"),
            ("milling", "mill"),
            ("messing", "mess"),
            ("meetings", "meet"),
        ] {
            assert_eq!(stem(input), expected, "{input}");
        }
    }

    #[test]
    fn remains_case_sensitive_like_opennlp() {
        assert_eq!(stem("Running"), "Run");
        assert_eq!(stem("running"), "run");
    }

    #[test]
    fn supplementary_scalars_match_java_char_array_behavior() {
        assert_eq!(stem("ba😀ing"), "ba😀");
    }
}
