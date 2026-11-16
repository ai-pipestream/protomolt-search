use super::*;

#[test]
fn a_part_bitmap_lands_at_its_base_and_ranges_clear() {
    let part: Vec<bool> = (0..150).map(|i| i % 3 == 0 || i == 149).collect();
    let words = {
        let mut words = Bits::from_bools(&part).words;
        // Stray bits past the part's rows must not leak.
        *words.last_mut().unwrap() |= u64::MAX << (150 % 64);
        words
    };
    for (len, base) in [
        (400, 0),
        (400, 1),
        (400, 64),
        (400, 77),
        (200, 77),
        (100, 90),
        (64, 64),
    ] {
        let mut bits = Bits::empty(len);
        if len > 3 {
            bits.set(3);
        }
        bits.or_words_at(&words, 150, base);
        let want: Vec<usize> = (0..len)
            .filter(|&s| s == 3 || (s >= base && s - base < 150 && part[s - base]))
            .collect();
        assert_eq!(
            bits.iter().collect::<Vec<_>>(),
            want,
            "len {len} base {base}"
        );
    }
    let mut bits = Bits::full(300);
    bits.clear_range(5, 5);
    bits.clear_range(60, 200);
    bits.clear_range(290, 999);
    let want: Vec<usize> = (0..300)
        .filter(|s| !(60..200).contains(s) && *s < 290)
        .collect();
    assert_eq!(bits.iter().collect::<Vec<_>>(), want);
    bits.retain(|slot| slot % 2 == 0);
    assert!(bits.iter().all(|slot| slot % 2 == 0));
    assert_eq!(
        bits.count() as usize,
        want.iter().filter(|s| *s % 2 == 0).count()
    );
}

#[test]
fn slices_and_words_keep_the_slots() {
    let bools: Vec<bool> = (0..300).map(|i| i % 7 == 0 || i == 63 || i == 64).collect();
    let bits = Bits::from_bools(&bools);
    for (start, end) in [
        (0, 300),
        (0, 0),
        (1, 64),
        (63, 65),
        (64, 128),
        (130, 299),
        (290, 400),
    ] {
        let part = bits.slice(start, end);
        let end = end.min(300);
        assert_eq!(
            part.to_bools(),
            bools[start.min(end)..end],
            "{start}..{end}"
        );
    }
    // Stray bits past the length and a short or long word list.
    let words = vec![u64::MAX; 2];
    let made = Bits::from_words(words, 70);
    assert_eq!(made, Bits::full(70));
    assert_eq!(
        Bits::from_words(vec![1], 130).iter().collect::<Vec<_>>(),
        [0]
    );
    assert_eq!(
        Bits::from_words(vec![1, 2, 4], 64),
        Bits::from_words(vec![1], 64)
    );
}

fn of(len: usize, slots: &[usize]) -> Bits {
    let mut b = Bits::empty(len);
    for &s in slots {
        b.set(s);
    }
    b
}

#[test]
fn full_and_live_trim_past_the_length() {
    let full = Bits::full(70);
    assert_eq!(full.count(), 70);
    assert!(!full.test(70));
    let live = Bits::live(Some(&[1 << 3, 1 << 5]), 70);
    assert_eq!(live.count(), 68);
    assert!(!live.test(3) && !live.test(69));
    assert!(live.test(4) && live.test(64));
    assert_eq!(live.iter().take(4).collect::<Vec<_>>(), vec![0, 1, 2, 4]);
}

#[test]
fn at_least_counts_across_planes() {
    let a = of(130, &[1, 2, 3, 64, 129]);
    let b = of(130, &[2, 3, 64, 100]);
    let c = of(130, &[3, 64, 129]);
    let d = of(130, &[3, 5]);
    let sets = [&a, &b, &c, &d];
    let two = Bits::at_least(&sets, 2, 130);
    assert_eq!(two.iter().collect::<Vec<_>>(), vec![2, 3, 64, 129]);
    let three = Bits::at_least(&sets, 3, 130);
    assert_eq!(three.iter().collect::<Vec<_>>(), vec![3, 64]);
    let four = Bits::at_least(&sets, 4, 130);
    assert_eq!(four.iter().collect::<Vec<_>>(), vec![3]);
    assert_eq!(Bits::at_least(&sets, 5, 130).count(), 0);
    assert_eq!(Bits::at_least(&sets, 0, 130).count(), 130);
    assert_eq!(
        Bits::at_least(&sets, 1, 130).iter().collect::<Vec<_>>(),
        vec![1, 2, 3, 5, 64, 100, 129]
    );
}

#[test]
fn count_range_is_the_count_of_iter_range() {
    let bits = Bits::from_bools(
        &(0..300)
            .map(|i| i % 7 == 0 || i % 64 == 63)
            .collect::<Vec<_>>(),
    );
    for (start, end) in [
        (0, 0),
        (0, 300),
        (5, 5),
        (63, 64),
        (63, 129),
        (64, 128),
        (1, 299),
        (290, 400),
        (400, 500),
    ] {
        assert_eq!(
            bits.count_range(start, end),
            bits.iter_range(start, end).count(),
            "{start}..{end}"
        );
    }
}

#[test]
fn iter_range_walks_a_span_wordwise() {
    let bits = of(200, &[0, 3, 63, 64, 65, 129, 199]);
    assert_eq!(
        bits.iter_range(0, 200).collect::<Vec<_>>(),
        vec![0, 3, 63, 64, 65, 129, 199]
    );
    assert_eq!(
        bits.iter_range(60, 130).collect::<Vec<_>>(),
        vec![63, 64, 65, 129]
    );
    assert_eq!(
        bits.iter_range(4, 63).collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(
        bits.iter_range(64, 64).collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(bits.iter_range(199, 400).collect::<Vec<_>>(), vec![199]);
    assert_eq!(
        bits.iter_range(200, 400).collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(
        Bits::empty(10).iter_range(0, 10).collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
}

#[test]
fn ranges_mark_the_rows_inside_each_range_within_the_length() {
    let bits = Bits::ranges(200, &[(0, 3), (64, 2), (190, 20)]);
    assert_eq!(
        bits.iter().collect::<Vec<_>>(),
        (0..3).chain(64..66).chain(190..200).collect::<Vec<_>>()
    );
    assert_eq!(Bits::ranges(10, &[]).count(), 0);
    assert_eq!(Bits::ranges(10, &[(20, 5)]).count(), 0);
}

#[test]
fn the_group_rule_follows_the_planner() {
    let live = Bits::live(Some(&[1 << 9]), 12);
    let a = of(12, &[1, 2, 3, 9]);
    let b = of(12, &[2, 3, 4]);
    // The rows with a vector, as a dense clause resolves.
    let vectors = of(12, &[0, 1, 2, 3, 4, 5]);
    // MUST intersects; a dense clause is one more required set.
    let (m, seeded) = group_members(&[a.clone(), vectors.clone(), b.clone()], &[], &[], 0, &live);
    assert_eq!(m.iter().collect::<Vec<_>>(), vec![2, 3]);
    assert!(!seeded);
    // A lone dense MUST: the rows with a vector, deleted rows included
    // only through the live seed, which this shape does not consult.
    let (m, seeded) = group_members(std::slice::from_ref(&vectors), &[], &[], 0, &live);
    assert_eq!(m.iter().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
    assert!(!seeded);
    // Only MUST_NOT: the live seed minus the clause.
    let (m, seeded) = group_members(&[], &[], std::slice::from_ref(&vectors), 0, &live);
    assert_eq!(m.iter().collect::<Vec<_>>(), vec![6, 7, 8, 10, 11]);
    assert!(seeded);
    // SHOULD count with a dense SHOULD counting for the vector rows.
    let (m, seeded) = group_members(&[], &[a.clone(), b.clone(), vectors.clone()], &[], 2, &live);
    assert_eq!(m.iter().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    assert!(!seeded);
    let (m, _) = group_members(&[], &[a.clone(), b.clone()], &[], 2, &live);
    assert_eq!(m.iter().collect::<Vec<_>>(), vec![2, 3]);
    // A MUST with an optional dense SHOULD keeps the rows without a
    // vector: the minimum is zero, so the SHOULD admits nothing away.
    let (m, _) = group_members(
        std::slice::from_ref(&a),
        std::slice::from_ref(&vectors),
        &[],
        0,
        &live,
    );
    assert_eq!(m.iter().collect::<Vec<_>>(), vec![1, 2, 3, 9]);
    // MUST plus a SHOULD minimum, then MUST_NOT.
    let (m, _) = group_members(
        std::slice::from_ref(&a),
        std::slice::from_ref(&b),
        &[of(12, &[3])],
        1,
        &live,
    );
    assert_eq!(m.iter().collect::<Vec<_>>(), vec![2]);
    // MUST_NOT dense: the members without a vector remain.
    let (m, _) = group_members(
        std::slice::from_ref(&a),
        &[],
        std::slice::from_ref(&vectors),
        0,
        &live,
    );
    assert_eq!(m.iter().collect::<Vec<_>>(), vec![9]);
}
