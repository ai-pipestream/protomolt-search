//! The rows a walk has reached.

/// A reset fills the whole set once the walk dirtied at least one word in
/// this many: a fill streams through the words in order, where the list
/// writes them one at a time, scattered.
const FILL_ONE_IN: usize = 16;

/// One bit per row. A walk sets bits in few of the words, so the set lists
/// each word it dirties once and a reset zeroes only those.
#[derive(Default)]
pub(super) struct Visited {
    words: Vec<u64>,
    /// The nonzero words, each once, in `touched[..dirty]`. One slot longer
    /// than `words`, so marking a row can always write the next slot and
    /// then keep it only when the row's word was zero: no branch on what
    /// the walk has reached.
    touched: Vec<u32>,
    dirty: usize,
}

impl Visited {
    /// Forget every row and cover rows `0..n`.
    pub(super) fn reset(&mut self, n: usize) {
        if self.dirty * FILL_ONE_IN >= self.words.len() {
            self.words.fill(0);
        } else {
            for &word in &self.touched[..self.dirty] {
                self.words[word as usize] = 0;
            }
        }
        self.dirty = 0;
        let words = n.div_ceil(64);
        if self.words.len() < words {
            self.words.resize(words, 0);
        }
        if self.touched.len() <= self.words.len() {
            self.touched.resize(self.words.len() + 1, 0);
        }
    }

    /// Mark `node`; true when it was not marked.
    #[inline]
    pub(super) fn insert(&mut self, node: u32) -> bool {
        let at = node >> 6;
        let bit = 1u64 << (node & 63);
        let word = self.words[at as usize];
        self.touched[self.dirty] = at;
        self.dirty += usize::from(word == 0);
        self.words[at as usize] = word | bit;
        word & bit == 0
    }

    #[inline]
    pub(super) fn contains(&self, node: u32) -> bool {
        self.words[(node >> 6) as usize] & (1u64 << (node & 63)) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::{Visited, FILL_ONE_IN};

    #[test]
    fn a_word_is_listed_once_however_many_of_its_rows_are_marked() {
        let mut visited = Visited::default();
        visited.reset(6400);
        for round in 0..3 {
            for node in 64..128 {
                assert_eq!(visited.insert(node), round == 0);
            }
        }
        assert!(visited.insert(5));
        assert!(!visited.insert(5));
        assert_eq!(visited.touched[..visited.dirty], [1, 0]);
        visited.reset(6400);
        assert_eq!(visited.dirty, 0);
        assert!(visited.words.iter().all(|&word| word == 0));
        assert!(!visited.contains(5) && !visited.contains(127));
    }

    /// Run `walks` walks of `marks` rows each over 6,400 rows against a
    /// model, checking two thousand times that a reset left the set empty.
    fn walks_match_a_model(walks: usize, marks: usize) {
        let rows = 6400usize;
        let mut visited = Visited::default();
        let mut model = vec![false; rows];
        let mut marked = Vec::new();
        let mut state = 0x5EED_000Cu64;
        for walk in 0..walks {
            visited.reset(rows);
            if walk.is_multiple_of(walks / 2000) {
                assert!(visited.words.iter().all(|&word| word == 0), "walk {walk}");
            }
            for node in marked.drain(..) {
                model[node] = false;
            }
            for _ in 0..marks {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let node = (state >> 33) as usize % rows;
                let fresh = !std::mem::replace(&mut model[node], true);
                assert_eq!(visited.insert(node as u32), fresh, "walk {walk}");
                marked.push(node);
            }
            let probe = walk % rows;
            assert_eq!(visited.contains(probe as u32), model[probe], "walk {walk}");
            assert!(
                visited.dirty <= marks.min(visited.words.len()),
                "walk {walk}"
            );
        }
    }

    #[test]
    fn a_reset_forgets_every_row_across_millions_of_sparse_walks() {
        // Four words of a hundred: each reset zeroes the listed words.
        const { assert!(4 * FILL_ONE_IN < 100) };
        walks_match_a_model(2_000_000, 4);
    }

    #[test]
    fn a_reset_forgets_every_row_of_dense_walks() {
        // Hundreds of marks dirty nearly every word: each reset fills.
        walks_match_a_model(20_000, 600);
    }

    #[test]
    fn graphs_of_different_sizes_share_one_set() {
        let mut visited = Visited::default();
        visited.reset(0);
        assert!(visited.words.is_empty());
        // 65 rows need a second word for the last one.
        visited.reset(65);
        assert!(visited.insert(64) && visited.insert(0));
        // Every word is listed: marking more rows writes the spare slot
        // and lists nothing.
        assert!(visited.insert(1) && !visited.insert(64));
        assert_eq!(visited.touched[..visited.dirty], [1, 0]);
        visited.reset(10);
        assert!(!visited.contains(64) && !visited.contains(0));
        assert!(visited.insert(9));
        visited.reset(100_000);
        assert_eq!(visited.words.len(), 1563);
        assert_eq!(visited.touched.len(), 1564);
        assert!(!visited.contains(9));
        assert!(visited.insert(99_999) && visited.contains(99_999));
        visited.reset(65);
        assert!(!visited.contains(99_999));
        assert_eq!(visited.words.len(), 1563);
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn a_row_beyond_the_covered_words_is_refused() {
        let mut visited = Visited::default();
        visited.reset(64);
        visited.insert(64);
    }
}
