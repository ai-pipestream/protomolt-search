//! Archive blobs and the copy-on-write pages that hold rows and identities.

use super::*;

#[derive(Debug)]
pub(super) enum ArchiveBlob {
    Memory(Arc<[u8]>),
    Spill { offset: u64, length: u64 },
}

impl ArchiveBlob {
    pub(super) fn len(&self) -> u64 {
        match self {
            Self::Memory(bytes) => bytes.len() as u64,
            Self::Spill { length, .. } => *length,
        }
    }
}

pub(super) const IDENTITY_PAGE_ROWS: usize = 1024;

/// Copy-on-write pages keep snapshot creation independent of row count.
#[derive(Clone, Debug)]
pub(super) struct SharedPages<T> {
    pub(super) pages: Arc<Vec<Arc<Vec<T>>>>,
    pub(super) len: usize,
}

impl<T> Default for SharedPages<T> {
    fn default() -> Self {
        Self {
            pages: Arc::default(),
            len: 0,
        }
    }
}

impl<T: Clone> SharedPages<T> {
    pub(super) fn len(&self) -> usize {
        self.len
    }
    pub(super) fn get(&self, index: usize) -> Option<&T> {
        self.pages
            .get(index / IDENTITY_PAGE_ROWS)?
            .get(index % IDENTITY_PAGE_ROWS)
    }
    pub(super) fn push(&mut self, value: T) {
        let pages = Arc::make_mut(&mut self.pages);
        if self.len.is_multiple_of(IDENTITY_PAGE_ROWS) {
            pages.push(Arc::new(Vec::new()));
        }
        Arc::make_mut(pages.last_mut().unwrap()).push(value);
        self.len += 1;
    }
    pub(super) fn resize_with(&mut self, len: usize, mut value: impl FnMut() -> T) {
        assert!(len >= self.len);
        while self.len < len {
            self.push(value());
        }
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &T> {
        self.pages.iter().flat_map(|page| page.iter())
    }
}

impl<T: Clone> std::ops::Index<usize> for SharedPages<T> {
    type Output = T;
    fn index(&self, index: usize) -> &T {
        self.get(index).expect("page index out of bounds")
    }
}

impl<T: Clone> std::ops::IndexMut<usize> for SharedPages<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        assert!(index < self.len);
        let pages = Arc::make_mut(&mut self.pages);
        &mut Arc::make_mut(&mut pages[index / IDENTITY_PAGE_ROWS])[index % IDENTITY_PAGE_ROWS]
    }
}

impl<T: Clone> FromIterator<T> for SharedPages<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut pages = Self::default();
        for value in iter {
            pages.push(value);
        }
        pages
    }
}

#[derive(Clone, Debug)]
pub(super) struct SharedIdentity {
    pub(super) document_key: Arc<[u8]>,
    pub(super) version: u64,
    pub(super) source: u32,
}

impl From<SourceIdentity> for SharedIdentity {
    fn from(identity: SourceIdentity) -> Self {
        Self {
            document_key: identity.document_key.into(),
            version: identity.version,
            source: identity.source,
        }
    }
}
