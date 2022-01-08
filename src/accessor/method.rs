use thiserror::Error;

use crate::buffer::manager::{self, BufferPoolManager};

#[derive(Debug, Error)]
pub enum Error {
    #[error("duplicate key")]
    DuplicateKey,
    #[error(transparent)]
    Buffer(#[from] manager::Error),
}

pub trait Iterable<T: BufferPoolManager> {
    type Item;
    fn next(&mut self, bufmgr: &mut T) -> Result<Option<Self::Item>, Error>;
}

pub trait SearchOption {}

pub trait AccessMethod<T: BufferPoolManager> {
    type Iterable: Iterable<T>;
    type SearchOption: SearchOption;

    fn search(
        &self,
        bufmgr: &mut T,
        search_option: Self::SearchOption,
    ) -> Result<Self::Iterable, Error>;
    fn insert(&self, bufmgr: &mut T, key: &[u8], value: &[u8]) -> Result<(), Error>;
}

pub type BoxedAccessMethod<T, U, V> = Box<dyn AccessMethod<T, Iterable = U, SearchOption = V>>;

pub trait HaveAccessMethod<T: BufferPoolManager> {
    type Iter: Iterable<T>;
    type SearchOption: SearchOption;

    fn table_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter, Self::SearchOption>>;
    fn index_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter, Self::SearchOption>>;
}
