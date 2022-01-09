use std::any::Any;

use thiserror::Error;

use super::entity::SearchMode;
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

pub trait AccessMethod<T: BufferPoolManager> {
    type Iterable: Iterable<T>;

    fn as_any(&self) -> &dyn Any;
    fn search(&self, bufmgr: &mut T, search_option: SearchMode) -> Result<Self::Iterable, Error>;
    fn insert(&self, bufmgr: &mut T, key: &[u8], value: &[u8]) -> Result<(), Error>;
}

pub type BoxedAccessMethod<'a, T, U> = Box<&'a dyn AccessMethod<T, Iterable = U>>;

pub trait HaveAccessMethod<T: BufferPoolManager> {
    type Iter: Iterable<T>;

    fn table_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter>>;
    fn index_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter>>;
}
