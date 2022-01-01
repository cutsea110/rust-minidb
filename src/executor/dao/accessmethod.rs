use thiserror::Error;

use crate::accessor::dao::bufferpool::{self, BufferPoolManager};

#[derive(Debug, Error)]
pub enum Error {
    #[error("duplicate key")]
    DuplicateKey,
    #[error(transparent)]
    Buffer(#[from] bufferpool::Error),
}

pub trait Iterable<T: BufferPoolManager> {
    fn next(&mut self, bufmgr: &mut T) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error>;
}

pub trait SearchOpt {}

pub trait AccessMethod<T: BufferPoolManager> {
    type Iterable: Iterable<T>;
    type Opt: SearchOpt;

    fn search(&self, bufmgr: &mut T, search_mode: Self::Opt) -> Result<Self::Iterable, Error>;
    fn insert(&self, bufmgr: &mut T, key: &[u8], value: &[u8]) -> Result<(), Error>;
}
