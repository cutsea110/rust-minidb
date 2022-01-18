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
    fn next(&mut self, bufmgr: &mut T) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error>;
}

pub trait AccessMethod<T: BufferPoolManager> {
    type Iterable: Iterable<T>;

    // レコードを検索する
    fn search(&self, bufmgr: &mut T, search_option: SearchMode) -> Result<Self::Iterable, Error>;
    // レコードを挿入する
    fn insert(&self, bufmgr: &mut T, key: &[u8], value: &[u8]) -> Result<(), Error>;
}

pub trait HaveAccessMethod<T: BufferPoolManager> {
    type Iter: Iterable<T>;

    // テーブルアクセサを取得する
    fn table_accessor(&self) -> Option<Box<&dyn AccessMethod<T, Iterable = Self::Iter>>>;
    // インデックスアクセサを取得する
    fn index_accessor(&self) -> Option<Box<&dyn AccessMethod<T, Iterable = Self::Iter>>>;
}
