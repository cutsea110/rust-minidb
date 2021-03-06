use anyhow::Result;

use crate::buffer::manager::BufferPoolManager;

pub trait Table<T: BufferPoolManager> {
    fn create(&mut self, bufmgr: &mut T) -> Result<()>;
    fn insert(&self, bufmgr: &mut T, record: &[&[u8]]) -> Result<()>;
}

pub trait UniqueIndex<T: BufferPoolManager> {
    // テーブルの CREATE
    fn create(&mut self, bufmgr: &mut T) -> Result<()>;
    // TABLE へのレコードの INSERT
    fn insert(&self, bufmgr: &mut T, pkey: &[u8], record: &[impl AsRef<[u8]>]) -> Result<()>;
}
