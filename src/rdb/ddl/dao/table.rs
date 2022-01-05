use anyhow::Result;

use crate::accessor::dao::bufferpool::BufferPoolManager;

pub trait Table<T: BufferPoolManager> {
    fn create(&mut self, bufmgr: &mut T) -> Result<()>;
    fn insert(&self, bufmgr: &mut T, record: &[&[u8]]) -> Result<()>;
}

pub trait UniqueIndex<T: BufferPoolManager> {
    fn create(&mut self, bufmgr: &mut T) -> Result<()>;
    fn insert(&self, bufmgr: &mut T, pkey: &[u8], record: &[impl AsRef<[u8]>]) -> Result<()>;
}
