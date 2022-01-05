use anyhow::Result;

use crate::accessor::dao::bufferpool::BufferPoolManager;

pub trait Rel<T: BufferPoolManager> {
    fn create(&mut self, bufmgr: &mut T) -> Result<()>;
    fn insert(&self, bufmgr: &mut T, record: &[&[u8]]) -> Result<()>;
}

pub trait UIdx<T: BufferPoolManager> {
    fn create(&mut self, bufmgr: &mut T) -> Result<()>;
    fn insert(&self, bufmgr: &mut T, pkey: &[u8], record: &[impl AsRef<[u8]>]) -> Result<()>;
}
