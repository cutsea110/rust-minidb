use anyhow::Result;

use super::entity::Tuple;
use crate::buffer::dao::bufferpool::BufferPoolManager;

pub trait Executor<T: BufferPoolManager> {
    fn next(&mut self, bufmgr: &mut T) -> Result<Option<Tuple>>;
}

pub type BoxExecutor<'a, T> = Box<dyn Executor<T> + 'a>;

pub trait PlanNode<T: BufferPoolManager> {
    fn start(&self, bufmgr: &mut T) -> Result<BoxExecutor<T>>;
}
