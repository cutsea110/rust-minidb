use anyhow::Result;

use super::entity::Tuple;
use crate::{accessor::method::HaveAccessMethod, buffer::manager::BufferPoolManager};

pub trait Executor<T: BufferPoolManager> {
    fn next(&mut self, bufmgr: &mut T) -> Result<Option<Tuple>>;
}

pub type BoxExecutor<'a, T> = Box<dyn Executor<T> + 'a>;

pub trait PlanNode<T: BufferPoolManager>: HaveAccessMethod<T> {
    fn start(&self, bufmgr: &mut T) -> Result<BoxExecutor<T>>;
}
