use std::cell::{Ref, RefMut};
use std::convert::identity;
use std::rc::Rc;

use bincode::Options;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use zerocopy::{AsBytes, ByteSlice};

use crate::accessor::dao::bufferpool::{self, BufferPoolManager};
use crate::buffer::dao::entity::PageId;

mod branch;
mod leaf;
mod meta;
mod node;

#[derive(Serialize, Deserialize)]
pub struct Pair<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> Pair<'a> {
    fn to_bytes(&self) -> Vec<u8> {
        bincode::options().serialize(self).unwrap()
    }

    fn from_bytes(bytes: &'a [u8]) -> Self {
        bincode::options().deserialize(bytes).unwrap()
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("duplicate key")]
    DuplicateKey,
    #[error(transparent)]
    Buffer(#[from] bufferpool::Error),
}

#[derive(Debug, Clone)]
pub enum SearchMode {
    Start,
    Key(Vec<u8>),
}

impl SearchMode {
    fn child_page_id(&self, branch: &branch::Branch<impl ByteSlice>) -> PageId {
        match self {
            SearchMode::Start => branch.child_at(0),
            SearchMode::Key(key) => branch.search_child(key),
        }
    }

    fn tuple_slot_id(&self, leaf: &leaf::Leaf<impl ByteSlice>) -> Result<usize, usize> {
        match self {
            SearchMode::Start => Err(0),
            SearchMode::Key(key) => leaf.search_slot_id(key),
        }
    }
}

pub struct BTree {
    pub meta_page_id: PageId,
}

impl BTree {
    pub fn create(bufmgr: &mut BufferPoolManager) -> Result<Self, Error> {
        let meta_buffer = bufmgr.create_page()?;
        let mut meta = meta::Meta::new(meta_buffer.page.borrow_mut() as RefMut<[_]>);
        let root_buffer = bufmgr.create_page()?;
        let mut root = node::Node::new(root_buffer.page.borrow_mut() as RefMut<[_]>);
        root.initialize_as_leaf();
        let mut leaf = leaf::Leaf::new(root.body);
        leaf.initialize();
        meta.header.root_page_id = root_buffer.page_id;
        Ok(Self::new(meta_buffer.page_id))
    }

    pub fn new(meta_page_id: PageId) -> Self {
        Self { meta_page_id }
    }
}
