use crate::storage::entity::PageId;
use std::cell::{Cell, RefCell};

pub const PAGE_SIZE: usize = 4096;

pub type Page = [u8; PAGE_SIZE];

#[derive(Debug, PartialEq, Eq)]
pub struct Buffer {
    pub page_id: PageId,
    pub page: RefCell<Page>,
    pub is_dirty: Cell<bool>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            page_id: Default::default(),
            page: RefCell::new([0u8; PAGE_SIZE]),
            is_dirty: Cell::new(false),
        }
    }
}
