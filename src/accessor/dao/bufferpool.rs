use super::entity::Buffer;
use crate::buffer::dao::entity::PageId;

use std::io;
use std::rc::Rc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
}

pub trait BufferPoolManagerDao {
    fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, Error>;
    fn create_page(&mut self) -> Result<Rc<Buffer>, Error>;
    fn flush(&mut self) -> Result<(), Error>;
}
