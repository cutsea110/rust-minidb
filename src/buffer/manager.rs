use super::entity::Buffer;
use crate::storage::entity::PageId;

use std::io;
use std::rc::Rc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
}

pub trait BufferPoolManager {
    // ページを取得する
    fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, Error>;
    // 新たにページを生成する
    fn create_page(&mut self) -> Result<Rc<Buffer>, Error>;
    // ストレージに書き出す
    fn flush(&mut self) -> Result<(), Error>;
}
