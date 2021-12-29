use super::entity::PageId;

use std::io::Result;

pub trait StorageManager {
    // 新しいページIDを採番する
    fn allocate_page(&mut self) -> PageId;
    // ページのデータを読み出す
    fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> Result<()>;
    // データをページに書き出す
    fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> Result<()>;
    // 同期処理
    fn sync(&mut self) -> Result<()>;
}
