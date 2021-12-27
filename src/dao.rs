pub mod entity {
    #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
    pub struct PageId(pub u64);
    impl PageId {
        pub const INVALID_PAGE_ID: PageId = PageId(u64::MAX);

        pub fn to_u64(self) -> u64 {
            self.0
        }
    }

    impl Default for PageId {
        fn default() -> Self {
            Self::INVALID_PAGE_ID
        }
    }
}

pub mod diskmanager {
    use super::entity::PageId;

    use std::io::Result;

    pub trait DiskManagerDao {
        // 新しいページIDを採番する
        fn allocate_page(&mut self) -> PageId;
        // ページのデータを読み出す
        fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> Result<()>;
        // データをページに書き出す
        fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> Result<()>;
        // 同期処理
        fn sync(&mut self) -> Result<()>;
    }

    pub trait HaveDiskManager {
        type DiskManagerDao: DiskManagerDao;

        fn disk(&mut self) -> &mut Self::DiskManagerDao;
    }
}
