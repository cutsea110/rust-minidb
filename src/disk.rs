use std::fs::{File, OpenOptions};
use std::io::{self, prelude::*, SeekFrom};
use std::path::Path;

use dao::diskmanager::*;

pub const PAGE_SIZE: usize = 4096;

pub struct DiskManager {
    // ヒープファイルのファイルディスクリプタ
    heap_file: File,
    // 採番するページを決めるカウンタ
    next_page_id: u64,
}

impl DiskManagerDao for DiskManager {
    fn new(heap_file: File) -> io::Result<Self> {
        let heap_file_size = heap_file.metadata()?.len();
        let next_page_id = heap_file_size / PAGE_SIZE as u64;
        Ok(Self {
            heap_file,
            next_page_id,
        })
    }
    fn open(heap_file_path: impl AsRef<Path>) -> io::Result<Self> {
        let heap_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(heap_file_path)?;
        Self::new(heap_file)
    }
    fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        PageId(page_id)
    }
    fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> io::Result<()> {
        // オフセットを計算
        let offset = PAGE_SIZE as u64 * page_id.to_u64();
        // ページ先頭へシーク
        self.heap_file.seek(SeekFrom::Start(offset))?;
        // データを読み出す
        self.heap_file.read_exact(data)
    }
    fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        // オフセットを計算
        let offset = PAGE_SIZE as u64 * page_id.to_u64();
        // ページ先頭へシーク
        self.heap_file.seek(SeekFrom::Start(offset))?;
        // データを書きこむ
        self.heap_file.write_all(data)
    }
}

pub mod mock {
    use std::fs::File;
    use std::io::{Read, Result};
    use std::path::Path;

    use super::dao::diskmanager::*;

    pub struct MockDiskManager {}

    impl DiskManagerDao for MockDiskManager {
        fn new(_: File) -> Result<Self> {
            Ok(Self {})
        }
        fn open(_: impl AsRef<Path>) -> Result<Self> {
            Ok(Self {})
        }
        fn allocate_page(&mut self) -> PageId {
            PageId(42)
        }
        fn read_page_data(&mut self, _: PageId, data: &mut [u8]) -> Result<()> {
            let mut hello: &[u8] = b"The quick brown fox jumps over the lazy dog";
            hello.read_exact(data)?;
            Ok(())
        }
        fn write_page_data(&mut self, _: PageId, _: &[u8]) -> Result<()> {
            Ok(())
        }
    }
}

pub mod dao {
    pub mod diskmanager {
        use std::fs::File;
        use std::io::Result;
        use std::path::Path;

        pub struct PageId(pub u64);
        impl PageId {
            pub fn to_u64(self) -> u64 {
                self.0
            }
        }

        pub trait DiskManagerDao {
            // コンストラクタ
            fn new(heap_file: File) -> Result<Self>
            where
                Self: Sized;
            // ファイルパスを指定して開く
            fn open(heap_file_path: impl AsRef<Path>) -> Result<Self>
            where
                Self: Sized;

            // 新しいページIDを採番する
            fn allocate_page(&mut self) -> PageId;
            // ページのデータを読み出す
            fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> Result<()>;
            // データをページに書き出す
            fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> Result<()>;
        }
    }
}
