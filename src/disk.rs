pub mod dao {
    pub mod diskmanager {
        use std::io::Result;

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

            fn disk_manager_dao(&mut self) -> &mut Self::DiskManagerDao;
        }
    }
}

pub mod disk {
    use std::fs::{File, OpenOptions};
    use std::io::{prelude::*, Result, SeekFrom};
    use std::path::Path;

    use super::dao::diskmanager::*;

    pub const PAGE_SIZE: usize = 4096;

    pub struct DiskManager {
        // ヒープファイルのファイルディスクリプタ
        heap_file: File,
        // 採番するページを決めるカウンタ
        next_page_id: u64,
    }

    impl DiskManager {
        pub fn new(heap_file: File) -> Result<Self> {
            let heap_file_size = heap_file.metadata()?.len();
            let next_page_id = heap_file_size / PAGE_SIZE as u64;
            Ok(Self {
                heap_file,
                next_page_id,
            })
        }

        pub fn open(heap_file_path: impl AsRef<Path>) -> Result<Self> {
            let heap_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(heap_file_path)?;
            Self::new(heap_file)
        }
    }

    impl DiskManagerDao for DiskManager {
        fn allocate_page(&mut self) -> PageId {
            let page_id = self.next_page_id;
            self.next_page_id += 1;
            PageId(page_id)
        }
        fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> Result<()> {
            // オフセットを計算
            let offset = PAGE_SIZE as u64 * page_id.to_u64();
            // ページ先頭へシーク
            self.heap_file.seek(SeekFrom::Start(offset))?;
            // データを読み出す
            self.heap_file.read_exact(data)
        }
        fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
            // オフセットを計算
            let offset = PAGE_SIZE as u64 * page_id.to_u64();
            // ページ先頭へシーク
            self.heap_file.seek(SeekFrom::Start(offset))?;
            // データを書きこむ
            self.heap_file.write_all(data)
        }
        fn sync(&mut self) -> Result<()> {
            self.heap_file.flush()?;
            self.heap_file.sync_all()
        }
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn test() {
            use super::super::dao::diskmanager::*;
            use super::{DiskManager, *};
            use tempfile::NamedTempFile;

            let (data_file, data_file_path) = NamedTempFile::new().unwrap().into_parts();
            let mut disk = DiskManager::new(data_file).unwrap();
            let mut hello = Vec::with_capacity(PAGE_SIZE);
            hello.extend_from_slice(b"hello");
            hello.resize(PAGE_SIZE, 0);
            let hello_page_id = disk.allocate_page();
            disk.write_page_data(hello_page_id, &hello).unwrap();
            let mut world = Vec::with_capacity(PAGE_SIZE);
            world.extend_from_slice(b"world");
            world.resize(PAGE_SIZE, 0);
            let world_page_id = disk.allocate_page();
            disk.write_page_data(world_page_id, &world).unwrap();
            drop(disk);
            let mut disk2 = DiskManager::open(&data_file_path).unwrap();
            let mut buf = vec![0; PAGE_SIZE];
            disk2.read_page_data(hello_page_id, &mut buf).unwrap();
            assert_eq!(hello, buf);
            disk2.read_page_data(world_page_id, &mut buf).unwrap();
            assert_eq!(world, buf);
        }
    }
}

pub mod mock {
    use std::io::Result;

    use super::dao::diskmanager::*;

    pub struct Mock {
        next_page_id: u64,
    }

    impl Mock {
        pub fn new() -> Result<Self>
        where
            Self: Sized,
        {
            Ok(Self { next_page_id: 0 })
        }
    }

    impl DiskManagerDao for Mock {
        fn allocate_page(&mut self) -> PageId {
            let page_id = self.next_page_id;
            self.next_page_id += 1;
            PageId(page_id)
        }
        fn read_page_data(&mut self, _: PageId, _: &mut [u8]) -> Result<()> {
            Ok(())
        }
        fn write_page_data(&mut self, _: PageId, _: &[u8]) -> Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> Result<()> {
            Ok(())
        }
    }
}

pub mod memory {
    use std::io::{Read, Result, Write};
    use std::vec::*;

    use zerocopy::AsBytes;

    use super::dao::diskmanager::*;

    pub const PAGE_SIZE: usize = 4096;

    pub struct MemoryManager {
        next_page_id: u64,
        heap: Vec<[u8; PAGE_SIZE]>,
    }

    impl MemoryManager {
        pub fn new() -> Result<Self> {
            Ok(Self {
                next_page_id: 0,
                heap: vec![],
            })
        }
    }

    impl DiskManagerDao for MemoryManager {
        fn allocate_page(&mut self) -> PageId {
            let page_id = self.next_page_id;
            self.next_page_id += 1;
            self.heap.push([0; PAGE_SIZE]);
            PageId(page_id)
        }
        fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> Result<()> {
            let mut row: &[u8] = self.heap[page_id.0 as usize].as_bytes();
            row.read_exact(data)?;
            Ok(())
        }
        fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
            let buf: &[u8] = data.as_bytes();
            let mut row: &mut [u8] = self.heap[page_id.0 as usize].as_bytes_mut();
            row.write_all(buf)?;
            Ok(())
        }
        fn sync(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn test() {
            use super::super::dao::diskmanager::*;
            use super::{MemoryManager, *};

            let mut memory = MemoryManager::new().unwrap();
            let mut hello = Vec::with_capacity(PAGE_SIZE);
            hello.extend_from_slice(b"hello");
            hello.resize(PAGE_SIZE, 0);
            let hello_page_id = memory.allocate_page();
            memory.write_page_data(hello_page_id, &hello).unwrap();
            let mut world = Vec::with_capacity(PAGE_SIZE);
            world.extend_from_slice(b"world");
            world.resize(PAGE_SIZE, 0);
            let world_page_id = memory.allocate_page();
            memory.write_page_data(world_page_id, &world).unwrap();

            let mut buf = vec![0; PAGE_SIZE];
            memory.read_page_data(hello_page_id, &mut buf).unwrap();
            assert_eq!(hello, buf);
            memory.read_page_data(world_page_id, &mut buf).unwrap();
            assert_eq!(world, buf);
        }
    }
}
