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

pub mod memory {
    use std::fs::File;
    use std::io::{Error, ErrorKind, Read, Result, Write};
    use std::path::Path;
    use std::vec::*;

    use zerocopy::AsBytes;

    use super::dao::diskmanager::*;

    pub const PAGE_SIZE: usize = 4096;

    pub struct MemoryManager {
        next_page_id: u64,
        heap: Vec<[u8; PAGE_SIZE]>,
    }

    impl MemoryManager {
        pub fn new_memory() -> Result<Self> {
            Ok(Self {
                next_page_id: 0,
                heap: vec![],
            })
        }
    }

    impl DiskManagerDao for MemoryManager {
        fn new(_: File) -> Result<Self> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "this method is not supported.",
            ))
        }
        fn open(_: impl AsRef<Path>) -> Result<Self> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "this method is not supported.",
            ))
        }
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
    }
}

pub mod dao {
    pub mod diskmanager {
        use std::fs::File;
        use std::io::Result;
        use std::path::Path;

        #[derive(Debug, Clone, Copy, Eq, PartialEq)]
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_disk_manager() {
        use super::*;
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

    #[test]
    fn test_memory_manager() {
        use super::dao::diskmanager::*;
        use super::memory::{MemoryManager, *};

        let mut memory = MemoryManager::new_memory().unwrap();
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
