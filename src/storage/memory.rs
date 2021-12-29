use std::io::{Read, Result, Write};
use std::vec::*;

use zerocopy::AsBytes;

use crate::accessor::dao::entity::PAGE_SIZE; // TODO: コンストラクタから貰いたい
use crate::buffer::dao::{entity::PageId, storage::*};

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

impl StorageManager for MemoryManager {
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
