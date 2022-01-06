use std::collections::HashMap;
use std::ops::{Index, IndexMut};
use std::rc::Rc;

use crate::buffer::{entity::Buffer, manager::*};
use crate::storage::{entity::PageId, manager::*};

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BufferId(usize);

#[derive(Debug, Default)]
struct Frame {
    usage_count: u64,
    buffer: Rc<Buffer>,
}

struct BufferPool {
    buffers: Vec<Frame>,
    next_victim_id: BufferId,
}

impl Index<BufferId> for BufferPool {
    type Output = Frame;
    fn index(&self, index: BufferId) -> &Self::Output {
        &self.buffers[index.0]
    }
}

impl IndexMut<BufferId> for BufferPool {
    fn index_mut(&mut self, index: BufferId) -> &mut Self::Output {
        &mut self.buffers[index.0]
    }
}

impl BufferPool {
    pub fn new(pool_size: usize) -> Self {
        let mut buffers = vec![];
        buffers.resize_with(pool_size, Default::default);
        let next_victim_id = BufferId::default();
        Self {
            buffers,
            next_victim_id,
        }
    }

    fn size(&self) -> usize {
        self.buffers.len()
    }

    // Clock-sweep
    fn evict(&mut self) -> Option<BufferId> {
        let pool_size = self.size();
        let mut consecutive_pinned = 0;
        let victim_id = loop {
            let next_victim_id = self.next_victim_id;
            let frame = &mut self[next_victim_id];
            if frame.usage_count == 0 {
                break self.next_victim_id;
            }
            if Rc::get_mut(&mut frame.buffer).is_some() {
                frame.usage_count -= 1;
                consecutive_pinned = 0;
            } else {
                consecutive_pinned += 1;
                if consecutive_pinned >= pool_size {
                    return None;
                }
            }
            self.next_victim_id = self.increment_id(self.next_victim_id);
        };
        Some(victim_id)
    }

    fn increment_id(&self, buffer_id: BufferId) -> BufferId {
        BufferId((buffer_id.0 + 1) % self.size())
    }
}

pub struct ClockSweepManager<T: StorageManager> {
    disk: T,
    pool: BufferPool,
    page_table: HashMap<PageId, BufferId>,
}

impl<T: StorageManager> ClockSweepManager<T> {
    pub fn new(disk: T, pool_size: usize) -> Self {
        let pool = BufferPool::new(pool_size);
        let page_table = HashMap::new();
        Self {
            disk,
            pool,
            page_table,
        }
    }
}

impl<T: StorageManager> BufferPoolManager for ClockSweepManager<T> {
    fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, Error> {
        if let Some(&buffer_id) = self.page_table.get(&page_id) {
            let frame = &mut self.pool[buffer_id];
            frame.usage_count += 1;
            return Ok(frame.buffer.clone());
        }
        let buffer_id = self.pool.evict().ok_or(Error::NoFreeBuffer)?;
        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;
        {
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();
            if buffer.is_dirty.get() {
                self.disk
                    .write_page_data(evict_page_id, buffer.page.get_mut())?;
            }
            buffer.page_id = page_id;
            buffer.is_dirty.set(false);
            self.disk.read_page_data(page_id, buffer.page.get_mut())?;
            frame.usage_count = 1;
        }
        let page = Rc::clone(&frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }

    fn create_page(&mut self) -> Result<Rc<Buffer>, Error> {
        let buffer_id = self.pool.evict().ok_or(Error::NoFreeBuffer)?;
        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;
        let page_id = {
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();
            if buffer.is_dirty.get() {
                self.disk
                    .write_page_data(evict_page_id, buffer.page.get_mut())?;
            }
            self.page_table.remove(&evict_page_id);
            let page_id = self.disk.allocate_page();
            *buffer = Buffer::default();
            buffer.page_id = page_id;
            buffer.is_dirty.set(true);
            frame.usage_count = 1;
            page_id
        };
        let page = Rc::clone(&frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }

    fn flush(&mut self) -> Result<(), Error> {
        for (&page_id, &buffer_id) in self.page_table.iter() {
            let frame = &self.pool[buffer_id];
            let mut page = frame.buffer.page.borrow_mut();
            self.disk.write_page_data(page_id, page.as_mut())?;
            frame.buffer.is_dirty.set(false);
        }
        self.disk.sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        buffer::entity::PAGE_SIZE,
        storage::{entity::PageId, manager::StorageManager},
    };
    use std::io::Result;

    #[derive(Debug, PartialEq)]
    enum Op {
        Alloc(PageId),
        Read(PageId),
        Write(PageId),
        Sync,
    }

    struct TraceStorage {
        next_page_id: u64,
        history: Vec<Op>,
    }

    impl TraceStorage {
        fn new() -> Self {
            Self {
                next_page_id: 1,
                history: vec![],
            }
        }
    }

    impl StorageManager for TraceStorage {
        fn allocate_page(&mut self) -> PageId {
            let pid = PageId(self.next_page_id);
            self.next_page_id += 1;
            self.history.push(Op::Alloc(pid));
            pid
        }
        fn read_page_data(&mut self, page_id: PageId, _data: &mut [u8]) -> Result<()> {
            self.history.push(Op::Read(page_id));
            Ok(())
        }
        fn write_page_data(&mut self, page_id: PageId, _data: &[u8]) -> Result<()> {
            self.history.push(Op::Write(page_id));
            Ok(())
        }
        fn sync(&mut self) -> Result<()> {
            self.history.push(Op::Sync);
            Ok(())
        }
    }

    #[test]
    fn create_page_test() {
        use super::*;

        let mock = TraceStorage::new();
        let mut bufmgr = ClockSweepManager::new(mock, 1);
        {
            let res = bufmgr.create_page();
            assert!(res.is_ok());
            let buffer = res.unwrap();
            assert_eq!(buffer.page_id, PageId(1));
            // Allocate
            assert_eq!(vec![Op::Alloc(PageId(1)),], bufmgr.disk.history);

            let res_err = bufmgr.create_page();
            assert!(res_err.is_err());
            // no storage access
            assert_eq!(vec![Op::Alloc(PageId(1)),], bufmgr.disk.history);
        }
        {
            let res = bufmgr.create_page();
            assert!(res.is_ok());
            let buffer = res.unwrap();
            assert_eq!(buffer.page_id, PageId(2));
            // Write & Allocate
            assert_eq!(
                vec![
                    Op::Alloc(PageId(1)),
                    Op::Write(PageId(1)),
                    Op::Alloc(PageId(2))
                ],
                bufmgr.disk.history
            );
        }
    }

    #[test]
    fn fetch_page_test() {
        use super::*;

        let mock = TraceStorage::new();
        let mut bufmgr = ClockSweepManager::new(mock, 1);
        {
            let res = bufmgr.fetch_page(PageId(1));
            assert!(res.is_ok());
            // Read
            assert_eq!(vec![Op::Read(PageId(1)),], bufmgr.disk.history);

            let res_same_page = bufmgr.fetch_page(PageId(1));
            assert!(res_same_page.is_ok());
            // no storage access(hit the cache)
            assert_eq!(vec![Op::Read(PageId(1)),], bufmgr.disk.history);

            let res_err = bufmgr.fetch_page(PageId(2));
            assert!(res_err.is_err());
            // no storage access
            assert_eq!(vec![Op::Read(PageId(1)),], bufmgr.disk.history);
        }
        {
            let res = bufmgr.fetch_page(PageId(2));
            assert!(res.is_ok());
            // Read * 2
            assert_eq!(
                vec![Op::Read(PageId(1)), Op::Read(PageId(2)),],
                bufmgr.disk.history
            );

            // write page data
            let buffer = res.unwrap();
            let mut page = buffer.page.borrow_mut();
            let all42 = [42u8; PAGE_SIZE];
            page.copy_from_slice(&all42);
            buffer.is_dirty.set(true);

            let res_same_page = bufmgr.fetch_page(PageId(2));
            assert!(res_same_page.is_ok());
            // no storage access(hit the cache)
            assert_eq!(
                vec![Op::Read(PageId(1)), Op::Read(PageId(2)),],
                bufmgr.disk.history
            );
        }
        {
            let res = bufmgr.fetch_page(PageId(1));
            assert!(res.is_ok());
            // Read * 2 & Write & Read
            assert_eq!(
                vec![
                    Op::Read(PageId(1)),
                    Op::Read(PageId(2)),
                    Op::Write(PageId(2)),
                    Op::Read(PageId(1)),
                ],
                bufmgr.disk.history
            );
        }
    }

    #[test]
    fn flush_test() {
        use super::*;

        let mock = TraceStorage::new();
        let mut bufmgr = ClockSweepManager::new(mock, 3);
        {
            let res = bufmgr.flush();
            assert!(res.is_ok());
            assert_eq!(vec![Op::Sync], bufmgr.disk.history);
        }
        {
            let _ = bufmgr.fetch_page(PageId(1));
            assert_eq!(vec![Op::Sync, Op::Read(PageId(1))], bufmgr.disk.history);
            let res = bufmgr.flush();
            assert!(res.is_ok());
            assert_eq!(
                vec![
                    Op::Sync,
                    Op::Read(PageId(1)),
                    Op::Write(PageId(1)),
                    Op::Sync,
                ],
                bufmgr.disk.history
            );
        }
        {
            let _ = bufmgr.fetch_page(PageId(2));
            let _ = bufmgr.fetch_page(PageId(3));
            assert_eq!(
                vec![
                    Op::Sync,
                    Op::Read(PageId(1)),
                    Op::Write(PageId(1)),
                    Op::Sync,
                    Op::Read(PageId(2)),
                    Op::Read(PageId(3)),
                ],
                bufmgr.disk.history
            );
            let res = bufmgr.flush();
            assert!(res.is_ok());
            // flush 操作が HashMap::iter() で順序が変わるのでログの数のみで確認
            // ここまでの 6 レコードに buffer Write 3 つと Sync の 4 レコードが追加
            assert_eq!(10, bufmgr.disk.history.len())
        }
    }
}
