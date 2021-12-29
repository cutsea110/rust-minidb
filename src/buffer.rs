// これは BufferPoolManager が依存するコアのドメインに移動させる
pub mod bufferdao {
    pub mod entity {
        use crate::buffer::dao::entity::PageId;
        use std::cell::{Cell, RefCell};

        pub const PAGE_SIZE: usize = 4096;

        pub type Page = [u8; PAGE_SIZE];

        #[derive(Debug)]
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
    }

    pub mod buffermanager {
        use crate::buffer::bufferdao::entity::Buffer;
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
    }
}

pub mod dao {
    pub mod entity {
        use std::convert::TryInto;

        use zerocopy::{AsBytes, FromBytes};

        #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, FromBytes, AsBytes)]
        #[repr(C)]
        pub struct PageId(pub u64);
        impl PageId {
            pub const INVALID_PAGE_ID: PageId = PageId(u64::MAX);

            pub fn valid(self) -> Option<PageId> {
                if self == Self::INVALID_PAGE_ID {
                    None
                } else {
                    Some(self)
                }
            }

            pub fn to_u64(self) -> u64 {
                self.0
            }
        }

        impl Default for PageId {
            fn default() -> Self {
                Self::INVALID_PAGE_ID
            }
        }

        impl From<Option<PageId>> for PageId {
            fn from(page_id: Option<PageId>) -> Self {
                page_id.unwrap_or_default()
            }
        }

        impl From<&[u8]> for PageId {
            fn from(bytes: &[u8]) -> Self {
                let arr = bytes.try_into().unwrap();
                PageId(u64::from_ne_bytes(arr))
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
    }
}

pub mod simple {
    use std::collections::HashMap;
    use std::ops::{Index, IndexMut};
    use std::rc::Rc;

    use crate::buffer::bufferdao::{buffermanager::*, entity::Buffer};
    use crate::buffer::dao::{diskmanager::*, entity::PageId};

    #[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
    pub struct BufferId(usize);

    #[derive(Debug, Default)]
    pub struct Frame {
        usage_count: u64,
        buffer: Rc<Buffer>,
    }

    pub struct BufferPool {
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

    pub struct BufferPoolManager<T>
    where
        T: DiskManagerDao,
    {
        disk: T,
        pool: BufferPool,
        page_table: HashMap<PageId, BufferId>,
    }

    impl<T> BufferPoolManager<T>
    where
        T: DiskManagerDao,
    {
        pub fn new(disk: T, pool: BufferPool) -> Self {
            let page_table = HashMap::new();
            Self {
                disk,
                pool,
                page_table,
            }
        }
    }

    impl<T> BufferPoolManagerDao for BufferPoolManager<T>
    where
        T: DiskManagerDao,
    {
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
    mod tests_with_disk {
        use super::{BufferPool, BufferPoolManager};
        use crate::buffer::bufferdao::buffermanager::*;
        use crate::disk::disk::{self, DiskManager};
        use tempfile::tempfile;

        #[test]
        fn test() {
            let mut hello = Vec::with_capacity(disk::PAGE_SIZE);
            hello.extend_from_slice(b"hello");
            hello.resize(disk::PAGE_SIZE, 0);
            let mut world = Vec::with_capacity(disk::PAGE_SIZE);
            world.extend_from_slice(b"world");
            world.resize(disk::PAGE_SIZE, 0);

            let disk = DiskManager::new(tempfile().unwrap()).unwrap();
            let pool = BufferPool::new(1);
            let mut bufmgr = BufferPoolManager::new(disk, pool);
            let page1_id = {
                let buffer = bufmgr.create_page().unwrap();
                assert!(bufmgr.create_page().is_err());
                let mut page = buffer.page.borrow_mut();
                page.copy_from_slice(&hello);
                buffer.is_dirty.set(true);
                buffer.page_id
            };
            {
                let buffer = bufmgr.fetch_page(page1_id).unwrap();
                let page = buffer.page.borrow();
                assert_eq!(&hello, page.as_ref());
            }
            let page2_id = {
                let buffer = bufmgr.create_page().unwrap();
                let mut page = buffer.page.borrow_mut();
                page.copy_from_slice(&world);
                buffer.is_dirty.set(true);
                buffer.page_id
            };
            {
                let buffer = bufmgr.fetch_page(page1_id).unwrap();
                let page = buffer.page.borrow();
                assert_eq!(&hello, page.as_ref());
            }
            {
                let buffer = bufmgr.fetch_page(page2_id).unwrap();
                let page = buffer.page.borrow();
                assert_eq!(&world, page.as_ref());
            }
        }
    }

    #[cfg(test)]
    mod tests_with_memory {
        use super::{BufferPool, BufferPoolManager};
        use crate::buffer::bufferdao::buffermanager::*;
        use crate::disk::memory::{self, MemoryManager};

        #[test]
        fn test_with_memory() {
            let mut hello = Vec::with_capacity(memory::PAGE_SIZE);
            hello.extend_from_slice(b"hello");
            hello.resize(memory::PAGE_SIZE, 0);
            let mut world = Vec::with_capacity(memory::PAGE_SIZE);
            world.extend_from_slice(b"world");
            world.resize(memory::PAGE_SIZE, 0);

            let memory = MemoryManager::new().unwrap();
            let pool = BufferPool::new(1);
            let mut bufmgr = BufferPoolManager::new(memory, pool);
            let page1_id = {
                let buffer = bufmgr.create_page().unwrap();
                assert!(bufmgr.create_page().is_err());
                let mut page = buffer.page.borrow_mut();
                page.copy_from_slice(&hello);
                buffer.is_dirty.set(true);
                buffer.page_id
            };
            {
                let buffer = bufmgr.fetch_page(page1_id).unwrap();
                let page = buffer.page.borrow();
                assert_eq!(&hello, page.as_ref());
            }
            let page2_id = {
                let buffer = bufmgr.create_page().unwrap();
                let mut page = buffer.page.borrow_mut();
                page.copy_from_slice(&world);
                buffer.is_dirty.set(true);
                buffer.page_id
            };
            {
                let buffer = bufmgr.fetch_page(page1_id).unwrap();
                let page = buffer.page.borrow();
                assert_eq!(&hello, page.as_ref());
            }
            {
                let buffer = bufmgr.fetch_page(page2_id).unwrap();
                let page = buffer.page.borrow();
                assert_eq!(&world, page.as_ref());
            }
        }
    }
}
