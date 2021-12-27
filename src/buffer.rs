pub mod dao {
    pub mod entity {
        use crate::disk::dao::entity::PageId;
        use std::cell::{Cell, RefCell};

        #[derive(Debug)]
        pub struct Buffer<T> {
            pub page_id: PageId,
            pub page: RefCell<T>,
            pub is_dirty: Cell<bool>,
        }

        impl<T> Default for Buffer<T>
        where
            T: Default,
        {
            fn default() -> Self {
                Self {
                    page_id: Default::default(),
                    page: RefCell::new(T::default()),
                    is_dirty: Cell::new(false),
                }
            }
        }
    }

    pub mod buffermanager {
        use crate::buffer::dao::entity::Buffer;
        use crate::disk::dao::entity::PageId;
        use std::io;
        use std::rc::Rc;

        #[derive(Debug, thiserror::Error)]
        pub enum Error {
            #[error(transparent)]
            Io(#[from] io::Error),
            #[error("no free buffer available in buffer pool")]
            NoFreeBuffer,
        }

        pub trait BufferPoolManagerDao<T> {
            fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer<T>>, Error>;
            fn create_page(&mut self) -> Result<Rc<Buffer<T>>, Error>;
            fn flush(&mut self) -> Result<(), Error>;
        }
    }
}

pub mod simple {
    use std::collections::HashMap;
    use std::ops::{Deref, DerefMut, Index, IndexMut};
    use std::rc::Rc;

    use crate::buffer::dao::{buffermanager::*, entity::Buffer};
    use crate::disk::dao::{diskmanager::*, entity::PageId};
    use crate::disk::disk::PAGE_SIZE; // TODO: これに依存したくない。Page は diskmanager にあるべき?

    #[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
    pub struct BufferId(usize);

    pub struct Page {
        bytes: [u8; PAGE_SIZE],
    }
    impl Default for Page {
        fn default() -> Self {
            Self {
                bytes: [0u8; PAGE_SIZE],
            }
        }
    }
    impl Deref for Page {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            &self.bytes
        }
    }
    impl DerefMut for Page {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.bytes
        }
    }

    #[derive(Debug, Default)]
    pub struct Frame<T> {
        usage_count: u64,
        buffer: Rc<Buffer<T>>,
    }

    pub struct BufferPool<T> {
        buffers: Vec<Frame<T>>,
        next_victim_id: BufferId,
    }

    impl<T> Index<BufferId> for BufferPool<T> {
        type Output = Frame<T>;
        fn index(&self, index: BufferId) -> &Self::Output {
            &self.buffers[index.0]
        }
    }

    impl<T> IndexMut<BufferId> for BufferPool<T> {
        fn index_mut(&mut self, index: BufferId) -> &mut Self::Output {
            &mut self.buffers[index.0]
        }
    }

    impl<T> BufferPool<T>
    where
        T: Default,
    {
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
        pool: BufferPool<Page>,
        page_table: HashMap<PageId, BufferId>,
    }

    impl<T> BufferPoolManager<T>
    where
        T: DiskManagerDao,
    {
        pub fn new(disk: T, pool: BufferPool<Page>) -> Self {
            let page_table = HashMap::new();
            Self {
                disk,
                pool,
                page_table,
            }
        }
    }

    impl<T> BufferPoolManagerDao<Page> for BufferPoolManager<T>
    where
        T: DiskManagerDao,
    {
        fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer<Page>>, Error> {
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

        fn create_page(&mut self) -> Result<Rc<Buffer<Page>>, Error> {
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
        use super::{BufferPool, BufferPoolManager};
        use crate::buffer::dao::buffermanager::*;
        use crate::disk::disk::{self, DiskManager};
        use tempfile::tempfile;

        #[test]
        fn test_with_disk() {
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
