use anyhow::Result;

use super::util::tuple;
use crate::accessor::method::AccessMethod;
use crate::buffer::manager::BufferPoolManager;
use crate::sql::ddl::table::{Table as ITable, UniqueIndex as IUniqueIndex};
use crate::storage::entity::PageId;

use super::btree::BTree;

#[derive(Debug)]
pub struct SimpleTable {
    pub meta_page_id: PageId,
    pub num_key_elems: usize,
}

impl<T: BufferPoolManager> ITable<T> for SimpleTable {
    fn create(&mut self, bufmgr: &mut T) -> Result<()> {
        let btree = BTree::create(bufmgr)?;
        self.meta_page_id = btree.meta_page_id;
        Ok(())
    }

    fn insert(&self, bufmgr: &mut T, record: &[&[u8]]) -> Result<()> {
        let btree = BTree::new(self.meta_page_id);
        let mut key = vec![];
        tuple::encode(record[..self.num_key_elems].iter(), &mut key);
        let mut value = vec![];
        tuple::encode(record[self.num_key_elems..].iter(), &mut value);
        btree.insert(bufmgr, &key, &value)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Table {
    pub meta_page_id: PageId,
    pub num_key_elems: usize,
    pub unique_indices: Vec<self::UniqueIndex>,
}

impl<T: BufferPoolManager> ITable<T> for self::Table {
    fn create(&mut self, bufmgr: &mut T) -> Result<()> {
        let btree = BTree::create(bufmgr)?;
        self.meta_page_id = btree.meta_page_id;
        for unique_index in &mut self.unique_indices {
            unique_index.create(bufmgr)?;
        }
        Ok(())
    }

    fn insert(&self, bufmgr: &mut T, record: &[&[u8]]) -> Result<()> {
        let btree = BTree::new(self.meta_page_id);
        let mut key = vec![];
        tuple::encode(record[..self.num_key_elems].iter(), &mut key);
        let mut value = vec![];
        tuple::encode(record[self.num_key_elems..].iter(), &mut value);
        btree.insert(bufmgr, &key, &value)?;
        for unique_index in &self.unique_indices {
            unique_index.insert(bufmgr, &key, record)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct UniqueIndex {
    pub meta_page_id: PageId,
    pub skey: Vec<usize>,
}

impl<T: BufferPoolManager> IUniqueIndex<T> for UniqueIndex {
    fn create(&mut self, bufmgr: &mut T) -> Result<()> {
        let btree = BTree::create(bufmgr)?;
        self.meta_page_id = btree.meta_page_id;
        Ok(())
    }

    fn insert(&self, bufmgr: &mut T, pkey: &[u8], record: &[impl AsRef<[u8]>]) -> Result<()> {
        let btree = BTree::new(self.meta_page_id);
        let mut skey = vec![];
        tuple::encode(
            self.skey.iter().map(|&index| record[index].as_ref()),
            &mut skey,
        );
        btree.insert(bufmgr, &skey, pkey)?;
        Ok(())
    }
}
