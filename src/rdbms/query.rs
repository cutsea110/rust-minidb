use anyhow::Result;

use super::util::tuple;
use crate::accessor::{
    entity::SearchMode,
    method::{AccessMethod, BoxedAccessMethod, HaveAccessMethod, Iterable},
};
use crate::buffer::manager::BufferPoolManager;
use crate::sql::dml::{entity::Tuple, query::*};
use crate::storage::entity::PageId;

use super::btree::{self, BTree};

pub type TupleSlice<'a> = &'a [Vec<u8>];

pub enum TupleSearchMode<'a> {
    Start,
    Key(&'a [&'a [u8]]),
}

impl<'a> TupleSearchMode<'a> {
    fn encode(&self) -> SearchMode {
        match self {
            TupleSearchMode::Start => SearchMode::Start,
            TupleSearchMode::Key(tuple) => {
                let mut key = vec![];
                tuple::encode(tuple.iter(), &mut key);
                SearchMode::Key(key)
            }
        }
    }
}

pub struct SeqScan<'a> {
    pub table_accessor: BTree,
    pub table_meta_page_id: PageId,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager> HaveAccessMethod<T> for SeqScan<'a> {
    type Iter = btree::Iter;

    fn table_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter>> {
        Some(Box::new(&self.table_accessor))
    }
    fn index_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter>> {
        None
    }
}

impl<'a, T: BufferPoolManager> PlanNode<T> for SeqScan<'a> {
    fn start(&self, bufmgr: &mut T) -> Result<BoxExecutor<T>> {
        let table_iter = self
            .table_accessor()
            .unwrap()
            .search(bufmgr, self.search_mode.encode())?;
        Ok(Box::new(ExecSeqScan {
            table_iter,
            while_cond: self.while_cond,
        }))
    }
}

pub struct ExecSeqScan<'a> {
    table_iter: btree::Iter,
    while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager> Executor<T> for ExecSeqScan<'a> {
    fn next(&mut self, bufmgr: &mut T) -> Result<Option<Tuple>> {
        let (pkey_bytes, tuple_bytes) = match self.table_iter.next(bufmgr)? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let mut pkey = vec![];
        tuple::decode(&pkey_bytes, &mut pkey);
        if !(self.while_cond)(&pkey) {
            return Ok(None);
        }
        let mut tuple = pkey;
        tuple::decode(&tuple_bytes, &mut tuple);
        Ok(Some(tuple))
    }
}

pub struct Filter<'a, T: BufferPoolManager> {
    pub inner_plan: &'a dyn PlanNode<T, Iter = btree::Iter>,
    pub cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager> HaveAccessMethod<T> for Filter<'a, T> {
    type Iter = btree::Iter;

    fn table_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter>> {
        None
    }
    fn index_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter>> {
        None
    }
}

impl<'a, T: BufferPoolManager> PlanNode<T> for Filter<'a, T> {
    fn start(&self, bufmgr: &mut T) -> Result<BoxExecutor<T>> {
        let inner_iter = self.inner_plan.start(bufmgr)?;
        Ok(Box::new(ExecFilter {
            inner_iter,
            cond: self.cond,
        }))
    }
}

pub struct ExecFilter<'a, T: BufferPoolManager> {
    inner_iter: BoxExecutor<'a, T>,
    cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager> Executor<T> for ExecFilter<'a, T> {
    fn next(&mut self, bufmgr: &mut T) -> Result<Option<Tuple>> {
        loop {
            match self.inner_iter.next(bufmgr)? {
                Some(tuple) => {
                    if (self.cond)(&tuple) {
                        return Ok(Some(tuple));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}

pub struct IndexScan<'a> {
    pub table_accessor: BTree,
    pub table_meta_page_id: PageId,
    pub index_accessor: BTree,
    pub index_meta_page_id: PageId,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager> HaveAccessMethod<T> for IndexScan<'a> {
    type Iter = btree::Iter;

    fn table_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter>> {
        Some(Box::new(&self.table_accessor))
    }
    fn index_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter>> {
        Some(Box::new(&self.index_accessor))
    }
}

impl<'a, T: BufferPoolManager> PlanNode<T> for IndexScan<'a> {
    fn start(&self, bufmgr: &mut T) -> Result<BoxExecutor<T>> {
        let table_btree = BTree::new(self.table_meta_page_id);
        let index_iter = self
            .index_accessor()
            .unwrap()
            .search(bufmgr, self.search_mode.encode())?;
        Ok(Box::new(ExecIndexScan {
            table_btree,
            index_iter,
            while_cond: self.while_cond,
        }))
    }
}

pub struct ExecIndexScan<'a> {
    table_btree: BTree,
    index_iter: btree::Iter,
    while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager> Executor<T> for ExecIndexScan<'a> {
    fn next(&mut self, bufmgr: &mut T) -> Result<Option<Tuple>> {
        let (skey_bytes, pkey_bytes) = match self.index_iter.next(bufmgr)? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let mut skey = vec![];
        tuple::decode(&skey_bytes, &mut skey);
        if !(self.while_cond)(&skey) {
            return Ok(None);
        }
        let mut table_iter = self
            .table_btree
            .search(bufmgr, SearchMode::Key(pkey_bytes))?;
        let (pkey_bytes, tuple_bytes) = table_iter.next(bufmgr)?.unwrap();
        let mut tuple = vec![];
        tuple::decode(&pkey_bytes, &mut tuple);
        tuple::decode(&tuple_bytes, &mut tuple);
        Ok(Some(tuple))
    }
}

pub struct IndexOnlyScan<'a> {
    pub index_accessor: BTree,
    pub index_meta_page_id: PageId,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager> HaveAccessMethod<T> for IndexOnlyScan<'a> {
    type Iter = btree::Iter;

    fn table_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter>> {
        None
    }
    fn index_accessor(&self) -> Option<BoxedAccessMethod<T, Self::Iter>> {
        Some(Box::new(&self.index_accessor))
    }
}

impl<'a, T: BufferPoolManager> PlanNode<T> for IndexOnlyScan<'a> {
    fn start(&self, bufmgr: &mut T) -> Result<BoxExecutor<T>> {
        let index_iter = self
            .index_accessor()
            .unwrap()
            .search(bufmgr, self.search_mode.encode())?;
        Ok(Box::new(ExecIndexOnlyScan {
            index_iter,
            while_cond: self.while_cond,
        }))
    }
}

pub struct ExecIndexOnlyScan<'a> {
    index_iter: btree::Iter,
    while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager> Executor<T> for ExecIndexOnlyScan<'a> {
    fn next(&mut self, bufmgr: &mut T) -> Result<Option<Tuple>> {
        let (skey_bytes, pkey_bytes) = match self.index_iter.next(bufmgr)? {
            Some(pair) => pair,
            None => return Ok(None),
        };
        let mut skey = vec![];
        tuple::decode(&skey_bytes, &mut skey);
        if !(self.while_cond)(&skey) {
            return Ok(None);
        }
        let mut tuple = skey;
        tuple::decode(&pkey_bytes, &mut tuple);
        Ok(Some(tuple))
    }
}
