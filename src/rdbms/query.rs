use anyhow::Result;

use super::util::tuple;
use crate::accessor::{
    entity::SearchMode,
    method::{AccessMethod, HaveAccessMethod, Iterable},
};
use crate::buffer::manager::BufferPoolManager;
use crate::sql::dml::{entity::Tuple, query::*};

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

pub struct SeqScan<'a, T: BufferPoolManager, U: Iterable<T>> {
    pub table_accessor: &'a dyn AccessMethod<T, Iterable = U>,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager, U: Iterable<T>> HaveAccessMethod<T> for SeqScan<'a, T, U> {
    type Iter = U;

    fn table_accessor(&self) -> Option<Box<&'a dyn AccessMethod<T, Iterable = Self::Iter>>> {
        Some(Box::new(self.table_accessor))
    }
    fn index_accessor(&self) -> Option<Box<&'a dyn AccessMethod<T, Iterable = Self::Iter>>> {
        None
    }
}

impl<'a, T: BufferPoolManager, U: 'static + Iterable<T>> PlanNode<T> for SeqScan<'a, T, U> {
    fn start(&self, bufmgr: &mut T) -> Result<BoxExecutor<T>> {
        let table_iter = self
            .table_accessor()
            .unwrap()
            .search(bufmgr, self.search_mode.encode())?;
        Ok(Box::new(ExecSeqScan {
            table_iter: Box::new(table_iter),
            while_cond: self.while_cond,
        }))
    }
}

pub struct ExecSeqScan<'a, T: BufferPoolManager> {
    table_iter: Box<dyn Iterable<T>>,
    while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager> Executor<T> for ExecSeqScan<'a, T> {
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

pub struct Filter<'a, T: BufferPoolManager, U: Iterable<T>> {
    pub inner_plan: &'a dyn PlanNode<T, Iter = U>,
    pub cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager, U: Iterable<T>> HaveAccessMethod<T> for Filter<'a, T, U> {
    type Iter = U;

    fn table_accessor(&self) -> Option<Box<&'a dyn AccessMethod<T, Iterable = Self::Iter>>> {
        None
    }
    fn index_accessor(&self) -> Option<Box<&'a dyn AccessMethod<T, Iterable = Self::Iter>>> {
        None
    }
}

impl<'a, T: BufferPoolManager, U: Iterable<T>> PlanNode<T> for Filter<'a, T, U> {
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

pub struct IndexScan<'a, T: BufferPoolManager, U: Iterable<T>> {
    pub table_accessor: &'a dyn AccessMethod<T, Iterable = U>,
    pub index_accessor: &'a dyn AccessMethod<T, Iterable = U>,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager, U: Iterable<T>> HaveAccessMethod<T> for IndexScan<'a, T, U> {
    type Iter = U;

    fn table_accessor(&self) -> Option<Box<&'a dyn AccessMethod<T, Iterable = Self::Iter>>> {
        Some(Box::new(self.table_accessor))
    }
    fn index_accessor(&self) -> Option<Box<&'a dyn AccessMethod<T, Iterable = Self::Iter>>> {
        Some(Box::new(self.index_accessor))
    }
}

impl<'a, T: BufferPoolManager, U: Iterable<T>> PlanNode<T> for IndexScan<'a, T, U> {
    fn start(&self, bufmgr: &mut T) -> Result<BoxExecutor<T>> {
        let table_btree = *self.table_accessor().unwrap();
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

pub struct ExecIndexScan<'a, T: BufferPoolManager, U: Iterable<T>> {
    table_btree: &'a dyn AccessMethod<T, Iterable = U>,
    index_iter: U,
    while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager, U: Iterable<T>> Executor<T> for ExecIndexScan<'a, T, U> {
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

pub struct IndexOnlyScan<'a, T: BufferPoolManager, U: Iterable<T>> {
    pub index_accessor: &'a dyn AccessMethod<T, Iterable = U>,
    pub search_mode: TupleSearchMode<'a>,
    pub while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager, U: Iterable<T>> HaveAccessMethod<T> for IndexOnlyScan<'a, T, U> {
    type Iter = U;

    fn table_accessor(&self) -> Option<Box<&'a dyn AccessMethod<T, Iterable = Self::Iter>>> {
        None
    }
    fn index_accessor(&self) -> Option<Box<&'a dyn AccessMethod<T, Iterable = Self::Iter>>> {
        Some(Box::new(self.index_accessor))
    }
}

impl<'a, T: BufferPoolManager, U: 'static + Iterable<T>> PlanNode<T> for IndexOnlyScan<'a, T, U> {
    fn start(&self, bufmgr: &mut T) -> Result<BoxExecutor<T>> {
        let index_iter = self
            .index_accessor()
            .unwrap()
            .search(bufmgr, self.search_mode.encode())?;
        Ok(Box::new(ExecIndexOnlyScan {
            index_iter: Box::new(index_iter),
            while_cond: self.while_cond,
        }))
    }
}

pub struct ExecIndexOnlyScan<'a, T: BufferPoolManager> {
    index_iter: Box<dyn Iterable<T>>,
    while_cond: &'a dyn Fn(TupleSlice) -> bool,
}

impl<'a, T: BufferPoolManager> Executor<T> for ExecIndexOnlyScan<'a, T> {
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::accessor::{entity::SearchMode, method};
    use crate::buffer::{
        entity::Buffer,
        manager::{BufferPoolManager, Error},
    };
    use crate::storage::entity::PageId;
    use std::rc::Rc;

    struct Empty {}
    impl BufferPoolManager for Empty {
        fn fetch_page(&mut self, _: PageId) -> Result<Rc<Buffer>, Error> {
            panic!("Not implement!")
        }
        fn create_page(&mut self) -> Result<Rc<Buffer>, Error> {
            panic!("Not implement!")
        }
        fn flush(&mut self) -> Result<(), Error> {
            panic!("Not implement!")
        }
    }

    struct Counter {
        next: u8,
    }
    impl Counter {
        fn new(init: u8) -> Self {
            Self { next: init }
        }
    }
    impl Iterable<Empty> for Counter {
        fn next(&mut self, _: &mut Empty) -> Result<Option<(Vec<u8>, Vec<u8>)>, method::Error> {
            let c = self.next;
            if c == u8::MAX {
                return Ok(None);
            } else {
                self.next += 1;
                let mut key = vec![];
                tuple::encode(vec![&[c]].iter(), &mut key);
                let mut val = vec![];
                tuple::encode(vec![&[c]].iter(), &mut val);
                Ok(Some((key, val)))
            }
        }
    }

    struct Generate {}
    impl AccessMethod<Empty> for Generate {
        type Iterable = Counter;
        fn search(
            &self,
            _: &mut Empty,
            search_option: SearchMode,
        ) -> Result<Self::Iterable, method::Error> {
            match search_option {
                SearchMode::Start => Ok(Counter::new(0)),
                SearchMode::Key(n) => Ok(Counter::new(n[0])),
            }
        }
        fn insert(&self, _: &mut Empty, _: &[u8], _: &[u8]) -> Result<(), method::Error> {
            panic!("Not implement!")
        }
    }

    #[test]
    fn seq_scan_test() {
        let mut bufmgr = Empty {};
        {
            let plan = SeqScan {
                table_accessor: &Generate {},
                search_mode: TupleSearchMode::Start,
                while_cond: &|_| true,
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let first = res1.unwrap().unwrap();
            assert_eq!(first, vec![&[0], &[0]]);

            let res2 = exec.next(&mut bufmgr);
            let second = res2.unwrap().unwrap();
            assert_eq!(second, vec![&[1], &[1]]);
        }
        {
            let plan = SeqScan {
                table_accessor: &Generate {},
                search_mode: TupleSearchMode::Key(&[&[42u8]]),
                while_cond: &|_| true,
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let first = res1.unwrap().unwrap();
            assert_eq!(first, vec![&[42], &[42]]);

            let res2 = exec.next(&mut bufmgr);
            let second = res2.unwrap().unwrap();
            assert_eq!(second, vec![&[43], &[43]]);
        }
        {
            let plan = SeqScan {
                table_accessor: &Generate {},
                search_mode: TupleSearchMode::Key(&[&[42u8]]),
                while_cond: &|_| false,
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let nodata = res1.unwrap();
            assert!(nodata.is_none());
        }
    }
    #[test]
    fn filter_test() {
        let mut bufmgr = Empty {};
        {
            let is_odd = |n: u8| n % 2 == 1;
            let plan = Filter {
                cond: &|record| is_odd(record[1].as_slice()[0]),
                inner_plan: &SeqScan {
                    table_accessor: &Generate {},
                    search_mode: TupleSearchMode::Start,
                    while_cond: &|_| true,
                },
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let first = res1.unwrap().unwrap();
            assert_eq!(first, vec![&[1], &[1]]);

            let res2 = exec.next(&mut bufmgr);
            let second = res2.unwrap().unwrap();
            assert_eq!(second, vec![&[3], &[3]]);
        }
        {
            let plan = Filter {
                cond: &|record| record[1].as_slice() < &[44u8],
                inner_plan: &SeqScan {
                    table_accessor: &Generate {},
                    search_mode: TupleSearchMode::Key(&[&[42u8]]),
                    while_cond: &|_| true,
                },
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let first = res1.unwrap().unwrap();
            assert_eq!(first, vec![&[42], &[42]]);

            let res2 = exec.next(&mut bufmgr);
            let second = res2.unwrap().unwrap();
            assert_eq!(second, vec![&[43], &[43]]);

            let res3 = exec.next(&mut bufmgr);
            let nodata = res3.unwrap();
            assert!(nodata.is_none());
        }
    }
    #[test]
    fn index_scan_test() {
        let mut bufmgr = Empty {};
        {
            let plan = IndexScan {
                table_accessor: &Generate {},
                index_accessor: &Generate {},
                search_mode: TupleSearchMode::Start,
                while_cond: &|_| true,
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let first = res1.unwrap().unwrap();
            assert_eq!(first, vec![&[0], &[0]]);

            let res2 = exec.next(&mut bufmgr);
            let second = res2.unwrap().unwrap();
            assert_eq!(second, vec![&[1], &[1]]);
        }
        {
            let plan = IndexScan {
                table_accessor: &Generate {},
                index_accessor: &Generate {},
                search_mode: TupleSearchMode::Key(&[&[42u8]]),
                while_cond: &|_| true,
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let first = res1.unwrap().unwrap();
            assert_eq!(first, vec![&[42], &[42]]);

            let res2 = exec.next(&mut bufmgr);
            let second = res2.unwrap().unwrap();
            assert_eq!(second, vec![&[43], &[43]]);
        }
        {
            let plan = IndexScan {
                table_accessor: &Generate {},
                index_accessor: &Generate {},
                search_mode: TupleSearchMode::Key(&[&[42u8]]),
                while_cond: &|_| false,
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let nodata = res1.unwrap();
            assert!(nodata.is_none());
        }
    }
    #[test]
    fn index_only_scan_test() {
        let mut bufmgr = Empty {};
        {
            let plan = IndexOnlyScan {
                index_accessor: &Generate {},
                search_mode: TupleSearchMode::Start,
                while_cond: &|_| true,
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let first = res1.unwrap().unwrap();
            assert_eq!(first, vec![&[0], &[0]]);

            let res2 = exec.next(&mut bufmgr);
            let second = res2.unwrap().unwrap();
            assert_eq!(second, vec![&[1], &[1]]);
        }
        {
            let plan = IndexOnlyScan {
                index_accessor: &Generate {},
                search_mode: TupleSearchMode::Key(&[&[42u8]]),
                while_cond: &|_| true,
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let first = res1.unwrap().unwrap();
            assert_eq!(first, vec![&[42], &[42]]);

            let res2 = exec.next(&mut bufmgr);
            let second = res2.unwrap().unwrap();
            assert_eq!(second, vec![&[43], &[43]]);
        }
        {
            let plan = IndexOnlyScan {
                index_accessor: &Generate {},
                search_mode: TupleSearchMode::Key(&[&[42u8]]),
                while_cond: &|_| false,
            };
            let mut exec = plan.start(&mut bufmgr).unwrap();

            let res1 = exec.next(&mut bufmgr);
            let nodata = res1.unwrap();
            assert!(nodata.is_none());
        }
    }
}
