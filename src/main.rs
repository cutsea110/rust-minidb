use anyhow::Result;

use minidb::buffer::manager::BufferPoolManager;
use minidb::sql::{ddl::table::Table as ITable, dml::query::*};
use minidb::storage::entity::PageId;

use minidb::rdbms::{
    btree::*, clocksweep::ClockSweepManager, disk::DiskManager, query::*, table::*, util::tuple,
};

fn main() -> Result<()> {
    // config
    let disk = DiskManager::open("sample-db.rly")?;
    let mut bufmgr = ClockSweepManager::new(disk, 10);

    let mut table = Table {
        meta_page_id: PageId(0),
        num_key_elems: 1,
        unique_indices: vec![UniqueIndex {
            meta_page_id: PageId::INVALID_PAGE_ID,
            skey: vec![2], // last_name
        }],
    };

    // init db
    table.create(&mut bufmgr)?;
    dbg!(&table);
    table.insert(&mut bufmgr, &[b"z", b"Alice", b"Smith"])?;
    table.insert(&mut bufmgr, &[b"x", b"Bob", b"Johnson"])?;
    table.insert(&mut bufmgr, &[b"y", b"Charlie", b"Williams"])?;
    table.insert(&mut bufmgr, &[b"w", b"Dave", b"Miller"])?;
    table.insert(&mut bufmgr, &[b"v", b"Eve", b"Brown"])?;

    bufmgr.flush()?;

    // query
    let table_accessor = &BTree::new(PageId(0));
    let index_accessor = &BTree::new(PageId(2));
    let plan = IndexScan {
        table_accessor,
        index_accessor,
        search_mode: TupleSearchMode::Key(&[b"Smith"]),
        while_cond: &|skey| skey[0].as_slice() == b"Smith",
    };
    let mut exec = plan.start(&mut bufmgr)?;

    while let Some(record) = exec.next(&mut bufmgr)? {
        println!("{:?}", tuple::Pretty(&record));
    }

    Ok(())
}
