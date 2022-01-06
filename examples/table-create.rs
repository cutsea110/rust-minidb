use anyhow::Result;

use minidb::buffer::manager::BufferPoolManager;
use minidb::sql::ddl::table::Table as ITable;
use minidb::storage::entity::PageId;

use minidb::rdbms::{
    clocksweep::{BufferPool, ClockSweepManager},
    disk::DiskManager,
    table::{Table, UniqueIndex},
};

fn main() -> Result<()> {
    let disk = DiskManager::open("table.rly")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = ClockSweepManager::new(disk, pool);

    let mut table = Table {
        meta_page_id: PageId(0),
        num_key_elems: 1,
        unique_indices: vec![UniqueIndex {
            meta_page_id: PageId::INVALID_PAGE_ID,
            skey: vec![2], // last_name
        }],
    };
    table.create(&mut bufmgr)?;
    dbg!(&table);
    table.insert(&mut bufmgr, &[b"z", b"Alice", b"Smith"])?;
    table.insert(&mut bufmgr, &[b"x", b"Bob", b"Johnson"])?;
    table.insert(&mut bufmgr, &[b"y", b"Charlie", b"Williams"])?;
    table.insert(&mut bufmgr, &[b"w", b"Dave", b"Miller"])?;
    table.insert(&mut bufmgr, &[b"v", b"Eve", b"Brown"])?;

    bufmgr.flush()?;
    Ok(())
}
