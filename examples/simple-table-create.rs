use anyhow::Result;

use minidb::buffer::manager::BufferPoolManager;
use minidb::sql::ddl::table::Table;
use minidb::storage::entity::PageId;

use minidb::rdbms::{clocksweep::ClockSweepManager, disk::DiskManager, table::SimpleTable};

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.rly")?;
    let mut bufmgr = ClockSweepManager::new(disk, 10);

    let mut table = SimpleTable {
        meta_page_id: PageId(0),
        num_key_elems: 1,
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
