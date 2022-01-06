use anyhow::Result;
use md5::Md5;
use sha1::{Digest, Sha1};

use minidb::buffer::manager::BufferPoolManager;
use minidb::sql::ddl::table::Table;
use minidb::storage::entity::PageId;

const NUM_ROWS: u32 = 10_000_000;

use minidb::rdbms::{
    clocksweep::{BufferPool, ClockSweepManager},
    disk::DiskManager,
    table::{UIdxTable, UIndex},
};

fn main() -> Result<()> {
    let disk = DiskManager::open("table_large.rly")?;
    let pool = BufferPool::new(1_000_000);
    let mut bufmgr = ClockSweepManager::new(disk, pool);

    let mut table = UIdxTable {
        meta_page_id: PageId(0),
        num_key_elems: 1,
        unique_indices: vec![UIndex {
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
    for i in 0u32..NUM_ROWS {
        let pkey = i.to_be_bytes();
        let md5 = Md5::digest(&pkey);
        let sha1 = Sha1::digest(&pkey);
        table.insert(&mut bufmgr, &[&pkey[..], &md5[..], &sha1[..]])?;
    }

    bufmgr.flush()?;
    Ok(())
}
