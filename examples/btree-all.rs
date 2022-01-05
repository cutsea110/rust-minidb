use anyhow::Result;

use minidb::accessor::dao::accessmethod::{AccessMethod, Iterable};
use minidb::storage::dao::entity::PageId;

use minidb::rdbms::{
    btree::{BTree, SearchMode},
    clocksweep::{BufferPool, ClockSweepManager},
    disk::DiskManager,
};

fn main() -> Result<()> {
    let disk = DiskManager::open("test.btr")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = ClockSweepManager::new(disk, pool);

    let btree = BTree::new(PageId(0));
    let mut iter = btree.search(&mut bufmgr, SearchMode::Start)?;

    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        println!("{:02x?} = {:02x?}", key, value);
    }
    Ok(())
}
