use anyhow::Result;

use minidb::accessor::btree::{AccessMethod, BTree, Iterable, SearchMode};
use minidb::buffer::clocksweep::{BufferPool, ClockSweepManager};
use minidb::buffer::dao::entity::PageId;
use minidb::storage::disk::DiskManager;

fn main() -> Result<()> {
    let disk = DiskManager::open("test.btr")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = ClockSweepManager::new(disk, pool);

    let btree = BTree::new(PageId(0));
    let mut iter = btree.search(&mut bufmgr, SearchMode::Key(b"Gifu".to_vec()))?;

    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        println!("{:02x?} = {:02x?}", key, value);
    }
    Ok(())
}
