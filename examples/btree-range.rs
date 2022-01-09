use anyhow::Result;

use minidb::accessor::{
    entity::SearchMode,
    method::{AccessMethod, Iterable},
};
use minidb::storage::entity::PageId;

use minidb::rdbms::{btree::BTree, clocksweep::ClockSweepManager, disk::DiskManager};

fn main() -> Result<()> {
    let disk = DiskManager::open("test.btr")?;
    let mut bufmgr = ClockSweepManager::new(disk, 10);

    let btree = BTree::new(PageId(0));
    let mut iter = btree.search(&mut bufmgr, SearchMode::Key(b"Gifu".to_vec()))?;

    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        println!("{:02x?} = {:02x?}", key, value);
    }
    Ok(())
}
