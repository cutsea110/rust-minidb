use anyhow::Result;

use minidb::accessor::{
    entity::SearchMode,
    method::{AccessMethod, Iterable},
};
use minidb::storage::entity::PageId;

use minidb::rdbms::{btree::BTree, clocksweep::ClockSweepManager, disk::DiskManager, util::tuple};

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.rly")?;
    let mut bufmgr = ClockSweepManager::new(disk, 10);

    let btree = BTree::new(PageId(0));
    let mut iter = btree.search(&mut bufmgr, SearchMode::Start)?;

    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        let mut record = vec![];
        tuple::decode(&key, &mut record);
        tuple::decode(&value, &mut record);
        println!("{:?}", tuple::Pretty(&record));
    }
    Ok(())
}
