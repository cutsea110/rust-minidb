use anyhow::Result;

use minidb::accessor::method::{AccessMethod, Iterable};
use minidb::storage::entity::PageId;

use minidb::rdbms::{
    btree::{BTree, SearchMode},
    clocksweep::ClockSweepManager,
    disk::DiskManager,
    util::tuple,
};

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.rly")?;
    let mut bufmgr = ClockSweepManager::new(disk, 10);

    let btree = BTree::new(PageId(0));
    let mut search_key = vec![];
    tuple::encode([b"y"].iter(), &mut search_key);
    let mut iter = btree.search(&mut bufmgr, SearchMode::Key(search_key))?;

    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        let mut record = vec![];
        tuple::decode(&key, &mut record);
        if record[0] != b"y" {
            break;
        }
        tuple::decode(&value, &mut record);
        println!("{:?}", tuple::Pretty(&record));
    }
    Ok(())
}
