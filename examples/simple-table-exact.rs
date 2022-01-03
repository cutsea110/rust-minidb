use anyhow::Result;

use minidb::accessor::btree::{BTree, SearchMode};
use minidb::buffer::clocksweep::{BufferPool, ClockSweepManager};
use minidb::buffer::dao::entity::PageId;
use minidb::executor::dao::accessmethod::{AccessMethod, Iterable};
use minidb::storage::disk::DiskManager;
use minidb::table::SimpleTable;
use minidb::tuple;

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.rly")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = ClockSweepManager::new(disk, pool);

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
