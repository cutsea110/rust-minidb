use anyhow::Result;
use md5::{Digest, Md5};

use minidb::accessor::method::AccessMethod;
use minidb::buffer::manager::BufferPoolManager;

use minidb::rdbms::{btree::BTree, clocksweep::ClockSweepManager, disk::DiskManager};

const NUM_PAIRS: u32 = 1_000_000;

fn main() -> Result<()> {
    let disk = DiskManager::open("large.btr")?;
    let mut bufmgr = ClockSweepManager::new(disk, 100);

    let btree = BTree::create(&mut bufmgr)?;
    for i in 1u32..=NUM_PAIRS {
        let pkey = i.to_be_bytes();
        let md5 = Md5::digest(&pkey);
        btree.insert(&mut bufmgr, &md5[..], &pkey[..])?;
    }
    bufmgr.flush()?;

    Ok(())
}
