use anyhow::Result;

use minidb::accessor::method::AccessMethod;
use minidb::buffer::manager::BufferPoolManager;

use minidb::rdbms::{btree::BTree, clocksweep::ClockSweepManager, disk::DiskManager};

fn main() -> Result<()> {
    let disk = DiskManager::open("test.btr")?;
    let mut bufmgr = ClockSweepManager::new(disk, 10);

    let btree = BTree::create(&mut bufmgr)?;

    btree.insert(&mut bufmgr, b"Kanagawa", b"Yokohama")?;
    btree.insert(&mut bufmgr, b"Osaka", b"Osaka")?;
    btree.insert(&mut bufmgr, b"Aichi", b"Nagoya")?;
    btree.insert(&mut bufmgr, b"Hokkaido", b"Sapporo")?;
    btree.insert(&mut bufmgr, b"Fukuoka", b"Fukuoka")?;
    btree.insert(&mut bufmgr, b"Hyogo", b"Kobe")?;

    bufmgr.flush()?;

    Ok(())
}
