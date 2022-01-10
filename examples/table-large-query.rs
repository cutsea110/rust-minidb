use anyhow::Result;

use minidb::rdbms::btree::BTree;
use minidb::sql::dml::query::PlanNode;
use minidb::storage::entity::PageId;

use minidb::rdbms::{clocksweep::ClockSweepManager, disk::DiskManager, query::*, util::tuple};

fn main() -> Result<()> {
    let disk = DiskManager::open("table_large.rly")?;
    let mut bufmgr = ClockSweepManager::new(disk, 10);
    let table_accessor = BTree::new(PageId(0));
    let index_accessor = BTree::new(PageId(2));

    let plan = IndexScan {
        table_accessor: &table_accessor,
        table_meta_page_id: PageId(0),
        index_accessor: &index_accessor,
        index_meta_page_id: PageId(2),
        search_mode: TupleSearchMode::Key(&[b"Smith"]),
        while_cond: &|skey| skey[0].as_slice() == b"Smith",
    };
    let mut exec = plan.start(&mut bufmgr)?;

    while let Some(record) = exec.next(&mut bufmgr)? {
        println!("{:?}", tuple::Pretty(&record));
    }
    Ok(())
}
