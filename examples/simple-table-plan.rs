use anyhow::Result;

use minidb::rdbms::btree::BTree;
use minidb::sql::dml::query::PlanNode;
use minidb::storage::entity::PageId;

use minidb::rdbms::{clocksweep::ClockSweepManager, disk::DiskManager, query::*, util::tuple};

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.rly")?;
    let mut bufmgr = ClockSweepManager::new(disk, 10);
    let table_accessor = BTree::new(PageId(0));

    let plan = Filter {
        cond: &|record| record[1].as_slice() < b"Dave",
        inner_plan: &SeqScan {
            table_accessor: &table_accessor,
            table_meta_page_id: PageId(0),
            search_mode: TupleSearchMode::Key(&[b"w"]),
            while_cond: &|pkey| pkey[0].as_slice() < b"z",
        },
    };
    let mut exec = plan.start(&mut bufmgr)?;

    while let Some(record) = exec.next(&mut bufmgr)? {
        println!("{:?}", tuple::Pretty(&record));
    }
    Ok(())
}
