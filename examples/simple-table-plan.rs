use anyhow::Result;

use minidb::buffer::clocksweep::{BufferPool, ClockSweepManager};
use minidb::buffer::dao::entity::PageId;
use minidb::executor::query::*;
use minidb::rdb::dml::dao::query::PlanNode;
use minidb::storage::disk::DiskManager;
use minidb::util::tuple;

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.rly")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = ClockSweepManager::new(disk, pool);

    let plan = Filter {
        cond: &|record| record[1].as_slice() < b"Dave",
        inner_plan: &SeqScan {
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
