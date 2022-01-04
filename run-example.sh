#!/bin/sh

rm test.btr large.btr simple.rly

cargo run --example btree-create

cargo run --example btree-query
cargo run --example btree-all
cargo run --example btree-range

cargo run --example btree-large --release
cargo run --example btree-large-query

cargo run --example simple-table-create
cargo run --example simple-table-all
cargo run --example simple-table-range
cargo run --example simple-table-scan
cargo run --example simple-table-exact

cargo run --example simple-table-plan
