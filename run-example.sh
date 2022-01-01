#!/bin/sh

rm test.btr large.btr

cargo run --example btree-create

cargo run --example btree-query
cargo run --example btree-all
cargo run --example btree-range

cargo run --example btree-large --release
cargo run --example btree-large-query

