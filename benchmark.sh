#!/bin/sh

FILE=$1
cargo build --release
hyperfine --warmup 1 -N "target/release/rust-billion-row-challenge --input /Users/sguimmara/Documents/git/1brc/data/$FILE.csv --method fd -q"