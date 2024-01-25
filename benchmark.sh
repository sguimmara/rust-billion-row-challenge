#!/bin/sh

FILE=$1
BIN="target/release/onebrc"

cargo test -q
cargo build --release -q

hyperfine --warmup 1 -N \
          -L processor sequential,parallel-channel \
          "$BIN --processor {processor} -q $FILE"