#!/bin/sh

FILE=$1
BIN="target/release/onebrc"

cargo test -q
cargo build --release -q

hyperfine --warmup 1 -N \
          -L reader memory-mapped \
          -L parser naive,vectorized \
          -L processor sequential,parallel-bulk \
          "$BIN --reader {reader} --parser {parser} --processor {processor} -q $FILE" \
          --export-markdown results.md