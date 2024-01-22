#!/bin/sh

FILE=$1
BIN="target/release/onebrc"

cargo test -q
cargo build --release -q

hyperfine --warmup 1 -N \
          -L parser chunk,memory-mapped \
          -L processor sequential,parallel-rayon \
          "$BIN --parser {parser} --processor {processor} -q $FILE" \
          --export-markdown results.md