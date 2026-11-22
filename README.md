# Shopping Sessions entry

A native pipeline for the Builders Buildathon Shopping Sessions track, written in Rust with the standard library only.

- Reads `$DATA_DIR` (default `/data`), writes `$OUTPUT_DIR/user_daily/` and `$OUTPUT_DIR/campaign_daily/` (default `/output`) as Parquet, using `$NCPU` threads.
- `docker build -t shopping-sessions .` then run it the way the scorer does. The image's `CMD` takes no arguments.
- Local run without Docker: `cargo build --release && target/release/sessions DATA_DIR OUTPUT_DIR`.

How it works, in order:

1. Links: parsed in parallel, each id interned in its own shard, then union-find over integer ids; each set is named by its byte-wise smallest id.
2. Events, first pass: every event id is hashed and marked in a 2-bit filter (seen once, seen twice). Only ids marked twice can be duplicates.
3. Events, second pass: parse and validate. An event whose id was seen once goes straight to its person's shard. The rest (all real duplicates and a few others) take an exact dedup by event id that keeps the smallest `ingest_ts`, with ids compared byte for byte.
4. Clicks go straight to their person's shard; copies of a click are identical, so they end up next to each other once sorted and only one is kept.
5. Per person: sessions, last-click credit and assists, then Parquet.

Ids of up to 15 bytes are compared by their bytes inline. Longer ids use a 128-bit fingerprint, and every match is confirmed byte for byte, with an exact fallback if two ever share one. There is no limit on id length or file count.

Timestamps take a fast path. A 20- or 25-byte F1/F2 value is checked 8 bytes at a time, and its day number comes from a closed-form formula. A 1- to 16-byte F3 value is checked and converted 8 digits at a time. Every other length goes to the plain parser, as does the manifest. A unit test (`cargo test --release`) checks that both parsers give the same result on edge dates and years 0 to 9999, invalid neighbours, mutated strings and digit strings of 1 to 25 bytes.

All code here is original to this entry. MIT licensed.
