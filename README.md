# SSP — Solana Snapshot Parser

Stream, parse, and analyze Solana snapshots without storing them on disk.

## What it does

SSP discovers Solana RPC nodes, finds the fastest snapshot source, and streams the snapshot directly into a parsing pipeline — no 100GB+ local copy needed.

```
RPC Discovery → HTTP Stream → zstd decompress → tar unpack → AppendVec parse → Parquet → DuckDB
```

## Usage

```bash
# Stream from network (full snapshot, ~100GB)
cargo run --release -- --discover

# Stream incremental snapshot (~1GB, good for testing)
cargo run --release -- --discover --incremental

# Parse local file
cargo run --release -- --path snapshot.tar.zst

# With filters
cargo run --release -- --path snapshot.tar.zst --owner <base58> --pubkey <base58>
cargo run --release -- --discover --incremental --include-dead
```

### CLI flags

| Flag                | Description                               |
| ------------------- | ----------------------------------------- |
| `--path <file>`     | Parse a local `.tar.zst` snapshot         |
| `--discover`        | Find fastest RPC node and stream snapshot |
| `--incremental`     | Use incremental snapshot instead of full  |
| `--owner <base58>`  | Filter by account owner                   |
| `--pubkey <base58>` | Filter by account pubkey                  |
| `--hash <base58>`   | Filter by account hash                    |
| `--include-dead`    | Include dead accounts (lamports == 0)     |

## Benchmarks

109GB full snapshot (`snapshot-389758228.tar.zst`), M1-series Mac:

| Stage                               | Time  | Throughput |
| ----------------------------------- | ----- | ---------- |
| Single-threaded (parse + write)     | ~887s | ~123 MB/s  |
| Multi-threaded (2+ parquet writers) | ~146s | ~746 MB/s  |

**6.1x speedup** from multithreaded parquet writers. Bottleneck is zstd decompression (single-threaded, sequential tar).

Network streaming tested with incremental snapshot — 1.2M accounts parsed directly from HTTP stream, zero disk usage for the snapshot itself.

## Architecture

```
src/
├── main.rs      # CLI args, pipeline orchestration
├── parser.rs    # zstd → tar → AppendVec binary parsing (bytemuck)
├── filters.rs   # Account filters (owner/pubkey/hash, dead account filtering)
├── rpc.rs       # RPC node discovery, parallel probing, speed testing (async)
└── db.rs        # Arrow schema, RecordBatch building, DuckDB queries
```

**Key design decisions:**

- `bytemuck` for zero-copy binary parsing (like Zig's packed struct overlay)
- `crossbeam-channel` bounded channel for backpressure between parser and writers
- Async (`tokio`) only for RPC discovery (probing 300+ nodes concurrently). Rest is threads.
- Parser accepts `impl Read` — same code handles both local files and HTTP streams

## Roadmap

- [x] Streaming parser (zstd → tar → AppendVec)
- [x] Multi-threaded parquet writers (6x speedup)
- [x] DuckDB integration (top accounts query)
- [x] CLI filters (owner, pubkey, hash, dead accounts)
- [x] RPC node discovery with parallel probing and speed testing
- [x] Network streaming — parse directly from HTTP, zero disk usage
- [ ] Ratatui TUI
  - [x] Live progress bar during download/parse
  - [ ] Query interface
  - [ ] base58 display for pubkeys
- [ ] Incremental snapshot merging (deltas on top of full)
- [ ] Resume download on network failure(restart stream)
- [ ] CSV/JSON export
- [ ] ClickHouse backend option

## Status

- In Development.

## Checkpoint

- Data blobs parsing

## Stack

Rust 2024, bytemuck, zstd, tar, crossbeam, arrow/parquet, DuckDB, reqwest, tokio, clap
