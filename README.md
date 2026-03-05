# SSP — Solana Snapshot Parser

Stream, parse, and analyze Solana snapshots without storing them on disk.

## What it does

SSP discovers Solana RPC nodes, finds the fastest snapshot source, and streams the snapshot directly into a parsing pipeline — no 100GB+ local copy needed.

```
RPC Discovery → HTTP Stream → zstd decompress → custom tar → AppendVec parse → Token decode → Parquet → DuckDB
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

| Flag                | Description                                                      |
| ------------------- | ---------------------------------------------------------------- |
| `--path <file>`     | Parse a local `.tar.zst` snapshot                                |
| `--discover`        | Find fastest RPC node and stream snapshot                        |
| `--incremental`     | Use incremental snapshot instead of full                         |
| `--owner <base58>`  | Filter by account owner                                          |
| `--pubkey <base58>` | Filter by account pubkey                                         |
| `--hash <base58>`   | Filter by account hash                                           |
| `--include-dead`    | Include dead accounts (lamports == 0)                            |
| `--include-spam`    | Decode all mints/token accounts (bypass Jupiter verified filter) |
| `--bench`           | Run pipeline benchmarks (requires --path)                        |

## Architecture

3-stage multithreaded pipeline connected via bounded crossbeam channels:

```
Decompressor thread          4 Parser threads              Writers
─────────────────           ────────────────           ──────────────
zstd → custom tar    →     bytemuck overlay     →     2 account writers
     raw buffers            + filter                   2 decoded writers
                            + token decode             (parquet files)
                                                           │
                                                       DuckDB SQL
```

```
src/
├── main.rs                             # CLI, pipeline orchestration
├── lib.rs                              # Module declarations
├── parser.rs                           # Custom tar parser, AppendVec parsing, stream_raw()
├── filters.rs                          # Account filters (owner/pubkey/hash, dead filtering)
├── pubkey.rs                           # Pubkey type (32 bytes, bytemuck Pod, base58)
├── db.rs                               # Arrow schema, RecordBatch, DuckDB queries
├── rpc.rs                              # RPC node discovery, probing, speed testing (async)
├── bench.rs                            # Pipeline stage benchmarks
├── known_mints.rs                      # Jupiter verified token list (embedded)
├── decoders.rs                         # Decoder trait, COptionPubkey
└── decoders/
    └── token_program.rs                # Mint/TokenAccount structs, COptionU64
        ├── mint.rs                     # MintDecoder (82-byte accounts)
        └── token_account.rs            # TokenAccountDecoder (165-byte accounts)
```

### Key design decisions

- **Custom tar parser** — replaced `tar` crate for performance and buffer control (~830 MB/s peak)
- **bytemuck** for zero-copy binary parsing (like Zig's packed struct overlay)
- **Buffer pooling** — recycled `Vec<u8>` between decompressor and parsers
- **crossbeam-channel** bounded channels for backpressure
- **Decoder trait** — pluggable token decoding (Mint, TokenAccount), writes to separate parquet files
- Async (`tokio`) only for RPC discovery (probing 300+ nodes concurrently). Rest is threads.
- Parser accepts `impl Read` — same code handles both local files and HTTP streams

## Benchmarks

109GB full snapshot (`snapshot-389758228.tar.zst`), 1.05B accounts, M1-series Mac:

| Configuration                      | Throughput                          |
| ---------------------------------- | ----------------------------------- |
| Single-threaded (parse + write)    | ~123 MB/s                           |
| Multi-threaded pipeline + filtered | ~840 MB/s sustained, ~870 MB/s peak |

Spam filter uses Jupiter's verified token list (4550 mints, embedded at compile time). Only verified mints and their token accounts are decoded. Use `--include-spam` to bypass.

## Roadmap

- [x] Streaming parser (zstd → custom tar → AppendVec)
- [x] Multi-threaded pipeline (decompressor + 4 parsers + 2+2 writers)
- [x] Buffer pooling
- [x] Token Program decoding (Mint + TokenAccount)
- [x] DuckDB integration
- [x] CLI filters (owner, pubkey, hash, dead accounts)
- [x] RPC node discovery + network streaming
- [x] Spam filter (Jupiter verified token list, `--include-spam` to bypass)
- [ ] Architecture refactor: extract core as reusable library crate
- [ ] Ratatui TUI (on top of core crate)
- [ ] More decoders (System, Stake, Vote, Token-2022)
- [ ] Parallel multi-node download
- [ ] Incremental snapshot merging
- [ ] Resume on network failure

## Status

In development.

## Stack

Rust 2024, bytemuck, zstd, crossbeam, arrow/parquet, DuckDB, reqwest, tokio, clap
