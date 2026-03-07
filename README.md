# SSP — Solana Snapshot Parser

Stream, parse, and analyze Solana snapshots without storing them on disk.

## What it does

SSP discovers Solana RPC nodes, finds the fastest snapshot source, and streams the snapshot directly into a parsing pipeline — no 100GB+ local copy needed.

```
RPC Discovery → HTTP Stream → zstd decompress → custom tar → AppendVec parse → Token decode → Parquet → DuckDB
```

## Usage

```bash
ssp                                     # interactive TUI (setup wizard → processing → query)
ssp --discover                          # CLI mode: stream full snapshot from fastest RPC node
ssp --discover --incremental            # CLI mode: stream incremental snapshot (~1GB)
ssp --path snapshot.tar.zst             # CLI mode: parse local file
ssp --path snapshot.tar.zst --owner <base58> --pubkey <base58>
```

Running `ssp` without flags opens an interactive TUI with source selection, file browser, filter toggles, live processing stats, and a DuckDB query interface with presets. With `--path` or `--discover`, it runs as a headless CLI pipeline.

### Flags

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
| `--bench`           | Run pipeline benchmarks (requires `--path`)                      |

## Architecture

Cargo workspace with two crates:

- **`ssp-core`** — library: parsing, filtering, decoding, record batches
- **`ssp-cli`** — binary (`ssp`): CLI, pipeline orchestration, RPC discovery, DuckDB, ratatui TUI

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
crates/
├── ssp-core/src/
│   ├── lib.rs                          # Public API
│   ├── parser.rs                       # Custom tar parser, AppendVec parsing, stream_raw()
│   ├── filters.rs                      # Account filters (owner/pubkey/hash, dead filtering)
│   ├── pubkey.rs                       # Pubkey type (32 bytes, bytemuck Pod, base58)
│   ├── record_batch.rs                 # Arrow schema, RecordBatch construction
│   └── decoders/
│       ├── mod.rs                      # Decoder trait, COptionPubkey
│       ├── known_mints.rs             # Jupiter verified token list (embedded)
│       └── token_program/
│           ├── mod.rs                  # Mint/TokenAccount structs, COptionU64
│           ├── mint.rs                 # MintDecoder (82-byte accounts)
│           └── token_account.rs        # TokenAccountDecoder (165-byte accounts)
└── ssp-cli/src/
    ├── main.rs                         # CLI args, entry point
    ├── pipeline.rs                     # Pipeline orchestration, threading, PipelineStats
    ├── db.rs                           # DuckDB views, query execution
    ├── rpc.rs                          # RPC node discovery, probing, speed testing (async)
    ├── bench.rs                        # Pipeline stage benchmarks
    └── tui/
        ├── mod.rs                      # App state, main loop, phase dispatch
        ├── setup.rs                    # Setup wizard (source, file picker, network, filters)
        ├── processing.rs               # Discovering + processing screens
        ├── query.rs                    # Query mode (input, presets, result table)
        └── helpers.rs                  # Formatting, table layout, dir listing
```

### Key design decisions

- **Custom tar parser** — replaced `tar` crate for performance and buffer control (~830 MB/s peak)
- **bytemuck** for zero-copy binary parsing (like Zig's packed struct overlay)
- **Buffer pooling** — recycling `Vec<u8>` between decompressor and parsers
- **crossbeam-channel** bounded channels for backpressure
- **Decoder trait** — pluggable token decoding (Mint, TokenAccount), writes to separate parquet files
- Async (`tokio`) only for RPC discovery (probing 300+ nodes concurrently); everything else uses threads
- Parser accepts `impl Read` — same code handles both local files and HTTP streams

## Benchmarks

109 GB full snapshot (`snapshot-389758228.tar.zst`), 1.05B accounts, M1-series Mac:

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
- [x] Architecture refactor: extract core as reusable library crate
- [x] Ratatui TUI
  - [x] Interactive setup wizard (source selection, file browser, filters)
  - [x] Live processing stats (progress, speed, ETA, pipeline health)
  - [x] DuckDB query interface with presets (F1-F5) and quick commands
  - [x] Scrollable result tables with adaptive column widths
- [x] Headless CLI mode (`--path`/`--discover` bypass TUI)
- [ ] More decoders (System, Stake, Vote, Token-2022)
- [ ] Parallel multi-node download
- [ ] Incremental snapshot merging
- [ ] Resume on network failure

## Status

In slow development :)

## Stack

Rust 2024, bytemuck, zstd, crossbeam, arrow/parquet, DuckDB, ratatui, crossterm, reqwest, tokio, clap
