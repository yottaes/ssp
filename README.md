# SSP

---

## Solana Snapshot Parser

### Roadmap

- [x] Streaming parser (zstd → tar → AppendVec)
- [x] DuckDB integration (Appender, file-based storage)
- [ ] Prototype — from file to DuckDB with query interface (in progress)
- [ ] Multithreading (crossbeam-channel pipeline)
- [ ] CSV/JSON/Parquet export
- [ ] Ratatui TUI
- [ ] `[u8; 32]` → base58 display
- [ ] Snapshot finder via Gossip protocol
- [ ] Building deltas using incremental snapshots
  - [ ] Incremental snapshot install schedule setup
  - [ ] Merge existing incremental snapshots if suitable (inc_T > full_T)
- [ ] ClickHouse
