# SSP

---

## Solana Snapshot Parser

### Roadmap

- [x] Streaming parser (zstd → tar → AppendVec)
- [x] DuckDB integration (Appender, file-based storage)
- [x] Prototype — from file to DuckDB
- [ ] Multithreading (crossbeam-channel pipeline)
- [ ] Ratatui TUI
  - [ ] Query interface
  - [ ] Graphics
  - [ ] Tables?
  - [ ] `[u8; 32]` → base58 display
- [ ] Snapshot finder via Gossip protocol
- [ ] Building deltas using incremental snapshots
  - [ ] Incremental snapshot install schedule setup
  - [ ] Merge existing incremental snapshots if suitable (inc_T > full_T)
- [ ] ClickHouse
- [ ] CSV/JSON/Parquet export
- [ ] Parsing right from a download stream with 0 extra disk usage
  - [ ] Checkpoints
