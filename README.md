# SSP

---

## Solana Snapshot Parser

### Roadmap

- [in-progress] prototype - from file to duckDB with query interface
- [soon] multithreading
- [soon] CSV/JSON export
- [soon] ratatui
- [] `[u8; 32]` -> base58
- [] snapshot finder via Gossip protocol
- [] Building deltas using incremental snapshots
  |- Incremental snapshot install schedule setup
  |- merge existing incremental snapshots if suitable(inc_T > full_T)
- [] ClickHouse
