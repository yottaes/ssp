use {
    crate::{Pubkey, filters::ResolvedFilters},
    arrow::array::RecordBatch,
    bytemuck::{Pod, Zeroable},
    crossbeam::channel::Sender,
    std::{
        collections::HashMap,
        fmt,
        io::{BufReader, Read},
        sync::atomic::{AtomicU64, Ordering},
    },
};

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct AccountHeader {
    pub write_version: u64,
    pub data_len: u64,
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub rent_epoch: u64,
    pub owner: Pubkey,
    pub executable: u8,
    pub padding: [u8; 7],
    pub hash: [u8; 32],
}
impl fmt::Display for AccountHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "version: {}, data_len: {}, pubkey: {}, lamports: {}, \
             rent_epoch: {}, owner: {}, executable: {}, hash: {}",
            self.write_version,
            self.data_len,
            self.pubkey,
            self.lamports,
            self.rent_epoch,
            self.owner,
            self.executable,
            bs58::encode(&self.hash).into_string(),
        )
    }
}

impl AccountHeader {
    /// Stage 1: zstd → tar → send raw buffers. Nothing else on this thread.
    pub fn stream_raw(
        reader: impl Read + Send,
        raw_tx: Sender<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let buffered = BufReader::with_capacity(1024 * 1024, reader);
        let mut decoder = zstd::Decoder::new(buffered)?;
        decoder.window_log_max(31)?;

        let mut archive = tar::Archive::new(decoder);
        let mut blocked: u64 = 0;

        for entry in archive.entries()? {
            let mut entry = entry?;

            let path = entry.path()?.display().to_string();
            if !path.contains("accounts/") {
                continue;
            }

            let size = entry.header().size()? as usize;
            let mut buf = Vec::with_capacity(size);
            entry.read_to_end(&mut buf)?;

            if raw_tx.is_full() {
                blocked += 1;
            }
            raw_tx.send(buf)?;
        }

        eprintln!("decompressor blocked {blocked} times (raw channel full)");
        Ok(())
    }

    /// Stage 2: parse raw AppendVec buffer into filtered account headers + decoded batches.
    pub fn parse_accounts(
        buf: &[u8],
        filters: &ResolvedFilters,
        decoders: &mut [Box<dyn crate::decoders::Decoder>],
        decoder_map: &HashMap<Pubkey, Vec<usize>>,
        decoded_tx: &Sender<(&'static str, RecordBatch)>,
        blocked_decoded: &AtomicU64,
    ) -> Vec<AccountHeader> {
        let mut offset = 0;
        let mut batch = Vec::new();

        while offset + size_of::<AccountHeader>() <= buf.len() {
            let header = bytemuck::from_bytes::<AccountHeader>(
                &buf[offset..offset + size_of::<AccountHeader>()],
            );

            offset += size_of::<AccountHeader>();

            let data = &buf[offset..offset + header.data_len as usize];

            offset += header.data_len as usize;

            offset = (offset + 7) & !7;

            // O(1) lookup by owner — skips entirely for programs without decoders
            if let Some(indices) = decoder_map.get(&header.owner) {
                for &idx in indices {
                    if decoders[idx].matches(&header.owner, header.data_len) {
                        if let Some(batch) = decoders[idx].decode(header.pubkey, data) {
                            if decoded_tx.is_full() {
                                blocked_decoded.fetch_add(1, Ordering::Relaxed);
                            }
                            let _ = decoded_tx.send((decoders[idx].name(), batch));
                        }
                        break;
                    }
                }
            }

            if !filters.matches(header) {
                continue;
            }
            batch.push(*header);
        }

        batch
    }
}
