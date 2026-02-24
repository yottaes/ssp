use {
    crate::{Pubkey, filters::ResolvedFilters},
    bytemuck::{Pod, Zeroable},
    crossbeam::channel::Sender,
    std::{
        fmt,
        io::{BufReader, Read},
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
    /// Producer: zstd → tar → parse → send filtered account batches.
    pub fn stream_parsed(
        reader: impl Read + Send,
        tx: Sender<Vec<AccountHeader>>,
        filters: &ResolvedFilters,
    ) -> anyhow::Result<()> {
        let buffered = BufReader::with_capacity(1024 * 1024, reader);
        let mut decoder = zstd::Decoder::new(buffered)?;
        decoder.window_log_max(31)?;

        let mut archive = tar::Archive::new(decoder);
        let mut blocked: u64 = 0;
        let mut buf = Vec::new();

        for entry in archive.entries()? {
            let mut entry = entry?;

            let path = entry.path()?.display().to_string();
            if !path.contains("accounts/") {
                continue;
            }

            buf.clear();
            entry.read_to_end(&mut buf)?;

            let batch = Self::parse_accounts(&buf, filters);

            if tx.is_full() {
                blocked += 1;
            }
            if !batch.is_empty() {
                tx.send(batch)?;
            }
        }

        eprintln!("producer blocked {blocked} times (channel was full)");
        Ok(())
    }

    /// Parse raw AppendVec buffer into filtered account headers.
    pub fn parse_accounts(buf: &[u8], filters: &ResolvedFilters) -> Vec<AccountHeader> {
        let mut offset = 0;
        let mut batch = Vec::new();

        while offset + size_of::<AccountHeader>() <= buf.len() {
            let header = bytemuck::from_bytes::<AccountHeader>(
                &buf[offset..offset + size_of::<AccountHeader>()],
            );

            offset += size_of::<AccountHeader>();

            offset += header.data_len as usize;

            offset = (offset + 7) & !7;

            if !filters.matches(header) {
                continue;
            }
            batch.push(*header);
        }

        batch
    }
}
