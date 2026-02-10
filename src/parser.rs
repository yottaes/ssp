use bytemuck::{Pod, Zeroable};
use crossbeam::channel::Sender;
use std::{
    fmt,
    fs::File,
    io::{BufReader, Read},
};

use crate::filters::ResolvedFilters;

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub struct AccountHeader {
    pub write_version: u64,
    pub data_len: u64,
    pub pubkey: [u8; 32],
    pub lamports: u64,
    pub rent_epoch: u64,
    pub owner: [u8; 32],
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
            bs58::encode(&self.pubkey).into_string(),
            self.lamports,
            self.rent_epoch,
            bs58::encode(&self.owner).into_string(),
            self.executable,
            bs58::encode(&self.hash).into_string(),
        )
    }
}

impl AccountHeader {
    pub fn parse_threaded(
        path: &str,
        filters: ResolvedFilters,
        tx: Sender<Vec<AccountHeader>>,
    ) -> anyhow::Result<()> {
        let reader = open_file(path)?;
        let mut decoder = zstd::Decoder::new(reader)?;
        decoder.window_log_max(31)?; // allow up to 2GB window

        let mut files = tar::Archive::new(decoder);
        let mut blocked: u64 = 0;
        let mut buf = Vec::new();

        for file in files.entries()? {
            let mut file = file?;

            let path = file.path()?.display().to_string();
            if !path.contains("accounts/") {
                continue;
            }
            buf.clear();
            file.read_to_end(&mut buf)?;

            let mut offset = 0;

            let mut batch = Vec::new();

            while offset + size_of::<AccountHeader>() <= buf.len() {
                //iterating while there is at least one header
                let header = bytemuck::from_bytes::<AccountHeader>(
                    &buf[offset..offset + size_of::<AccountHeader>()],
                );

                offset += size_of::<AccountHeader>();

                let data = &buf[offset..offset + header.data_len as usize];

                offset += data.len() as usize;
                offset = (offset + 7) & !7; // align to next 8-byte boundary

                if !filters.matches(header) {
                    continue;
                }
                batch.push(*header);
            }

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
}

fn open_file(path: &str) -> Result<BufReader<File>, anyhow::Error> {
    let file = std::fs::File::open(path)?;
    Ok(BufReader::with_capacity(1024 * 1024, file))
}
