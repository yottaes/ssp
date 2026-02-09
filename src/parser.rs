use bytemuck::{Pod, Zeroable};
use crossbeam::channel::Sender;
use std::{
    fs::File,
    io::{BufReader, Read},
};

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

impl AccountHeader {
    pub fn parse_threaded(path: &str, tx: Sender<Vec<AccountHeader>>) -> anyhow::Result<()> {
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
                batch.push(*header);

                offset += size_of::<AccountHeader>();

                let data = &buf[offset..offset + header.data_len as usize];
                offset += data.len() as usize;
                offset = (offset + 7) & !7; // align to next 8-byte boundary
            }

            if tx.is_full() {
                blocked += 1;
            }

            tx.send(batch)?;
        }

        eprintln!("producer blocked {blocked} times (channel was full)");
        Ok(())
    }
    pub fn parse_bench(path: &str) -> anyhow::Result<u64> {
        let reader = open_file(path)?;
        let mut decoder = zstd::Decoder::new(reader)?;
        decoder.window_log_max(31)?;

        let mut files = tar::Archive::new(decoder);
        let mut buf = Vec::new();
        let mut count = 0u64;

        for file in files.entries()? {
            let mut file = file?;

            let path = file.path()?.display().to_string();
            if !path.contains("accounts/") {
                continue;
            }
            buf.clear();
            file.read_to_end(&mut buf)?;

            let mut offset = 0;
            while offset + size_of::<AccountHeader>() <= buf.len() {
                let header = bytemuck::from_bytes::<AccountHeader>(
                    &buf[offset..offset + size_of::<AccountHeader>()],
                );
                count += 1;
                offset += size_of::<AccountHeader>() + header.data_len as usize;
                offset = (offset + 7) & !7;
            }
        }

        Ok(count)
    }
}

fn open_file(path: &str) -> Result<BufReader<File>, anyhow::Error> {
    let file = std::fs::File::open(path)?;
    Ok(BufReader::with_capacity(1024 * 1024, file))
}
