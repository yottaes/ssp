use bytemuck::{Pod, Zeroable};
use duckdb::Appender;
use std::{
    fs::File,
    io::{BufReader, Read},
};

use crate::db::DuckDB;

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
    pub fn parse(path: &str, appender: &mut Appender) -> anyhow::Result<()> {
        let reader = open_file(path);
        let decoder = zstd::Decoder::new(reader)?;
        let mut files = tar::Archive::new(decoder);

        for file in files.entries()? {
            let mut file = file?;

            let path = file.path()?.display().to_string();
            if !path.contains("accounts/") {
                continue;
            }
            // let size = file.size();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;

            let mut offset = 0;
            while offset + size_of::<AccountHeader>() <= buf.len() {
                //iterating while there is at least one header
                let header = bytemuck::from_bytes::<AccountHeader>(
                    &buf[offset..offset + size_of::<AccountHeader>()],
                );
                offset += size_of::<AccountHeader>();

                let data = &buf[offset..offset + header.data_len as usize];
                offset += data.len() as usize;

                // align to next 8-byte boundary
                offset = (offset + 7) & !7;

                DuckDB::append_row(appender, header)?
            }
        }

        Ok(())
    }
}

fn open_file(path: &str) -> BufReader<File> {
    let file = std::fs::File::open(path).unwrap();
    std::io::BufReader::new(file)
}
