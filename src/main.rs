use std::env::args;
use std::fs::File;
use std::io::{BufReader, Read};

use anyhow::Ok;
use bytemuck::{Pod, Zeroable};

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
struct AccountHeader {
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

fn main() -> Result<(), anyhow::Error> {
    let args: Vec<String> = args().collect();
    if args.is_empty() {
        panic!("Please provide file path!");
    }
    let path = &args[1];
    let reader = open_file(path);
    let decoder = zstd::Decoder::new(reader)?;
    let mut files = tar::Archive::new(decoder);

    for file in files.entries()? {
        let mut file = file?;

        let path = file.path()?.display().to_string();
        if !path.contains("accounts/") {
            continue;
        }

        let size = file.size();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // println!("file: {}, size: {}", path, size);

        let mut offset = 0;
        while offset + size_of::<AccountHeader>() <= buf.len() {
            let header = bytemuck::from_bytes::<AccountHeader>(
                &buf[offset..offset + size_of::<AccountHeader>()],
            );
            offset += size_of::<AccountHeader>();

            let data = &buf[offset..offset + header.data_len as usize];
            offset += header.data_len as usize;

            // align to next 8-byte boundary
            offset = (offset + 7) & !7;

            // print_account(header, data);
        }
    }

    Ok(())
}

fn print_account(header: &AccountHeader, data: &[u8]) {
    println!(
        "v:{} data_len:{} lamports:{} pubkey:{:x?}",
        header.write_version,
        data.len(),
        header.lamports,
        header.pubkey
    );
}

fn open_file(path: &str) -> BufReader<File> {
    let file = std::fs::File::open(path).unwrap();
    std::io::BufReader::new(file)
}
