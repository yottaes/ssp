use std::io::{self, BufReader, Read};
use std::time::Instant;

use crate::parser::AccountHeader;

/// Benchmark each pipeline stage separately to find the bottleneck.
pub fn run(reader: impl Read + Send) {
    let buffered = BufReader::with_capacity(1024 * 1024, reader);

    // Stage 1: zstd only — decompress to sink
    let start = Instant::now();
    let mut decoder = zstd::Decoder::new(buffered).expect("zstd init failed");
    decoder.window_log_max(31).unwrap();
    let bytes = io::copy(&mut decoder, &mut io::sink()).expect("zstd decompress failed");
    let elapsed = start.elapsed().as_secs_f64();
    let gb = bytes as f64 / 1_073_741_824.0;
    eprintln!(
        "[zstd only]       {:.2} GB in {:.1}s — {:.0} MB/s decompressed",
        gb,
        elapsed,
        (bytes as f64 / 1_048_576.0) / elapsed
    );
}

pub fn run_tar(reader: impl Read + Send) {
    let buffered = BufReader::with_capacity(1024 * 1024, reader);

    // Stage 2: zstd + tar — iterate entries, read_to_end, no parsing
    let start = Instant::now();
    let mut decoder = zstd::Decoder::new(buffered).expect("zstd init failed");
    decoder.window_log_max(31).unwrap();
    let mut archive = tar::Archive::new(decoder);
    let mut buf = Vec::new();
    let mut total_bytes: u64 = 0;
    let mut entries: u64 = 0;
    let mut account_files: u64 = 0;

    for entry in archive.entries().expect("tar entries failed") {
        let mut entry = entry.expect("tar entry failed");
        let path = entry.path().unwrap().display().to_string();
        entries += 1;

        if !path.contains("accounts/") {
            continue;
        }
        account_files += 1;
        buf.clear();
        entry.read_to_end(&mut buf).expect("read_to_end failed");
        total_bytes += buf.len() as u64;
    }

    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "[zstd + tar]      {:.2} GB in {:.1}s — {:.0} MB/s ({} entries, {} account files)",
        total_bytes as f64 / 1_073_741_824.0,
        elapsed,
        (total_bytes as f64 / 1_048_576.0) / elapsed,
        entries,
        account_files
    );
}

pub fn run_full(reader: impl Read + Send) {
    let buffered = BufReader::with_capacity(1024 * 1024, reader);

    // Stage 3: zstd + tar + parse — full pipeline minus channel/writers
    let start = Instant::now();
    let mut decoder = zstd::Decoder::new(buffered).expect("zstd init failed");
    decoder.window_log_max(31).unwrap();
    let mut archive = tar::Archive::new(decoder);
    let mut buf = Vec::new();
    let mut total_accounts: u64 = 0;

    for entry in archive.entries().expect("tar entries failed") {
        let mut entry = entry.expect("tar entry failed");
        let path = entry.path().unwrap().display().to_string();

        if !path.contains("accounts/") {
            continue;
        }
        buf.clear();
        entry.read_to_end(&mut buf).expect("read_to_end failed");

        let mut offset = 0;
        while offset + size_of::<AccountHeader>() <= buf.len() {
            let header = bytemuck::from_bytes::<AccountHeader>(
                &buf[offset..offset + size_of::<AccountHeader>()],
            );
            offset += size_of::<AccountHeader>();
            offset += header.data_len as usize;
            offset = (offset + 7) & !7;
            total_accounts += 1;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "[zstd + tar + parse] {:.1}s — {} accounts parsed",
        elapsed, total_accounts
    );
}
