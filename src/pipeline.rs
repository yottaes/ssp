use arrow::array::RecordBatch;
use crossbeam::channel;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::Pubkey;
use crate::db;
use crate::decoders::Decoder;
use crate::decoders::known_mints;
use crate::decoders::token_program::mint::MintDecoder;
use crate::decoders::token_program::token_account::TokenAccountDecoder;
use crate::filters::ResolvedFilters;
use crate::parser::AccountHeader;

const NUM_WRITERS: usize = 2;
const NUM_PARSERS: usize = 4;
const NUM_DECODED_WRITERS: usize = 2;

fn format_rows(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B rows", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M rows", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K rows", n as f64 / 1_000.0)
    } else {
        format!("{n} rows")
    }
}

fn make_progress_bar(total_size: Option<u64>) -> ProgressBar {
    let pb = ProgressBar::new(total_size.unwrap_or(0));
    if total_size.is_some() {
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{bar:40.cyan/blue}] {decimal_bytes}/{decimal_total_bytes} ({decimal_bytes_per_sec}) | {msg} | ETA: {eta}",
                )
                .unwrap()
                .progress_chars("=>-"),
        );
    } else {
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {decimal_bytes} ({decimal_bytes_per_sec}) | {msg}")
                .unwrap(),
        );
    }
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

pub fn run(
    reader: impl Read + Send + 'static,
    total_size: Option<u64>,
    filters: ResolvedFilters,
) -> anyhow::Result<()> {
    let known_mints = Arc::new(known_mints::load());
    eprintln!("loaded {} known mints", known_mints.len());

    let start = std::time::Instant::now();
    let pb = make_progress_bar(total_size);
    let reader = pb.wrap_read(reader);

    // Stage 1: zstd → tar → raw buffers
    let (raw_tx, raw_rx) = channel::bounded::<Vec<u8>>(128);
    let (recycle_tx, recycle_rx) = channel::bounded(1024);

    let decompress =
        std::thread::spawn(move || AccountHeader::stream_raw(reader, raw_tx, recycle_rx));

    // Stage 2: parse raw buffers → account headers + decoded batches
    let (tx, rx) = channel::bounded::<Vec<AccountHeader>>(128);
    let (decoded_tx, decoded_rx) = channel::bounded::<(&'static str, RecordBatch)>(256);

    let filters = Arc::new(filters);
    let rows_received = Arc::new(AtomicU64::new(0));
    let acct_writer_starved = Arc::new(AtomicU64::new(0));
    let decoded_writer_starved = Arc::new(AtomicU64::new(0));
    let parser_blocked_tx = Arc::new(AtomicU64::new(0));
    let parser_blocked_decoded = Arc::new(AtomicU64::new(0));

    let parsers: Vec<_> = (0..NUM_PARSERS)
        .map(|_| {
            let raw_rx = raw_rx.clone();
            let tx = tx.clone();
            let decoded_tx = decoded_tx.clone();
            let filters = filters.clone();
            let blocked_tx = parser_blocked_tx.clone();
            let blocked_decoded = parser_blocked_decoded.clone();
            let recycle_tx = recycle_tx.clone();
            let known_mints = known_mints.clone();

            std::thread::spawn(move || -> anyhow::Result<()> {
                let mut decoders: Vec<Box<dyn Decoder>> = vec![
                    Box::new(MintDecoder::new(known_mints.clone())),
                    Box::new(TokenAccountDecoder::new(known_mints)),
                ];

                let mut decoder_map: HashMap<Pubkey, Vec<usize>> = HashMap::new();
                for (i, dec) in decoders.iter().enumerate() {
                    decoder_map.entry(dec.owner()).or_default().push(i);
                }

                while let Ok(buf) = raw_rx.recv() {
                    let batch = AccountHeader::parse_accounts(
                        &buf,
                        &filters,
                        &mut decoders,
                        &decoder_map,
                        &decoded_tx,
                        &blocked_decoded,
                    );
                    if !batch.is_empty() {
                        if tx.is_full() {
                            blocked_tx.fetch_add(1, Ordering::Relaxed);
                        }
                        tx.send(batch)?;
                    }
                    let _ = recycle_tx.send(buf);
                }

                for dec in decoders.iter_mut() {
                    if let Some(batch) = dec.flush() {
                        let _ = decoded_tx.send((dec.name(), batch));
                    }
                }

                Ok(())
            })
        })
        .collect();

    drop(raw_rx);
    drop(tx);
    drop(decoded_tx);

    // Stage 3: write parquet
    let schema = Arc::new(db::account_schema());

    let writers: Vec<_> = (0..NUM_WRITERS)
        .map(|i| {
            let rx = rx.clone();
            let schema = schema.clone();
            let rows = rows_received.clone();
            let starving = acct_writer_starved.clone();

            std::thread::spawn(move || -> anyhow::Result<()> {
                let file = File::create(format!("accounts_{i}.parquet"))?;
                let props = WriterProperties::builder()
                    .set_dictionary_enabled(false)
                    .set_compression(Compression::SNAPPY)
                    .set_max_row_group_size(1_000_000)
                    .build();
                let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

                while let Ok(batch) = {
                    if rx.is_empty() {
                        starving.fetch_add(1, Ordering::Relaxed);
                    }
                    rx.recv()
                } {
                    rows.fetch_add(batch.len() as u64, Ordering::Relaxed);
                    if !batch.is_empty() {
                        let record_batch = db::build_record_batch(&batch)?;
                        writer.write(&record_batch)?;
                    }
                }
                writer.close()?;
                Ok(())
            })
        })
        .collect();

    drop(rx);

    let decoded_writers: Vec<_> = (0..NUM_DECODED_WRITERS)
        .map(|i| {
            let decoded_rx = decoded_rx.clone();
            let starving = decoded_writer_starved.clone();

            std::thread::spawn(move || -> anyhow::Result<()> {
                let mut writers: HashMap<&'static str, ArrowWriter<File>> = HashMap::new();
                while let Ok((name, batch)) = {
                    if decoded_rx.is_empty() {
                        starving.fetch_add(1, Ordering::Relaxed);
                    }
                    decoded_rx.recv()
                } {
                    let writer = writers.entry(name).or_insert_with(|| {
                        let file = File::create(format!("{name}_{i}.parquet")).unwrap();
                        let props = WriterProperties::builder()
                            .set_dictionary_enabled(false)
                            .set_compression(Compression::SNAPPY)
                            .set_max_row_group_size(1_000_000)
                            .build();
                        ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap()
                    });
                    writer.write(&batch)?;
                }

                for (_, writer) in writers {
                    writer.close()?;
                }
                Ok(())
            })
        })
        .collect();

    drop(decoded_rx);

    // Progress updater
    let pb_msg = pb.clone();
    let rows_msg = rows_received.clone();
    let progress_handle = std::thread::spawn(move || {
        while !pb_msg.is_finished() {
            pb_msg.set_message(format_rows(rows_msg.load(Ordering::Relaxed)));
            std::thread::sleep(Duration::from_millis(200));
        }
    });

    // Join all threads
    decompress.join().expect("decompressor panicked")?;
    for h in parsers {
        h.join().expect("parser panicked")?;
    }
    for h in writers {
        h.join().expect("writer panicked")?;
    }
    for h in decoded_writers {
        h.join().expect("decoded writer panicked")?;
    }

    let total_rows = rows_received.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    pb.finish_with_message(format_rows(total_rows));
    let _ = progress_handle.join();

    let secs = elapsed.as_secs();
    eprintln!("done in {}m {}s", secs / 60, secs % 60);
    eprintln!(
        "parsers blocked: tx={} decoded={}  |  writers starved: acct={} decoded={}",
        parser_blocked_tx.load(Ordering::Relaxed),
        parser_blocked_decoded.load(Ordering::Relaxed),
        acct_writer_starved.load(Ordering::Relaxed),
        decoded_writer_starved.load(Ordering::Relaxed),
    );

    // DuckDB queries
    let db = db::DuckDB::open()?;
    let count = db.query_top_accounts("accounts_*.parquet")?;
    println!("total accounts: {}", count);

    if has_parquet("mints") {
        println!("\n--- Mints ---");
        db.query_decoded("mints_*.parquet", "supply")?;
    }
    if has_parquet("token_accounts") {
        println!("\n--- Token Accounts ---");
        db.query_decoded("token_accounts_*.parquet", "amount")?;
    }

    Ok(())
}

fn has_parquet(prefix: &str) -> bool {
    (0..NUM_DECODED_WRITERS)
        .any(|i| std::path::Path::new(&format!("{prefix}_{i}.parquet")).exists())
}
