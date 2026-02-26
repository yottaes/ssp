use arrow::array::RecordBatch;
use clap::Parser;
use crossbeam::channel;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use ssp::decoders::Decoder;
use ssp::decoders::token_program::mint::MintDecoder;
use ssp::decoders::token_program::token_account::TokenAccountDecoder;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ssp::Pubkey;
use ssp::bench;
use ssp::db::{self, DuckDB};
use ssp::filters::Filters;
use ssp::parser::AccountHeader;
use ssp::rpc;

#[derive(Parser, Debug)]
#[command(version, about)]
pub struct CliArgs {
    #[arg(short, long)]
    path: Option<String>,

    #[arg(long)]
    bench: bool,

    #[arg(long)]
    discover: bool,

    #[arg(long)]
    incremental: bool,

    #[command(flatten)]
    filters: Filters,
}

const NUM_WRITERS: usize = 1;
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

fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();

    if args.bench {
        let path = args.path.as_deref().expect("--bench requires --path");
        eprintln!("=== Stage 1: zstd only ===");
        bench::run(std::fs::File::open(path)?);
        eprintln!("\n=== Stage 2: zstd + tar ===");
        bench::run_tar(std::fs::File::open(path)?);
        eprintln!("\n=== Stage 3: zstd + tar + parse ===");
        bench::run_full(std::fs::File::open(path)?);
        return Ok(());
    }

    let filters = args.filters.resolve()?;

    let (reader, total_size): (Box<dyn Read + Send>, Option<u64>) = if let Some(path) = &args.path {
        let size = std::fs::metadata(path)?.len();
        (Box::new(std::fs::File::open(path)?), Some(size))
    } else if args.discover {
        let rt = tokio::runtime::Runtime::new()?;
        let source = rt.block_on(rpc::find_fastest_snapshot(None, args.incremental))?;
        eprintln!(
            "streaming from {} ({:.1} MB/s, {:.1} GB)",
            source.url,
            source.speed_mbps,
            source.size.unwrap_or(0) as f64 / 1_073_741_824.0
        );
        let resp = reqwest::blocking::Client::builder()
            .timeout(None)
            .build()?
            .get(&source.url)
            .send()?;
        (Box::new(resp), source.size)
    } else {
        anyhow::bail!("provide --path <file> or --discover");
    };

    let pb = ProgressBar::new(total_size.unwrap_or(0));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) | {msg} | ETA: {eta}",
            )
            .unwrap()
            .progress_chars("=>-"),
    );
    if total_size.is_none() {
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {bytes} ({bytes_per_sec}) | {msg}")
                .unwrap(),
        );
    }
    pb.enable_steady_tick(Duration::from_millis(100));

    let reader = pb.wrap_read(reader);

    // Stage 1: zstd → tar → raw buffers (dedicated thread, no parsing)
    let (raw_tx, raw_rx) = channel::bounded::<Vec<u8>>(128);

    let decompress = std::thread::spawn(move || AccountHeader::stream_raw(reader, raw_tx));

    // Stage 2: parse raw buffers → account headers + decoded batches (N parser threads)
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

            std::thread::spawn(move || -> anyhow::Result<()> {
                let mut decoders: Vec<Box<dyn Decoder>> = vec![
                    Box::new(MintDecoder::new()),
                    Box::new(TokenAccountDecoder::new()),
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
                }

                // Flush remaining decoded data
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

    // Stage 3: write parquet (account writers + decoded writer)
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

    let pb_msg = pb.clone();
    let rows_msg = rows_received.clone();
    let progress_handle = std::thread::spawn(move || {
        while !pb_msg.is_finished() {
            let r = rows_msg.load(Ordering::Relaxed);
            pb_msg.set_message(format_rows(r));
            std::thread::sleep(Duration::from_millis(200));
        }
    });

    decompress.join().expect("decompressor panicked")?;
    for handle in parsers {
        handle.join().expect("parser panicked")?;
    }
    for handle in writers {
        handle.join().expect("writer panicked")?;
    }

    for handle in decoded_writers {
        handle.join().expect("decoded writer panicked")?;
    }

    let total_rows = rows_received.load(Ordering::Relaxed);
    pb.finish_with_message(format_rows(total_rows));
    let _ = progress_handle.join();

    eprintln!(
        "parsers blocked: tx={} decoded={}  |  writers starved: acct={} decoded={}",
        parser_blocked_tx.load(Ordering::Relaxed),
        parser_blocked_decoded.load(Ordering::Relaxed),
        acct_writer_starved.load(Ordering::Relaxed),
        decoded_writer_starved.load(Ordering::Relaxed),
    );

    let db = DuckDB::open()?;
    let count = db.query_top_accounts("accounts_*.parquet")?;
    println!("total accounts: {}", count);

    if std::path::Path::new("mints_0.parquet").exists() {
        println!("\n--- Mints ---");
        db.query_decoded("mints_*.parquet", "supply")?;
    }

    if std::path::Path::new("token_accounts_0.parquet").exists() {
        println!("\n--- Token Accounts ---");
        db.query_decoded("token_accounts_*.parquet", "amount")?;
    }

    Ok(())
}
