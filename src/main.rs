use clap::Parser;
use crossbeam::channel;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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

const NUM_WRITERS: usize = 3;

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

    let (tx, rx) = channel::bounded::<Vec<AccountHeader>>(128);

    let decompress = std::thread::spawn(move || AccountHeader::stream_parsed(reader, tx, &filters));

    let schema = Arc::new(db::account_schema());

    let rows_received = Arc::new(AtomicU64::new(0));
    let consumer_starved = Arc::new(AtomicU64::new(0));

    let writers: Vec<_> = (0..NUM_WRITERS)
        .map(|i| {
            let rx = rx.clone();
            let schema = schema.clone();

            let rows = rows_received.clone();
            let starving = consumer_starved.clone();
            std::thread::spawn(move || -> anyhow::Result<()> {
                let file = File::create(format!("accounts_{i}.parquet"))?;
                let mut writer = ArrowWriter::try_new(file, schema, None)?;

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

    let pb_msg = pb.clone();
    let rows_msg = rows_received.clone();
    let progress_handle = std::thread::spawn(move || {
        while !pb_msg.is_finished() {
            let r = rows_msg.load(Ordering::Relaxed);
            pb_msg.set_message(format_rows(r));
            std::thread::sleep(Duration::from_millis(200));
        }
    });

    decompress.join().expect("producer panicked")?;

    for handle in writers {
        handle.join().expect("writer panicked")?;
    }

    let total_rows = rows_received.load(Ordering::Relaxed);
    pb.finish_with_message(format_rows(total_rows));
    let _ = progress_handle.join();

    eprintln!(
        "starving {} times",
        consumer_starved.load(Ordering::Relaxed)
    );

    let db = DuckDB::open()?;
    let count = db.query_top_accounts("accounts_*.parquet")?;
    println!("total: {}", count);

    Ok(())
}
