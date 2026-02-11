use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
mod parser;
use crossbeam::channel;
use parser::AccountHeader;

mod db;
use crate::db::DuckDB;
use parquet::arrow::ArrowWriter;

mod filters;
use filters::Filters;

mod rpc;

use clap::Parser;

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

const NUM_WRITERS: usize = 6;

fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();

    let filters = args.filters.resolve()?;

    let reader: Box<dyn Read + Send> = if let Some(path) = &args.path {
        Box::new(std::fs::File::open(path)?)
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
        Box::new(resp)
    } else {
        anyhow::bail!("provide --path <file> or --discover");
    };

    let (tx, rx) = channel::bounded::<Vec<AccountHeader>>(128);

    let decompress =
        std::thread::spawn(move || AccountHeader::parse_threaded(reader, filters, tx));

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
                    let record_batch = db::build_record_batch(&batch)?;
                    writer.write(&record_batch)?;
                }
                writer.close()?;

                Ok(())
            })
        })
        .collect();

    drop(rx);

    decompress.join().expect("producer panicked")?;

    for handle in writers {
        handle.join().expect("writer panicked")?;
    }

    eprintln!("Rows received {}", rows_received.load(Ordering::Relaxed));

    eprintln!(
        "starving {} times",
        consumer_starved.load(Ordering::Relaxed)
    );

    let db = DuckDB::open()?;
    let count = db.query_top_accounts("accounts_*.parquet")?;
    println!("total: {}", count);

    Ok(())
}
