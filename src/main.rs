use std::env::args;
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

mod parser;
use crossbeam::channel;
use parser::AccountHeader;

mod db;
use crate::db::DuckDB;
use parquet::arrow::ArrowWriter;

const NUM_WRITERS: usize = 6;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = args().collect();
    if args.len() < 2 {
        panic!("Please provide file path!");
    }
    let path = args[1].clone();

    if args.get(2).is_some_and(|a| a == "--bench") {
        let count = AccountHeader::parse_bench(&path)?;
        println!("bench: {count} accounts");
        return Ok(());
    }

    let (tx, rx) = channel::bounded::<Vec<AccountHeader>>(128);

    let decompress = std::thread::spawn(move || AccountHeader::parse_threaded(&path, tx));

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
                    rows.fetch_add(1, Ordering::Relaxed);
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
