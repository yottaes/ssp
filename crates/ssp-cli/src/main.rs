use clap::Parser;
use std::io::{self, Read, Write};
use std::sync::Arc;
use std::time::Instant;

use ssp_core::Pubkey;
use ssp_core::filters::ResolvedFilters;

mod bench;
mod db;
mod pipeline;
mod rpc;
mod tui;

#[derive(clap::Args, Debug, Clone)]
pub struct Filters {
    #[arg(long)]
    pub owner: Option<String>,

    #[arg(long)]
    pub hash: Option<String>,

    #[arg(long)]
    pub pubkey: Option<String>,

    #[arg(long, default_value = "false")]
    pub include_dead: bool,

    #[arg(long, default_value = "false")]
    pub include_spam: bool,
}

impl Filters {
    pub fn resolve(&self) -> Result<ResolvedFilters, anyhow::Error> {
        Ok(ResolvedFilters {
            owner: Pubkey::try_from_b58(self.owner.as_deref())?,
            hash: decode_b58_32(&self.hash)?,
            pubkey: Pubkey::try_from_b58(self.pubkey.as_deref())?,
            include_dead: self.include_dead,
            include_spam: self.include_spam,
        })
    }
}

fn decode_b58_32(input: &Option<String>) -> Result<Option<[u8; 32]>, anyhow::Error> {
    input
        .as_deref()
        .map(|s| {
            let mut buf = [0u8; 32];
            bs58::decode(s).onto(&mut buf)?;
            Ok(buf)
        })
        .transpose()
}

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

    #[arg(long, conflicts_with = "download_incremental")]
    download_full: bool,

    #[arg(long, conflicts_with = "download_full")]
    download_incremental: bool,

    #[arg(long, default_value = ".")]
    output: String,

    #[command(flatten)]
    filters: Filters,
}

fn download_snapshot(incremental: bool, output_dir: &str) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    let source = rt.block_on(rpc::find_fastest_snapshot(None, incremental))?;

    let filename = reqwest::Url::parse(&source.url)
        .ok()
        .and_then(|u| {
            u.path_segments()?
                .last()
                .filter(|s| !s.is_empty())
                .map(String::from)
        })
        .unwrap_or_else(|| {
            if incremental {
                "incremental-snapshot.tar.zst"
            } else {
                "snapshot.tar.zst"
            }
            .into()
        });

    let dir = std::path::Path::new(output_dir);
    std::fs::create_dir_all(dir)?;
    let dest = dir.join(&filename);

    eprintln!(
        "downloading to {} ({:.1} MB/s, {:.1} GB)",
        dest.display(),
        source.speed_mbps,
        source.size.unwrap_or(0) as f64 / 1_073_741_824.0
    );

    let mut resp = reqwest::blocking::Client::builder()
        .timeout(None)
        .build()?
        .get(&source.url)
        .send()?;

    let total = resp
        .content_length()
        .or(source.size)
        .filter(|&s| s > 0);

    let mut file = std::fs::File::create(&dest)?;
    let mut buf = vec![0u8; 1024 * 1024];
    let mut downloaded: u64 = 0;
    let start = Instant::now();
    let mut last_print = Instant::now();

    loop {
        let n = resp.read(&mut buf)?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])?;
        downloaded += n as u64;

        if last_print.elapsed().as_millis() >= 500 {
            let elapsed = start.elapsed().as_secs_f64();
            let speed = if elapsed > 0.5 {
                downloaded as f64 / elapsed / 1_048_576.0
            } else {
                0.0
            };
            if let Some(t) = total {
                let pct = downloaded as f64 / t as f64 * 100.0;
                eprint!(
                    "\r  {:.2} / {:.2} GB  ({:.0}%)  {:.1} MB/s    ",
                    downloaded as f64 / 1e9,
                    t as f64 / 1e9,
                    pct,
                    speed
                );
            } else {
                eprint!(
                    "\r  {:.2} GB  {:.1} MB/s    ",
                    downloaded as f64 / 1e9,
                    speed
                );
            }
            io::stderr().flush().ok();
            last_print = Instant::now();
        }
    }

    let elapsed = start.elapsed();
    eprintln!(
        "\ndone: {:.2} GB in {:.0}s → {}",
        downloaded as f64 / 1e9,
        elapsed.as_secs_f64(),
        dest.display()
    );

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();

    if args.download_full || args.download_incremental {
        return download_snapshot(args.download_incremental, &args.output);
    }

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

    if !args.bench && args.path.is_none() && !args.discover {
        return tui::run_interactive();
    }

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
        unreachable!()
    };

    let stats = Arc::new(pipeline::PipelineStats::new());
    let stats_clone = stats.clone();

    let start = std::time::Instant::now();
    pipeline::run(reader, filters, stats_clone)?;
    let elapsed = start.elapsed();

    let rows = stats.rows_parsed.load(std::sync::atomic::Ordering::Relaxed);
    let bytes = stats.bytes_read.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!(
        "done: {} rows, {:.1} GB read in {:.1}s",
        rows,
        bytes as f64 / 1_073_741_824.0,
        elapsed.as_secs_f64(),
    );

    Ok(())
}
