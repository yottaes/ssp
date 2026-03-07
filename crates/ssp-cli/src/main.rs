use clap::Parser;
use std::io::Read;
use std::sync::Arc;

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

    #[command(flatten)]
    filters: Filters,
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
