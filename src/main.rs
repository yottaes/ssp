use clap::Parser;
use std::io::Read;

use ssp::bench;
use ssp::filters::Filters;
use ssp::pipeline;
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

    pipeline::run(reader, total_size, filters)
}
