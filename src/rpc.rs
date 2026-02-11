use anyhow::{Context, bail};
use reqwest::Client;
use reqwest::redirect::Policy;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

const DEFAULT_RPC: &str = "https://api.mainnet-beta.solana.com";
const FULL_SNAPSHOT_PATHS: &[&str] = &["/snapshot.tar.zst", "/snapshot.tar.bz2"];
const INC_SNAPSHOT_PATHS: &[&str] = &["/incremental-snapshot.tar.zst", "/incremental-snapshot.tar.bz2"];
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const SPEED_TEST_BYTES: usize = 1024 * 1024;
const MAX_CONCURRENT: usize = 64;

#[derive(Debug, Deserialize)]
struct RpcResponse {
    result: Vec<RpcNode>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct RpcNode {
    pub rpc: Option<String>,
}

pub struct SnapshotSource {
    pub url: String,
    pub size: Option<u64>,
    pub speed_mbps: f64,
}

struct SnapshotCandidate {
    url: String,
    size: Option<u64>,
}

/// Fetches cluster nodes from Solana RPC and returns only nodes that serve RPC.
async fn get_rpc_nodes(rpc_url: Option<&str>) -> anyhow::Result<Vec<RpcNode>> {
    let url = rpc_url.unwrap_or(DEFAULT_RPC);
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getClusterNodes"
    });

    let resp: RpcResponse = client
        .post(url)
        .json(&body)
        .send()
        .await
        .context("failed to fetch cluster nodes")?
        .json()
        .await
        .context("failed to parse cluster nodes response")?;

    let rpc_nodes: Vec<RpcNode> = resp
        .result
        .into_iter()
        .filter(|n| n.rpc.is_some())
        .collect();

    eprintln!("found {} RPC nodes out of cluster", rpc_nodes.len());
    Ok(rpc_nodes)
}

/// HEAD request without following redirects. Any 3xx with Location = snapshot found.
/// Resolves relative Location headers against the original URL.
async fn resolve_snapshot_url(client: &Client, base_url: &str) -> Option<String> {
    let resp = client.head(base_url).send().await.ok()?;
    let status = resp.status().as_u16();

    if (300..400).contains(&status) {
        let location = resp.headers().get("location")?.to_str().ok()?;
        // Resolve relative redirects (e.g. "/snapshot-123.tar.zst") against original URL
        let base = reqwest::Url::parse(base_url).ok()?;
        let resolved = base.join(location).ok()?;
        return Some(resolved.to_string());
    }

    None
}

/// Probe a single node: try snapshot paths, return first hit.
async fn probe_node(
    probe_client: &Client,
    size_client: &Client,
    node: &RpcNode,
    paths: &[&str],
) -> Option<SnapshotCandidate> {
    let rpc_addr = node.rpc.as_ref()?;

    for path in paths {
        let probe_url = format!("http://{}{}", rpc_addr, path);

        if let Some(download_url) = resolve_snapshot_url(probe_client, &probe_url).await {
            let size = size_client
                .head(&download_url)
                .send()
                .await
                .ok()
                .and_then(|r| {
                    r.headers()
                        .get("content-length")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                });

            return Some(SnapshotCandidate {
                url: download_url,
                size,
            });
        }
    }

    None
}

/// Phase 1: Probe all nodes concurrently for snapshot availability.
async fn probe_nodes(nodes: &[RpcNode], paths: &'static [&'static str]) -> Vec<SnapshotCandidate> {
    let probe_client = Client::builder()
        .timeout(PROBE_TIMEOUT)
        .redirect(Policy::none())
        .build()
        .expect("failed to build probe client");

    let size_client = Client::builder()
        .timeout(PROBE_TIMEOUT)
        .build()
        .expect("failed to build size client");

    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut handles = Vec::new();

    for node in nodes {
        let probe_client = probe_client.clone();
        let size_client = size_client.clone();
        let node = node.clone();
        let sem = sem.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            probe_node(&probe_client, &size_client, &node, paths).await
        }));
    }

    let mut candidates = Vec::new();
    for handle in handles {
        if let Ok(Some(candidate)) = handle.await {
            candidates.push(candidate);
        }
    }

    eprintln!(
        "probed {} nodes, {} serve snapshots",
        nodes.len(),
        candidates.len()
    );
    candidates
}

/// Phase 2: Speed test — download 1MB from each candidate, measure throughput.
async fn speed_test(candidates: Vec<SnapshotCandidate>) -> Vec<(SnapshotCandidate, f64)> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build speed test client");

    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut handles = Vec::new();

    for candidate in candidates {
        let client = client.clone();
        let sem = sem.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();

            let start = Instant::now();
            let mut resp = match client.get(&candidate.url).send().await {
                Ok(r) => r,
                Err(_) => return None,
            };

            let mut total = 0usize;
            while let Ok(Some(chunk)) = resp.chunk().await {
                total += chunk.len();
                if total >= SPEED_TEST_BYTES {
                    break;
                }
            }

            let elapsed = start.elapsed().as_secs_f64();
            if total > 0 && elapsed > 0.0 {
                let mbps = (total as f64 / 1_048_576.0) / elapsed;
                Some((candidate, mbps))
            } else {
                None
            }
        }));
    }

    let mut results = Vec::new();
    for handle in handles {
        if let Ok(Some(result)) = handle.await {
            results.push(result);
        }
    }

    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    results
}

/// Finds the fastest snapshot source:
/// 1. Fetches cluster nodes from RPC
/// 2. Probes all RPC nodes concurrently (HEAD requests)
/// 3. Speed tests candidates (downloads 1MB sample)
/// 4. Returns the fastest node
pub async fn find_fastest_snapshot(
    rpc_url: Option<&str>,
    incremental: bool,
) -> anyhow::Result<SnapshotSource> {
    let paths = if incremental {
        INC_SNAPSHOT_PATHS
    } else {
        FULL_SNAPSHOT_PATHS
    };
    let nodes = get_rpc_nodes(rpc_url).await?;
    let candidates = probe_nodes(&nodes, paths).await;

    if candidates.is_empty() {
        bail!("no snapshot sources found among {} RPC nodes", nodes.len());
    }

    eprintln!("speed testing {} candidates...", candidates.len());
    let ranked = speed_test(candidates).await;

    if ranked.is_empty() {
        bail!("all speed tests failed");
    }

    for (i, (candidate, mbps)) in ranked.iter().enumerate().take(5) {
        eprintln!("  #{}: {:.1} MB/s — {}", i + 1, mbps, candidate.url);
    }

    let (best, speed) = ranked.into_iter().next().unwrap();

    eprintln!(
        "selected: {} ({:.1} MB/s, {:.1} GB)",
        best.url,
        speed,
        best.size.unwrap_or(0) as f64 / 1_073_741_824.0
    );

    Ok(SnapshotSource {
        url: best.url,
        size: best.size,
        speed_mbps: speed,
    })
}
