use crate::parser::AccountHeader;
use clap::Args;

#[derive(Args, Debug, Clone)]
pub struct Filters {
    #[arg(long)]
    pub owner: Option<String>,

    #[arg(long)]
    pub hash: Option<String>,

    #[arg(long)]
    pub pubkey: Option<String>,
}

pub struct ResolvedFilters {
    pub owner: Option<[u8; 32]>,
    pub hash: Option<[u8; 32]>,
    pub pubkey: Option<[u8; 32]>,
}

impl Filters {
    pub fn resolve(&self) -> Result<ResolvedFilters, anyhow::Error> {
        Ok(ResolvedFilters {
            owner: decode_b58_32(&self.owner)?,
            hash: decode_b58_32(&self.hash)?,
            pubkey: decode_b58_32(&self.pubkey)?,
        })
    }
}

impl ResolvedFilters {
    pub fn matches(&self, header: &AccountHeader) -> bool {
        let owner = self.owner.is_none_or(|o| o == header.owner);
        let hash = self.hash.is_none_or(|h| h == header.hash);
        let pubkey = self.pubkey.is_none_or(|pk| pk == header.pubkey);

        owner && hash && pubkey
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
