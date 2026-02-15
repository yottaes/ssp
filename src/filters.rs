use crate::Pubkey;
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

    #[arg(long, default_value = "false")]
    pub include_dead: bool,
}

pub struct ResolvedFilters {
    pub owner: Option<Pubkey>,
    pub hash: Option<[u8; 32]>,
    pub pubkey: Option<Pubkey>,
    pub include_dead: bool,
}

impl Filters {
    pub fn resolve(&self) -> Result<ResolvedFilters, anyhow::Error> {
        Ok(ResolvedFilters {
            owner: Pubkey::try_from_b58(self.owner.as_deref())?,
            hash: decode_b58_32(&self.hash)?,
            pubkey: Pubkey::try_from_b58(self.pubkey.as_deref())?,
            include_dead: self.include_dead,
        })
    }
}

impl ResolvedFilters {
    pub fn matches(&self, header: &AccountHeader) -> bool {
        if !self.include_dead && header.lamports == 0 {
            return false;
        }

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
