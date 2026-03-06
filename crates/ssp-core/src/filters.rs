use crate::Pubkey;
use crate::parser::AccountHeader;

pub struct ResolvedFilters {
    pub owner: Option<Pubkey>,
    pub hash: Option<[u8; 32]>,
    pub pubkey: Option<Pubkey>,
    pub include_dead: bool,
    pub include_spam: bool,
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
