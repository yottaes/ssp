use std::collections::HashSet;
use crate::Pubkey;

static MINTS_TXT: &str = include_str!("known_mints.txt");

pub fn load() -> HashSet<Pubkey> {
    let mut set = HashSet::with_capacity(6000);
    for line in MINTS_TXT.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(pk) = Pubkey::from_b58(line) {
            set.insert(pk);
        }
    }
    set
}
