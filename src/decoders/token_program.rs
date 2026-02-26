pub mod mint;
pub mod token_account;

use super::COptionPubkey;
use bytemuck::{Pod, Zeroable};

use crate::Pubkey;

pub const BATCH_THRESHOLD: usize = 16_384;
pub const TOKEN_PROGRAM: Pubkey = Pubkey::new([
    6, 221, 246, 225, 215, 101, 161, 147, 217, 203, 225, 70, 206, 235, 121, 172, 28, 180, 133, 237,
    95, 91, 55, 145, 58, 140, 245, 133, 126, 255, 0, 169,
]);

//Solana specific C like OptionPubkey struct for C compatability.

#[derive(Zeroable, Clone, Copy, Debug)]
#[repr(C, packed)]
pub struct COptionU64 {
    tag: u32, // 0 = None, 1 = Some
    value: u64,
}
unsafe impl Pod for COptionU64 {}

impl COptionU64 {
    pub fn get(&self) -> Option<u64> {
        if self.tag == 1 {
            Some(self.value)
        } else {
            None
        }
    }
}

#[derive(Zeroable, Clone, Copy, Debug)]
#[repr(C, packed)]
pub struct Mint {
    pub mint_authority: COptionPubkey,
    pub supply: u64,
    pub decimals: u8,
    pub is_initialized: u8,
    pub freeze_authority: COptionPubkey,
}
unsafe impl Pod for Mint {}

impl Mint {
    pub const SIZE: usize = 82;

    pub fn is_nft(&self) -> bool {
        self.supply == 1 && self.decimals == 0
    }
}

#[derive(Zeroable, Clone, Copy, Debug)]
#[repr(C, packed)]
pub struct TokenAccount {
    pub mint: Pubkey,
    pub owner: Pubkey,
    pub amount: u64,
    pub delegate: COptionPubkey,
    pub state: u8,
    pub is_native: COptionU64,
    pub delegated_amount: u64,
    pub close_authority: COptionPubkey,
}
unsafe impl Pod for TokenAccount {}

impl TokenAccount {
    pub const SIZE: usize = 165;
}

//Comptime size checks.
const _: () = assert!(size_of::<Mint>() == Mint::SIZE);
const _: () = assert!(size_of::<TokenAccount>() == TokenAccount::SIZE);
