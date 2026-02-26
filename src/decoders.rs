use arrow::{array::RecordBatch, datatypes::Schema};

use crate::Pubkey;
use bytemuck::{Pod, Zeroable};

//
pub mod token_program;

pub trait Decoder: Send {
    fn name(&self) -> &str;
    fn owner(&self) -> Pubkey;
    fn schema(&self) -> &Schema;
    fn matches(&self, owner: &Pubkey, data_len: u64) -> bool;
    fn decode(&mut self, pubkey: Pubkey, data: &[u8]) -> Option<RecordBatch>;
    fn flush(&mut self) -> Option<RecordBatch>;
}

#[derive(Zeroable, Clone, Copy, Debug)]
#[repr(C, packed)]
pub struct COptionPubkey {
    tag: u32, // 0 = None, 1 = Some
    value: Pubkey,
}
unsafe impl Pod for COptionPubkey {}

impl COptionPubkey {
    pub fn get(&self) -> Option<Pubkey> {
        if self.tag == 1 {
            Some(self.value)
        } else {
            None
        }
    }
}
