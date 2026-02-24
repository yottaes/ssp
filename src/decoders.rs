use arrow::{array::RecordBatch, datatypes::Schema};

use crate::Pubkey;

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
