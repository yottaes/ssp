use arrow::{
    array::{BinaryBuilder, BooleanBuilder, RecordBatch, UInt8Builder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
};
use std::sync::Arc;

use super::{BATCH_THRESHOLD, Mint, TOKEN_PROGRAM};
use crate::Pubkey;

pub struct MintDecoder {
    schema: Schema,
    rows: usize,
    pubkey_b: BinaryBuilder,
    mint_authority_b: BinaryBuilder,
    freeze_authority_b: BinaryBuilder,
    supply_b: UInt64Builder,
    decimals_b: UInt8Builder,
    is_initialized_b: BooleanBuilder,
}

impl Default for MintDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl MintDecoder {
    pub fn new() -> Self {
        Self {
            schema: Schema::new(vec![
                Field::new("pubkey", DataType::Binary, false),
                Field::new("mint_authority", DataType::Binary, true),
                Field::new("freeze_authority", DataType::Binary, true),
                Field::new("supply", DataType::UInt64, false),
                Field::new("decimals", DataType::UInt8, false),
                Field::new("is_initialized", DataType::Boolean, false),
            ]),
            rows: 0,
            pubkey_b: BinaryBuilder::new(),
            mint_authority_b: BinaryBuilder::new(),
            freeze_authority_b: BinaryBuilder::new(),
            supply_b: UInt64Builder::new(),
            decimals_b: UInt8Builder::new(),
            is_initialized_b: BooleanBuilder::new(),
        }
    }

    fn build_batch(&mut self) -> Option<RecordBatch> {
        if self.rows == 0 {
            return None;
        }
        self.rows = 0;

        RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![
                Arc::new(self.pubkey_b.finish()),
                Arc::new(self.mint_authority_b.finish()),
                Arc::new(self.freeze_authority_b.finish()),
                Arc::new(self.supply_b.finish()),
                Arc::new(self.decimals_b.finish()),
                Arc::new(self.is_initialized_b.finish()),
            ],
        )
        .ok()
    }
}

impl crate::decoders::Decoder for MintDecoder {
    fn name(&self) -> &str {
        "mints"
    }

    fn owner(&self) -> Pubkey {
        TOKEN_PROGRAM
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn matches(&self, owner: &Pubkey, data_len: u64) -> bool {
        owner == &TOKEN_PROGRAM && data_len == Mint::SIZE as u64
    }

    fn decode(&mut self, pubkey: Pubkey, data: &[u8]) -> Option<RecordBatch> {
        let mint = bytemuck::from_bytes::<Mint>(data);

        self.pubkey_b.append_value(pubkey);

        match mint.mint_authority.get() {
            Some(pk) => self.mint_authority_b.append_value(pk),
            None => self.mint_authority_b.append_null(),
        }
        match mint.freeze_authority.get() {
            Some(pk) => self.freeze_authority_b.append_value(pk),
            None => self.freeze_authority_b.append_null(),
        }

        self.supply_b.append_value(mint.supply);
        self.decimals_b.append_value(mint.decimals);
        self.is_initialized_b.append_value(mint.is_initialized != 0);

        self.rows += 1;

        if self.rows >= BATCH_THRESHOLD {
            self.build_batch()
        } else {
            None
        }
    }

    fn flush(&mut self) -> Option<RecordBatch> {
        self.build_batch()
    }
}
