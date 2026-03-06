use crate::parser::AccountHeader;
use arrow::array::{ArrayRef, BinaryArray, BooleanArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub fn account_schema() -> Schema {
    Schema::new(vec![
        Field::new("pubkey", DataType::Binary, false),
        Field::new("lamports", DataType::UInt64, false),
        Field::new("owner", DataType::Binary, false),
        Field::new("data_len", DataType::UInt64, false),
        Field::new("executable", DataType::Boolean, false),
        Field::new("rent_epoch", DataType::UInt64, false),
    ])
}

pub fn build_record_batch(headers: &[AccountHeader]) -> anyhow::Result<RecordBatch> {
    let pubkeys: ArrayRef = Arc::new(BinaryArray::from_iter_values(
        headers.iter().map(|h| h.pubkey),
    ));
    let lamports: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        headers.iter().map(|h| h.lamports),
    ));
    let owners: ArrayRef = Arc::new(BinaryArray::from_iter_values(
        headers.iter().map(|h| h.owner),
    ));
    let data_lens: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        headers.iter().map(|h| h.data_len),
    ));
    let executables: ArrayRef = Arc::new(BooleanArray::from(
        headers
            .iter()
            .map(|h| h.executable == 1)
            .collect::<Vec<bool>>(),
    ));
    let rent_epochs: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        headers.iter().map(|h| h.rent_epoch),
    ));

    let batch = RecordBatch::try_new(
        Arc::new(account_schema()),
        vec![
            pubkeys,
            lamports,
            owners,
            data_lens,
            executables,
            rent_epochs,
        ],
    )?;

    Ok(batch)
}
