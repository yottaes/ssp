use crate::parser::AccountHeader;
use arrow::array::{ArrayRef, BinaryArray, BooleanArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use duckdb::Connection;
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

    //TODO: for data blobs in the future.
    // let data_blobs: ArrayRef = Arc::new(BinaryArray::from(
    //     headers.iter().map(|h| h.data_blob.as_slice()),
    // ));

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

pub struct DuckDB {
    connection: Connection,
}

impl DuckDB {
    pub fn open() -> Result<Self, anyhow::Error> {
        let conn = Connection::open_in_memory()?;
        Ok(DuckDB { connection: conn })
    }

    pub fn query_top_accounts(&self, parquet_path: &str) -> Result<i64, anyhow::Error> {
        let mut count_stmt = self
            .connection
            .prepare(&format!("SELECT COUNT(*) FROM '{}'", parquet_path))?;
        let count: i64 = count_stmt.query_row([], |row| row.get(0))?;

        let mut top_ten_stmt = self.connection.prepare(&format!(
            "SELECT pubkey, lamports FROM '{}' WHERE lamports > 0 ORDER BY lamports DESC LIMIT 10",
            parquet_path
        ))?;

        let rows = top_ten_stmt.query_map([], |row| {
            let pubkey: Vec<u8> = row.get(0)?;
            let lamports: u64 = row.get(1)?;
            Ok((pubkey, lamports))
        })?;

        for row in rows {
            let (pubkey, lamports) = row?;
            println!("pubkey: {:x?}, lamports: {}", pubkey, lamports);
        }

        Ok(count)
    }
}
