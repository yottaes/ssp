use duckdb::Connection;

pub struct DuckDB {
    connection: Connection,
}

impl DuckDB {
    pub fn open() -> Result<Self, anyhow::Error> {
        let conn = Connection::open_in_memory()?;
        Ok(DuckDB { connection: conn })
    }

    pub fn query_decoded(&self, parquet_path: &str, sort_col: &str) -> Result<(), anyhow::Error> {
        let count: i64 = self
            .connection
            .prepare(&format!("SELECT COUNT(*) FROM '{}'", parquet_path))?
            .query_row([], |row| row.get(0))?;
        println!("  total: {}", count);

        let sql = format!(
            "SELECT * FROM '{}' ORDER BY {} DESC LIMIT 10",
            parquet_path, sort_col
        );

        let mut stmt = self.connection.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            let mut cols = Vec::new();
            let mut i = 0;
            loop {
                match row.get::<_, duckdb::types::Value>(i) {
                    Ok(val) => cols.push(val),
                    Err(_) => break,
                }
                i += 1;
            }
            Ok(cols)
        })?;

        for row in rows {
            let cols = row?;
            for val in &cols {
                match val {
                    duckdb::types::Value::Blob(bytes) => {
                        print!("{} ", bs58::encode(bytes).into_string())
                    }
                    duckdb::types::Value::Null => print!("None "),
                    other => print!("{:?} ", other),
                }
            }
            println!();
        }

        Ok(())
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
