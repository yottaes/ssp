use crate::AccountHeader;
use duckdb::{Appender, Connection, params};

pub struct DuckDB {
    connection: Connection,
}

impl DuckDB {
    pub fn init(conn: Connection) -> Result<Self, anyhow::Error> {
        eprintln!("DROPPING TABLE! DELETE IN PRODUCTION!");
        conn.execute("DROP TABLE IF EXISTS accounts", [])?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS accounts(
            pubkey BLOB,
            lamports UBIGINT,
            owner BLOB,
            data_len UBIGINT,
            executable BOOLEAN,
            rent_epoch UBIGINT
        )",
            [],
        )?;

        Ok(DuckDB { connection: conn })
    }

    pub fn appender(&self) -> Result<Appender<'_>, duckdb::Error> {
        self.connection.appender("accounts")
    }

    pub fn query_top_accounts(&self) -> Result<i64, anyhow::Error> {
        let mut count_stmt = self.connection.prepare("SELECT COUNT(*) FROM accounts")?;
        let count: i64 = count_stmt.query_row([], |row| row.get(0))?;

        let mut top_ten_stmt = self.connection.prepare(
            "SELECT pubkey, lamports FROM accounts WHERE lamports > 0 ORDER BY lamports DESC LIMIT 10",
        )?;

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

    pub fn append_row(appender: &mut Appender, header: &AccountHeader) -> anyhow::Result<()> {
        appender.append_row(params![
            header.pubkey.as_slice(),
            header.lamports,
            header.owner.as_slice(),
            header.data_len,
            header.executable == 1,
            header.rent_epoch
        ])?;
        Ok(())
    }
}
