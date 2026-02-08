use std::env::args;
mod parser;
use parser::AccountHeader;

mod db;

use duckdb::Connection;

use crate::db::DuckDB;

fn main() -> Result<(), anyhow::Error> {
    let args: Vec<String> = args().collect();
    if args.is_empty() {
        panic!("Please provide file path!");
    }
    let path = &args[1];

    let conn = Connection::open("accounts.db")?;
    let db = DuckDB::init(conn)?;
    let mut appender = db.appender()?;

    AccountHeader::parse(path, |header| DuckDB::append_row(&mut appender, header))?;

    let count = db.query_top_accounts()?;
    println!("total: {}", count);

    Ok(())
}
