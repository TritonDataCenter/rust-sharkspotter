use diesel::pg::PgConnection;
use diesel::prelude::*;

static DB_URL: &str = "postgres://postgres:postgres@";

pub fn connect_db(db_name: &str) -> Result<PgConnection, String> {
    let connect_url = format!("{}/{}", DB_URL, db_name);
    PgConnection::establish(&connect_url).map_err(|e| e.to_string())
}

pub fn create_db(db_name: &str) -> Result<usize, String> {
    let create_query = format!("CREATE DATABASE \"{}\"", db_name);
    let conn = PgConnection::establish(&DB_URL).map_err(|e| e.to_string())?;

    conn.execute(&create_query).map_err(|e| e.to_string())
}

pub fn create_and_connect_db(db_name: &str) -> Result<PgConnection, String> {
    create_db(db_name)?;
    connect_db(db_name)
}

pub fn create_tables(conn: &PgConnection) -> Result<(), String> {
    let create_stubs = "CREATE TABLE mantastubs(
        id TEXT PRIMARY KEY,
        key TEXT,
        etag TEXT,
        shards Int[]
    );";

    let create_duplicates = "CREATE TABLE mantaduplicates(
        id TEXT PRIMARY KEY,
        key TEXT,
        object Jsonb
    );";

    println!("Creating stub and duplicate tables");

    conn.execute(&create_stubs).map_err(|e| {
        format!("Could not create stub table: {}", e.to_string())
    })?;

    conn.execute(&create_duplicates).map_err(|e| {
        format!("Could not create duplicate table: {}", e.to_string())
    })?;

    Ok(())
}
