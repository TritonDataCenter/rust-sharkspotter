/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use crate::config::{validate_config, Config};
use crate::directdb::MorayMantaBucketObjectEssential;
use crossbeam_channel as crossbeam;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::{DatabaseErrorKind, Error::DatabaseError};
use futures::{pin_mut, TryStreamExt};
use lazy_static::lazy_static;
use serde::Serialize;
use serde_json::Value;
use slog::{debug, error, info, warn, Logger};
use std::io::{Error, ErrorKind};
use std::sync::Mutex;
use threadpool::ThreadPool;
use tokio_postgres::{NoTls, Row};

lazy_static! {
    static ref DUP_ERROR_LIST: Mutex<Vec<std::io::Error>> = Mutex::new(vec![]);
}

#[derive(Debug)]
pub struct DuplicateInfo {
    stub: MantaStub,
    manta_value: Value,
}

table! {
    use diesel::sql_types::{Text, Array, Integer, Bool};
    mantastubs(id) {
        id -> Text,
        key -> Text,
        etag -> Text,
        duplicate -> Bool,
        shards -> Array<Integer>,
    }
}

#[derive(Clone, Debug, Insertable, AsChangeset, Queryable, Serialize)]
#[table_name = "mantastubs"]
pub struct MantaStub {
    id: String,
    key: String,
    etag: String,
    duplicate: bool,
    shards: Vec<i32>,
}

table! {
    use diesel::sql_types::{Text, Jsonb};
    mantaduplicates(id) {
        id -> Text,
        key -> Text,
        object -> Jsonb,
    }
}

#[derive(
    Insertable,
    Queryable,
    Identifiable,
    AsChangeset,
    AsExpression,
    Debug,
    Default,
    Clone,
    PartialEq,
)]
#[table_name = "mantaduplicates"]
struct MantaDuplicate {
    id: String,
    key: String,
    object: Value,
}

pub fn run_duplicate_detector(
    configuration: &Config,
    log: Logger,
    dup_tx: crossbeam_channel::Sender<DuplicateInfo>,
) -> Result<(), Error> {
    let mut conf = configuration.clone();
    if let Err(e) = validate_config(&mut conf) {
        warn!(log, "{}", e);
    }

    let pool = ThreadPool::with_name("shard_scanner".into(), conf.max_threads);

    for shard in conf.min_shard..=conf.max_shard {
        run_shard_thread(&pool, shard, &dup_tx, &conf, &log);
    }

    pool.join();

    let mut error_strings = String::new();
    let error_list = DUP_ERROR_LIST.lock().unwrap();
    for error in error_list.iter() {
        if error.kind() == ErrorKind::BrokenPipe {
            continue;
        }
        error_strings = format!("{}{}\n", error_strings, error);
    }

    if !error_strings.is_empty() {
        let msg = format!(
            "Sharkspotter encountered the following errors:\n{}",
            error_strings
        );
        return Err(Error::new(ErrorKind::Other, msg));
    }

    Ok(())
}

fn run_shard_thread(
    pool: &ThreadPool,
    shard: u32,
    dup_tx: &crossbeam_channel::Sender<DuplicateInfo>,
    conf: &Config,
    log: &Logger,
) {
    let th_dup_tx = dup_tx.clone();
    let th_conf = conf.clone();
    let th_log = log.clone();

    pool.execute(move || {
        // In test we noticed that the basic scheduler outperformed both the
        // `threaded_scheduler()` with tuned thread counts and the default
        // thread counts provided by `Runtime::new()` by 33%.  It also does not
        // create any additional LWPs.
        let mut rt = match tokio::runtime::Builder::new()
            .enable_all()
            .basic_scheduler()
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                error!(th_log, "could not create runtime: {}", e);
                DUP_ERROR_LIST.lock().expect("ERROR_LIST lock").push(e);
                return;
            }
        };

        if let Err(e) =
            rt.block_on(scan_shard(shard, th_conf, th_log.clone(), th_dup_tx))
        {
            // We use BrokenPipe in directdb::send_matching_object() to
            // indicate that our receiver has shutdown.
            // This is not an error in the context of lib sharkspotter.  The
            // consumer of sharkspotter may encounter an error which causes
            // it to stop receiving objects, but that error should be
            // handled by the consumer not here.
            if e.kind() != ErrorKind::BrokenPipe {
                error!(th_log, "shard thread error: {}", e);
            }
            DUP_ERROR_LIST.lock().expect("ERROR_LIST lock").push(e);
        }
    });
}

pub async fn scan_shard(
    shard: u32,
    conf: Config,
    log: Logger,
    dup_tx: crossbeam::Sender<DuplicateInfo>,
) -> Result<(), Error> {
    let local_db_conn =
        crate::db::connect_db(&conf.db_name).expect("Connect to local db");

    let shard_host_name =
        format!("{}.rebalancer-postgres.{}", shard, conf.domain);

    debug!(log, "Connecting to {}", shard_host_name);
    // Connect to this shard's reblancer-postgres moray database.
    let (client, connection) = tokio_postgres::Config::new()
        .host(shard_host_name.as_str())
        .user("postgres")
        .dbname("moray")
        .keepalives_idle(std::time::Duration::from_secs(30))
        .connect(NoTls)
        .await
        .map_err(|e| {
            error!(log, "failed to connect to {}: {}", &shard_host_name, e);
            Error::new(ErrorKind::Other, e)
        })?;

    let task_host_name = shard_host_name.clone();
    let task_log = log.clone();

    tokio::spawn(async move {
        connection.await.map_err(|e| {
            error!(
                task_log,
                "could not communicate with {}: {}", task_host_name, e
            );
            Error::new(ErrorKind::Other, e)
        })?;
        Ok::<(), Error>(())
    });

    let rows = client
        .query_raw("SELECT * from manta where type='object'", vec![])
        .await
        .map_err(|e| {
            error!(log, "query error for {}: {}", &shard_host_name, e);
            Error::new(ErrorKind::Other, e)
        })?;

    pin_mut!(rows);
    // Iterate over the rows in the stream.  For each one determine if it
    // matches the shark we are looking for.
    while let Some(row) = rows
        .try_next()
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?
    {
        if let Err(e) =
            check_for_duplicate(&row, shard, &log, &local_db_conn, &dup_tx)
        {
            return Err(e);
        }
    }

    Ok(())
}

fn check_for_duplicate(
    row: &Row,
    shard: u32,
    log: &Logger,
    conn: &PgConnection,
    dup_tx: &crossbeam_channel::Sender<DuplicateInfo>,
) -> Result<(), Error> {
    let moray_object: MorayMantaBucketObjectEssential =
        serde_postgres::from_row(&row).map_err(|e| {
            error!(
                log,
                "Error deserializing record as moray manta object: {}", e
            );
            Error::new(ErrorKind::Other, e)
        })?;

    let id = moray_object.objectid;
    let key = moray_object._key;
    let etag = moray_object._etag.clone();

    let manta_value_str = moray_object._value.as_str();
    let manta_value: Value = serde_json::from_str(manta_value_str)
        .map_err(|e| Error::new(ErrorKind::Other, e))?;

    let shards = vec![shard as i32];

    let stub = MantaStub {
        id,
        key: key.clone(),
        etag,
        duplicate: false,
        shards,
    };

    match insert_stub(&stub, conn) {
        Err(DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => {
            info!(log, "Found duplicate {}", key);
            let dup_info = DuplicateInfo { stub, manta_value };

            dup_tx.send(dup_info).unwrap_or_else(|_| {
                panic!("Error sending duplicate info for shard: {}", shard)
            });
            Ok(())
        }
        Ok(_) => Ok(()),
        _ => panic!("Unknown database error"),
    }
}

fn insert_stub(
    stub: &MantaStub,
    conn: &PgConnection,
) -> diesel::result::QueryResult<usize> {
    use self::mantastubs::dsl::mantastubs;

    diesel::insert_into(mantastubs).values(stub).execute(conn)
}

// We've found a duplicate.  This function needs to do 2 things.
// 1. Get the current stub etag and compare it to the current etag.  If they
// don't match we have a problem.
// 2. If the etags do match put the duplicate in a database by itself, and
// update the stub's etags.
fn handle_duplicate(
    conf: &Config,
    dup_info: DuplicateInfo,
    log: &Logger,
    conn: &PgConnection,
) {
    use self::mantastubs::dsl::{id as stub_id, mantastubs};

    let stub = dup_info.stub;
    let manta_value = dup_info.manta_value;
    let resident_stubs: Vec<MantaStub> = mantastubs
        .filter(stub_id.eq(&stub.id))
        .load::<MantaStub>(conn)
        .expect("Attempt to get stub that does not exist");

    assert_eq!(resident_stubs.len(), 1, "expected 1 manta stub");

    let resident_etag = resident_stubs[0].etag.clone();
    if stub.etag != resident_etag {
        error!(
            log,
            "Found two metadata entries with different etags for {:#?}", stub
        );
        return;
    }

    update_stub(conf, &stub);
    insert_metadata_into_duplicate_table(&stub, &manta_value, log, conn);
}

pub fn handle_duplicate_thread(
    conf: Config,
    dup_rx: crossbeam_channel::Receiver<DuplicateInfo>,
    log: Logger,
) {
    let conn =
        crate::db::connect_db(&conf.db_name).expect("Connect to local db");

    loop {
        match dup_rx.recv() {
            Ok(dup_info) => {
                handle_duplicate(&conf, dup_info, &log, &conn);
            }
            Err(e) => {
                let msg = format!("Exiting duplicate handler thread: {}", e);
                info!(log, "{}", msg);
                break;
            }
        }
    }
}

// Diesel doesn't have the ability to concatenate arrays yet.  Also since we
// are multi-threaded we can't do two queries... one to get the array, and
// one to set it to a concatenated version. So we need to use the postgres
// crate here to issue the update query directly.
fn update_stub(conf: &Config, stub: &MantaStub) {
    let connect_string = format!(
        "host=localhost user=postgres password=postgres dbname={}",
        conf.db_name
    );

    // TODO: Test me
    let mut client = pg::Client::connect(&connect_string, pg::NoTls)
        .expect("PG Connection error");

    // If this fails we might lose track of data, so panic.
    // https://stackoverflow.com/questions/29319801/how-to-append-a-new-item-into-the-array-type-column-in-postgresql
    client.execute(
        "UPDATE mantastubs SET duplicate = 'yes', shards = mantastubs.shards \
        || $2 WHERE id = $1;",
        &[&stub.id, &stub.shards],
    ).expect("Upsert error");
}

// Insert duplicate metadata entry for safe keeping. We ignore conflicts
// because this table is only populated when the first duplicate is found.
// If multiple duplicates are found we don't need to update the metadata.  We
// should have already validated that etags match.
fn insert_metadata_into_duplicate_table(
    stub: &MantaStub,
    manta_value: &Value,
    log: &Logger,
    conn: &PgConnection,
) {
    use self::mantaduplicates::dsl::mantaduplicates;

    let duplicate = MantaDuplicate {
        id: stub.id.clone(),
        key: stub.key.clone(),
        object: manta_value.to_owned(),
    };

    match diesel::insert_into(mantaduplicates)
        .values(duplicate)
        .execute(conn)
    {
        Ok(_) => (),
        Err(DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => (),
        Err(e) => {
            error!(
                log,
                "Encountered error while attempting to update duplicate \
                 table {}",
                e.to_string()
            );
            panic!("Duplicate insertion error");
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{connect_or_create_db, create_tables};
    use crate::util;
    use serde_json::json;

    #[test]
    fn test_update_stub() {
        use self::mantaduplicates::dsl::mantaduplicates;
        use self::mantastubs::dsl::{id as stub_id, mantastubs};

        let _guard = util::init_global_logger(None);
        let log = slog_scope::logger();
        let db_name = "test_update".to_string();
        let mut conf = Config::default();

        let id = "id1".to_string();
        let key = "key1".to_string();
        let etag = "etag1".to_string();
        let duplicate = false;
        let shards = vec![1];
        let mut expected_shards = vec![1, 2];

        let stub = MantaStub {
            id: id.clone(),
            key: key.clone(),
            etag: etag.clone(),
            duplicate,
            shards,
        };

        conf.db_name = db_name;

        let conn = connect_or_create_db(&conf.db_name).expect("create db");
        for t in ["mantastubs", "mantaduplicates"].iter() {
            let drop_query = format!("DROP TABLE {}", t);
            let _ = conn.execute(&drop_query); // ignore error
        }

        create_tables(&conn).expect("create tables");
        insert_stub(&stub, &conn).expect("insert stub");

        let new_stub = MantaStub {
            id,
            key,
            etag,
            duplicate,
            shards: vec![2],
        };

        let manta_value = json!({
            "somekey": "some value"
        });

        let dup_info = DuplicateInfo {
            stub: new_stub,
            manta_value,
        };

        handle_duplicate(&conf, dup_info, &log, &conn);

        let stubs: Vec<MantaStub> = mantastubs
            .filter(stub_id.eq(&stub.id))
            .load::<MantaStub>(&conn)
            .expect("Attempt to get stub that does not exist");

        println!("stubs {:#?}", stubs);
        assert_eq!(stubs.len(), 1);

        let mut found_shards = stubs[0].shards.clone();

        expected_shards.sort();
        found_shards.sort();

        assert_eq!(expected_shards, found_shards, "expected vs found shards");
        assert!(stubs[0].duplicate);

        let duplicates: Vec<MantaDuplicate> = mantaduplicates
            .load::<MantaDuplicate>(&conn)
            .expect("Attempt to get duplicate that does not exist");

        println!("Duplicates: {:#?}", duplicates);
        assert_eq!(duplicates.len(), 1);
    }
}
