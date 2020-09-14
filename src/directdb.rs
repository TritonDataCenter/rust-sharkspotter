/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use crossbeam_channel as crossbeam;
use futures::{pin_mut, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use slog::{debug, error, trace, warn, Logger};
use std::io::{Error, ErrorKind};
use tokio_postgres::{NoTls, Row};

use crate::config::Config;
use crate::{
    config, get_sharks_from_manta_obj, object_id_from_manta_obj,
    SharkspotterMessage,
};

// Unfortunately the Manta records in the moray database are slightly
// different from what we get back from the moray service (both for the
// `findobjects` and `sql` endpoints.  So if we are going direct to the database
// we need to use a different struct to represent the record (DB schema).
//
// moray=> SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_name = 'manta';
// table_name | column_name | data_type
// ------------+-------------+-----------
//  manta      | _id         | bigint
//  manta      | _txn_snap   | integer
//  manta      | _key        | text
//  manta      | _value      | text
//  manta      | _etag       | character
//  manta      | _mtime      | bigint
//  manta      | _vnode      | bigint
//  manta      | dirname     | text
//  manta      | name        | text
//  manta      | owner       | text
//  manta      | objectid    | text
//  manta      | type        | text
#[derive(Deserialize, Serialize)]
struct MorayMantaBucketObject {
    _id: i64,
    _txn_snap: Option<i32>,
    _key: String,
    _value: String,
    _etag: String,
    _mtime: i64,
    _vnode: i64,
    dirname: String,
    name: String,
    owner: String,
    objectid: String,
    #[serde(alias = "type")]
    record_type: String,
}

pub async fn get_objects_from_shard(
    shard: u32,
    conf: Config,
    log: Logger,
    obj_tx: crossbeam::Sender<SharkspotterMessage>,
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
        let val_str: &str = row.get("_value");
        let value: Value = serde_json::from_str(val_str)
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
        if let Err(e) = check_value_for_match(
            &value,
            &row,
            &conf,
            shard,
            &obj_tx,
            &log,
            &local_db_conn,
        ) {
            return Err(e);
        }
    }

    Ok(())
}

// Move me:
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::{DatabaseErrorKind, Error::DatabaseError};

fn check_value_for_match(
    value: &Value,
    row: &Row,
    conf: &config::Config,
    shard: u32,
    obj_tx: &crossbeam_channel::Sender<SharkspotterMessage>,
    log: &Logger,
    local_db_conn: &PgConnection,
) -> Result<(), Error> {
    let obj_id = object_id_from_manta_obj(value)
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let sharks = get_sharks_from_manta_obj(value, log)?;

    trace!(log, "sharkspotter checking {}", obj_id);
    match conf.clone().filter_type {
        config::FilterType::Shark(filter_sharks) => sharks
            .iter()
            .filter(|s| filter_sharks.contains(&s.manta_storage_id))
            .try_for_each(|s| {
                send_matching_object(
                    row,
                    &s.manta_storage_id,
                    shard,
                    &obj_tx,
                    log,
                )
            }),
        config::FilterType::NumCopies(num_copies) => {
            if sharks.len() as u32 > num_copies {
                send_matching_object(row, "", shard, &obj_tx, log)
            } else {
                Ok(())
            }
        }
        config::FilterType::Duplicates => {
            check_for_duplicate(row, shard, log, local_db_conn)
        }
    }
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
struct MantaStub {
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

// Save this off.  We need to detect the conflict and insert a record into
// the duplicate table.
/*
   let mut client = pg::Client::connect(
       "host=localhost user=postgres",
       pg::NoTls
   );

   client.execute(
       "INSERT INTO mantastubs (id, key, etag, shards) \
       VALUES ($1, $2, $3, $4) \
       ON CONFLICT (id)\
       DO UPDATE SET shards = mantastubs.shards || EXCLUDED.shards;
       ",
       &[&stub.id, &stub.key, &stub.etag, &stub.shards],
   ).expect("Upsert error");

*/

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

// Diesel doesn't have the ability to concatenate arrays yet.  Also since we
// are multi-threaded we can't do two queries... one to get the array, and
// one to set it to a concatenated version. So we need to use the postgres
// crate here to issue the update query directly.
fn update_stub(stub: &MantaStub) {
    let mut client =
        pg::Client::connect("host=localhost user=postgres", pg::NoTls)
            .expect("PG Connection error");

    // If this fails we might lose track of data, so panic.
    client.execute(
        "UPDATE mantastubs SET duplicate = 'yes', shards = mantastubs.shards \
        || $2 WHERE id = $1;",
        &[&stub.id, &stub.shards],
    ).expect("Upsert error");
}

// We've found a duplicate.  This function needs to do 2 things.
// 1. Get the current stub etag and compare it to the current etag.  If they
// don't match we have a problem.
// 2. If the etags do match put the duplicate in a database by itself, and
// update the stub's etags.
fn handle_duplicate(
    stub: &MantaStub,
    manta_value: &Value,
    log: &Logger,
    conn: &PgConnection,
) {
    use self::mantastubs::dsl::{id as stub_id, mantastubs};

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

    update_stub(stub);
    insert_metadata_into_duplicate_table(stub, manta_value, log, conn);
}

fn insert_stub(
    stub: &MantaStub,
    conn: &PgConnection,
) -> diesel::result::QueryResult<usize> {
    use self::mantastubs::dsl::mantastubs;

    diesel::insert_into(mantastubs).values(stub).execute(conn)
}

fn check_for_duplicate(
    row: &Row,
    shard: u32,
    log: &Logger,
    conn: &PgConnection,
) -> Result<(), Error> {
    let moray_object: MorayMantaBucketObject = serde_postgres::from_row(&row)
        .map_err(|e| {
        error!(
            log,
            "Error deserializing record as moray manta object: {}", e
        );
        Error::new(ErrorKind::Other, e)
    })?;

    let etag = moray_object._etag.clone();
    let id = moray_object.objectid;
    let manta_value_str = moray_object._value.as_str();
    let manta_value: Value = serde_json::from_str(manta_value_str)
        .map_err(|e| Error::new(ErrorKind::Other, e))?;

    let key = moray_object._key;
    let shards = vec![shard as i32];

    let stub = MantaStub {
        id,
        key,
        etag,
        duplicate: false,
        shards,
    };

    match insert_stub(&stub, conn) {
        Err(DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => {
            handle_duplicate(&stub, &manta_value, log, conn);
            Ok(())
        }
        Ok(_) => Ok(()),
        _ => panic!("Unknown database error"),
    }
}

fn send_matching_object(
    row: &Row,
    shark_name: &str,
    shard: u32,
    obj_tx: &crossbeam_channel::Sender<SharkspotterMessage>,
    log: &Logger,
) -> Result<(), Error> {
    trace!(log, "Found matching record: {:#?}", &row);
    let moray_object: MorayMantaBucketObject = serde_postgres::from_row(&row)
        .map_err(|e| {
        error!(log, "Error deserializing record as manta object: {}", e);
        Error::new(ErrorKind::Other, e)
    })?;

    let etag = moray_object._etag.clone();
    let manta_value_str = moray_object._value.as_str();
    let manta_value: Value = serde_json::from_str(manta_value_str)
        .map_err(|e| Error::new(ErrorKind::Other, e))?;

    debug!(log, "Sending value: {:#?}", manta_value);

    let msg = SharkspotterMessage {
        manta_value,
        etag,
        shark: shark_name.to_string(),
        shard,
    };

    if let Err(e) = obj_tx.send(msg) {
        warn!(log, "Tx channel disconnected: {}", e);
        return Err(Error::new(ErrorKind::BrokenPipe, e));
    }
    Ok(())
}
