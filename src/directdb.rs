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
    get_sharks_from_manta_obj, object_id_from_manta_obj, SharkspotterMessage,
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
            &conf.sharks,
            shard,
            &obj_tx,
            &log,
        ) {
            return Err(e);
        }
    }

    Ok(())
}

fn check_value_for_match(
    value: &Value,
    row: &Row,
    filter_sharks: &[String],
    shard: u32,
    obj_tx: &crossbeam_channel::Sender<SharkspotterMessage>,
    log: &Logger,
) -> Result<(), Error> {
    let obj_id = object_id_from_manta_obj(value)
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let sharks = get_sharks_from_manta_obj(value, log)?;

    trace!(log, "sharkspotter checking {}", obj_id);
    sharks
        .iter()
        .filter(|s| filter_sharks.contains(&s.manta_storage_id))
        .try_for_each(|s| {
            send_matching_object(row, &s.manta_storage_id, shard, &obj_tx, log)
        })
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
