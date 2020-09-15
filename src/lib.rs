/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

// For reference here is a sample moray manta bucket entry.  The _value
// portion is the manta object metadata.
// {
//   "bucket": "manta",
//   "_count": 224574,
//   "_etag": "7712D647",
//   "_id": 114590,
//   "_mtime": 1570611723074,
//   "key": "/61368287-aa5b-6c0f-f3a9-931a228215e4/stor/logs/manatee-sitter/2019/10/09/08/07e023da.log",
//   "_value": {
//     "contentLength": 9099176,
//     "contentMD5": "L9NrIZXTY37AYVZN9+gZ7w==",
//     "contentType": "text/plain",
//     "creator": "61368287-aa5b-6c0f-f3a9-931a228215e4",
//     "dirname": "/61368287-aa5b-6c0f-f3a9-931a228215e4/stor/logs/manatee-sitter/2019/10/09/08",
//     "etag": "2e08b069-d132-c25c-920c-945e3329e450",
//     "headers": {},
//     "key": "/61368287-aa5b-6c0f-f3a9-931a228215e4/stor/logs/manatee-sitter/2019/10/09/08/07e023da.log",
//     "mtime": 1570611723062,
//     "name": "07e023da.log",
//     "objectId": "2e08b069-d132-c25c-920c-945e3329e450",
//     "owner": "61368287-aa5b-6c0f-f3a9-931a228215e4",
//     "roles": [],
//     "sharks": [
//       {
//         "datacenter": "ruidc0",
//         "manta_storage_id": "3.stor.east.joyent.us"
//       },
//       {
//         "datacenter": "ruidc0",
//         "manta_storage_id": "1.stor.east.joyent.us"
//       }
//     ],
//     "type": "object",
//     "vnode": 23352
//   }
// }

pub mod config;
pub mod db;
pub mod directdb;
pub mod util;

#[macro_use]
extern crate diesel;

use lazy_static::lazy_static;
use libmanta::moray::MantaObjectShark;
use moray::client::MorayClient;
use moray::objects as moray_objects;
use serde::Deserialize;
use serde_json::{self, Value};
use slog::{debug, error, warn, Logger};
use std::io::{Error, ErrorKind};
use std::net::IpAddr;
use std::sync::Mutex;
use threadpool::ThreadPool;
use trust_dns_resolver::Resolver;

lazy_static! {
    static ref ERROR_LIST: Mutex<Vec<std::io::Error>> = Mutex::new(vec![]);
}

#[derive(Deserialize, Debug, Clone)]
struct IdRet {
    max: String,
}

#[derive(Debug)]
pub struct DuplicateInfo {
    stub: directdb::MantaStub,
    manta_value: Value,
}

#[derive(Debug)]
pub struct SharkspotterMessage {
    pub manta_value: Value,
    pub etag: String,
    pub shark: String,
    pub shard: u32,
}

fn _parse_max_id_value(val: Value, log: &Logger) -> Result<u64, Error> {
    if val.is_array() {
        let val_arr = val.as_array().unwrap();

        if val_arr.len() != 1 {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Expected single element got {}", val_arr.len()),
            ));
        }
    } else {
        return Err(Error::new(ErrorKind::Other, "Expected array"));
    }

    let max = match val[0].get("max") {
        Some(m) => m.to_owned(),
        None => {
            return Err(Error::new(
                ErrorKind::Other,
                "Query missing 'max' value",
            ));
        }
    };

    let max_num: u64 = match max {
        Value::Number(n) => {
            debug!(log, "Parsing largest id value as Number");
            match n.as_u64() {
                Some(num_64) => num_64,
                None => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "Error converting number to u64",
                    ));
                }
            }
        }
        Value::String(s) => {
            debug!(log, "Parsing largest id value as String");
            match s.parse() {
                Ok(snum) => snum,
                Err(e) => {
                    let msg =
                        format!("Error parsing max value as String: {}", e);
                    return Err(Error::new(ErrorKind::Other, msg));
                }
            }
        }
        _ => {
            debug!(log, "largest id value is unknown variant {:#?}", max);
            return Err(Error::new(
                ErrorKind::Other,
                "Error max value was not a string or a number",
            ));
        }
    };

    Ok(max_num)
}

/// Find the largest _id/_idx in the database.
fn find_largest_id_value(
    log: &Logger,
    mclient: &mut MorayClient,
    id: &str,
) -> Result<u64, Error> {
    let mut ret: u64 = 0;
    debug!(log, "Finding largest ID value as '{}'", id);
    mclient.sql(
        format!("SELECT MAX({}) FROM manta;", id).as_str(),
        vec![],
        r#"{"limit": 1, "no_count": true}"#,
        |resp| {
            // The expected response is:
            //  [{
            //      "max": <value>
            //  }]
            //
            //  Where <value> is either a String or a Number.

            ret = match _parse_max_id_value(resp.to_owned(), log) {
                Ok(max_num) => max_num,
                Err(e) => {
                    return Err(e);
                }
            };
            Ok(())
        },
    )?;
    Ok(ret)
}

fn _log_return_error(log: &Logger, msg: &str) -> Result<(), Error> {
    error!(log, "{}", msg);
    Err(Error::new(ErrorKind::Other, msg))
}

pub fn get_sharks_from_manta_obj(
    value: &Value,
    log: &Logger,
) -> Result<Vec<MantaObjectShark>, Error> {
    match value.get("sharks") {
        Some(s) => {
            if !s.is_array() {
                let msg = format!("Sharks are not in an array {:#?}", s);
                error!(log, "{}", msg);
                return Err(Error::new(ErrorKind::Other, msg));
            }

            serde_json::from_value::<Vec<MantaObjectShark>>(s.clone()).map_err(
                |e| {
                    let msg = format!(
                        "Could not deserialize sharks value {:#?}. ({})",
                        s, e
                    );
                    error!(log, "{}", msg);
                    Error::new(ErrorKind::Other, msg)
                },
            )
        }
        None => {
            let msg = format!("Missing 'sharks' field {:#?}", value);
            error!(log, "{}", msg);
            Err(Error::new(ErrorKind::Other, msg))
        }
    }
}

/// Pull the "_value" out of the moray object without using rust structures.
/// This takes a moray bucket entry in the form of a serde Value and returns
/// a manta object metadata entry in the form of a serde Value.
pub fn manta_obj_from_moray_obj(moray_obj: &Value) -> Result<Value, String> {
    match moray_obj.get("_value") {
        Some(val) => {
            let val_clone = val.clone();
            let str_val = match val_clone.as_str() {
                Some(s) => s,
                None => {
                    return Err(format!(
                        "Could not format entry as string {:#?}",
                        val
                    ));
                }
            };

            match serde_json::from_str(str_val) {
                Ok(o) => Ok(o),
                Err(e) => Err(format!(
                    "Could not format entry as object {:#?} ({})",
                    val, e
                )),
            }
        }
        None => {
            Err(format!("Missing '_value' in Moray entry {:#?}", moray_obj))
        }
    }
}

pub fn object_id_from_manta_obj(manta_obj: &Value) -> Result<String, String> {
    manta_obj
        .get("objectId")
        .ok_or_else(|| {
            format!("Missing 'objectId' in Manta Object {:#?}", manta_obj)
        })
        .and_then(|obj_id_val| {
            obj_id_val.as_str().ok_or_else(|| {
                format!("Could not format objectId ({}) as string", obj_id_val)
            })
        })
        .and_then(|o| Ok(o.to_string()))
}

pub fn etag_from_moray_value(moray_value: &Value) -> Result<String, Error> {
    match moray_value.get("_etag") {
        Some(tag) => match serde_json::to_string(tag) {
            Ok(t) => Ok(t.replace("\"", "")),
            Err(e) => {
                let msg = format!("Cannot convert etag to string: {}", e);
                Err(Error::new(ErrorKind::Other, msg))
            }
        },
        None => {
            let msg = format!("Missing etag: {:#?}", moray_value);
            Err(Error::new(ErrorKind::Other, msg))
        }
    }
}

// TODO: add tests for this function
// See block comment at top of a file for an example of the object this is
// working with.
/// Called for every object that is read in by the query executed in
/// read_chunk().  For a given object:
///     1. Validate it is of the right form.
///     2. Get it's "_value" which is the manta object metadata(*).
///     3. Check if the manta object metadata is for an object that is on the
///        shark that the caller is looking for.
///     4. Return the entire moray entry as a serde Value
///
/// (*): The manta object metadata does not have a consistent schema, so the
/// only thing we look for is the "sharks" array which should always be there
/// regardless of the schema.  If it is not then we can't really filter on
/// the shark so we log an error and move on, not returning the value to the
/// caller.
fn query_handler<F>(
    log: &Logger,
    val: &Value,
    shard_num: u32,
    sharks_requested: &[String],
    handler: &mut F,
) -> Result<(), Error>
where
    F: FnMut(Value, &str, &str, u32) -> Result<(), Error>,
{
    match val.as_array() {
        Some(v) => {
            if v.len() > 1 {
                warn!(
                    log,
                    "Expected 1 value, got {}.  Using first entry.",
                    v.len()
                );
            }
        }
        None => {
            return _log_return_error(log, "Entry is not an array");
        }
    }

    let moray_value = match val.get(0) {
        Some(v) => v,
        None => {
            return _log_return_error(log, "Entry is empty");
        }
    };

    let moray_object: Value = match serde_json::from_value(moray_value.clone())
    {
        Ok(mo) => mo,
        Err(e) => {
            let msg = format!(
                "Could not deserialize moray value {:#?}. ({})",
                moray_value, e
            );
            return _log_return_error(log, &msg);
        }
    };

    // here rui
    let manta_value = match manta_obj_from_moray_obj(&moray_object) {
        Ok(v) => v,
        Err(e) => {
            return _log_return_error(log, &e);
        }
    };

    let sharks = get_sharks_from_manta_obj(&manta_value, &log)?;

    // Filter on shark
    sharks
        .iter()
        .filter(|s| sharks_requested.contains(&s.manta_storage_id))
        .try_for_each(|s| {
            // TODO: handle error
            let etag = etag_from_moray_value(&moray_value)?;
            handler(
                manta_value.clone(),
                etag.as_str(),
                s.manta_storage_id.as_str(),
                shard_num,
            )
        })?;

    Ok(())
}

fn chunk_query(id_name: &str, begin: u64, end: u64, count: u64) -> String {
    format!(
        "SELECT * FROM manta WHERE {} >= {} AND \
         {} <= {} AND type = 'object' limit {};",
        id_name, begin, id_name, end, count
    )
}

/// Make the actual sql query and call the query_handler to handle processing
/// every object that is returned in the chunk.
fn read_chunk<F>(
    log: &Logger,
    mclient: &mut MorayClient,
    query: &str,
    shard_num: u32,
    sharks: &[String],
    handler: &mut F,
) -> Result<(), Error>
where
    F: FnMut(Value, &str, &str, u32) -> Result<(), Error>,
{
    match mclient.sql(query, vec![], r#"{"timeout": 10000}"#, |a| {
        query_handler(log, a, shard_num, sharks, handler)
    }) {
        Ok(()) => Ok(()),
        Err(e) => {
            eprintln!("Got error: {}", e);
            Err(e)
        }
    }
}

/// Find the maximum _id/_idx and, starting at 0 iterate over every entry up
/// to the max.  For each chunk call read_chunk.
fn iter_ids<F>(
    id_name: &str,
    moray_socket: &str,
    conf: &config::Config,
    log: Logger,
    shard_num: u32,
    mut handler: F,
) -> Result<(), Error>
where
    F: FnMut(Value, &str, &str, u32) -> Result<(), Error>,
{
    let mut mclient = MorayClient::from_str(moray_socket, log.clone(), None)?;

    let mut start_id = conf.begin;
    let mut end_id = conf.begin + conf.chunk_size - 1;
    let largest_id = match find_largest_id_value(&log, &mut mclient, id_name) {
        Ok(id) => id,
        Err(e) => {
            error!(&log, "Error finding largest ID: {}, using 0", e);
            0
        }
    };

    let mut remaining = largest_id - conf.begin + 1;
    assert!(largest_id + 1 >= remaining);

    if end_id > conf.end {
        end_id = conf.end;
    }

    while remaining > 0 {
        let query = chunk_query(id_name, start_id, end_id, conf.chunk_size);
        match read_chunk(
            &log,
            &mut mclient,
            query.as_str(),
            shard_num,
            &conf.sharks,
            &mut handler,
        ) {
            Ok(()) => (),
            Err(e) => return Err(e),
        };

        start_id = end_id + 1;
        if start_id > largest_id {
            break;
        }

        end_id = start_id + conf.chunk_size - 1;
        if end_id > largest_id {
            end_id = largest_id
        }

        remaining = largest_id - start_id + 1;
        assert!(largest_id + 1 >= remaining);

        // Find the percent value rounded to the thousand-th of a percent.
        let percent_complete =
            (1.0 - remaining as f64 / largest_id as f64) * 100.0;
        let percent_complete = (percent_complete * 1000.0).round() / 1000.0;

        debug!(
            &log,
            "chunk scanned";
            "index" => id_name,
            "shard" => shard_num,
            "start_id" => start_id,
            "end_id" => end_id,
            "remaining_count" => remaining,
            "percent_complete" => percent_complete
        );
    }

    Ok(())
}

fn lookup_ip_str(host: &str) -> Result<String, Error> {
    let resolver = Resolver::from_system_conf()?;
    let response = resolver.lookup_ip(host)?;
    let ip: Vec<IpAddr> = response.iter().collect();

    Ok(ip[0].to_string())
}

fn shark_fix_common(conf: &mut config::Config, log: &Logger) {
    let mut new_sharks = Vec::with_capacity(conf.sharks.len());

    for shark in conf.sharks.iter() {
        if !shark.contains(conf.domain.as_str()) {
            let new_shark = format!("{}.{}", shark, conf.domain);
            warn!(log,
                  "Domain \"{}\" not found in storage node string:\"{}\", using \"{}\"",
                  conf.domain,
                  shark,
                  new_shark
            );

            new_sharks.push(new_shark);
        } else {
            new_sharks.push(shark.to_owned());
        }
    }
    conf.sharks = new_sharks;
}

fn validate_sharks(conf: &config::Config, log: &Logger) -> Result<(), Error> {
    if conf.skip_validate_sharks {
        return Ok(());
    }

    let sharks = &conf.sharks;
    let domain = &conf.domain;
    let shard1_moray = format!("1.moray.{}", domain);
    let moray_ip = lookup_ip_str(shard1_moray.as_str())?;
    let moray_socket = format!("{}:{}", moray_ip, 2021);
    let opts = moray_objects::MethodOptions::default();
    let mut mclient =
        MorayClient::from_str(moray_socket.as_str(), log.clone(), None)?;

    for shark in sharks.iter() {
        let mut count = 0;
        let filter = format!("manta_storage_id={}", shark);
        mclient.find_objects(
            "manta_storage",
            filter.as_str(),
            &opts,
            |_| {
                count += 1;
                Ok(())
            },
        )?;

        if count > 1 {
            return Err(Error::new(
                ErrorKind::Other,
                format!("More than one shark with name \"{}\" found", shark),
            ));
        }

        if count == 0 {
            return Err(Error::new(
                ErrorKind::Other,
                format!("No shark with name \"{}\" found", shark),
            ));
        }
    }

    Ok(())
}

/// Main entry point to for the sharkspotter library.  Callers need to
/// provide a closure that takes a serde Value and a u32 shard number as its
/// arguments.
/// Sharkspotter works by first getting the maximum and minimum _id and _idx
/// for a given moray bucket (which is always "manta"), and then querying for
/// entries in a user configurable chunk size.
pub fn run<F>(
    configuration: &config::Config,
    log: Logger,
    mut handler: F,
) -> Result<(), Error>
where
    F: FnMut(Value, &str, &str, u32) -> Result<(), Error>,
{
    let mut conf = configuration.clone();

    if let config::FilterType::Shark(_) = conf.filter_type {
        shark_fix_common(&mut conf, &log);
        validate_sharks(&conf, &log)?;
    }

    for i in conf.min_shard..=conf.max_shard {
        let moray_host = format!("{}.moray.{}", i, conf.domain);
        let moray_ip = lookup_ip_str(moray_host.as_str())?;
        let moray_socket = format!("{}:{}", moray_ip, 2021);

        // TODO: MANTA-4912
        // We can have both _id and _idx, we don't have to have both, but we
        // need at least 1.  This is an error that should be passed back to
        // the caller via the handler as noted in MANTA-4912.
        for id in ["_id", "_idx"].iter() {
            if let Err(e) =
                iter_ids(id, &moray_socket, &conf, log.clone(), i, &mut handler)
            {
                error!(&log, "Encountered error scanning shard {} ({})", i, e);
            }
        }
    }

    Ok(())
}

fn start_iter_ids_thread(
    id_name: &str,
    shard_num: u32,
    moray_ip: String,
    obj_tx: crossbeam_channel::Sender<SharkspotterMessage>,
    log: Logger,
    conf: config::Config,
) -> impl Fn() -> () {
    let moray_socket = format!("{}:{}", moray_ip, 2020);
    let id_string = id_name.to_string();

    move || {
        if let Err(e) = iter_ids(
            id_string.as_str(),
            &moray_socket,
            &conf,
            log.clone(),
            shard_num,
            |manta_value, etag, shark, shard_num| {
                let msg = SharkspotterMessage {
                    manta_value,
                    etag: etag.to_string(),
                    shark: shark.to_string(),
                    shard: shard_num,
                };
                obj_tx
                    .send(msg)
                    .map_err(|e| Error::new(ErrorKind::Other, e))
            },
        ) {
            error!(
                &log,
                "Encountered error scanning shard {} ({})", shard_num, e
            );
            // TODO: MANTA-5360
        }
    }
}

fn run_moray_shard_thread(
    pool: &ThreadPool,
    shard: u32,
    obj_tx: &crossbeam_channel::Sender<SharkspotterMessage>,
    conf: &config::Config,
    log: &Logger,
) -> Result<(), Error> {
    let moray_host = format!("{}.moray.{}", shard, conf.domain);
    let moray_ip = lookup_ip_str(moray_host.as_str())?;

    // TODO: MANTA-4912
    // We can have both _id and _idx, we don't have to have both, but we
    // need at least 1.  This is an error that should be passed back to
    // the caller via the handler as noted in MANTA-4912.
    // See also MANTA-5360

    // Create a thread for both _id and _idx in case we have both.
    for id in ["_id", "_idx"].iter() {
        pool.execute(start_iter_ids_thread(
            id,
            shard,
            moray_ip.clone(),
            obj_tx.clone(),
            log.clone(),
            conf.clone(),
        ));
    }

    Ok(())
}

fn run_direct_db_shard_thread(
    pool: &ThreadPool,
    shard: u32,
    obj_tx: &crossbeam_channel::Sender<SharkspotterMessage>,
    conf: &config::Config,
    log: &Logger,
) {
    let th_obj_tx = obj_tx.clone();
    let th_conf = conf.clone();
    let th_log = log.clone();

    pool.execute(move || {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        if let Err(e) = rt.block_on(directdb::get_objects_from_shard(
            shard, th_conf, th_log, th_obj_tx,
        )) {
            ERROR_LIST.lock().expect("ERROR_LIST lock").push(e);
        }
    });
}

// XXX: ugh copied code....
/*
pub fn run_duplicate_checker(
    config: &config::Config,
    log: Logger,
    obj_tx: crossbeam_channel::Sender<DuplicateInfo>,
) -> Result<(), Error> {
    let mut conf = config.clone();
    if let Err(e) = config::validate_config(&mut conf) {
        warn!(log, "{}", e);
    }

    let pool = ThreadPool::with_name("shard_scanner".into(), conf.max_threads);


    for shard in conf.min_shard..=conf.max_shard {
        if conf.direct_db {
            run_direct_db_shard_thread(&pool, shard, &obj_tx, &conf, &log);
        } else {
            run_moray_shard_thread(&pool, shard, &obj_tx, &conf, &log)?;
        }
    }

    pool.join();

    let mut error_strings = String::new();
    let error_list = ERROR_LIST.lock().unwrap();
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

 */

/// Same as the regular `run` method, but instead we spawn a new thread per
/// shard and send the information back to the caller via a crossbeam
/// mpmc channel.
pub fn run_multithreaded(
    config: &config::Config,
    log: Logger,
    obj_tx: crossbeam_channel::Sender<SharkspotterMessage>,
) -> Result<(), Error> {
    let mut conf = config.clone();
    if let Err(e) = config::validate_config(&mut conf) {
        warn!(log, "{}", e);
    }

    let pool = ThreadPool::with_name("shard_scanner".into(), conf.max_threads);

    if let config::FilterType::Shark(_) = conf.filter_type {
        shark_fix_common(&mut conf, &log);
        validate_sharks(&conf, &log)?;
    }

    for shard in conf.min_shard..=conf.max_shard {
        if conf.direct_db {
            run_direct_db_shard_thread(&pool, shard, &obj_tx, &conf, &log);
        } else {
            run_moray_shard_thread(&pool, shard, &obj_tx, &conf, &log)?;
        }
    }

    pool.join();

    let mut error_strings = String::new();
    let error_list = ERROR_LIST.lock().unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn _parse_max_id_value_test() {
        let _guard = util::init_global_logger(None);
        let log = slog_scope::logger();

        // not an array
        let num_value_no_arr = json!({
            "max": 12341234, // You, you try.  You try to get by.
        });
        assert!(_parse_max_id_value(num_value_no_arr, &log).is_err());

        // bad variant "bool"
        let num_value_bad_variant = json!([{
            "max": false, // You're never gonna pull it off you shouldn't
                          // even try.
        }]);
        assert!(_parse_max_id_value(num_value_bad_variant, &log).is_err());

        // not a string number
        let num_value_bad_string = json!([{
            "max": "Every single second is a moment in time.",
        }]);
        assert!(_parse_max_id_value(num_value_bad_string, &log).is_err());

        // parse-able string
        let num_value_string = json!([{
            "max": "12341234",
        }]);
        assert!(_parse_max_id_value(num_value_string, &log).is_ok());

        // number
        let num_value_num = json!([{
            "max": 12341234,
        }]);
        assert!(_parse_max_id_value(num_value_num, &log).is_ok());
    }
}
