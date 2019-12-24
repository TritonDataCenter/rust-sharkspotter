// Copyright 2019 Joyent, Inc.

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

#[macro_use]
extern crate clap;

pub mod config;
pub mod util;

use libmanta::moray::MantaObjectShark;
use moray::client::MorayClient;
use moray::objects as moray_objects;
use serde::Deserialize;
use serde_json::{self, Value};
use slog::{debug, error, warn, Logger};
use std::io::{Error, ErrorKind};
use std::net::IpAddr;
use trust_dns_resolver::Resolver;

#[derive(Deserialize, Debug, Clone)]
struct IdRet {
    max: String,
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
            //            ret = max_num;
            Ok(())
        },
    )?;
    Ok(ret)
}

fn _log_return_error(log: &Logger, msg: &str) -> Result<(), Error> {
    error!(log, "{}", msg);
    Err(Error::new(ErrorKind::Other, msg))
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
    shark: &str,
    handler: &mut F,
) -> Result<(), Error>
where
    F: FnMut(Value, u32) -> Result<(), Error>,
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

    let moray_object = match serde_json::from_value(moray_value.clone()) {
        Ok(mo) => mo,
        Err(e) => {
            let msg = format!(
                "Could not deserialize moray value {:#?}. ({})",
                moray_value, e
            );
            return _log_return_error(log, &msg);
        }
    };

    let _value = match manta_obj_from_moray_obj(moray_value) {
        Ok(v) => v,
        Err(e) => {
            return _log_return_error(log, &e);
        }
    };

    let sharks: Vec<MantaObjectShark> = match _value.get("sharks") {
        Some(s) => {
            if !s.is_array() {
                let msg = format!("Sharks are not in an array {:#?}", s);
                return _log_return_error(log, &msg);
            }
            match serde_json::from_value::<Vec<MantaObjectShark>>(s.clone()) {
                Ok(mos) => mos,
                Err(e) => {
                    let msg = format!(
                        "Could not deserialize sharks value {:#?}. ({})",
                        s, e
                    );
                    return _log_return_error(log, &msg);
                }
            }
        }
        None => {
            let msg = format!("Missing 'sharks' field {:#?}", _value);
            return _log_return_error(log, &msg);
        }
    };

    // Filter on shark
    if !sharks.iter().any(|s| s.manta_storage_id == shark) {
        return Ok(());
    }

    handler(moray_object, shard_num)?;

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
    shark: &str,
    handler: &mut F,
) -> Result<(), Error>
where
    F: FnMut(Value, u32) -> Result<(), Error>,
{
    match mclient.sql(query, vec![], r#"{"timeout": 10000}"#, |a| {
        query_handler(log, a, shard_num, shark, handler)
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
    F: FnMut(Value, u32) -> Result<(), Error>,
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
            &conf.shark,
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

        debug!(
            &log,
            "shard: {} | start_id: {} | end_id: {} | remaining: {}",
            shard_num,
            start_id,
            end_id,
            remaining
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

fn validate_shark(shark: &str, log: Logger, domain: &str) -> Result<(), Error> {
    let shard1_moray = format!("1.moray.{}", domain);
    let moray_ip = lookup_ip_str(shard1_moray.as_str())?;
    let moray_socket = format!("{}:{}", moray_ip, 2021);
    let mut mclient =
        MorayClient::from_str(moray_socket.as_str(), log.clone(), None)?;

    let filter = format!("manta_storage_id={}", shark);
    let opts = moray_objects::MethodOptions::default();
    let mut count = 0;
    mclient.find_objects("manta_storage", filter.as_str(), &opts, |_| {
        count += 1;
        Ok(())
    })?;

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

    Ok(())
}

/// Main entry point to for the sharkspotter library.  Callers need to
/// provide a closure that takes a serde Value and a u32 shard number as its
/// arguments.
/// Sharkspotter works by first getting the maximum and minimum _id and _idx
/// for a given moray bucket (which is always "manta"), and then querying for
/// entries in a user configurable chunk size.
pub fn run<F>(
    mut conf: config::Config,
    log: Logger,
    mut handler: F,
) -> Result<(), Error>
where
    F: FnMut(Value, u32) -> Result<(), Error>,
{
    if !conf.shark.contains(conf.domain.as_str()) {
        let new_shark = format!("{}.{}", conf.shark, conf.domain);
        warn!(log,
            "Domain \"{}\" not found in storage node string:\"{}\", using \"{}\"",
            conf.domain,
            conf.shark,
            new_shark
            );

        conf.shark = new_shark;
    }

    if !conf.skip_validate_shark {
        match validate_shark(&conf.shark, log.clone(), &conf.domain) {
            Ok(()) => (),
            Err(e) => {
                error!(log, "{}", e);
                return Err(e);
            }
        }
    }

    for i in conf.min_shard..=conf.max_shard {
        let moray_host = format!("{}.moray.{}", i, conf.domain);
        let moray_ip = match lookup_ip_str(moray_host.as_str()) {
            Ok(ip) => ip,
            Err(e) => {
                error!(
                    &log,
                    "Error looking up moray host, skipping shard. {:#?}", e
                );
                continue;
            }
        };
        let moray_socket = format!("{}:{}", moray_ip, 2021);

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use util;

    #[test]
    fn _parse_max_id_value_test() {
        let log = util::init_plain_logger();

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
