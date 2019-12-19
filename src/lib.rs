// Copyright 2019 Joyent, Inc.

#[macro_use]
extern crate clap;

pub mod config;

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

fn find_largest_id_value(
    mclient: &mut MorayClient,
    id: &str,
) -> Result<u64, Error> {
    let mut ret: u64 = 0;
    mclient.sql(
        format!("SELECT MAX({}) FROM manta;", id).as_str(),
        vec![],
        r#"{"limit": 1, "no_count": true}"#,
        |resp| {
            if resp.is_array() {
                let resp_arr = resp.as_array().unwrap();

                if resp_arr.len() != 1 {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!(
                            "Expected single element got {}",
                            resp_arr.len()
                        ),
                    ));
                }
            } else {
                return Err(Error::new(ErrorKind::Other, "Expected array"));
            }

            serde_json::from_value::<IdRet>(resp[0].clone()).and_then(
                |max_obj: IdRet| {
                    let max: u64 = max_obj.max.parse().unwrap();
                    ret = max;
                    Ok(())
                },
            )?;
            Ok(())
        },
    )?;
    Ok(ret)
}

fn _log_return_error(log: &Logger, msg: &str) -> Result<(), Error> {
    error!(log, "{}", msg);
    Err(Error::new(ErrorKind::Other, msg))
}

// TODO: add tests for this function
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
            /*
            error!(log, "Entry is not an array");
            return Err(Error::new(ErrorKind::Other, "Entry is not an array"));
            */
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

    let _value = match moray_value.get("_value") {
        Some(val) => val,
        None => {
            let msg =
                format!("Missing '_value' in Moray entry {:#?}", moray_value);
            return _log_return_error(log, &msg);
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

    /*
    let manta_obj: MantaObject = match serde_json::from_value(
        manta_value.clone())
    {
        Ok(mo) => mo,
        Err(e) => {
            let msg = format!(
                "Could not deserialize manta value {:#?}. ({})",
                manta_value,
                e
            );
            return _log_return_error(log, &msg);
        }
    };
    */

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
    let largest_id = match find_largest_id_value(&mut mclient, id_name) {
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
            "start_id: {} | end_id: {} | remaining: {}",
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

    match validate_shark(&conf.shark, log.clone(), &conf.domain) {
        Ok(()) => (),
        Err(e) => {
            error!(log, "{}", e);
            return Err(e);
        }
    }

    for i in conf.min_shard..=conf.max_shard {
        let moray_host = format!("{}.moray.{}", i, conf.domain);
        let moray_ip = lookup_ip_str(moray_host.as_str())?;
        let moray_socket = format!("{}:{}", moray_ip, 2021);

        iter_ids("_id", &moray_socket, &conf, log.clone(), i, &mut handler)?;
        iter_ids("_idx", &moray_socket, &conf, log.clone(), i, &mut handler)?;
    }

    Ok(())
}
