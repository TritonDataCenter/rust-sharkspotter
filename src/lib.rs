// Copyright 2019 Joyent, Inc.

#[macro_use]
extern crate clap;

pub mod config;

use libmanta::moray::MantaObject;
use moray::client::MorayClient;
use serde::Deserialize;
use serde_json::{self, Value};
use slog::Logger;
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

// TODO: add tests for this function
fn query_handler<F>(
    val: &Value,
    shard_num: u32,
    handler: &mut F,
) -> Result<(), Error>
where
    F: FnMut(MantaObject, u32) -> Result<(), Error>,
{
    // TODO:
    // - are we sure this is always only 1 element?
    // - Handle an object that doesn't have a '_value' without panicking?
    let manta_str: String =
        serde_json::from_value(val[0]["_value"].clone()).unwrap();
    let manta_obj: MantaObject =
        serde_json::from_str(manta_str.as_str()).unwrap();

    handler(manta_obj, shard_num)?;

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
    mclient: &mut MorayClient,
    query: &str,
    shard_num: u32,
    handler: &mut F,
) -> Result<(), Error>
where
    F: FnMut(MantaObject, u32) -> Result<(), Error>,
{
    match mclient.sql(query, vec![], r#"{"timeout": 10000}"#, |a| {
        query_handler(a, shard_num, handler)
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
    F: FnMut(MantaObject, u32) -> Result<(), Error>,
{
    let mut mclient = MorayClient::from_str(moray_socket, log, None)?;
    let mut start_id = conf.begin;
    let mut end_id = conf.begin + conf.chunk_size - 1;
    let largest_id = match find_largest_id_value(&mut mclient, id_name) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("Error finding largest ID: {}, using 0", e);
            0
        }
    };

    let mut remaining = largest_id - conf.begin + 1;

    if end_id > conf.end {
        end_id = conf.end;
    }

    while remaining > 0 {
        let query = chunk_query(id_name, start_id, end_id, conf.chunk_size);
        match read_chunk(&mut mclient, query.as_str(), shard_num, &mut handler)
        {
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

        println!(
            "start_id: {} | end_id: {} | remaining: {}",
            start_id, end_id, remaining
        );
    }

    Ok(())
}

pub fn run<F>(
    conf: &config::Config,
    log: Logger,
    mut handler: F,
) -> Result<(), Error>
where
    F: FnMut(MantaObject, u32) -> Result<(), Error>,
{
    let resolver = Resolver::from_system_conf().unwrap();

    for i in conf.min_shard..=conf.max_shard {
        let moray_host = format!("{}.moray.{}", i, conf.domain);
        let response = resolver.lookup_ip(moray_host.as_str())?;

        let moray_ip: Vec<IpAddr> = response.iter().collect();
        let moray_ip = moray_ip[0];

        let moray_socket = format!("{}:{}", moray_ip.to_string(), 2021);

        iter_ids("_id", &moray_socket, &conf, log.clone(), i, &mut handler)?;
        iter_ids("_idx", &moray_socket, &conf, log.clone(), i, &mut handler)?;
    }

    Ok(())
}
