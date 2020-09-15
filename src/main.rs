/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use chrono::{DateTime, Utc};
/// Run sharkspotter as a commandline tool.
///
/// By default sharkspotter will place the manta object metadata into a file
/// in json format.  The file will be of the form:
///      <shark name>/shard_<shard_num>.objs
///
/// This file can be parsed with the `json` tool which allows users to filter
/// on certain fields.
///
use crossbeam_channel::{self, Receiver, Sender};
use serde_json::Value;
use sharkspotter::config::Config;
use sharkspotter::{duplicate, util, SharkspotterMessage};
use slog::{trace, Logger};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::prelude::*;
use std::io::{BufWriter, Error, ErrorKind};
use std::path::Path;
use std::process;
use std::thread;

fn write_mobj_to_file<W>(
    mut writer: W,
    manta_obj: Value,
    conf: &Config,
) -> Result<(), Error>
where
    W: Write,
{
    let out_bytes: Vec<u8>;
    let obj_id_only = conf.obj_id_only;

    if obj_id_only {
        let obj_id = sharkspotter::object_id_from_manta_obj(&manta_obj)
            .map_err(|e| {
                eprintln!("{}", e);
                Error::new(ErrorKind::Other, e)
            })?;
        out_bytes = obj_id.as_bytes().to_owned();
    } else {
        out_bytes = serde_json::to_vec(&manta_obj)?;
    }

    writer.write_all(&out_bytes)?;
    writer.write_all(b"\n")?;

    Ok(())
}

fn run_multithreaded<F>(
    conf: &Config,
    log: Logger,
    mut on_recv: F,
) -> Result<(), Error>
where
    F: 'static
        + std::marker::Send
        + FnMut(SharkspotterMessage) -> Result<(), Error>,
{
    let channel: (Sender<SharkspotterMessage>, Receiver<SharkspotterMessage>) =
        crossbeam_channel::bounded(100);
    let obj_tx = channel.0;
    let obj_rx = channel.1;
    let handle = thread::spawn(move || {
        while let Ok(msg) = obj_rx.recv() {
            on_recv(msg)?;
        }
        Ok(())
    });

    sharkspotter::run_multithreaded(conf, log, obj_tx)?;
    handle.join().expect("sharkspotter reader join")
}

fn run_with_file_map(conf: Config, log: Logger) -> Result<(), Error> {
    let domain_prefix = format!(".{}", conf.domain);
    let mut file_map = HashMap::new();
    let filename =
        |shark: &str, shard| format!("{}/shard_{}.objs", shark, shard);

    for shark in conf.sharks.iter() {
        let dirname = format!("./{}", shark);
        fs::create_dir(dirname.as_str())?;

        for shard in conf.min_shard..=conf.max_shard {
            let fname = filename(shark, shard);
            let path = Path::new(fname.as_str());
            let file = match OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(path)
            {
                Err(e) => panic!(
                    "Couldn't create output file '{}': {}",
                    path.display(),
                    e
                ),
                Ok(file) => file,
            };

            file_map.insert(fname, BufWriter::new(file));
        }
    }

    if conf.multithreaded {
        let closure_conf = conf.clone();
        run_multithreaded(&conf, log.clone(), move |msg| {
            let shark = msg.shark.replace(&domain_prefix, "");
            let shard = msg.shard;
            trace!(&log, "shark: {}, shard: {}", shark, shard);

            // Only sharks that are in the config.sharks vector should be
            // passed to the callback.  If we see a shark that wasn't
            // specified that represents a programmer error.
            let file = file_map
                .get_mut(&filename(shark.as_str(), shard))
                .expect("unexpected shark");

            write_mobj_to_file(file, msg.manta_value, &closure_conf)
        })
    } else {
        sharkspotter::run(
            &conf,
            log.clone(),
            |manta_obj, _etag, shark, shard| {
                let shark = shark.replace(&domain_prefix, "");
                trace!(&log, "shark: {}, shard: {}", shark, shard);

                let file =
                    file_map.get_mut(&filename(shark.as_str(), shard)).unwrap();

                write_mobj_to_file(file, manta_obj, &conf)
            },
        )
    }
}

fn run_with_user_file(
    filename: String,
    conf: Config,
    log: Logger,
) -> Result<(), Error> {
    let path = Path::new(filename.as_str());
    let mut file = match OpenOptions::new().append(true).create(true).open(path)
    {
        Err(e) => {
            panic!("Couldn't create output file '{}': {}", path.display(), e)
        }
        Ok(file) => file,
    };

    if conf.multithreaded {
        let closure_conf = conf.clone();
        run_multithreaded(&conf, log, move |msg| {
            write_mobj_to_file(&mut file, msg.manta_value, &closure_conf)
        })
    } else {
        sharkspotter::run(&conf, log, |moray_obj, _etag, _shark, _shard| {
            write_mobj_to_file(&mut file, moray_obj, &conf)
        })
    }
}

fn create_mantastub_database(conf: &mut Config) -> Result<(), String> {
    let now: DateTime<Utc> = Utc::now();
    let db_name = now.format("%Y%m%dT%H%M%S").to_string();

    println!("Creating database {}", db_name);
    conf.db_name = db_name;

    let conn = sharkspotter::db::create_and_connect_db(&conf.db_name)
        .expect("Could not create database");

    sharkspotter::db::create_tables(&conn)
}

fn run_duplicate_check(mut conf: Config, log: Logger) -> Result<(), Error> {
    // Hack.  We need the channel to send different info, but the rest of the
    // code is built to only handle certain messages.
    let (dup_tx, dup_rx) = crossbeam_channel::bounded(10);
    let mut handles = vec![];

    create_mantastub_database(&mut conf)
        .map_err(|e| Error::new(ErrorKind::Other, e))?;

    for i in 0..10 {
        let th_dup_rx = dup_rx.clone();
        let th_conf = conf.clone();
        let th_log = log.clone();
        handles.push(
            thread::Builder::new()
                .name(format!("dup_handler_{}", i))
                .spawn(move || {
                    duplicate::handle_duplicate_thread(
                        th_conf, th_dup_rx, th_log,
                    )
                })
                .expect("spawn duplicate handler"),
        );
    }

    let ret = duplicate::run_duplicate_detector(&conf, log, dup_tx);

    for h in handles {
        h.join().expect("join handler thread");
    }

    ret
}

fn main() -> Result<(), Error> {
    let conf = Config::from_args().unwrap_or_else(|err| {
        eprintln!("Error parsing args: {}", err);
        process::exit(1);
    });

    let _guard = util::init_global_logger(Some(conf.log_level));
    let log = slog_scope::logger();

    if conf.duplicate_detect {
        run_duplicate_check(conf, log)
    } else {
        let filename = conf.output_file.clone();

        match filename {
            Some(fname) => run_with_user_file(fname, conf, log),
            None => run_with_file_map(conf, log),
        }
    }
}
