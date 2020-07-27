/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

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
use sharkspotter::{util, SharkspotterMessage};
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
    moray_obj: Value,
    conf: &Config,
) -> Result<(), Error>
where
    W: Write,
{
    let out_bytes: Vec<u8>;
    let full_object = conf.full_moray_obj;
    let obj_id_only = conf.obj_id_only;

    if !full_object {
        let out_obj = sharkspotter::manta_obj_from_moray_obj(&moray_obj)
            .map_err(|e| {
                eprintln!("{}", e);
                Error::new(ErrorKind::Other, e)
            })?;

        if obj_id_only {
            let obj_id = sharkspotter::object_id_from_manta_obj(&out_obj)
                .map_err(|e| {
                    eprintln!("{}", e);
                    Error::new(ErrorKind::Other, e)
                })?;
            out_bytes = obj_id.as_bytes().to_owned();
        } else {
            out_bytes = serde_json::to_vec(&out_obj)?;
        }
    } else {
        assert!(!obj_id_only);
        out_bytes = serde_json::to_vec(&moray_obj)?;
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
        crossbeam_channel::bounded(10);
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

            write_mobj_to_file(file, msg.value, &closure_conf)
        })
    } else {
        sharkspotter::run(&conf, log.clone(), |moray_obj, shark, shard| {
            let shark = shark.replace(&domain_prefix, "");
            trace!(&log, "shark: {}, shard: {}", shark, shard);

            let file =
                file_map.get_mut(&filename(shark.as_str(), shard)).unwrap();

            write_mobj_to_file(file, moray_obj, &conf)
        })
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
            write_mobj_to_file(&mut file, msg.value, &closure_conf)
        })
    } else {
        sharkspotter::run(&conf, log, |moray_obj, _shark, _shard| {
            write_mobj_to_file(&mut file, moray_obj, &conf)
        })
    }
}

fn main() -> Result<(), Error> {
    let conf = Config::from_args().unwrap_or_else(|err| {
        eprintln!("Error parsing args: {}", err);
        process::exit(1);
    });

    let _guard = util::init_global_logger(None);
    let log = slog_scope::logger();

    let filename = conf.output_file.clone();

    match filename {
        Some(fname) => run_with_user_file(fname, conf, log),
        None => run_with_file_map(conf, log),
    }
}
