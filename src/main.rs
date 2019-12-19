// Copyright 2019 Joyent, Inc.

use serde_json::Value;
use sharkspotter::config::Config;
use slog::{o, Drain, Logger};
use std::collections::HashMap;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::Error;
use std::path::Path;
use std::process;
use std::sync::Mutex;

fn write_mobj_to_file(file: &mut File, moray_obj: Value) -> Result<(), Error> {
    let manta_obj = match sharkspotter::manta_obj_from_moray_obj(&moray_obj) {
        Ok(mo) => mo,
        Err(e) => {
            eprintln!("{}", e);
            return Ok(());
        }
    };

    let object_id = match manta_obj.get("objectId") {
        Some(oid) => match serde_json::to_string(oid) {
            Ok(o) => o,
            Err(e) => {
                eprintln!(
                    "Could not deserialize objectId {:#?}, {}",
                    manta_obj, e
                );
                return Ok(());
            }
        },
        None => {
            eprintln!("Could not get objectId from value {:#?}", manta_obj);
            return Ok(());
        }
    };

    println!("{}", object_id);

    let buf = serde_json::to_string(&manta_obj)?;
    file.write_all(buf.as_bytes())?; // TODO: match
    file.write_all(b"\n")?;

    Ok(())
}

fn run_with_file_map(conf: Config, log: Logger) -> Result<(), Error> {
    let mut file_map = HashMap::new();

    for i in conf.min_shard..=conf.max_shard {
        let filename = format!("shard_{}_{}.objs", i, conf.shark);
        let path = Path::new(filename.as_str());
        let file = match OpenOptions::new().append(true).create(true).open(path)
        {
            Err(e) => panic!(
                "Couldn't create output file '{}': {}",
                path.display(),
                e
            ),
            Ok(file) => file,
        };

        file_map.insert(i, file);
    }

    sharkspotter::run(conf, log, |moray_obj, shard| {
        let file = file_map.get_mut(&shard).unwrap();

        write_mobj_to_file(file, moray_obj)
    })
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

    sharkspotter::run(conf, log, |moray_obj, _| {
        write_mobj_to_file(&mut file, moray_obj)
    })
}

fn main() -> Result<(), Error> {
    let conf = Config::from_args(env::args()).unwrap_or_else(|err| {
        eprintln!("Error parsing args: {}", err);
        process::exit(1);
    });

    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );

    let filename = conf.output_file.clone();
    match filename {
        Some(fname) => run_with_user_file(fname, conf, log),
        None => run_with_file_map(conf, log),
    }
}
