// Copyright 2019 Joyent, Inc.

extern crate sharkspotter;

use std::collections::HashMap;
use std::env;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::Error;
use std::path::Path;
use std::process;

use sharkspotter::config::Config;

fn main() -> Result<(), Error> {
    let conf = Config::from_args(env::args()).unwrap_or_else(|err| {
        eprintln!("Error parsing args: {}", err);
        process::exit(1);
    });

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
    sharkspotter::run(&conf, |mobj, shard| {
        println!("{} | {}", shard, mobj.object_id);
        let file = file_map.get_mut(&shard).unwrap();
        let buf = serde_json::to_string(&mobj)?;
        file.write_all(buf.as_bytes())?; // TODO: match

        Ok(())
    })
}
