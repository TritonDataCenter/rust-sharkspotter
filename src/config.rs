/*
 * Copyright 2019 Joyent, Inc.
 */

use clap::{App, AppSettings, Arg};
use std::io::Error;

#[derive(Debug)]
pub struct Config {
    pub min_shard: u32,
    pub max_shard: u32,
    pub domain: String,
    pub shark: String,
    pub chunk_size: u64,
    pub begin: u64,
    pub end: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min_shard: 1,
            max_shard: 1,
            domain: String::from(""),
            shark: String::from(""),
            begin: 0,
            end: 0,
            chunk_size: 100,
        }
    }
}

impl Config {
    // TODO:
    // Allow the option of selecting which data to keep
    pub fn from_args(_args: std::env::Args) -> Result<Config, Error> {
        let matches = App::new("sharkspotter")
            .version("0.1.0")
            .about("A tool for finding all of the Manta objects that reside on a given shark \
            (storage zone).")
            .setting(AppSettings::ArgRequiredElseHelp)
            .arg(Arg::with_name("min_shard")
                .short("m")
                .long("min_shard")
                .value_name("MIN_SHARD")
                .help("Beginning shard number (default: 1)")
                .takes_value(true))
            .arg(Arg::with_name("max_shard")
                .short("M")
                .long("max_shard")
                .value_name("MAX_SHARD")
                .help("Ending shard number (default: 1)")
                .takes_value(true))
            .arg(Arg::with_name("domain")
                .short("d")
                .long("domain")
                .value_name("MORAY_DOMAIN")
                .help("Domain that the moray zones are in")
                .required(true)
                .takes_value(true))
            .arg(Arg::with_name("shark")
                .short("s")
                .long("shark")
                .value_name("STORAGE_ID")
                .help("Find objects that belong to this shark")
                .required(true)
                .takes_value(true))
            .arg(Arg::with_name("chunk-size")
                .short("c")
                .long("chunk-size")
                .value_name("NUM_RECORDS")
                .help("number of records to scan per call to moray (default: \
                100)")
                .takes_value(true))
            .arg(Arg::with_name("begin-index")
                .short("b")
                .long("begin")
                .value_name("INDEX")
                .help("index to being scanning at (default: 0)")
                .takes_value(true))
            .arg(Arg::with_name("end-index")
                .short("e")
                .long("end")
                .value_name("INDEX")
                .help("index to stop scanning at (default: 0)")
                .takes_value(true))
            .get_matches();

        let mut config = Config::default();

        if let Ok(max_shard) = value_t!(matches, "max_shard", u32) {
            config.max_shard = max_shard;
        }

        if let Ok(min_shard) = value_t!(matches, "min_shard", u32) {
            config.min_shard = min_shard;
        }

        if let Ok(begin) = value_t!(matches, "begin-index", u64) {
            config.begin = begin;
        }

        if let Ok(end) = value_t!(matches, "end-index", u64) {
            config.end = end;
        }

        if let Ok(chunk_size) = value_t!(matches, "chunk-size", u64) {
            config.chunk_size = chunk_size;
        }

        config.domain = matches.value_of("domain").unwrap().to_string();
        config.shark = matches.value_of("shark").unwrap().to_string();

        Ok(config)
    }
}
