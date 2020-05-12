// Copyright 2019 Joyent, Inc.

use clap::{App, AppSettings, Arg};
use std::io::Error;

#[derive(Debug)]
pub struct Config {
    pub min_shard: u32,
    pub max_shard: u32,
    pub domain: String,
    pub sharks: Vec<String>,
    pub chunk_size: u64,
    pub begin: u64,
    pub end: u64,
    pub skip_validate_sharks: bool,
    pub output_file: Option<String>,
    pub full_moray_obj: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min_shard: 1,
            max_shard: 1,
            domain: String::from(""),
            sharks: vec![String::from("")],
            begin: 0,
            end: 0,
            chunk_size: 100,
            skip_validate_sharks: false,
            output_file: None,
            full_moray_obj: false,
        }
    }
}

impl Config {
    // TODO:
    // Allow the option of selecting which data to keep
    pub fn from_args(_args: std::env::Args) -> Result<Config, Error> {
        let version = env!("CARGO_PKG_VERSION");
        let matches = App::new("sharkspotter")
            .version(version)
            .about("A tool for finding all of the Manta objects that reside \
            on a given set of sharks (storage zones).")
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
                .number_of_values(1) // only 1 value per occurrence
                .multiple(true) // allow multiple occurrences
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
            .arg(Arg::with_name("output_file")
                .short("f")
                .long("file")
                .value_name("FILE_NAME")
                .help("output filename (default <shark>/shard_<shard_num>.objs")
                .takes_value(true))
            .arg(Arg::with_name("skip_validate_sharks")
                .short("x")
                .help("Skip shark validation. Useful if shark is in readonly \
                mode.")
                .takes_value(false))
            .arg(Arg::with_name("full_moray_object")
                .short("F")
                .long("full_object")
                .help("Write full moray objects to file instead of just the \
                manta objects.")
                .takes_value(false))
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

        if let Ok(output_file) = value_t!(matches, "output_file", String) {
            config.output_file = Some(output_file);
        }

        if matches.is_present("x") {
            config.skip_validate_sharks = true;
        }

        if matches.is_present("full_moray_object") {
            config.full_moray_obj = true;
        }

        config.domain = matches.value_of("domain").unwrap().to_string();
        config.sharks = matches
            .values_of("shark")
            .unwrap()
            .map(String::from)
            .collect();

        Ok(config)
    }
}
