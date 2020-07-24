/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use clap::{value_t, App, AppSettings, Arg, ArgMatches};
use std::io::Error;

const MAX_THREADS: usize = 100;

#[derive(Clone, Debug)]
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
    pub multithreaded: bool,
    pub max_threads: usize,
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
            chunk_size: 1000,
            skip_validate_sharks: false,
            output_file: None,
            full_moray_obj: false,
            multithreaded: false,
            max_threads: 50,
        }
    }
}

impl<'a, 'b> Config {
    pub fn get_app() -> App<'a, 'b> {
        let version = env!("CARGO_PKG_VERSION");
        App::new("sharkspotter")
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
            .arg(Arg::with_name("multithreaded")
                .short("T")
                .help("Run with multiple threads, one per shard")
                .long("multithreaded")
                .takes_value(false))
            .arg(Arg::with_name("max_threads")
                .short("t")
                .help("maximum number of threads to run with")
                .long("max_threads")
                .requires("multithreaded")
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
    }

    // TODO: This has grown over time and is now causing a clippy warning.
    // We should consider using a yaml file to parse the matches.
    #[allow(clippy::cognitive_complexity)]
    fn config_from_matches(matches: ArgMatches) -> Result<Config, Error> {
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

        if matches.is_present("skip_validate_sharks") {
            config.skip_validate_sharks = true;
        }

        if matches.is_present("full_moray_object") {
            config.full_moray_obj = true;
        }

        if matches.is_present("multithreaded") {
            config.multithreaded = true;
        }

        if let Ok(max_threads) = value_t!(matches, "max_threads", usize) {
            config.max_threads = max_threads;
        }

        config.domain = matches.value_of("domain").unwrap().to_string();
        config.sharks = matches
            .values_of("shark")
            .unwrap()
            .map(String::from)
            .collect();

        if let Err(e) = validate_config(&mut config) {
            eprintln!("{}", e);
        }

        Ok(config)
    }

    pub fn from_args() -> Result<Config, Error> {
        let matches = Self::get_app().get_matches();
        Self::config_from_matches(matches)
    }
}

pub fn validate_config(conf: &mut Config) -> Result<(), String> {
    let mut ret = Ok(());

    if conf.max_threads > 100 {
        ret = Err(format!(
            "Max threads of {} exceeds max.  Setting to {}.",
            conf.max_threads, MAX_THREADS
        ));
        conf.max_threads = MAX_THREADS;
    }

    ret
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_args() {
        let args = vec![
            "target/debug/sharkspotter",
            "-F",
            "-x",
            "--domain",
            "east.joyent.us",
            "--shark",
            "1.stor",
            "--shark",
            "2.stor",
            "-m",
            "1",
            "-M",
            "2",
            "-e",
            "10",
            "-b",
            "3",
            "-c",
            "20",
            "-f",
            "foo.txt",
        ];

        let matches = Config::get_app().get_matches_from(args);
        let config = Config::config_from_matches(matches).expect("config");

        assert!(config.full_moray_obj);
        assert!(config.skip_validate_sharks);

        assert_eq!(config.max_shard, 2);
        assert_eq!(config.min_shard, 1);
        assert_eq!(config.begin, 3);
        assert_eq!(config.end, 10);
        assert_eq!(config.chunk_size, 20);

        assert_eq!(config.output_file, Some(String::from("foo.txt")));
        assert_eq!(config.domain, String::from("east.joyent.us"));

        assert_eq!(
            config.sharks,
            vec![String::from("1.stor"), String::from("2.stor")]
        );
    }
}
