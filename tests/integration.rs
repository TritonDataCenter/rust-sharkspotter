/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

extern crate assert_cli;

#[cfg(test)]
mod cli {
    use assert_cli;

    #[test]
    fn missing_all_args() {
        let error_string = format!("sharkspotter {}
A tool for finding all of the Manta objects that reside on a given set of sharks (storage zones).

USAGE:
    sharkspotter [FLAGS] [OPTIONS] --domain <MORAY_DOMAIN> --shark <STORAGE_ID>...

FLAGS:
    -D, --direct_db         use direct DB access instead of moray
    -h, --help              Prints help information
    -T, --multithreaded     Run with multiple threads, one per shard
    -O, --object_id_only    Output only the object ID
    -x                      Skip shark validation. Useful if shark is in readonly mode.
    -V, --version           Prints version information

OPTIONS:
    -b, --begin <INDEX>                index to being scanning at (default: 0)
    -c, --chunk-size <NUM_RECORDS>     number of records to scan per call to moray (default: 100)
    -d, --domain <MORAY_DOMAIN>        Domain that the moray zones are in
    -e, --end <INDEX>                  index to stop scanning at (default: 0)
    -l, --log_level <log_level>        Set log level
    -M, --max_shard <MAX_SHARD>        Ending shard number (default: 1)
    -t, --max_threads <max_threads>    maximum number of threads to run with
    -m, --min_shard <MIN_SHARD>        Beginning shard number (default: 1)
    -f, --file <FILE_NAME>             output filename (default <shark>/shard_<shard_num>.objs
    -s, --shark <STORAGE_ID>...        Find objects that belong to this shark
", env!("CARGO_PKG_VERSION"));

        assert_cli::Assert::main_binary()
            .fails()
            .and()
            .stderr()
            .contains(error_string.as_str())
            .unwrap();
    }

    #[test]
    fn missing_all_required_args() {
        const ERROR_STRING: &str =
            "error: The following required arguments were not provided:
    --domain <MORAY_DOMAIN>
    --shark <STORAGE_ID>";

        assert_cli::Assert::main_binary()
            .with_args(&["-m 1 -M 1 -c 1000"])
            .fails()
            .and()
            .stderr()
            .contains(ERROR_STRING)
            .unwrap();
    }

    #[test]
    fn invalid_arg() {
        const ERROR_STRING: &str = "error: Found argument '-z' which wasn't \
                                    expected, or isn't valid in this context";

        assert_cli::Assert::main_binary()
            .with_args(&["-z foo"])
            .fails()
            .and()
            .stderr()
            .contains(ERROR_STRING)
            .unwrap()
    }

    #[test]
    fn missing_shark() {
        const ERROR_STRING: &str =
            "error: The following required arguments were not provided:
    --shark <STORAGE_ID>";

        assert_cli::Assert::main_binary()
            .with_args(&["-d east.joyent.us -m 1 -M 1"])
            .fails()
            .and()
            .stderr()
            .contains(ERROR_STRING)
            .unwrap()
    }

    #[test]
    fn missing_domain() {
        const ERROR_STRING: &str =
            "error: The following required arguments were not provided:
    --domain <MORAY_DOMAIN>";

        assert_cli::Assert::main_binary()
            .with_args(&["-m 1 -M 1 -s 1.stor"])
            .fails()
            .and()
            .stderr()
            .contains(ERROR_STRING)
            .unwrap()
    }
}

mod direct_db {
    use sharkspotter::{
        config, object_id_from_manta_obj, run_multithreaded, util,
        SharkspotterMessage,
    };
    use slog::{warn, Level, Logger};
    use std::collections::HashSet;
    use std::thread;

    fn get_ids_from_direct_db(
        conf: config::Config,
        log: Logger,
    ) -> Result<HashSet<String>, std::io::Error> {
        let mut obj_ids = HashSet::new();
        let th_log = log.clone();
        let (obj_tx, obj_rx): (
            crossbeam_channel::Sender<SharkspotterMessage>,
            crossbeam_channel::Receiver<SharkspotterMessage>,
        ) = crossbeam_channel::bounded(5);

        let handle = thread::spawn(move || {
            // TODO: call run_multithreaded directly?
            run_multithreaded(&conf, th_log, obj_tx)
        });

        loop {
            match obj_rx.recv() {
                Ok(ssmsg) => {
                    let obj_id = object_id_from_manta_obj(&ssmsg.manta_value)
                        .expect("obj id");

                    // Assert no duplicates
                    assert!(obj_ids.insert(obj_id));
                }
                Err(e) => {
                    warn!(log, "Could not RX, TX channel dropped: {}", e);
                    break;
                }
            }
        }

        if let Err(e) = handle.join().expect("thread join") {
            return Err(e);
        }

        Ok(obj_ids)
    }

    #[test]
    fn directdb_test() {
        // The log level has a significant impact on the runtime of this test.
        // If an error is encountered consider bumping this log level to
        // Trace and re-running.
        let conf = config::Config {
            direct_db: true,
            min_shard: 1,
            max_shard: 2,
            domain: "east.joyent.us".to_string(),
            sharks: vec!["1.stor.east.joyent.us".to_string()],
            log_level: Level::Info,
            ..Default::default()
        };
        let _guard = util::init_global_logger(Some(conf.log_level));
        let log = slog_scope::logger();

        let first_count = get_ids_from_direct_db(conf.clone(), log.clone())
            .expect("first count")
            .len();
        let second_count = get_ids_from_direct_db(conf.clone(), log.clone())
            .expect("second count")
            .len();

        assert_eq!(first_count, second_count);
    }

    #[test]
    // Test that the proper error is returned when we attempt to connect to a
    // non-existant rebalancer-postgres database.  Use a ridiculously high
    // shard number to ensure a connection failure.
    fn directdb_test_connect_fail() {
        let conf = config::Config {
            direct_db: true,
            min_shard: 999999,
            max_shard: 999999,
            domain: "east.joyent.us".to_string(),
            sharks: vec!["1.stor.east.joyent.us".to_string()],
            log_level: Level::Trace,
            ..Default::default()
        };
        let _guard = util::init_global_logger(Some(conf.log_level));
        let log = slog_scope::logger();

        assert!(get_ids_from_direct_db(conf.clone(), log.clone()).is_err());
    }
}
