/*
 * Copyright 2019 Joyent, Inc.
 */

extern crate assert_cli;

#[cfg(test)]
mod integration {
    use assert_cli;

    #[test]
    fn missing_all_args() {
        const ERROR_STRING: &str = "sharkspotter 0.1.0
A tool for finding all of the Manta objects that reside on a given shark (storage zone).

USAGE:
    sharkspotter [OPTIONS] --domain <MORAY_DOMAIN> --shark <STORAGE_ID>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --begin <INDEX>               index to being scanning at (default: 0)
    -c, --chunk-size <NUM_RECORDS>    number of records to scan per call to moray (default: 100)
    -d, --domain <MORAY_DOMAIN>       Domain that the moray zones are in
    -e, --end <INDEX>                 index to stop scanning at (default: 0)
    -M, --max_shard <MAX_SHARD>       Ending shard number (default: 1)
    -m, --min_shard <MIN_SHARD>       Beginning shard number (default: 1)
    -s, --shark <STORAGE_ID>          Find objects that belong to this shark";

        assert_cli::Assert::main_binary()
            .fails()
            .and()
            .stderr()
            .contains(ERROR_STRING)
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
