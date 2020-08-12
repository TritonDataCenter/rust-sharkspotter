# sharkspotter
A tool for finding all of the Manta objects that reside on a given shark (storage zone).


## Build
```
cargo build
cd target/<debug | release>/
./sharkspotter
```

## Usage
```
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
```

## Example
```
$ cargo run -- --domain east.joyent.us --shark 1.stor -M 1 -m 1
$ json -f ./1.stor/shard_1.objs
```

You can also specify multiple sharks like so:
```
$ cargo run -- --domain east.joyent.us --shark 1.stor --shark 2.stor -M 1 -m 1
$ json -f ./1.stor/shard_1.objs
$ json -f ./2.stor/shard_1.objs
```

__This can be a big file so [json](https://github.com/trentm/json) may struggle with it__

## Development

Before integration run:
```
cargo fmt
cargo clippy
```

and 

```
cargo test
```
