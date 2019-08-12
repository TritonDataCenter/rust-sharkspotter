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
    -s, --shark <STORAGE_ID>          Find objects that belong to this shark
```

## Example
```
$ cargo run -- --domain east.joyent.us --shark 1.stor -M 1 -m 1
$ json -f shard_1_1.stor.objs -g
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
