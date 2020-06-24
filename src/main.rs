/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
/// Run sharkspotter as a commandline tool.
///
/// By default sharkspotter will place the manta object metadata into a file
/// in json format.  The file will be of the form:
///      <shark name>/shard_<shard_num>.objs
///
/// This file can be parsed with the `json` tool which allows users to filter
/// on certain fields.
///
use std::io::{BufWriter, Error, ErrorKind};
use std::path::Path;
use std::process;
use std::thread;

use crossbeam_channel::{self, Receiver, Sender};
use serde_json::Value;
use sharkspotter::config::Config;
use sharkspotter::{util, SharkspotterMessage};
use slog::{debug, Logger};

// fn write_mobj_to_file<W>(
//     writer: &W,
//     moray_obj: &Value,
//     full_object: bool,
// ) -> Result<(), Error>
// fn write_mobj_to_file(
//     writer: BufWriter<File>,
//     moray_obj: &Value,
//     full_object: bool,
// ) -> Result<(), Error>
// // where
// //     W: Write,
// {
//     if !full_object {
//         let out_obj = match sharkspotter::manta_obj_from_moray_obj(&moray_obj) {
//             Ok(mo) => mo,
//             Err(e) => {
//                 eprintln!("{}", e);
//                 return Ok(());
//             }
//         };
//         serde_json::to_writer(writer, &out_obj)?;
//     // writer.write_all(&out_obj.into())?;
//     } else {
//         serde_json::to_writer(writer, moray_obj)?;
//         // writer.write_all(moray_obj.to_string())?;
//     }

//     writer.write_all(b"\n")?;

//     Ok(())
// }

fn run_multithreaded<F>(
    conf: Config,
    log: Logger,
    mut on_recv: F,
) -> Result<(), Error>
where
    F: 'static
        + std::marker::Send
        + FnMut(SharkspotterMessage) -> Result<(), Error>,
{
    // let channel: (Sender<SharkspotterMessage>, Receiver<SharkspotterMessage>) =
    //     crossbeam_channel::bounded(10);
    // let obj_tx = channel.0;
    // let obj_rx = channel.1;
    // let handle = thread::spawn(move || {
    //     while let Ok(msg) = obj_rx.recv() {
    //         on_recv(msg)?;
    //     }
    //     Ok(())
    // });

    // sharkspotter::run_multithreaded(conf, log, obj_tx)?;
    // handle.join().expect("sharkspotter reader join")
    std::unimplemented!();
}

fn run_with_file_map(conf: Config, log: Logger) -> Result<(), Error> {
    let domain_prefix = format!(".{}", conf.domain);
    let full_object = conf.full_moray_obj;
    let mut file_map = HashMap::new();
    let filename =
        |shark: &str, shard| format!("{}/shard_{}.objs", shark, shard);

    for shark in conf.sharks.iter() {
        let dirname = format!("./{}", shark);
        fs::create_dir(dirname.as_str())?;

        // Open a file per shard. Spawn a thread for each file that manages the
        // IO to each shard file. Create a channel to communicate with the
        // thread.
        for shard in conf.min_shard..=conf.max_shard {
            let fname = filename(shark, shard);
            let fname_clone = fname.clone();

            let channel: (Sender<Value>, Receiver<Value>) =
                crossbeam_channel::bounded(100);
            let obj_tx = channel.0;
            let obj_rx = channel.1;
            let log_clone = log.clone();
            let _handle: thread::JoinHandle<Result<(), Error>> =
                thread::spawn(move || {
                    let path = Path::new(fname_clone.as_str());
                    let mut file = BufWriter::new(
                        OpenOptions::new()
                            .append(true)
                            .create_new(true)
                            .open(path)
                            .unwrap_or_else(|e| {
                                panic!(
                                    "Couldn't create output file '{}': {}",
                                    path.display(),
                                    e
                                )
                            }),
                    );
                    while let Ok(msg) = obj_rx.recv() {
                        debug!(log_clone, "Recevied message: {}", &msg);
                        if !full_object {
                            let out_obj =
                                match sharkspotter::manta_obj_from_moray_obj(
                                    &msg,
                                ) {
                                    Ok(mo) => mo,
                                    Err(e) => {
                                        eprintln!("{}", e);
                                        return Ok(());
                                    }
                                };
                            serde_json::to_writer(&mut file, &out_obj)?;
                        // writer.write_all(&out_obj.into())?;
                        } else {
                            serde_json::to_writer(&mut file, &msg)?;
                            // writer.write_all(moray_obj.to_string())?;
                        }

                        // file.write_all(b"\n")?;

                        // let _ = write_mobj_to_file(file, &msg, full_object);
                    }
                    Ok(())
                });

            file_map.insert(fname, obj_tx);
        }
    }

    // let file_map = Arc::new(file_map);

    if conf.multithreaded {
        run_multithreaded(conf, log.clone(), move |msg| {
            let shark = msg.shark.replace(&domain_prefix, "");
            let shard = msg.shard;
            debug!(log, "shark: {}, shard: {}", shark, shard);

            // Only sharks that are in the config.sharks vector should be
            // passed to the callback.  If we see a shark that wasn't
            // specified that represents a programmer error.
            let file_sender = file_map
                .get(&filename(shark.as_str(), shard))
                .expect("unexpected shark");

            file_sender
                .send(msg.value.clone())
                .map_err(|_e| Error::new(ErrorKind::Other, "err"))
            // write_mobj_to_file(file, &msg.value, full_object)
        })
    } else {
        sharkspotter::run(conf, log.clone(), move |moray_obj, shark, shard| {
            let shark = shark.replace(&domain_prefix, "");
            debug!(log, "shark: {}, shard: {}", shark, shard);

            let file_sender =
                file_map.get(&filename(shark.as_str(), shard)).unwrap();

            debug!(log, "Sending message to file thread: {}", &moray_obj);
            file_sender
                .send(moray_obj.clone())
                .map_err(|_e| Error::new(ErrorKind::Other, "err"))
            // write_mobj_to_file(file, moray_obj, full_object)
        })
    }
}

fn run_with_user_file(
    filename: String,
    conf: Config,
    log: Logger,
) -> Result<(), Error> {
    // let path = Path::new(filename.as_str());
    // let full_object = conf.full_moray_obj;
    // let mut file = match OpenOptions::new().append(true).create(true).open(path)
    // {
    //     Err(e) => {
    //         panic!("Couldn't create output file '{}': {}", path.display(), e)
    //     }
    //     Ok(file) => file,
    // };

    // if conf.multithreaded {
    //     run_multithreaded(conf, log, move |msg| {
    //         write_mobj_to_file(&mut file, &msg.value, full_object)
    //     })
    // } else {
    //     sharkspotter::run(conf, log, move |moray_obj, _shark, _shard| {
    //         write_mobj_to_file(&mut file, moray_obj, full_object)
    //     })
    // }
    std::unimplemented!()
}

fn main() -> Result<(), Error> {
    let conf = Config::from_args().unwrap_or_else(|err| {
        eprintln!("Error parsing args: {}", err);
        process::exit(1);
    });

    let log = util::init_plain_logger();
    let filename = conf.output_file.clone();

    match filename {
        Some(fname) => run_with_user_file(fname, conf, log),
        None => run_with_file_map(conf, log),
    }
}
