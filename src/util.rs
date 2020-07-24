/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use slog::{o, Drain, Level, LevelFilter, Logger};
use std::io;
use std::sync::Mutex;
use clap::{crate_name, crate_version};


fn create_bunyan_logger<W>(io: W, level: Level) -> Logger
where
    W: io::Write + std::marker::Send + 'static,
{
    Logger::root(
        Mutex::new(LevelFilter::new(
            slog_bunyan::with_name(crate_name!(), io).build(),
            level,
        ))
        .fuse(),
        o!("build-id" => crate_version!()),
    )
}

pub fn init_global_logger(
    log_level: Option<Level>,
) -> slog_scope::GlobalLoggerGuard {
    let mut level = Level::Trace;

    if let Some(l) = log_level {
        level = l;
    }

    let log = create_bunyan_logger(std::io::stdout(), level);
    slog_scope::set_global_logger(log)
}
