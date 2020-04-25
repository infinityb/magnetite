#![allow(dead_code)]

use clap::{App, AppSettings, Arg};

use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;

mod cmdlet;
mod model;
mod scheduler;
mod storage;
mod tracker;
mod utils;
mod vfs;

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

fn main() -> Result<(), failure::Error> {
    let mut my_subscriber_builder = FmtSubscriber::builder();

    let app = App::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <stacey.ell@gmail.com>")
        .about("Demonstration Torrent Seeder")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .subcommand(cmdlet::seed::get_subcommand())
        .subcommand(cmdlet::dump_torrent_info::get_subcommand())
        .subcommand(cmdlet::assemble_mse_tome::get_subcommand())
        .subcommand(cmdlet::validate_mse_tome::get_subcommand())
        .subcommand(cmdlet::host::get_subcommand())
        .subcommand(cmdlet::webserver::get_subcommand());

    #[cfg(feature = "with-fuse")]
    let app = app.subcommand(cmdlet::fuse_mount::get_subcommand());
    let matches = app.get_matches();

    let verbosity = matches.occurrences_of("v");
    let should_print_test_logging = 4 < verbosity;

    my_subscriber_builder = my_subscriber_builder.with_max_level(match verbosity {
        0 => TracingLevelFilter::ERROR,
        1 => TracingLevelFilter::WARN,
        2 => TracingLevelFilter::INFO,
        3 => TracingLevelFilter::DEBUG,
        _ => TracingLevelFilter::TRACE,
    });

    tracing::subscriber::set_global_default(my_subscriber_builder.finish())
        .expect("setting tracing default failed");

    if should_print_test_logging {
        print_test_logging();
    }

    let (sub_name, args) = matches.subcommand();
    let main_function = match sub_name {
        cmdlet::seed::SUBCOMMAND_NAME => cmdlet::seed::main,
        cmdlet::dump_torrent_info::SUBCOMMAND_NAME => cmdlet::dump_torrent_info::main,
        cmdlet::assemble_mse_tome::SUBCOMMAND_NAME => cmdlet::assemble_mse_tome::main,
        cmdlet::validate_mse_tome::SUBCOMMAND_NAME => cmdlet::validate_mse_tome::main,
        #[cfg(feature = "with-fuse")]
        cmdlet::fuse_mount::SUBCOMMAND_NAME => cmdlet::fuse_mount::main,
        cmdlet::host::SUBCOMMAND_NAME => cmdlet::host::main,
        cmdlet::webserver::SUBCOMMAND_NAME => cmdlet::webserver::main,
        _ => panic!("bad argument parse"),
    };
    main_function(args.expect("subcommand args"))
}

#[allow(clippy::cognitive_complexity)] // macro bug around event!()
fn print_test_logging() {
    event!(Level::TRACE, "logger initialized - trace check");
    event!(Level::DEBUG, "logger initialized - debug check");
    event!(Level::INFO, "logger initialized - info check");
    event!(Level::WARN, "logger initialized - warn check");
    event!(Level::ERROR, "logger initialized - error check");
}
