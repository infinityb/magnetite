#![allow(dead_code)]

use clap::{App, AppSettings, Arg, ArgAction, value_parser};
use futures::future::FutureExt;
use metrics_runtime::Receiver;
use tokio::runtime::Runtime;
use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;

mod bittorrent;
mod cmdlet;
mod model;
mod scheduler;
mod storage;
mod tracker;
mod utils;
mod vfs;
mod clients;

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

fn main() -> Result<(), anyhow::Error> {
    let mut rt = Runtime::new().unwrap();

    let metrics_rx = Receiver::builder()
        .build()
        .expect("failed to create receiver");

    let metrics_controller = metrics_rx.controller();
    metrics_rx.install();

    let mut my_subscriber_builder = FmtSubscriber::builder();

    let mut app = App::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <stacey.ell@gmail.com>")
        .about("Demonstration Torrent Seeder")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("v")
                .short('v')
                .action(ArgAction::Count)
                .value_parser(value_parser!(u8).range(..5))
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::with_name("prometheus-bind-address")
                .long("prometheus-bind-address")
                .value_name("[ADDRESS]")
                .help("The address to bind to for prometheus")
                .takes_value(true),
        )
        .subcommand(cmdlet::seed::get_subcommand())
        .subcommand(cmdlet::daemon::get_subcommand());

    #[cfg(feature = "with-mse")]
    let app = app
            .subcommand(cmdlet::assemble_mse_tome::get_subcommand())
            .subcommand(cmdlet::validate_mse_tome::get_subcommand())
            .subcommand(cmdlet::host::get_subcommand())
            .subcommand(cmdlet::webserver::get_subcommand());

    let app = app
        .subcommand(cmdlet::dump_torrent_info::get_subcommand())
        .subcommand(cmdlet::validate_torrent_data::get_subcommand());

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

    if let Some(addr) = matches.value_of("prometheus-bind-address") {
        let addr = addr.parse().unwrap();
        rt.spawn(async move {
            let prom = metrics_runtime::observers::PrometheusBuilder::new();

            let ex = metrics_runtime::exporters::HttpExporter::new(metrics_controller, prom, addr);

            if let Err(err) = ex.async_run().await {
                event!(Level::ERROR, "metrics exporter returned an error: {}", err);
            }
        });
    }

    let (sub_name, args) = matches.subcommand().unwrap();
    let main_future = match sub_name {
        cmdlet::download::SUBCOMMAND_NAME => cmdlet::download::main(args).boxed(),
        cmdlet::seed::SUBCOMMAND_NAME => cmdlet::seed::main(args).boxed(),
        cmdlet::dump_torrent_info::SUBCOMMAND_NAME => cmdlet::dump_torrent_info::main(args).boxed(),

        // #[cfg(feature = "with-mse")]
        // cmdlet::host::SUBCOMMAND_NAME => cmdlet::host::main(args).boxed(),
        // #[cfg(feature = "with-mse")]
        // cmdlet::webserver::SUBCOMMAND_NAME => cmdlet::webserver::main(args).boxed(),
        // #[cfg(feature = "with-mse")]
        // cmdlet::assemble_mse_tome::SUBCOMMAND_NAME => cmdlet::assemble_mse_tome::main(args).boxed(),
        // #[cfg(feature = "with-mse")]
        // cmdlet::validate_mse_tome::SUBCOMMAND_NAME => cmdlet::validate_mse_tome::main(args).boxed(),
        // #[cfg(feature = "with-fuse")]
        // cmdlet::fuse_mount::SUBCOMMAND_NAME => cmdlet::fuse_mount::main(args).boxed(),

        cmdlet::daemon::SUBCOMMAND_NAME => cmdlet::daemon::main(args).boxed(),
        cmdlet::validate_torrent_data::SUBCOMMAND_NAME => {
            cmdlet::validate_torrent_data::main(args).boxed()
        }
        _ => panic!("bad argument parse"),
    };

    rt.block_on(main_future)?;

    Ok(())
}

#[allow(clippy::cognitive_complexity)] // macro bug around event!()
fn print_test_logging() {
    event!(Level::TRACE, "logger initialized - trace check");
    event!(Level::DEBUG, "logger initialized - debug check");
    event!(Level::INFO, "logger initialized - info check");
    event!(Level::WARN, "logger initialized - warn check");
    event!(Level::ERROR, "logger initialized - error check");
}
