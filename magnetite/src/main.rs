use std::any::Any;
use std::sync::Arc;

use clap::{App, AppSettings, Arg};
use futures::future::FutureExt;
use metrics_runtime::Receiver;
use tokio::runtime::Runtime;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;

use crate::utils::close_waiter;

mod bittorrent;
mod cmdlet;
mod control;
mod model;
mod scheduler;
mod storage;
mod tracker;
mod utils;
mod vfs;

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

type BusMessage = Arc<dyn Any + Send + Sync>;

#[derive(Clone)]
pub struct CommonInit {
    ebus: broadcast::Sender<BusMessage>,
    init_sig: mpsc::Sender<()>,
    term_sig: crate::utils::close_waiter::Done,
}

fn main() -> Result<(), failure::Error> {
    let mut rt = Runtime::new().unwrap();

    let metrics_rx = Receiver::builder()
        .build()
        .expect("failed to create receiver");

    let metrics_controller = metrics_rx.controller();
    metrics_rx.install();

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
        .arg(
            Arg::with_name("prometheus-bind-address")
                .long("prometheus-bind-address")
                .value_name("[ADDRESS]")
                .help("The address to bind to for prometheus")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("control-bind-address")
                .long("control-bind-address")
                .value_name("[ADDRESS]")
                .help("The address to bind to for control commands")
                .takes_value(true),
        )
        .subcommand(cmdlet::host::get_subcommand())
        .subcommand(cmdlet::ctl::get_subcommand())
        .subcommand(cmdlet::dump_torrent_info::get_subcommand())
        .subcommand(cmdlet::assemble_mse_tome::get_subcommand())
        .subcommand(cmdlet::validate_mse_tome::get_subcommand())
        .subcommand(cmdlet::validate_torrent_data::get_subcommand());

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

    let (init_sig, init_sig_rx) = mpsc::channel(1);
    let (holder, term_sig) = close_waiter::channel();
    rt.spawn(async move {
        signal::ctrl_c().await.expect("failed to listen for event");
        event!(Level::INFO, "got ctrl+c - finishing up pending work");
        drop(holder);
    });
    let (ebus, _) = broadcast::channel(1024);
    let common_init = CommonInit {
        ebus,
        init_sig,
        term_sig,
    };

    let (sub_name, args) = matches.subcommand();
    let args = args.expect("subcommand args");

    if let Some(addr) = matches.value_of("control-bind-address") {
        if sub_name != cmdlet::ctl::SUBCOMMAND_NAME {
            let addr = addr.to_string();
            let common_init = common_init.clone();
            rt.spawn(async move {
                if let Err(err) = crate::control::start_control_service(common_init, &addr).await {
                    event!(Level::ERROR, "control service shut down: {}", err);
                };
            });
        }
    }

    let main_future = match sub_name {
        cmdlet::ctl::SUBCOMMAND_NAME => cmdlet::ctl::main(args).boxed(),
        // cmdlet::seed::SUBCOMMAND_NAME => cmdlet::seed::main(common_init, args).boxed(),
        cmdlet::dump_torrent_info::SUBCOMMAND_NAME => cmdlet::dump_torrent_info::main(args).boxed(),
        cmdlet::assemble_mse_tome::SUBCOMMAND_NAME => cmdlet::assemble_mse_tome::main(args).boxed(),
        cmdlet::validate_mse_tome::SUBCOMMAND_NAME => cmdlet::validate_mse_tome::main(args).boxed(),
        cmdlet::host::SUBCOMMAND_NAME => cmdlet::host::main(common_init, args).boxed(),
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
