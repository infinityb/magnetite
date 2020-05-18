use std::convert::Infallible;
use std::net::SocketAddr;

use hyper::{Body, Request, Response, Server};
use hyper::service::{make_service_fn, service_fn};
use tokio::runtime::Runtime;
use clap::{App, AppSettings, Arg};
use metrics_core::{Builder, Drain, Observe};
use metrics_runtime::Receiver;
use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

mod torrentid;

async fn handle(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    Ok(Response::new(Body::from("Hello World")))
}

struct TrackerState {
    torrents: BTreeMap<TorrentID, >
}

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("A demonstration webserver")
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("FILE")
                .help("config file")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("bind-address")
                .long("bind-address")
                .value_name("[ADDRESS]")
                .help("The address to bind to")
                .required(true)
                .takes_value(true),
        )
}

fn main() -> Result<(), failure::Error> {
    let mut rt = Runtime::new()?;

    let metrics_rx = Receiver::builder()
        .build()
        .expect("failed to create receiver");

    let metrics_controller_prom_http = metrics_rx.controller();
    metrics_rx.install();

    let app = App::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <stacey.ell@gmail.com>")
        .about("Demonstration Torrent Tracker")
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::with_name("bind-address")
                .long("bind-address")
                .value_name("[ADDRESS]")
                .help("The address to bind to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("prometheus-bind-address")
                .long("prometheus-bind-address")
                .value_name("[ADDRESS]")
                .help("The address to bind to for prometheus")
                .takes_value(true),
        );

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
        rt.spawn(async move {
            let prom = metrics_runtime::observers::PrometheusBuilder::new();

            let ex = metrics_runtime::exporters::HttpExporter::new(
                metrics_controller_prom_http,
                prom,
                addr.parse().unwrap(),
            );

            if let Err(err) = ex.async_run().await {
                event!(Level::ERROR, "metrics exporter returned an error: {}", err);
            }
        });
    }
    
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let make_svc = make_service_fn(move |socket: &AddrStream| {
        async move {
            let fs_impl = fs_impl.clone();
            let service = service_fn(move |req: Request<Body>| {
                let fs_impl = fs_impl.clone();
                async move {
                    let v = service_request(fs_impl, remote_addr, req).await;
                    Ok::<_, Infallible>(v)
                }
            });

            Ok::<_, Infallible>(service)
        }
    });

    rt.block_on(async {
        let make_service = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(handle))
        });

        let server = Server::bind(&addr).serve(make_service);

        server.await
    })?;

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
