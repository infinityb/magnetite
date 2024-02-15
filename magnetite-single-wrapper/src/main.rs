use std::net::TcpListener;
use std::os::fd::OwnedFd;
use std::io;
use std::process::{Command, Stdio};

use clap::{Arg, ArgAction, value_parser};
use tokio::runtime;
use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

fn main() -> Result<(), failure::Error> {
    let mut my_subscriber_builder = FmtSubscriber::builder()
        // .with_ansi(false)
        .json()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL);
        //.pretty();

    let app = clap::Command::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <stacey.ell@gmail.com>")
        .about("Demonstration Torrent Seeder Launcher")
        .arg(
            Arg::new("v")
                .short('v')
                .action(ArgAction::Count)
                .value_parser(value_parser!(u8).range(..5))
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::new("bind-address")
                .default_value("0.0.0.0:51409")
                .value_parser(value_parser!(std::net::SocketAddr))
                .help("set bind address"),
        );

    let matches = app.get_matches();
    let verbosity = matches.get_count("v");
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

    let rt = runtime::Builder::new_current_thread().enable_io().enable_time().build()?;
    rt.block_on(async {
        if let Err(e) = main2(&matches).await {
            let root = e.find_root_cause();
            if root.downcast_ref::<io::Error>().is_some() {
                event!(Level::ERROR, "--- i/o error +++: {}", e);
                return Err(e);
            }
            event!(Level::ERROR, "{}", e);
            return Err(e)
        }
        Ok(())
    })
}

async fn main2(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    let bind_address: &std::net::SocketAddr = matches.get_one("bind-address").expect("required");
    let listener = TcpListener::bind(bind_address)?;
    for (task, stream) in listener.incoming().enumerate() {
        let s = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed accepting stream: {e:?}");
                continue;
            }
        };

        let spawn_res = Command::new("magnetite-single")
            .arg("-vvv")
            .stdin(Stdio::from(OwnedFd::from(s)))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            // .create_pidfd(true)
            .spawn();

        let mut child = match spawn_res {
            Ok(child) => child,
            Err(e) => {
                eprintln!("failed to spawn magnetite-single: {e:?}");
                continue;
            }
        };

        std::thread::spawn(move || {
            match child.wait() {
                Ok(status) => {
                    if !status.success() {
                        eprintln!("exited with: {status:?}");
                    }
                }
                Err(e) => eprintln!("orphaning task#{task}: {e}"),
            };
        });
    }
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
