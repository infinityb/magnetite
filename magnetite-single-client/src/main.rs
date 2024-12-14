use std::fs::{File};
use std::io::{self, Read};
use std::path::PathBuf;

use clap::{Arg, ArgAction, value_parser};
use failure::ResultExt;
use tracing::{span, event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;
use magnetite_single_api::{futures, tokio};
use magnetite_single_api::proto::ChangeTorrentStatusRequest;
use magnetite_model::TorrentMetaWrapped;

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

fn main() -> Result<(), failure::Error> {
    let mut my_subscriber_builder = FmtSubscriber::builder()
        .json()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL);

    let app = clap::Command::new(CARGO_PKG_NAME)
        .version(CARGO_PKG_VERSION)
        .author("Stacey Ell <stacey.ell@gmail.com>")
        .about("Demonstration Torrent Seeder")
        .arg(
            Arg::new("v")
                .short('v')
                .action(ArgAction::Count)
                .value_parser(value_parser!(u8).range(..5))
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::new("torrent-file")
                .long("torrent-file")
                .required(true)
                .action(ArgAction::Set)
                .value_parser(value_parser!(PathBuf))
                .help("read this file"),
        )
        .arg(
            Arg::new("data-file")
                .long("data-file")
                .required(true)
                .action(ArgAction::Set)
                // .value_parser(value_parser!(PathBuf))
                .help("read this file"),
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

    let rt = tokio::runtime::Builder::new_current_thread().enable_io().enable_time().build()?;
    rt.block_on(async {
        if let Err(root) = main2(&matches).await {
            if root.downcast_ref::<io::Error>().is_some() {
                event!(Level::ERROR, "--- i/o error +++: {}", root);
                return Err(root);
            }
            event!(Level::ERROR, "{}", root);
            return Err(root)
        }
        Ok(())
    })
}


async fn main2(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    use magnetite_single_api::proto::magnetite_client::MagnetiteClient;
    use magnetite_single_api::proto::AddTorrentRequest;
    let mut client = MagnetiteClient::connect("http://[::1]:10000").await?;

    let filename = matches.get_one::<PathBuf>("torrent-file").unwrap();
    let mut file = File::open(filename)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    let xx = TorrentMetaWrapped::from_bytes(&buf)?;
    let x = client.add_torrent(AddTorrentRequest {
        info_hash: xx.info_hash.hex().to_string(),
        torrent_data: buf,
        file_path: matches.get_one::<String>("data-file").unwrap().clone(),
    }).await?;

    let x = client.change_torrent_status(ChangeTorrentStatusRequest {
        info_hash: xx.info_hash.hex().to_string(),
        set_active: true,
    }).await?;

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

