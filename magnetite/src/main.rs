use clap::{App, AppSettings, Arg};

use tracing::{event, Level};
use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
use tracing_subscriber::FmtSubscriber;

mod cmdlet;
mod model;
mod storage;
mod tracker;
mod scheduler;

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_NAME: &str = env!("CARGO_PKG_NAME");

fn main() -> Result<(), failure::Error> {
    let mut my_subscriber_builder = FmtSubscriber::builder();

    let matches = App::new(CARGO_PKG_NAME)
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
        .subcommand(cmdlet::fuse_mount::get_subcommand())
        .get_matches();

    let verbosity = matches.occurrences_of("v");
    let print_test_logging = 4 < verbosity;

    match verbosity {
        0 => {
            my_subscriber_builder = my_subscriber_builder.with_max_level(TracingLevelFilter::ERROR)
        }
        1 => my_subscriber_builder = my_subscriber_builder.with_max_level(TracingLevelFilter::WARN),
        2 => my_subscriber_builder = my_subscriber_builder.with_max_level(TracingLevelFilter::INFO),
        3 => {
            my_subscriber_builder = my_subscriber_builder.with_max_level(TracingLevelFilter::DEBUG)
        }
        _ => {
            my_subscriber_builder = my_subscriber_builder.with_max_level(TracingLevelFilter::TRACE)
        }
    };

    tracing::subscriber::set_global_default(my_subscriber_builder.finish())
        .expect("setting tracing default failed");

    if print_test_logging {
        event!(Level::TRACE, "logger initialized - trace check");
        event!(Level::DEBUG, "logger initialized - debug check");
        event!(Level::INFO, "logger initialized - info check");
        event!(Level::WARN, "logger initialized - warn check");
        event!(Level::ERROR, "logger initialized - error check");
    }

    let (sub_name, args) = matches.subcommand();
    let main_function = match sub_name {
        cmdlet::seed::SUBCOMMAND_NAME => cmdlet::seed::main,
        cmdlet::dump_torrent_info::SUBCOMMAND_NAME => cmdlet::dump_torrent_info::main,
        cmdlet::assemble_mse_tome::SUBCOMMAND_NAME => cmdlet::assemble_mse_tome::main,
        cmdlet::validate_mse_tome::SUBCOMMAND_NAME => cmdlet::validate_mse_tome::main,
        cmdlet::fuse_mount::SUBCOMMAND_NAME => cmdlet::fuse_mount::main,
        _ => panic!("bad argument parse"),
    };
    main_function(args.expect("subcommand args"))
}

// #[cfg(test)]
// mod tests {
//     use crate::model::proto::{Handshake, HANDSHAKE_PREFIX};

//     #[test]
//     fn parse_header_full() {
//         static HEADER: &'static [u8] = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x01\x14\xbe\xab\xae\xc8W\x99\x7f]p\xc1\x94\x10`kI\x1b\xb4\xf1\x9b-lt0D00-\xea/\x9f}\xd4\xb1\xa1$\xde\xaf\xe9\xb6";
//         let header = Handshake::new(HEADER).unwrap();
//         assert_eq!(header.get_protocol(), HANDSHAKE_PREFIX);
//         assert_eq!(header.get_reserved(), b"\x00\x00\x00\x00\x00\x10\x00\x01");
//         assert_eq!(
//             header.get_info_hash(),
//             b"\x14\xbe\xab\xae\xc8W\x99\x7f]p\xc1\x94\x10`kI\x1b\xb4\xf1\x9b"
//         );
//         assert!(header.has_peer_id());
//         assert_eq!(
//             header.get_peer_id(),
//             Some(&b"-lt0D00-\xea/\x9f}\xd4\xb1\xa1$\xde\xaf\xe9\xb6"[..])
//         );
//     }

//     #[test]
//     fn parse_header_partial() {
//         static HEADER: &'static [u8] = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x01\x14\xbe\xab\xae\xc8W\x99\x7f]p\xc1\x94\x10`kI\x1b\xb4\xf1\x9b";
//         let header = Handshake::new(HEADER).unwrap();
//         assert_eq!(header.get_protocol(), HANDSHAKE_PREFIX);
//         assert_eq!(header.get_reserved(), b"\x00\x00\x00\x00\x00\x10\x00\x01");
//         assert_eq!(
//             header.get_info_hash(),
//             b"\x14\xbe\xab\xae\xc8W\x99\x7f]p\xc1\x94\x10`kI\x1b\xb4\xf1\x9b"
//         );
//         assert!(!header.has_peer_id());
//         assert_eq!(header.get_peer_id(), None);
//     }

//     #[test]
//     fn parse_header_oversized() {
//         static HEADER: &'static [u8] = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x01\x14\xbe\xab\xae\xc8W\x99\x7f]p\xc1\x94\x10`kI\x1b\xb4\xf1\x9b-lt0D00-\xea/\x9f}\xd4\xb1\xa1$\xde\xaf\xe9\xb6xxx";
//         let header = Handshake::new(HEADER).unwrap();
//         assert_eq!(header.as_bytes().len(), 68);
//     }
// }

// #[cfg(feature = "afl")]
// pub mod afl {
//     use crate::model::proto::{Handshake, HANDSHAKE_PREFIX};
//     use std::io::{self, Read};

//     pub fn afl_header_buf() {
//         let mut buf = Vec::new();
//         io::stdin().take(1 << 20).read_to_end(&mut buf).unwrap();
//         let _ = Header::new(&buf);
//     }
// }
