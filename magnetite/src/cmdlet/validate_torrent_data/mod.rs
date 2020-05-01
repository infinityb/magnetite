use std::fs::File;
use std::io::Read;
use std::path::Path;

use clap::{App, Arg, SubCommand};
use sha1::{Digest, Sha1};
use tokio::runtime::Runtime;
use tracing::{event, Level};

use crate::model::{StorageEngineCorruption, TorrentID, TorrentMetaWrapped};
use crate::storage::{multi_file, GetPieceRequest, PieceStorageEngineDumb};
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "validate-torrent-data";

pub fn get_subcommand() -> App<'static, 'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("Validate a directory or file against a torrent")
        .arg(
            Arg::with_name("torrent-file")
                .long("torrent-file")
                .value_name("FILE")
                .help("The torrent file to serve")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("target")
                .long("target")
                .value_name("PATH")
                .help("The target")
                .required(true)
                .takes_value(true),
        )
}

pub fn main(matches: &clap::ArgMatches) -> Result<(), failure::Error> {
    let mut rt = Runtime::new()?;

    let torrent_file = matches.value_of_os("torrent-file").unwrap();
    let torrent_file = Path::new(torrent_file).to_owned();

    let target_path = matches.value_of_os("target").unwrap();
    let target_path = Path::new(target_path).to_owned();

    let mut by = Vec::new();
    let mut file = File::open(&torrent_file).unwrap();
    file.read_to_end(&mut by).unwrap();
    let torrent = TorrentMetaWrapped::from_bytes(&by)?;

    let mut reg_files = Vec::new();
    for file in &torrent.meta.info.files {
        reg_files.push(multi_file::FileInfo {
            file_size: file.length,
            rel_path: file.path.clone(),
        });
    }

    let mut mf_builder = multi_file::MultiFileStorageEngine::builder();
    mf_builder.register_info_hash(
        &torrent.info_hash,
        multi_file::Registration {
            base_dir: target_path,
            files: reg_files,
        },
    );

    let storage_engine = mf_builder.build();

    let mut detected_broken_piece = false;
    for (idx, ps) in torrent.piece_shas.iter().enumerate() {
        let req = GetPieceRequest {
            content_key: torrent.info_hash,
            piece_sha: *ps,
            piece_length: torrent.meta.info.piece_length,
            total_length: torrent.total_length,
            piece_index: idx as u32,
        };
        let piece = rt.block_on(storage_engine.get_piece_dumb(&req))?;
        let mut hasher = Sha1::new();
        hasher.input(&piece[..]);
        let sha = hasher.result();
        let mut sha_outcome = TorrentID::zero();
        sha_outcome.as_mut_bytes().copy_from_slice(&sha[..]);

        let piece_is_valid = ps == &sha_outcome;

        event!(
            Level::INFO,
            piece_index = req.piece_index,
            piece_is_valid = piece_is_valid
        );

        if !piece_is_valid {
            if !detected_broken_piece {
                event!(
                    Level::ERROR,
                    piece_index = req.piece_index,
                    piece_is_valid = piece_is_valid
                );
            }
            detected_broken_piece = true;
        }
    }

    if detected_broken_piece {
        return Err(StorageEngineCorruption.into());
    }

    Ok(())
}
