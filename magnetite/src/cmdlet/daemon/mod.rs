use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::PathBuf;

use serde::{Serializer, Deserializer};
use clap::{App, Arg, SubCommand};
use sha1::{Digest, Sha1};
use serde::{Deserialize, Serialize};

use magnetite_common::TorrentId;

use magnetite_model::TorrentMeta;
use crate::CARGO_PKG_VERSION;

pub const SUBCOMMAND_NAME: &str = "daemon";

pub fn get_subcommand() -> App<'static> {
    SubCommand::with_name(SUBCOMMAND_NAME)
        .version(CARGO_PKG_VERSION)
        .about("run magnetite daemon")
        .arg(
            Arg::with_name("config.format")
                .long("config.format")
                .value_name("FORMAT")
                .possible_values(&["toml", "json"])
                .default_value("toml")
                .help("config file format")
        )
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("FILE")
                .help("config file")
                .required(true)
                .takes_value(true),
        )
}

#[derive(Debug)]
enum BindAddress {
    NetworkSocket(SocketAddr),
    UnixSocket(PathBuf),
}

#[derive(Debug, Deserialize, Serialize)]
struct Config {
    data_bind_address: Vec<BindAddress>,
    mgmt_bind_address: Vec<BindAddress>,
}

pub async fn main(matches: &clap::ArgMatches) -> Result<(), anyhow::Error> {
    let config_file = matches.value_of_os("config").unwrap();
    let config_file = Path::new(config_file).to_owned();

    let mut by = Vec::new();
    let mut file = File::open(&config_file)?;
    file.read_to_end(&mut by)?;

    let config: Config;
    if matches.value_of("config.format").unwrap() == "json" {
        config = serde_json::from_slice(&by[..])?;
    } else {
        config = toml::de::from_slice(&by[..])?;
    }

    println!("{:?}", config);
    let xx = toml::ser::to_string_pretty(&config).unwrap();
    println!("{}", xx);
    println!("{}",  serde_json::to_string_pretty(&config).unwrap());

    //


    Ok(())
}

impl Serialize for BindAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let buf = match *self {
            BindAddress::NetworkSocket(SocketAddr::V4(ref v4)) => {
                format!("ipv4@{}", v4)
            }
            BindAddress::NetworkSocket(SocketAddr::V6(ref v6)) => {
                format!("ipv6@{}", v6)
            }
            BindAddress::UnixSocket(ref uu) => {
                format!("unix@{}", uu.display())
            }
        };

        serializer.serialize_str(&buf[..])
    }
}

impl<'de> Deserialize<'de> for BindAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BindAddressVisitor)
    }
}

struct BindAddressVisitor;

impl<'de> serde::de::Visitor<'de> for BindAddressVisitor {
    type Value = BindAddress;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an IP-port pair or path")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        use serde::de::Unexpected;

        const INV_BIND_ADDRESS: &str = "hxxp://TODO"; // TODO
        // const INV_BIND_ADDRESS: &str = "https://magnetite-docs.yshi.org/e/inv-bind-address";

        fn err<E>(s: &str, msg: &str) -> E where E: serde::de::Error {
            if msg.len() != 0 {
                let new_msg = format!("a bind address (see {}, {})", INV_BIND_ADDRESS, msg);
                serde::de::Error::invalid_value(Unexpected::Str(s), &&new_msg[..])
            } else {
                let new_msg = format!("a bind address (see {})", INV_BIND_ADDRESS);
                serde::de::Error::invalid_value(Unexpected::Str(s), &&new_msg[..])
            }
        }

        if s.len() == 0 {
            return Err(err(s, ""))
        }
        if let Some(ipv4) = s.strip_prefix("ipv4@") {
            let v4 = ipv4.parse::<SocketAddrV4>()
                .map_err(|e| err(s, &e.to_string()))?;
            return Ok(BindAddress::NetworkSocket(SocketAddr::V4(v4)));
        }
        if let Some(ipv6) = s.strip_prefix("ipv6@") {
            let v6 = ipv6.parse::<SocketAddrV6>()
                .map_err(|e| err(s, &e.to_string()))?;
            return Ok(BindAddress::NetworkSocket(SocketAddr::V6(v6)));
        }
        if let Some(unix) = s.strip_prefix("unix@") {
            return Ok(BindAddress::UnixSocket(PathBuf::from(s)));
        }

        if let Ok(sa) = s.parse::<SocketAddr>() {
            return Ok(BindAddress::NetworkSocket(sa))
        }

        return Ok(BindAddress::UnixSocket(PathBuf::from(s)));
    }
}
