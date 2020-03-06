const LIBTORRENT_RAKSHASA_EXAMPLE = b"-lt0D60-\xB1\xD5\xE7\xA1XUD\x5C\xC1\xE8\xD6\xA5";

fn version_charint(v: u8) -> u8 {
    match v {
        b'0'..=b'9' => v - b'0',
        b'A'..=b'Z' => v - b'A' + 10,
        b'A'..=b'z' => v - b'a' + 36,
        _ => 0,
    }
}

struct VersionCharintThreeDigit<'a>(&'a [u8]);

impl fmt::Display for VersionCharintThreeDigit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}.{}", 
            version_charint(self[0]),
            version_charint(self[1]),
            version_charint(self[2]))
    }
}


fn is_azureus_style(peer_id: &[u8; 20]) -> bool {
    peer_id[0] == b'-' && peer_id[7] == b'-'
}

fn decode_transmission_version(peer_id: &[u8; 20]) -> String {
    if peer_id[3..].starts_with("000") {
        return format!("Transmission 0.{}", peer_id[6]);
    }
    if peer_id[3..].starts_with("00") {
        return format!("Transmission 0.{}{}", peer_id[5], peer_id[6]);
    }
    let mut plus = "";
    if peer_id[6] == b'X' || peer_id[6] == b'Z' {
        plus = "+";
    }
    
    return format!("Transmission {}.{}{}{}", peer_id[3], peer_id[4], peer_id[5], plus)
}

pub fn decode_version(peer_id: &[u8; 20]) -> String {
    if is_azureus_style(peer_id) {
        match &peer_id[1..2] {
            "TR" => decode_transmission_version(peer_id),
            "lt" => {
                format!("Rakshasa libtorrent {}",
                    VersionCharintThreeDigit(&peer_id[3..]))
            }
            "LT" => {
                format!("Rasterbar libTorrent {}",
                    VersionCharintThreeDigit(&peer_id[3..]))
            }
            _ => return "Unknown".to_string(),
        }
    }

    "Unknown".to_string()
}