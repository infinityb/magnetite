use std::fmt;

fn version_charint(v: u8) -> u8 {
    match v {
        b'0'..=b'9' => v - b'0',
        b'A'..=b'Z' => v - b'A' + 10,
        b'A'..=b'z' => v - b'a' + 36,
        _ => 0,
    }
}

struct VersionCharintThreeDigit<'a>(&'a [u8]);

impl<'a> fmt::Display for VersionCharintThreeDigit<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}.{}", 
            version_charint(self.0[3]),
            version_charint(self.0[4]),
            version_charint(self.0[5]))
    }
}

fn is_azureus_style(peer_id: &[u8]) -> bool {
    if peer_id.len() != 20 {
        return false;
    }
    peer_id[0] == b'-' && peer_id[7] == b'-'
}

fn decode_transmission_version(peer_id: &[u8]) -> String {
    if peer_id.len() != 20 {
        return "Unknown b".to_string();
    }

    if peer_id[3..].starts_with(b"000") {
        return format!("Transmission 0.{}", peer_id[6]);
    }
    if peer_id[3..].starts_with(b"00") {
        return format!("Transmission 0.{}{}", peer_id[5], peer_id[6]);
    }
    let mut plus = "";
    if peer_id[6] == b'X' || peer_id[6] == b'Z' {
        plus = "+";
    }
    
    return format!("Transmission {}.{}{}{}", peer_id[3], peer_id[4], peer_id[5], plus)
}

pub fn decode_version(peer_id: &[u8]) -> String {
    if peer_id.len() != 20 {
        return "Unknown a".to_string();
    }

    if is_azureus_style(peer_id) {
        return match &peer_id[1..3] {
            b"TR" => decode_transmission_version(peer_id),
            b"lt" => {
                format!("Rakshasa libtorrent {}",
                    VersionCharintThreeDigit(&peer_id[3..]))
            }
            b"LT" => {
                format!("Rasterbar libTorrent {}",
                    VersionCharintThreeDigit(&peer_id[3..]))
            }
            _ => return "Unknown x".to_string(),
        }
    }

    "Unknown y".to_string()
}

#[cfg(test)]
mod tests {
    use super::decode_version;
    const LIBTORRENT_RAKSHASA_EXAMPLE: &[u8] = b"-lt0D60-\xB1\xD5\xE7\xA1XUD\x5C\xC1\xE8\xD6\xA5";

    #[test]
    fn decode_rakshasa_example() {
        let xx = decode_version(LIBTORRENT_RAKSHASA_EXAMPLE);
        assert_eq!("Rakshasa libtorrent 0.13.6", &*xx);
    }
}
