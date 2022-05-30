use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash, Hasher};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::ops::RangeInclusive;
use std::time::{Duration, Instant};

use smallvec::SmallVec;

use magnetite_common::TorrentId;

use crate::{general_heap_entry_push_or_replace, GeneralHeapEntry};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Hash)]
enum AddressFamily {
    AddrV4,
    AddrV6,
}

fn socket_addr_zero() -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([0; 4]), 0))
}

fn socket_addr_v6_zero() -> SocketAddr {
    SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from([0x0000; 8]), 0, 0, 0))
}

fn socket_addr_max() -> SocketAddr {
    SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from([0xFFFF; 8]), 0xFFFF, 0, 0))
}

fn socket_addr_incr(s: &SocketAddr) -> Option<SocketAddr> {
    match s {
        SocketAddr::V4(v4) => {
            let ip = *v4.ip();
            let port = v4.port();
            if port != 0xFFFF {
                return Some(SocketAddr::V4(SocketAddrV4::new(ip, port + 1)));
            }
            let mut ipbuf: [u8; 4] = ip.octets();
            for v in ipbuf.iter_mut().rev() {
                if *v != 0xFF {
                    *v += 1;
                    return Some(SocketAddr::V4(SocketAddrV4::new(ipbuf.into(), 0)));
                }
            }
            return Some(socket_addr_v6_zero());
        }
        SocketAddr::V6(v6) => {
            let ip = *v6.ip();
            let port = v6.port();
            if port != 0xFFFF {
                return Some(SocketAddr::V6(SocketAddrV6::new(ip, port + 1, 0, 0)));
            }
            let mut ipbuf: [u16; 8] = ip.segments();
            for v in ipbuf.iter_mut().rev() {
                if *v != 0xFFFF {
                    *v += 1;
                    return Some(SocketAddr::V6(SocketAddrV6::new(ipbuf.into(), 0, 0, 0)));
                }
            }
            return None;
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Hash)]
struct TrackerKey {
    info_hash: TorrentId,
    // address_family: AddressFamily,
    cookie: u64,
    address: SocketAddr,
}

impl TrackerKey {
    pub fn zero() -> TrackerKey {
        TrackerKey {
            info_hash: TorrentId::zero(),
            cookie: 0,
            address: socket_addr_zero(),
        }
    }

    pub fn max_value() -> TrackerKey {
        TrackerKey {
            info_hash: TorrentId::max_value(),
            cookie: u64::max_value(),
            address: socket_addr_max(),
        }
    }

    pub fn torrent_id_search_range(v: &TorrentId) -> RangeInclusive<TrackerKey> {
        std::ops::RangeInclusive::new(
            TrackerKey {
                info_hash: *v,
                cookie: 0,
                address: socket_addr_zero(),
            },
            TrackerKey {
                info_hash: *v,
                cookie: u64::max_value(),
                address: socket_addr_max(),
            },
        )
    }

    pub fn incr(&self) -> Option<TrackerKey> {
        let mut next = *self;

        if let Some(v) = socket_addr_incr(&self.address) {
            next.address = v;
            return Some(next);
        }

        if u64::max_value() != next.cookie {
            next.cookie += 1;
            next.address = socket_addr_zero();
            return Some(next);
        }

        for v in next.info_hash.as_mut_bytes().iter_mut().rev() {
            if *v != 0xFF {
                *v += 1;
                next.cookie = 0;
                next.address = socket_addr_zero();
                return Some(next);
            }
        }

        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrackerValue {
    peer_id: Option<TorrentId>,
    expiration: Instant,
}

impl TrackerValue {
    fn is_expired(&self, now: &Instant) -> bool {
        self.expiration <= *now
    }
}

#[derive(Debug, Clone)]
pub struct Tracker {
    peer_announce_duration: Duration,
    tick_cleanup_resume_key: TrackerKey,
    tick_cleanup_node_traversal_limit: usize,
    token_order_hash: RandomState,
    torrent_peers: BTreeMap<TrackerKey, TrackerValue>,
}

pub struct TrackerSearch {
    pub now: Instant,
    pub info_hash: TorrentId,
    pub cookie: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct TrackerCleanupStats {
    pub visited_nodes: usize,
    pub evicted_nodes: usize,
    pub search_inits: usize,
    tick_cleanup_resume_key_start: TrackerKey,
    tick_cleanup_resume_key_final: TrackerKey,
}

pub struct AnnounceCtx {
    pub now: Instant,
}

impl Tracker {
    pub fn new() -> Tracker {
        Tracker {
            peer_announce_duration: Duration::new(30 * 60, 0),
            tick_cleanup_resume_key: TrackerKey::zero(),
            tick_cleanup_node_traversal_limit: 5_000,
            token_order_hash: RandomState::new(),
            torrent_peers: BTreeMap::new(),
        }
    }

    pub fn get_torrent_peers_count(&self) -> usize {
        self.torrent_peers.len()
    }

    pub fn get_torrent_peers_debug<'a>(&'a self) -> impl Debug + 'a {
        &self.torrent_peers
    }

    pub fn get_peers_by_torrent(&self) -> BTreeMap<TorrentId, Vec<SocketAddr>> {
        let mut out = BTreeMap::new();
        for (tpk, tpv) in &self.torrent_peers {
            let vv = out.entry(tpk.info_hash).or_insert_with(Vec::new);
            vv.push(tpk.address);
        }
        out
    }

    pub fn insert_announce(
        &mut self,
        info_hash: &TorrentId,
        address: &SocketAddr,
        ctx: &AnnounceCtx,
    ) {
        let expiration = ctx.now + self.peer_announce_duration;
        let mut hasher = self.token_order_hash.build_hasher();
        Hash::hash(address, &mut hasher);
        let cookie = hasher.finish();
        self.torrent_peers.insert(
            TrackerKey {
                info_hash: info_hash.clone(),
                cookie,
                address: address.clone(),
            },
            TrackerValue {
                peer_id: None,
                expiration,
            },
        );
    }

    pub fn search_announce<'a, A>(&'a self, search: &TrackerSearch, into: &mut SmallVec<A>) -> bool
    where
        A: smallvec::Array<Item = &'a SocketAddr>,
    {
        type HeapEntry<'a> = GeneralHeapEntry<u64, &'a SocketAddr>;
        let mut heap = BinaryHeap::new();

        for (k, v) in self
            .torrent_peers
            .range(TrackerKey::torrent_id_search_range(&search.info_hash))
        {
            if v.is_expired(&search.now) {
                continue;
            }
            // max-heap so get the bitwise not of the xor-distance between the search cookie
            // and the entry/peer cookie.
            //
            // The search cookie is generated from the peers identity and a 15 minute randomly
            // generated value.  This causes the lookup results to be deterministic (ignoring
            // insertions / expirations) for a given peer which removes the incentive to query
            // rapidly.  The peers identity can be based on the IP+port or the peer id.
            general_heap_entry_push_or_replace(
                &mut heap,
                A::size(),
                HeapEntry {
                    dist_key: !(search.cookie ^ k.cookie),
                    value: &k.address,
                },
            );
        }

        for entry in heap.drain() {
            into.push(entry.value);
        }

        true
    }

    pub fn tick_cleanup(&mut self, now: &Instant) -> TrackerCleanupStats {
        // didn't see a strong perf improvement over 12.
        const EXPIRATION_TMP_LIMIT_STACK: usize = 12;
        const EXPIRATION_TMP_LIMIT: usize = EXPIRATION_TMP_LIMIT_STACK;

        let mut stats = TrackerCleanupStats {
            visited_nodes: 0,
            evicted_nodes: 0,
            search_inits: 0,
            tick_cleanup_resume_key_start: self.tick_cleanup_resume_key,
            tick_cleanup_resume_key_final: self.tick_cleanup_resume_key,
        };
        let mut tick_cleanup_node_traversal_limit = self.tick_cleanup_node_traversal_limit;

        let mut looped_around = false;
        let mut first_visited_key: TrackerKey = self.tick_cleanup_resume_key;
        let mut last_visited_key: TrackerKey = self.tick_cleanup_resume_key;
        let mut expirations: SmallVec<[TrackerKey; EXPIRATION_TMP_LIMIT_STACK]> = SmallVec::new();

        while tick_cleanup_node_traversal_limit > 0 {
            let mut last_visited_key_tmp: &TrackerKey = &last_visited_key;

            let mut found = false;
            stats.search_inits += 1;
            for (k, v) in self.torrent_peers.range(last_visited_key_tmp..) {
                found = true;
                tick_cleanup_node_traversal_limit -= 1;
                stats.visited_nodes += 1;
                if tick_cleanup_node_traversal_limit == 0 {
                    break;
                }

                last_visited_key_tmp = k;
                if v.is_expired(now) {
                    expirations.push(*k);
                }
                if expirations.len() == EXPIRATION_TMP_LIMIT {
                    // TODO: benchmarks then maybe we should make the list of expirations tracked
                    // each loop double every time.  sounds like it would be self-tuning?
                    break;
                }
                if looped_around && first_visited_key <= *last_visited_key_tmp {
                    break;
                }
            }

            last_visited_key = last_visited_key_tmp.incr().unwrap_or_else(TrackerKey::zero);

            for v in &expirations {
                self.torrent_peers.remove(v);
            }

            stats.evicted_nodes += expirations.len();
            expirations.clear();

            if tick_cleanup_node_traversal_limit == 0 {
                break;
            }
            if looped_around && first_visited_key <= last_visited_key {
                break;
            }
            if !found {
                looped_around = true;
                last_visited_key = TrackerKey::zero();
            }
        }

        self.tick_cleanup_resume_key = last_visited_key;
        stats.tick_cleanup_resume_key_final = last_visited_key;
        stats
    }
}

#[cfg(test)]
mod test {
    use std::collections::hash_map::RandomState;
    use std::collections::BTreeSet;
    use std::fmt::Debug;
    use std::hash::{BuildHasher, Hasher};
    use std::net::{SocketAddr, SocketAddrV4};
    use std::time::Duration;
    use std::time::Instant;

    use rand::{thread_rng, Rng, RngCore};
    use smallvec::SmallVec;

    use magnetite_common::TorrentId;

    use super::{
        socket_addr_max, socket_addr_zero, AnnounceCtx, Tracker, TrackerKey, TrackerSearch,
        TrackerValue,
    };

    fn p<F>(s: &str) -> F
    where
        F: std::str::FromStr,
        <F as std::str::FromStr>::Err: Debug,
    {
        s.parse::<F>().unwrap()
    }

    #[test]
    fn TrackerKey_incr_simple() {
        assert!(TrackerKey::max_value().incr().is_none());
        assert_eq!(
            TrackerKey::zero().incr().unwrap(),
            TrackerKey {
                info_hash: TorrentId::zero(),
                cookie: 0,
                address: SocketAddr::V4(SocketAddrV4::new([0; 4].into(), 1)),
            }
        );
    }

    #[test]
    fn TrackerKey_incr_cookie_boundary() {
        let start_tid = TorrentId([
            0x62, 0x9C, 0x86, 0xB1, 0xB3, 0x53, 0xC9, 0xCD, 0x4F, 0xFD, 0xD0, 0x33, 0x57, 0xB5,
            0xE8, 0xFC, 0x46, 0xB0, 0x3E, 0x1D,
        ]);
        let final_tid = TorrentId([
            0x62, 0x9C, 0x86, 0xB1, 0xB3, 0x53, 0xC9, 0xCD, 0x4F, 0xFD, 0xD0, 0x33, 0x57, 0xB5,
            0xE8, 0xFC, 0x46, 0xB0, 0x3E, 0x1E,
        ]);
        assert_eq!(
            TrackerKey {
                info_hash: start_tid,
                cookie: u64::max_value(),
                address: socket_addr_max(),
            }
            .incr()
            .unwrap(),
            TrackerKey {
                info_hash: final_tid,
                cookie: 0,
                address: socket_addr_zero(),
            }
        );

        let start_tid = TorrentId([
            0x62, 0x9C, 0x86, 0xB1, 0xB3, 0x53, 0xC9, 0xCD, 0x4F, 0xFD, 0xD0, 0x33, 0x57, 0xB5,
            0xE8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        ]);
        let final_tid = TorrentId([
            0x62, 0x9C, 0x86, 0xB1, 0xB3, 0x53, 0xC9, 0xCD, 0x4F, 0xFD, 0xD0, 0x33, 0x57, 0xB5,
            0xE9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        ]);
        assert_eq!(
            TrackerKey {
                info_hash: start_tid,
                cookie: u64::max_value(),
                address: socket_addr_max(),
            }
            .incr()
            .unwrap(),
            TrackerKey {
                info_hash: final_tid,
                cookie: 0,
                address: socket_addr_zero(),
            }
        );
    }

    #[test]
    fn x() {
        let now = Instant::now();
        let mut rng = thread_rng();

        let s = RandomState::new();
        let mut tracker = Tracker::new();
        let mut last_torrent_id = None;
        for j in 0..200 {
            let mut torrent_id = TorrentId::zero();
            rng.fill_bytes(&mut torrent_id.0);

            for i in 0..100 {
                let port: u16 = rng.gen_range(1 << 10..1 << 15);
                let mut ipaddr: [u8; 4] = [0; 4];
                ipaddr[0] = rng.gen_range(1..240);
                ipaddr[1] = rng.gen();
                ipaddr[2] = rng.gen();
                ipaddr[3] = rng.gen();
                let address = SocketAddr::V4(SocketAddrV4::new(ipaddr.into(), port));

                let mut hasher = s.build_hasher();
                hasher.write(&ipaddr[..]);
                let expiration = if rng.gen_range(0_u8..100) <= 5 {
                    now - Duration::new(120, 0)
                } else {
                    now + Duration::new(120, 0)
                };

                tracker.torrent_peers.insert(
                    TrackerKey {
                        info_hash: torrent_id,
                        cookie: hasher.finish(),
                        address,
                    },
                    TrackerValue {
                        peer_id: None,
                        expiration,
                    },
                );
            }

            last_torrent_id = Some(torrent_id);
        }

        let last_torrent_id = last_torrent_id.unwrap();
        let search_a = &TrackerSearch {
            now: now,
            info_hash: last_torrent_id,
            cookie: 0x7FFF_FFFF_FFFF_FFFF,
        };
        let search_b = TrackerSearch {
            now: now,
            info_hash: last_torrent_id,
            cookie: rng.gen(),
        };

        let mut sinto: SmallVec<[&SocketAddr; 16]> = SmallVec::new();
        let before = Instant::now();
        tracker.search_announce(&search_a, &mut sinto);
        eprintln!("searched in {:?}", before.elapsed());
        let cloned_ai: Vec<_> = sinto.into_iter().map(Clone::clone).collect();
        let mut sinto: SmallVec<[&SocketAddr; 16]> = SmallVec::new();
        tracker.search_announce(&search_b, &mut sinto);
        eprintln!("searched in {:?}", before.elapsed());
        let cloned_bi: Vec<_> = sinto.into_iter().map(Clone::clone).collect();

        for i in 0..30 {
            eprintln!(
                "starting eviction {} on {:?}",
                i, tracker.tick_cleanup_resume_key
            );
            let before = Instant::now();
            let res = tracker.tick_cleanup(&now);
            eprintln!("    evicted in {:?} - {:?}", before.elapsed(), res);
        }

        let mut sinto: SmallVec<[&SocketAddr; 16]> = SmallVec::new();
        let before = Instant::now();
        tracker.search_announce(&search_a, &mut sinto);
        eprintln!("searched in {:?}", before.elapsed());
        let cloned_af: Vec<_> = sinto.into_iter().map(Clone::clone).collect();
        let mut sinto: SmallVec<[&SocketAddr; 16]> = SmallVec::new();
        tracker.search_announce(&search_b, &mut sinto);
        eprintln!("searched in {:?}", before.elapsed());
        let cloned_bf: Vec<_> = sinto.into_iter().map(Clone::clone).collect();

        assert_eq!(cloned_ai, cloned_af);
        assert_eq!(cloned_bi, cloned_bf);
        panic!();
    }

    fn default_test_tracker() -> Tracker {
        let mut tracker = Tracker::new();
        tracker.insert_announce(
            &p("c34bf4b88cb74cab053e70490e04be120a1a469e"),
            &p("10.0.1.1:1200"),
            &AnnounceCtx {
                now: Instant::now() - Duration::new(30 * 60 + 1, 0),
            },
        );

        tracker.insert_announce(
            &p("c34bf4b88cb74cab053e70490e04be120a1a469e"),
            &p("10.0.1.1:1234"),
            &AnnounceCtx {
                now: Instant::now(),
            },
        );
        tracker.insert_announce(
            &p("c34bf4b88cb74cab053e70490e04be120a1a469e"),
            &p("10.0.1.1:1235"),
            &AnnounceCtx {
                now: Instant::now(),
            },
        );
        tracker.insert_announce(
            &p("ff4bf4b88cb74cab053e70490e04be120a1a469e"),
            &p("192.168.1.1:1234"),
            &AnnounceCtx {
                now: Instant::now(),
            },
        );

        tracker
    }

    #[test]
    fn Tracker__search_announce() {
        let unexpected_values: &[SocketAddr] = &[p("10.0.1.1:1200"), p("192.168.1.1:1234")];
        let expected_values: &[SocketAddr] = &[p("10.0.1.1:1234"), p("10.0.1.1:1235")];

        let tracker = default_test_tracker();

        let search_a = TrackerSearch {
            now: Instant::now(),
            info_hash: p("c34bf4b88cb74cab053e70490e04be120a1a469e"),
            cookie: 0x7FFF_FFFF_FFFF_FFFF,
        };
        let mut sinto: SmallVec<[&SocketAddr; 16]> = SmallVec::new();
        tracker.search_announce(&search_a, &mut sinto);
        let mut addrs: BTreeSet<_> = sinto.into_iter().cloned().collect();
        println!("{:?}", addrs);
        for uv in unexpected_values {
            assert!(!addrs.contains(uv), "unexpectedly found value");
        }
        for ev in expected_values {
            assert!(addrs.contains(ev), "value missing when expected");
        }
    }

    #[test]
    fn Tracker_verify_socketaddr_invariants() {
        // we depend on these being true for our range queries and incrementation logic
        // to work, so let us test them to ensure our future.
        let saddr_zero: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let saddr_v4_max: SocketAddr = "255.255.255.255:65535".parse().unwrap();
        let saddr_v6_min: SocketAddr = "[::]:0".parse().unwrap();
        let saddr_max: SocketAddr = "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
            .parse()
            .unwrap();

        assert!(saddr_zero < saddr_v4_max);
        assert!(saddr_zero < saddr_v6_min);
        assert!(saddr_zero < saddr_max);

        assert!(saddr_v4_max < saddr_v6_min);
        assert!(saddr_v4_max < saddr_max);

        assert!(saddr_v6_min < saddr_max);
    }
}
