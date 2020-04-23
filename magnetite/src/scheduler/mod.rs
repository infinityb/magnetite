use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

use smallvec::Array;
use smallvec::SmallVec;

use crate::model::proto::Message;
use crate::model::proto::PieceSlice;
use crate::model::{BitField, TorrentID};

#[derive(Debug)]
struct PieceSet {
    // current -> length
    pieces: BTreeMap<u32, u32>,
}

impl PieceSet {
    pub fn remove(&mut self, piece_id: u32) -> bool {
        if let Some((k, v)) = self.pieces.range(..piece_id).rev().next() {
            let start = *k;
            let end = *k + *v;

            if start <= piece_id && piece_id < end {
                let piece_next = piece_id + 1;
                let length1 = piece_id - start;
                let length2 = end - piece_next;
                if length1 > 0 {
                    self.pieces.insert(start, length1);
                }
                if length2 > 0 {
                    self.pieces.insert(piece_next, length2);
                }
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    fn join_parts(&mut self, root_key: u32) {
        struct Action {
            new_length: u32,
            delete_key: u32,
        }

        let mut action = None;
        if let Some(length) = self.pieces.get(&root_key) {
            let next_key = root_key + length;
            if let Some(length_next) = self.pieces.get(&next_key) {
                action = Some(Action {
                    new_length: length + length_next,
                    delete_key: root_key + length,
                });
            }
        }
        if let Some(a) = action {
            self.pieces.remove(&a.delete_key);
            self.pieces.insert(root_key, a.new_length);
        }
    }

    fn cleanup_range(&mut self, start_key: u32, length: u32) {
        // we need to find entries that newly overlap with the bulk insertion.
        let new_end = start_key + length;
        while let Some((k, v)) = self.pieces.range_mut(start_key..start_key + length).next() {
            let start = *k;
            let end = *k + *v;
            drop((k, v));

            if end <= new_end {
                // wholely included in our span, just remove key
                self.pieces.remove(&start);
            } else {
                // overlapping but not wholely included - we can take the max of (end, new_end)
                // to combine it with the new span.  end is already known to be larger than new_end.
                self.pieces.insert(start_key, end - start_key);
                return;
            }
        }
    }

    pub fn bulk_add(&mut self, piece_id: u32, length: u32) -> bool {
        if let Some((k, v)) = self.pieces.range_mut(..piece_id).rev().next() {
            // extend the found slice, if we border it.
            let start = *k;
            let end = *k + *v;

            if start <= piece_id {
                if piece_id < end {
                    *v += length - end + piece_id;
                    self.cleanup_range(piece_id, length);
                    self.join_parts(start);
                    return true;
                }
                return false;
            }
            self.pieces.insert(piece_id, length);
            self.join_parts(start);
            return true;
        }

        self.pieces.insert(piece_id, length);
        self.join_parts(piece_id);

        true
    }

    pub fn add(&mut self, piece_id: u32) -> bool {
        self.bulk_add(piece_id, 1)
    }

    pub fn iter(&self) -> PieceSetIter {
        PieceSetIter {
            range_start: 0,
            range_end: 0,
            ps: self.pieces.iter(),
        }
    }

    // pub fn random<R: rand::Rng>(&self, r: &mut R, amount: usize) -> PieceSetRandom {
    //     let length = self.pieces.values().sum();
    //     let indices = rand::seq::index::sample(rng, length as usize, amount);
    //     for (k, v) in &self.pieces {
    //         //
    //     }

    //     PieceSetRandom::new {
    //         piece_set: self,
    //         indices: indices.into_iter(),
    //     }
    // }
}

struct PieceSetIter<'a> {
    range_start: u32,
    range_end: u32,
    ps: std::collections::btree_map::Iter<'a, u32, u32>,
}

impl<'a> Iterator for PieceSetIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range_start < self.range_end {
            let prev_range_start = self.range_start;
            self.range_start += 1;
            return Some(prev_range_start);
        }
        if let Some((k, v)) = self.ps.next() {
            self.range_start = *k;
            self.range_end = *k + *v;
        }
        if self.range_start < self.range_end {
            let prev_range_start = self.range_start;
            self.range_start += 1;
            return Some(prev_range_start);
        }
        None
    }
}

// struct PieceSetRandom<'a> {
//     piece_set: &'a PieceSet,
//     indices: rand::seq::index::IndexVecIntoIter,
// }

// impl Iterator for PieceSetRandom {
//     type Item = u32;

//     fn next(&mut self) -> Option<Self::Item> {

//         self.indices
//     }
// }

pub struct SummingBitField {
    damage_counter: u16,
    bit_length: u32,
    next_piece_candidates: Box<usize>,
    data: Box<[u8]>,
}

#[inline]
fn add_one_clamping_lob_u8(value: u8) -> u8 {
    if value == 0x7F {
        return 0x7F;
    }
    if value & 0x80 > 0 {
        return value;
    }
    value + 1
}

#[inline]
fn sub_one_clamping_lob_u8(value: u8) -> u8 {
    if value == 0 {
        return 0;
    }
    if value & 0x80 > 0 {
        return value;
    }
    value - 1
}

impl SummingBitField {
    pub fn add_bitfield(&mut self, bf: &BitField) {
        self.damage_counter = self.damage_counter.saturating_add(1);

        for (o, is_set) in self.data.iter_mut().zip(bf.iter()) {
            if is_set {
                *o = add_one_clamping_lob_u8(*o);
            }
        }
    }

    pub fn remove_bitfield(&mut self, bf: &BitField) {
        self.damage_counter = self.damage_counter.saturating_add(1);
        for (o, is_set) in self.data.iter_mut().zip(bf.iter()) {
            if is_set {
                *o = sub_one_clamping_lob_u8(*o);
            }
        }
    }
}

pub trait SchedulerPolicy {
    fn should_regenerate(&self, bf: &SummingBitField) -> bool;
}

pub struct DefaultSchedulerPolicy;

impl SchedulerPolicy for DefaultSchedulerPolicy {
    fn should_regenerate(&self, bf: &SummingBitField) -> bool {
        bf.damage_counter > 256
    }
}

struct DefaultPieceSelectionStrategy {
    piece_length: u64,

    // ground truth data
    high_priority: PieceSet,
    normal_priority: PieceSet,
    rarities: SummingBitField,

    // cached data
    last_target_update: Instant,
    target_chunks: VecDeque<PieceSlice>,
    piece_assignments: HashMap<u32, TorrentID>,

    // other
    in_progress: HashMap<u32, PieceState>,
    peer_data: HashMap<TorrentID, Box<PeerState>>,
}

impl DefaultPieceSelectionStrategy {
    pub fn finish_piece(&mut self, piece_id: u32) {
        self.in_progress.remove(&piece_id);
    }

    pub fn finish_chunk(&mut self, _piece_id: u32, _chunk_id: u32) {
        //
    }

    pub fn get_work<A>(&mut self, _bytes: u64, _into: &mut SmallVec<A>)
    where
        A: Array<Item = (TorrentID, u32, u32)>,
    {
        // find the 100 rarest pieces which are of high priority. if we don't
        // yet have 100 pieces, continue the same logic but with normal
        // priority pieces.
        let _submitted_bytes = 0;
        let _high_p = self.high_priority.iter();
        let _normal_p = self.normal_priority.iter();

        // into.append(())
        // DOWNLOAD_CHUNK_SIZE
        // while submitted_bytes < bytes {
        //     for (_k, _v) in self.in_progress.iter() {
        //         //
        //     }
        // }

        unimplemented!();
    }
}

const SLIDING_WINDOW_RATE_SLOTS: usize = 8;

struct SlidingWindowRate {
    last_value_per_second: u64,
    data: SmallVec<[(Instant, u64); SLIDING_WINDOW_RATE_SLOTS]>,
}

impl SlidingWindowRate {
    pub fn add_data(&mut self, when: Instant, value: u64) {
        if SLIDING_WINDOW_RATE_SLOTS <= self.data.len() {
            let d = self.data.iter_mut().min_by_key(|d| d.0).unwrap();
            *d = (when, value);
        } else {
            self.data.push((when, value));
        }

        let oldest = self.data.iter().min_by_key(|d| d.0).unwrap();
        let newest = self.data.iter().max_by_key(|d| d.0).unwrap();

        let mut total_value = 0;
        for (_, value) in &self.data {
            total_value += value;
        }

        self.last_value_per_second =
            (1_000_000 * u128::from(total_value) / (newest.0 - oldest.0).as_micros()) as u64;
    }

    pub fn get_rate_per_second(&self) -> u64 {
        self.last_value_per_second
    }
}

struct PeerState {
    piece_length: u64,
    piece_count: u32,
    peer_bitfield: BitField,
    exposed_bitfield: BitField,
    // 4MB worth in-place for 256kB pieces. 128MB for 16MB pieces.
    // Will probably need to allocate for high-bandwidth peers on low-piece-size torrents.
    downloading_current_pieces: SmallVec<[u32; 16]>,
    // how many bytes we have outstanding from this peer (download).
    outstanding_bytes: u64,
    // // TODO:
    // // * peers get credit when they send us pieces that pass torrent validation.  Peers with
    // //   higher amounts of credit might be treated preferrentially.  Credit is reduced when
    // //   a peer downloads some data, based on a global resource utilization factor.
    // peer_credit: u64,
    peer_downloaded: u64,
    peer_download_rate: SlidingWindowRate,
    peer_estimated_download_rate: SlidingWindowRate,
    peer_uploaded: u64,
    peer_upload_rate: SlidingWindowRate,

    // A piece slice is downloaded by the client when the client is interested in a
    // peer, and that peer is not choking the client.
    // A piece slice is uploaded by a client when the client is not choking a peer,
    // and that peer is interested in the client.
    am_choked: bool,
    am_interested: bool,
    peer_choked: bool,
    peer_interested: bool,

    //
    extensions: HashSet<ProtocolExtension>,
}

impl PeerState {
    pub fn add_download_bytes(&mut self, bytes: u64) {
        self.peer_downloaded += bytes;
        self.peer_download_rate.add_data(Instant::now(), bytes);
    }

    pub fn add_upload_rate(&mut self, bytes: u64) {
        self.peer_uploaded += bytes;
        self.peer_upload_rate.add_data(Instant::now(), bytes);
    }

    pub fn apply_message_bulk(&mut self, messages: &[Message]) {
        let mut have_acc = 0;
        let mut download_acc = 0;
        for m in messages {
            match m {
                Message::Keepalive => (),
                Message::Choke => self.am_choked = true,
                Message::Unchoke => self.am_choked = false,
                Message::Interested => self.peer_interested = true,
                Message::Uninterested => self.peer_interested = false,
                Message::Have { piece_id } => {
                    if self.peer_bitfield.set(*piece_id, true) {
                        have_acc += self.piece_length;
                    }
                }
                Message::Bitfield { ref field_data } => {
                    // FIXME: validate incoming bitfield length and kill client if invalid
                    if self.peer_bitfield.data.len() == field_data.as_slice().len() {
                        self.peer_bitfield.data = field_data.as_slice().to_vec().into_boxed_slice();
                    }
                }
                Message::Piece { data, .. } => {
                    download_acc += data.as_slice().len() as u64;
                }
                Message::Request(..) | Message::Cancel(..) | Message::Port { .. } => (),
            }
        }
        if have_acc > 0 {
            self.peer_estimated_download_rate
                .add_data(Instant::now(), have_acc);
        }
        if download_acc > 0 {
            self.peer_download_rate
                .add_data(Instant::now(), download_acc);
        }
    }
}

enum ProtocolExtension {
    //
}

struct PieceState {
    last_update: Instant,
    chunks_downloaded: PieceSet,
    chunks_to_request: PieceSet,
    involved_peers: HashSet<TorrentID>,
}

pub struct ChunkLocation {
    piece_id: u32,
    chunk_offset: u32,
    chunk_length: u32,
}
