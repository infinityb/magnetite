use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::mem;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::fmt::Debug;

use futures::Future;

use crate::storage::{GetDataVec, GetDataRequest, GetDataResponsePending, GetDataResponse};
use crate::model::{InternalError, MagnetiteError, TorrentMetaWrapped};

pub struct FileSpan<'a> {
    pub fully_qualified_path: Cow<'a, Path>,
    pub offset: u64,
    pub length: u32,
}

struct FileSpanUnpacked<'a> {
    span: FileSpan<'a>,
}

impl<'a> Iterator for FileSpanUnpacked<'a> {
    type Item = FileSpan<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}


#[derive(Debug)]
pub struct BackingStoreTome {
    pub file_path: PathBuf,
}

#[derive(Debug)]
pub struct BackingStoreMultiFile {
    pub base_dir: PathBuf,
    pub piece_file_paths: BTreeMap<u64, FileInfo>,
}

#[derive(Debug)]
pub struct FileInfo {
    pub rel_path: PathBuf,
    pub file_size: u64,
}

trait FileSpanAware {
    fn put_piece_file_spans_into<'a>(&'a self, global_offset: u64, request_length: u32, out: &mut Vec<FileSpan<'a>>) -> Result<(), MagnetiteError>;
}

impl BackingStoreTome {
    pub fn from_file_path(file_path: &Path) -> BackingStoreTome {
        BackingStoreTome {
            file_path: PathBuf::from(file_path),
        }
    }
}

impl FileSpanAware for BackingStoreTome {
    fn put_piece_file_spans_into<'a>(
        &'a self,
        offset: u64,
        length: u32,
        out: &mut Vec<FileSpan<'a>>,
    ) -> Result<(), MagnetiteError> {
        out.push(FileSpan {
            fully_qualified_path: Cow::Borrowed(&self.file_path),
            offset,
            length,
        });
        Ok(())
    }
}

impl BackingStoreMultiFile {
    pub fn from_torrent_meta(
        base_path: &Path,
        wrapped: &TorrentMetaWrapped,
    ) -> BackingStoreMultiFile {
        let mut acc = 0;
        let mut piece_file_paths: BTreeMap<u64, FileInfo> = Default::default();
        for f in &wrapped.meta.info.files {
            piece_file_paths.insert(
                acc,
                FileInfo {
                    rel_path: f.path.clone(),
                    file_size: f.length,
                },
            );
            acc += f.length;
        }

        BackingStoreMultiFile {
            base_dir: PathBuf::from(base_path),
            piece_file_paths,
        }
    }
}

impl FileSpanAware for BackingStoreMultiFile {
    fn put_piece_file_spans_into<'a>(
        &'a self,
        global_offset: u64,
        request_length: u32,
        out: &mut Vec<FileSpan<'a>>,
    ) -> Result<(), MagnetiteError> {
        let mut global_offset_acc = global_offset;
        let mut req_size_acc = request_length as u64;
        while 0 < req_size_acc {
            let (file_global_offset, file_info) = self
                .piece_file_paths
                .range(..=global_offset_acc)
                .rev()
                .next()
                .ok_or_else(|| InternalError {
                    msg: "failed to find span",
                })?;

            let file_rel_offset = global_offset_acc - *file_global_offset;
            assert!(file_rel_offset < file_info.file_size);

            let mut file_remaining: u64 = file_info.file_size - file_rel_offset;
            if req_size_acc < file_remaining {
                file_remaining = req_size_acc;
            }
            req_size_acc -= file_remaining;
            global_offset_acc += file_remaining;

            let fully_qualified_path = Cow::Owned(self.base_dir.join(&file_info.rel_path));
            out.push(FileSpan {
                fully_qualified_path,
                offset: file_rel_offset,
                length: file_remaining as u32,
            });
        }

        Ok(())
    }
}

fn mul_u32_to_u64(left: u32, right: u32) -> u64 {
    u64::from(left) * u64::from(right)
}

fn fma_u32_to_u64(left: u32, right: u32, add: u32) -> u64 {
    // overflow impossible: u32::max * u32::max + u32::max <= u64::max
    u64::from(left) * u64::from(right) + u64::from(add)
}

struct LastSentPieceSource<'a> {
    file_path: &'a Path,
    file_offset: u64,
    open_file: File,
}

pub trait PieceStorageEngineDumb: Debug {
    fn get_piece_dumb<'a>(
        &'a self,
        req: &GetDataRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Box<GetDataResponse>, MagnetiteError>> + Send + 'a>>;
}

// impl<T> PieceStorageEngineDumb for Box<T>
// where
//     T: PieceStorageEngineDumb + ?Sized,
// {
//     fn get_piece_dumb(
//         &self,
//         req: &GetDataRequest,
//     ) -> Pin<Box<dyn Future<Output = Result<Box<GetDataResponse>, MagnetiteError>> + Send>> {
//         PieceStorageEngineDumb::get_piece_dumb(&**self, req)
//     }
// }

// impl<T> PieceStorageEngineDumb for Arc<T>
// where
//     T: PieceStorageEngineDumb + ?Sized,
// {
//     fn get_piece_dumb(
//         &self,
//         req: &GetDataRequest,
//     ) -> Pin<Box<dyn Future<Output = Result<Box<GetDataResponse>, MagnetiteError>> + Send>> {
//         PieceStorageEngineDumb::get_piece_dumb(&**self, req)
//     }
// }

impl<T> PieceStorageEngineDumb for T where T: FileSpanAware + Debug {
    fn get_piece_dumb<'a>(
        &'a self,
        req: &GetDataRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Box<GetDataResponse>, MagnetiteError>> + Send + 'a>> {
        let mut spans: Vec<FileSpan> = Vec::new();
        let req: GetDataRequest = req.clone();

        let global_offset = fma_u32_to_u64(req.piece_locator.piece_index, req.piece_locator.piece_length, req.piece_rel_byte_offset);
        if let Err(err) = self.put_piece_file_spans_into(global_offset, req.length, &mut spans) {
            return Box::pin(async { Err(err) });
        }

        // FIXME: we can use iovecs here.
        Box::pin(async {
            // 
            // let request_length = mul_u32_to_u64(req.block_fetch_count, req.piece_length);
            //
            let mut offset_cache: Option<LastSentPieceSource> = None;
            // let mut last_file = None;

            let pending = GetDataResponsePending::from(req);
            
            // assert_eq!(
            //     pending.individual_remaining().map(|x| x.piece_length as u64).sum(),
            //     spans.iter().map(|x| x.length as u64).sum());
            
            if pending.is_complete() {
                // nothing requested  - the remaining iterator returned no values, so we're in the
                // complete state.
                return Ok(Box::new(pending.finalize()))
            }

            // let mut piece_buffers = Vec::new();
            let mut to_fill = pending.remaining_span_iter();

            let mut current_data_req;
            let mut current_piece_buffer;
            let mut current_piece_buffer_offset;
            if let Some(v) = to_fill.next() {
                current_data_req = v;
                current_piece_buffer = vec![0; v.length as usize];
                current_piece_buffer_offset = 0;
            } else {
                unreachable!("ensured that the iterator is non-empty");
            }

            for span in &spans {
                let span_length = u64::from(span.length);
                if current_piece_buffer[current_piece_buffer_offset..].is_empty() {
                    let mut blocks: GetDataVec<Arc<[u8]>> = Default::default();
                    blocks.push(mem::replace(&mut current_piece_buffer, Vec::new()).into_boxed_slice().into());
                    pending.merge(&GetDataResponse {
                        request: current_data_req,
                        blocks,
                    });

                    if let Some(v) = to_fill.next() {
                        current_data_req = v;
                        current_piece_buffer_offset = 0;
                    } else {
                        unreachable!("shouldn't get here if spans and to-fill are the same length");
                    }
                }

                let mut reuse_open_file: Option<File> = None;
                let mut suppress_seek = false;
                if let Some(oc) = offset_cache.take() {
                    if oc.file_path == span.fully_qualified_path {
                        reuse_open_file = Some(oc.open_file);
                        if oc.file_offset == span.offset {
                            suppress_seek = true;
                        }
                    } else if span.offset == 0 {
                        suppress_seek = true;
                    }
                }

                let mut file = if let Some(of) = reuse_open_file {
                    of
                } else {
                    File::open(&span.fully_qualified_path)?
                };
                if !suppress_seek {
                    file.seek(SeekFrom::Start(span.offset))?;
                }

                loop {
                    let piece_remaining = &mut current_piece_buffer[current_piece_buffer_offset..];
                    let got = file.read(piece_remaining)?;
                    current_piece_buffer_offset += got;
                    if piece_remaining.is_empty() || got == 0 {
                        break;
                    }
                }

                offset_cache = Some(LastSentPieceSource {
                    file_path: &span.fully_qualified_path,
                    file_offset: span.offset + span_length,
                    open_file: file,
                });
            }

            return Ok(Box::new(pending.finalize()))
        })
    }
}
