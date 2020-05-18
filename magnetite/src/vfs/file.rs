use std::io::SeekFrom;

use tokio::fs::File;

struct File<P> {
    // FIXME: we should be re-opening so we get a unique file handle or using pread64 so we can
    // read the shared file handle without locking.
    underlying_file: P,
    content_info_man: Arc<Mutex<ContentInfoManager>>,
    underlying_file_start_pos: u64,
    piece_length: u64,
    length: u64,

    // relative to the logical file.
    current_offset: u64,
    read_future: Option<PendingRead>,
}

struct PendingRead {
    // for asserting, because I'm not confident in my full understanding of async.
    // imo, this *should* never trigger but let's see.
    read_time_offset: u64,
    future: Pin<Box<dyn Future<Output=Result<Bytes, MagnetiteError>> + Send>>,
}

impl<P> AsyncRead for File<P> where P: PieceStorageEngineDumb + Send + Sync + 'static {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8]
    ) -> Poll<Result<usize>> {
        loop {
            if let Some(ref mut fut) = self.read_future {
                match self.fut.future.poll(cx) {
                    Poll::Ready(piece) => {
                        assert_eq!(fut.read_time_offset, self.current_offset);

                        let global_offset = self.underlying_file_start_pos + self.current_offset;
                        let piece_offset = global_offset % self.piece_length;

                        let mut piece_slice = &piece[piece_offset as usize..];
                        if buf.len() < piece_slice.len() {
                            piece_slice = &piece_slice[..buf.len()];
                        }

                        buf[..piece_slice.len()].copy_from_slice(piece_slice);
                    },
                    Poll::Pending => return Poll::Pending,
                }
            }

            if self.length <= self.current_offset {
                return Poll::Ready(Ok(0));
            }

            let piece_index = (self.underlying_file_start_pos + self.current_offset) / self.piece_length;

            self.read_future = Some(PendingRead {
                read_time_offset: self.current_offset,
                future: underlying_file.get_piece(piece_index),
            });
        }
    }
}

impl<P> AsyncSeek for File<P> where P: PieceStorageEngineDumb + Send + Sync + 'static {
    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<u64>> {
        Poll::Ready(Ok(self.current_offset))
    }

    fn start_seek(
        self: Pin<&mut Self>,
        cx: &mut Context,
        position: SeekFrom
    ) -> Poll<Result<()>> {
        // cancel any futures left over, they'll be invalid?.
        self.read_future = None;

        match position {
            SeekFrom::Start(v) => {
                self.current_offset = v;
            },
            SeekFrom::End(v) => {
                match Ord::cmp(v, 0) {
                    Ordering::Less => {
                        let back = -v as u64;
                        if self.length < back {
                            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid argument")));
                        }
                        self.current_offset = self.length - back;
                    }
                    Ordering::Equal => {
                        self.current_offset = length;
                    }
                    Ordering::Greater => {
                        match self.length.checked_add(v as u64) {
                            Some(v) = self.current_offset = v,
                            None => return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid argument"))),
                        }
                    }
                }
            },
            SeekFrom::Current(v) => {
                match Ord::cmp(v, 0) {
                    Ordering::Less => {
                        let back = -v as u64;
                        if self.current_offset < back {
                            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid argument")));
                        }
                        self.current_offset = self.current_offset - back;
                    }
                    Ordering::Equal => (),
                    Ordering::Greater => {
                        match self.length.checked_add(v as u64) {
                            Some(v) = self.current_offset = v,
                            None => return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid argument"))),
                        }
                    }
                }
            }
        };
        Poll::Ready(Ok(()))
    }
}


