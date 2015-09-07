use std::io::{self, Read, Write};
use std::net::{TcpStream, SocketAddr};
use mio::tcp::TcpStream as MioTcpStream;
use mio::buf::RingBuf;

pub trait Protocol {
    fn try_read(&mut self, ingress: &mut RingBuf) -> Result<(), ()> {
        Ok(())
    }

    fn try_write(&mut self, egress: &mut RingBuf) -> Result<(), ()> {
        Ok(())
    }
}

pub struct BufferedConn<T> {
    peer_addr: SocketAddr,
    conn: MioTcpStream,
    egress_buf: RingBuf,
    ingress_buf: RingBuf,
    inner: T,
}

fn should_continue(val: Option<usize>) -> bool {
    match val {
        None => false,
        // Does this happen in the wild?
        Some(0) => false,
        Some(len) => true,
    }
}

impl<T> BufferedConn<T> where T: Protocol {
    pub fn new(inner: T, conn: MioTcpStream) -> io::Result<BufferedConn<T>> {
        let peer_addr = try!(conn.peer_addr());
        Ok(BufferedConn {
            peer_addr: peer_addr,
            conn: conn,
            egress_buf: RingBuf::new(2 << 18),
            ingress_buf: RingBuf::new(2 << 18),
            inner: inner,
        })
    }

    pub fn populate_read(&mut self) -> io::Result<()> {
        use mio::TryRead;
        loop {
            let rd_byt = try!(self.conn.try_read_buf(&mut self.ingress_buf));
            if !should_continue(rd_byt) {
                return Ok(());
            }
            try!(self.inner.try_read(&mut self.ingress_buf).map_err(|()| {
                io::Error::new(io::ErrorKind::Other, "try_read failure")
            }))
        }
    }

    pub fn flush_write(&mut self) -> io::Result<()> {
        use mio::TryWrite;
        loop {
            let wr_byt = try!(self.conn.try_write_buf(&mut self.egress_buf));
            if !should_continue(wr_byt) {
                return Ok(());
            }

            try!(self.inner.try_write(&mut self.egress_buf).map_err(|()| {
                io::Error::new(io::ErrorKind::Other, "try_write failure")
            }))
        }
    }
}


pub fn copy<R: Read, W: Write>(reader: &mut R, writer: &mut W, max: u64) -> io::Result<u64> {
    use std::io::ErrorKind;
    const DEFAULT_BUF_SIZE: usize = 64 * 1024;

    let mut buf = [0; DEFAULT_BUF_SIZE];
    let mut written = 0;
    loop {
        let remaining = ::std::cmp::min((max - written) as usize, buf.len());
        if remaining == 0 {
            return Ok(written);
        }

        let len = match reader.read(&mut buf[..remaining]) {
            Ok(0) => return Ok(written),
            Ok(len) => len,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        try!(writer.write_all(&buf[..len]));
        written += len as u64;
    }
}

pub struct BlockingBufferedConn<T> {
    peer_addr: SocketAddr,
    conn: TcpStream,
    egress_buf: RingBuf,
    ingress_buf: RingBuf,
    inner: T,
}

impl<T> BlockingBufferedConn<T> where T: Protocol {
    pub fn new(inner: T, conn: TcpStream) -> io::Result<BlockingBufferedConn<T>> {
        let peer_addr = try!(conn.peer_addr());
        Ok(BlockingBufferedConn {
            peer_addr: peer_addr,
            conn: conn,
            egress_buf: RingBuf::new(2 << 18),
            ingress_buf: RingBuf::new(2 << 18),
            inner: inner,
        })
    }

    pub fn populate_read(&mut self) -> io::Result<()> {
        use mio::buf::MutBuf;

        let mut emptied_ctr = 0;
        loop {
            let remaining = MutBuf::remaining(&self.ingress_buf) as u64;
            let copied = try!(copy(&mut self.conn, &mut self.ingress_buf, remaining));
            if copied != 0 {
                emptied_ctr = 0;
            } else {
                emptied_ctr += 1;
            }

            try!(self.inner.try_read(&mut self.ingress_buf).map_err(|()| {
                io::Error::new(io::ErrorKind::Other, "try_read failure")
            }));

            if emptied_ctr > 2 {
                return Ok(())
            }
        }
    }

    pub fn flush_write(&mut self) -> io::Result<()> {
        use mio::buf::Buf;

        let mut emptied_ctr = 0;
        loop {
            let remaining = Buf::remaining(&self.egress_buf) as u64;
            let copied = try!(copy(&mut self.egress_buf, &mut self.conn, remaining));
            if copied != 0 {
                emptied_ctr = 0;
            } else {
                emptied_ctr += 1;
            }

            try!(self.inner.try_write(&mut self.egress_buf).map_err(|()| {
                io::Error::new(io::ErrorKind::Other, "try_write failure")
            }));

            if emptied_ctr > 2 {
                return Ok(())
            }
        }
    }

    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn egress_mut(&mut self) -> &mut RingBuf {
        &mut self.egress_buf
    }

    pub fn ingress_mut(&mut self) -> &mut RingBuf {
        &mut self.ingress_buf
    }
}
