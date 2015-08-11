use std::io;
use std::net::SocketAddr;
use mio::tcp::TcpStream;
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
    conn: TcpStream,
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
    pub fn new(inner: T, conn: TcpStream) -> io::Result<BufferedConn<T>> {
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
