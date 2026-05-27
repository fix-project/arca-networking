use arca_pipe::{BidirectionalPipe, PipeError, Read, Write};
use crate::dataframe::{DataFrameHeader, FrameType};

#[derive(Debug)]
pub enum StreamError {
    WriteClosed,
    ConnectionReset,
    MessageTooLarge,
}

pub struct SyncStream<'a> {
    pub conn_id: u32,
    pipe: BidirectionalPipe<'a>,
    write_closed: bool,
    read_closed: bool,
    current_frame_remaining: u32,
}

impl<'a> SyncStream<'a> {
    pub fn from_pipe(conn_id: u32, pipe: BidirectionalPipe<'a>) -> Self {
        Self { conn_id, pipe, write_closed: false, read_closed: false, current_frame_remaining: 0 }
    }

    pub fn send(&mut self, buf: &[u8]) -> Result<usize, StreamError> {
        if self.write_closed {
            return Err(StreamError::WriteClosed);
        }
        if buf.is_empty() {
            return Ok(0);
        }
        if buf.len() > u32::MAX as usize {
            return Err(StreamError::MessageTooLarge);
        }
        write_all(&mut self.pipe, DataFrameHeader::new(FrameType::Data, buf.len() as u32).as_bytes());
        write_all(&mut self.pipe, buf);
        Ok(buf.len())
    }

    pub fn recv(&mut self, buf: &mut [u8]) -> Result<usize, StreamError> {
        if self.read_closed {
            return Ok(0);
        }

        if self.current_frame_remaining == 0 {
            let mut header_bytes = [0u8; core::mem::size_of::<DataFrameHeader>()];
            read_exact(&mut self.pipe, &mut header_bytes);
            let frame_type = FrameType::from_u32(u32::from_le_bytes(header_bytes[0..4].try_into().unwrap()));
            let payload_len = u32::from_le_bytes(header_bytes[4..8].try_into().unwrap());

            match frame_type {
                None => {
                    self.read_closed = true;
                    write_all(&mut self.pipe, DataFrameHeader::new(FrameType::Rst, 0).as_bytes());
                    self.write_closed = true;
                    return Err(StreamError::ConnectionReset);
                }
                Some(FrameType::Fin) => {
                    self.read_closed = true;
                    return Ok(0);
                }
                Some(FrameType::Rst) => {
                    self.read_closed = true;
                    self.write_closed = true;
                    return Err(StreamError::ConnectionReset);
                }
                Some(FrameType::Data) => {
                    self.current_frame_remaining = payload_len;
                }
            }
        }

        let to_read = (self.current_frame_remaining as usize).min(buf.len());
        read_exact(&mut self.pipe, &mut buf[..to_read]);
        self.current_frame_remaining -= to_read as u32;
        Ok(to_read)
    }

    pub fn shutdown(&mut self) {
        if self.write_closed {
            return;
        }
        write_all(&mut self.pipe, DataFrameHeader::new(FrameType::Fin, 0).as_bytes());
        self.write_closed = true;
    }

    pub fn send_rst(&mut self) {
        if !self.write_closed {
            write_all(&mut self.pipe, DataFrameHeader::new(FrameType::Rst, 0).as_bytes());
            self.write_closed = true;
        }
        self.read_closed = true;
    }

    pub fn is_closed(&self) -> bool {
        self.write_closed && self.read_closed
    }

    /// True when both FINs have been exchanged and the other side has consumed
    /// all outgoing bytes, meaning shared memory is safe to free.
    pub fn ready_to_free(&self) -> bool {
        self.is_closed() && self.pipe.outgoing_bytes_remaining() == 0
    }
}

fn write_all<W: Write>(pipe: &mut W, mut src: &[u8]) {
    while !src.is_empty() {
        match pipe.write(src) {
            Ok(n) => src = &src[n..],
            Err(PipeError::WouldBlock) => {}
        }
    }
}

fn read_exact<R: Read>(pipe: &mut R, mut dst: &mut [u8]) {
    while !dst.is_empty() {
        match pipe.read(dst) {
            Ok(n) => dst = &mut dst[n..],
            Err(PipeError::WouldBlock) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arca_pipe::{BidirectionalPipe, SharedMemoryRegion, Side};

    #[repr(align(8))]
    struct Aligned<const N: usize>([u8; N]);

    macro_rules! stream_pair {
        ($ring:expr, $mem:ident, $a:ident, $b:ident) => {
            let mut $mem = Aligned([0u8; 2 * (16 + $ring)]);
            let region = unsafe {
                SharedMemoryRegion::from_raw($mem.0.as_mut_ptr(), $mem.0.len() as u64)
            };
            let pipe_a = BidirectionalPipe::new(&region, $ring, Side::A);
            let pipe_b = BidirectionalPipe::new(&region, $ring, Side::B);
            let mut $a = SyncStream::from_pipe(1, pipe_a);
            let mut $b = SyncStream::from_pipe(1, pipe_b);
        };
    }

    #[test]
    fn send_recv_data() {
        stream_pair!(128, mem, a, b);
        assert_eq!(a.send(b"hello").unwrap(), 5);
        let mut buf = [0u8; 5];
        assert_eq!(b.recv(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn shutdown_sends_fin_to_peer() {
        stream_pair!(64, mem, a, b);
        a.shutdown();
        let mut buf = [0u8; 8];
        assert_eq!(b.recv(&mut buf).unwrap(), 0);
        assert!(!b.is_closed());
    }

    #[test]
    fn recv_rst_closes_both_sides() {
        stream_pair!(64, mem, a, b);
        b.send_rst();
        assert!(b.is_closed());
        let mut buf = [0u8; 8];
        assert!(matches!(a.recv(&mut buf), Err(StreamError::ConnectionReset)));
        assert!(a.is_closed());
    }

    #[test]
    fn send_after_shutdown_errors() {
        stream_pair!(64, mem, a, _b);
        a.shutdown();
        assert!(matches!(a.send(b"x"), Err(StreamError::WriteClosed)));
    }

    #[test]
    fn recv_after_read_close_returns_zero() {
        stream_pair!(64, mem, a, b);
        a.shutdown();
        let mut buf = [0u8; 8];
        b.recv(&mut buf).unwrap(); // consumes FIN, sets read_closed
        assert_eq!(b.recv(&mut buf).unwrap(), 0); // read_closed → immediate zero
    }

    #[test]
    fn is_closed_requires_both_sides() {
        stream_pair!(128, mem, a, b);
        assert!(!a.is_closed());
        a.shutdown();
        assert!(!a.is_closed()); // write_closed but read still open
        let mut buf = [0u8; 8];
        b.recv(&mut buf).unwrap();
        b.shutdown();
        a.recv(&mut buf).unwrap();
        assert!(a.is_closed());
    }

    #[test]
    fn recv_partial_read() {
        stream_pair!(128, mem, a, b);
        assert_eq!(a.send(b"hello").unwrap(), 5);
        let mut buf = [0u8; 3];
        assert_eq!(b.recv(&mut buf).unwrap(), 3);
        assert_eq!(&buf, b"hel");
        let mut buf2 = [0u8; 3];
        assert_eq!(b.recv(&mut buf2).unwrap(), 2);
        assert_eq!(&buf2[..2], b"lo");
    }

    #[test]
    fn ready_to_free_after_both_fins_exchanged() {
        stream_pair!(128, mem, a, b);
        a.shutdown();
        let mut buf = [0u8; 8];
        b.recv(&mut buf).unwrap(); // B reads A's FIN, draining A's outgoing ring
        b.shutdown();
        a.recv(&mut buf).unwrap(); // A reads B's FIN
        assert!(a.ready_to_free());
    }
}
