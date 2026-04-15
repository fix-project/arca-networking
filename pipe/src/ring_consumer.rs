use crate::error::PipeError;
use crate::ring::{RingData, RingHeader};
use crate::traits;
use core::sync::atomic::Ordering;

/// Consumer (read) end of a single SPSC ring buffer.
pub struct RingConsumer<'a> {
    header: &'a RingHeader,
    data: RingData,
}

impl<'a> RingConsumer<'a> {
    pub fn new(header: &'a RingHeader, data: RingData) -> Self {
        Self { header, data }
    }
}

impl<'a> traits::Read for RingConsumer<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, PipeError> {
        let used = self.header.used_space();
        if used == 0 {
            return Err(PipeError::WouldBlock);
        }

        let n = core::cmp::min(buf.len() as u64, used) as usize;
        let cursor = self.header.read_cursor.load(Ordering::Relaxed);
        self.data.read_at(cursor, &mut buf[..n]);
        self.header.read_cursor.store(cursor + n as u64, Ordering::Release);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Read;
    use core::sync::atomic::AtomicU64;

    fn header(read: u64, write: u64) -> RingHeader {
        RingHeader {
            read_cursor: AtomicU64::new(read),
            write_cursor: AtomicU64::new(write),
        }
    }

    #[test]
    fn simple_read() {
        let mut mem = *b"hello...";
        let h = header(0, 5);
        let data = unsafe { RingData::new(mem.as_mut_ptr(), 8) };
        let mut c = RingConsumer::new(&h, data);
        let mut out = [0u8; 8];
        assert_eq!(c.read(&mut out).unwrap(), 5);
        assert_eq!(&out[..5], b"hello");
        assert_eq!(h.read_cursor.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn partial_read() {
        let mut mem = *b"abcdefgh";
        let h = header(0, 8);
        let data = unsafe { RingData::new(mem.as_mut_ptr(), 8) };
        let mut c = RingConsumer::new(&h, data);
        let mut out = [0u8; 3];
        assert_eq!(c.read(&mut out).unwrap(), 3);
        assert_eq!(&out, b"abc");
    }

    #[test]
    fn wrap_around() {
        let mut mem = *b"WXYZabcd";
        let h = header(5, 12);
        let data = unsafe { RingData::new(mem.as_mut_ptr(), 8) };
        let mut c = RingConsumer::new(&h, data);
        let mut out = [0u8; 8];
        assert_eq!(c.read(&mut out).unwrap(), 7);
        assert_eq!(&out[..7], b"bcdWXYZ");
    }

    #[test]
    fn empty_ring_blocks() {
        let mut mem = [0u8; 4];
        let h = header(4, 4);
        let data = unsafe { RingData::new(mem.as_mut_ptr(), 4) };
        let mut c = RingConsumer::new(&h, data);
        let mut out = [0u8; 4];
        assert!(matches!(c.read(&mut out), Err(PipeError::WouldBlock)));
    }
}
