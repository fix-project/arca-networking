use crate::error::PipeError;
use crate::ring_header::RingHeader;
use crate::traits;
use core::sync::atomic::Ordering;

/// Consumer (read) end of a single SPSC ring buffer.
pub struct RingConsumer<'a> {
    header: &'a RingHeader,
    data: *const u8,
    ring_size: u64,
}

impl<'a> RingConsumer<'a> {
    /// # Safety
    /// - `data` must point to a valid, readable region of `ring_size` bytes.
    /// - The memory must remain valid for `'a`.
    /// - At most one `RingConsumer` per ring.
    pub unsafe fn new(header: &'a RingHeader, data: *const u8, ring_size: u64) -> Self {
        Self { header, data, ring_size }
    }
}

impl<'a> traits::Read for RingConsumer<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, PipeError> {
        let used = self.header.used_space();
        if used == 0 {
            return Err(PipeError::WouldBlock);
        }

        let n = core::cmp::min(buf.len() as u64, used) as usize;
        let read_cursor = self.header.read_cursor.load(Ordering::Relaxed);
        let offset = (read_cursor % self.ring_size) as usize;
        let ring_size = self.ring_size as usize;

        let first = core::cmp::min(n, ring_size - offset);
        unsafe {
            core::ptr::copy_nonoverlapping(self.data.add(offset), buf.as_mut_ptr(), first);
            if n > first {
                core::ptr::copy_nonoverlapping(self.data, buf.as_mut_ptr().add(first), n - first);
            }
        }

        self.header.read_cursor.store(read_cursor + n as u64, Ordering::Release);
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
        let data = *b"hello...";
        let h = header(0, 5);
        let mut c = unsafe { RingConsumer::new(&h, data.as_ptr(), 8) };
        let mut out = [0u8; 8];
        assert_eq!(c.read(&mut out).unwrap(), 5);
        assert_eq!(&out[..5], b"hello");
        assert_eq!(h.read_cursor.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn partial_read() {
        let data = *b"abcdefgh";
        let h = header(0, 8);
        let mut c = unsafe { RingConsumer::new(&h, data.as_ptr(), 8) };
        let mut out = [0u8; 3];
        assert_eq!(c.read(&mut out).unwrap(), 3);
        assert_eq!(&out, b"abc");
    }

    #[test]
    fn wrap_around() {
        let data = *b"WXYZabcd";
        let h = header(5, 12);
        let mut c = unsafe { RingConsumer::new(&h, data.as_ptr(), 8) };
        let mut out = [0u8; 8];
        assert_eq!(c.read(&mut out).unwrap(), 7);
        assert_eq!(&out[..7], b"bcdWXYZ");
    }

    #[test]
    fn empty_ring_blocks() {
        let data = [0u8; 4];
        let h = header(4, 4);
        let mut c = unsafe { RingConsumer::new(&h, data.as_ptr(), 4) };
        let mut out = [0u8; 4];
        assert!(matches!(c.read(&mut out), Err(PipeError::WouldBlock)));
    }
}
