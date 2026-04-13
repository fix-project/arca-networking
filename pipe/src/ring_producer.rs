use crate::error::PipeError;
use crate::ring_header::RingHeader;
use crate::traits;
use core::sync::atomic::Ordering;

/// Producer (write) end of a single SPSC ring buffer.
pub struct RingProducer<'a> {
    header: &'a RingHeader,
    data: *mut u8,
    ring_size: u64,
}

impl<'a> RingProducer<'a> {
    /// # Safety
    /// - `data` must point to a valid, writable region of `ring_size` bytes.
    /// - The memory must remain valid for `'a`.
    /// - At most one `RingProducer` per ring.
    pub unsafe fn new(header: &'a RingHeader, data: *mut u8, ring_size: u64) -> Self {
        Self { header, data, ring_size }
    }
}

impl<'a> traits::Write for RingProducer<'a> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, PipeError> {
        let free = self.header.free_space(self.ring_size);
        if free == 0 {
            return Err(PipeError::WouldBlock);
        }

        let n = core::cmp::min(buf.len() as u64, free) as usize;
        let write_cursor = self.header.write_cursor.load(Ordering::Relaxed);
        let offset = (write_cursor % self.ring_size) as usize;
        let ring_size = self.ring_size as usize;

        let first = core::cmp::min(n, ring_size - offset);
        unsafe {
            core::ptr::copy_nonoverlapping(buf.as_ptr(), self.data.add(offset), first);
            if n > first {
                core::ptr::copy_nonoverlapping(buf.as_ptr().add(first), self.data, n - first);
            }
        }

        self.header.write_cursor.store(write_cursor + n as u64, Ordering::Release);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Write;
    use core::sync::atomic::AtomicU64;

    fn header() -> RingHeader {
        RingHeader {
            read_cursor: AtomicU64::new(0),
            write_cursor: AtomicU64::new(0),
        }
    }

    #[test]
    fn simple_write() {
        let h = header();
        let mut data = [0u8; 16];
        let mut p = unsafe { RingProducer::new(&h, data.as_mut_ptr(), 16) };
        assert_eq!(p.write(b"hello").unwrap(), 5);
        assert_eq!(&data[..5], b"hello");
        assert_eq!(h.write_cursor.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn fill_to_full() {
        let h = header();
        let mut data = [0u8; 8];
        let mut p = unsafe { RingProducer::new(&h, data.as_mut_ptr(), 8) };
        assert_eq!(p.write(b"abcdefghij").unwrap(), 8);
        assert_eq!(&data, b"abcdefgh");
    }

    #[test]
    fn wrap_around() {
        let h = header();
        h.read_cursor.store(5, Ordering::Relaxed);
        h.write_cursor.store(5, Ordering::Relaxed);
        let mut data = [0u8; 8];
        let mut p = unsafe { RingProducer::new(&h, data.as_mut_ptr(), 8) };
        assert_eq!(p.write(b"XYZW").unwrap(), 4);
        assert_eq!(&data[5..8], b"XYZ");
        assert_eq!(&data[..1], b"W");
    }

    #[test]
    fn full_ring_blocks() {
        let h = header();
        h.write_cursor.store(4, Ordering::Relaxed);
        let mut data = [0u8; 4];
        let mut p = unsafe { RingProducer::new(&h, data.as_mut_ptr(), 4) };
        assert!(matches!(p.write(b"x"), Err(PipeError::WouldBlock)));
    }
}
