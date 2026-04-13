use core::sync::atomic::{AtomicU64, Ordering};

/// Header for a single SPSC ring buffer, stored in shared memory.
///
/// Cursors are monotonically increasing logical offsets. 
/// Physical positions are `cursor % ring_size`.
#[repr(C)]
pub struct RingHeader {
    pub read_cursor: AtomicU64,
    pub write_cursor: AtomicU64,
}

impl RingHeader {
    /// Bytes available to read. Called by the consumer.
    pub fn used_space(&self) -> u64 {
        let write = self.write_cursor.load(Ordering::Acquire);
        let read = self.read_cursor.load(Ordering::Relaxed);
        write.wrapping_sub(read)
    }

    /// Bytes available to write. Called by the producer.
    pub fn free_space(&self, capacity: u64) -> u64 {
        let write = self.write_cursor.load(Ordering::Relaxed);
        let read = self.read_cursor.load(Ordering::Acquire);
        capacity - write.wrapping_sub(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(read: u64, write: u64) -> RingHeader {
        RingHeader {
            read_cursor: AtomicU64::new(read),
            write_cursor: AtomicU64::new(write),
        }
    }

    #[test]
    fn empty_ring() {
        let h = header(0, 0);
        assert_eq!(h.used_space(), 0);
        assert_eq!(h.free_space(64), 64);
    }

    #[test]
    fn partial_fill() {
        let h = header(10, 40);
        assert_eq!(h.used_space(), 30);
        assert_eq!(h.free_space(64), 34);
    }

    #[test]
    fn full_ring() {
        let h = header(100, 164);
        assert_eq!(h.used_space(), 64);
        assert_eq!(h.free_space(64), 0);
    }
}
