use crate::error::PipeError;
use crate::ring_header::RingHeader;
use crate::traits;

/// Consumer (read) end of a single SPSC ring buffer.
///
/// Reads bytes from the ring and advances `read_cursor`.
/// Uses `Acquire` ordering on `write_cursor` loads to pair with
/// the producer's `Release` store — ensuring written bytes are visible.
///
/// Stores to `read_cursor` use `Relaxed` ordering — a stale read by the
/// producer only causes it to conservatively see less free space, which is
/// still correct.
pub struct RingConsumer<'a> {
    header: &'a RingHeader,
    data: *const u8,
    ring_size: u64,
}

impl<'a> RingConsumer<'a> {
    /// Create a new consumer from a ring header and data region.
    ///
    /// # Safety
    /// - `data` must point to a valid, readable region of `ring_size` bytes.
    /// - The memory must remain valid for the lifetime `'a`.
    /// - There must be at most one `RingConsumer` for a given ring.
    pub unsafe fn new(header: &'a RingHeader, data: *const u8, ring_size: u64) -> Self {
        Self {
            header,
            data,
            ring_size,
        }
    }
}

impl<'a> traits::Read for RingConsumer<'a> {
    fn read(&mut self, _buf: &mut [u8]) -> Result<usize, PipeError> {
        // TODO: Implement read logic
        //
        // 1. Load write_cursor (Acquire) and read_cursor (Acquire)
        // 2. Calculate available data: write_cursor - read_cursor
        // 3. If available == 0, return Err(WouldBlock)
        // 4. Calculate how many bytes to read: min(buf.len(), available)
        // 5. Calculate physical read position: read_cursor % ring_size
        // 6. Handle wrap-around with two copy_nonoverlapping calls if needed
        // 7. Release fence, then store new read_cursor with Relaxed ordering
        // 8. Return Ok(bytes_read)
        todo!()
    }
}
