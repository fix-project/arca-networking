use core::sync::atomic::AtomicU64;

/// Header for a single SPSC ring buffer, stored in shared memory.
///
/// Both cursors are monotonically increasing logical offsets — they never wrap.
/// Physical positions are derived via modulo: `physical_pos = cursor % ring_size`.
///
/// This avoids the classic "is the ring full or empty?" ambiguity:
/// - Available data (for reader): `write_cursor - read_cursor`
/// - Free space (for writer):     `ring_size - (write_cursor - read_cursor)`
#[repr(C)]
pub struct RingHeader {
    /// Total bytes consumed by the reader. Monotonically increasing.
    pub read_cursor: AtomicU64,
    /// Total bytes produced by the writer. Monotonically increasing.
    pub write_cursor: AtomicU64,
}
