/// Owns a reference to a shared memory region.
///
/// Instead of exposing raw pointers and `unsafe` at every call site, we wrap
/// the shared memory region in a type that guarantees validity — pushing the
/// `unsafe` into a single place. After constructing a `SharedMemoryRegion`,
/// all pipe construction and usage is safe.
///
/// How the shared memory pointer is obtained (hypervisor mapping, POSIX shm,
/// etc.) is outside this type's scope — we assume both sides have a way to
/// get it.
pub struct SharedMemoryRegion {
    ptr: *mut u8,
    len: u64,
}

impl SharedMemoryRegion {
    /// Create a new shared memory region from a raw pointer.
    ///
    /// This is the one and only unsafe entry point for the pipe library.
    ///
    /// # Safety
    /// - `ptr` must point to a valid, read-write memory region of at least `len` bytes.
    /// - The memory must remain valid for the lifetime of this `SharedMemoryRegion`.
    /// - The memory must be shared between both sides of the pipe (e.g. via
    ///   hypervisor page mapping or POSIX shared memory).
    /// - The memory must be zero-initialized before the first pipe is created from it.
    pub unsafe fn from_raw(_ptr: *mut u8, _len: u64) -> Self {
        todo!()
    }

    /// Returns a raw pointer to the start of the shared memory region.
    pub fn as_ptr(&self) -> *mut u8 {
        todo!()
    }

    /// Returns the length of the shared memory region in bytes.
    pub fn len(&self) -> u64 {
        todo!()
    }

    /// Returns true if the shared memory region has zero length.
    pub fn is_empty(&self) -> bool {
        todo!()
    }
}
