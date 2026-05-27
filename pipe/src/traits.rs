use crate::error::PipeError;

/// Read bytes from a pipe. Analogous to std::io::Read.
///
/// Partial reads are normal — `read` may return fewer bytes than `buf.len()`.
/// The caller loops if it needs more. This matches `std::io` semantics.
pub trait Read {
    /// Try to read bytes into `buf`.
    ///
    /// Returns `Ok(n)` where `n > 0` is the number of bytes read,
    /// or `Err(WouldBlock)` if no data is currently available.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, PipeError>;
}

/// Write bytes to a pipe. Analogous to std::io::Write.
///
/// Partial writes are normal — `write` may accept fewer bytes than `buf.len()`.
/// The caller loops if it needs to write more. This matches `std::io` semantics.
pub trait Write {
    /// Try to write bytes from `buf`.
    ///
    /// Returns `Ok(n)` where `n > 0` is the number of bytes written,
    /// or `Err(WouldBlock)` if the ring is currently full.
    fn write(&mut self, buf: &[u8]) -> Result<usize, PipeError>;
}
