use crate::error::PipeError;
use crate::ring_consumer::RingConsumer;
use crate::ring_producer::RingProducer;
use crate::shared_memory_region::SharedMemoryRegion;
use crate::traits;

/// Which side of the pipe this endpoint represents.
///
/// Side A writes to Ring A->B and reads from Ring B->A.
/// Side B writes to Ring B->A and reads from Ring A->B.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    A,
    B,
}

/// One endpoint of a bidirectional pipe.
///
/// Owns the producer half of one ring and the consumer half of the other.
/// Implements both `Read` (via the consumer) and `Write` (via the producer).
///
/// ## Memory layout
/// [HeaderA (16 bytes)] [Ring A->B data (ring_size)] [HeaderB (16 bytes)] [Ring B->A data (ring_size)]
pub struct BidirectionalPipe<'a> {
    writer: RingProducer<'a>,
    reader: RingConsumer<'a>,
}

impl<'a> BidirectionalPipe<'a> {
    /// Create a pipe endpoint from a shared memory region.
    ///
    /// One side must zero-initialize the memory before either side creates a pipe.
    /// Exactly one `Side::A` and one `Side::B` should be created for a given region.
    pub fn new(_region: &'a SharedMemoryRegion, _ring_size: u64, _side: Side) -> Self {
        todo!()
    }

    /// Total bytes of shared memory needed for a given ring_size.
    pub const fn required_size(_ring_size: u64) -> u64 {
        todo!()
    }

    /// Split into independent read and write halves.
    ///
    /// This allows reading and writing concurrently without needing
    /// `&mut self` for both operations — similar to `TcpStream::split()`.
    pub fn split(&mut self) -> (&mut RingConsumer<'a>, &mut RingProducer<'a>) {
        (&mut self.reader, &mut self.writer)
    }
}

impl<'a> traits::Read for BidirectionalPipe<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, PipeError> {
        self.reader.read(buf)
    }
}

impl<'a> traits::Write for BidirectionalPipe<'a> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, PipeError> {
        self.writer.write(buf)
    }
}
