use crate::error::PipeError;
use crate::ring::{RingData, RingHeader};
use crate::ring_consumer::RingConsumer;
use crate::ring_producer::RingProducer;
use crate::shared_memory_region::SharedMemoryRegion;
use crate::traits;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    A,
    B,
}

/// One endpoint of a bidirectional pipe.
///
/// Memory layout: `[HeaderA][Ring A->B data][HeaderB][Ring B->A data]`.
pub struct BidirectionalPipe<'a> {
    writer: RingProducer<'a>,
    reader: RingConsumer<'a>,
}

const HEADER_SIZE: u64 = core::mem::size_of::<RingHeader>() as u64;

impl<'a> BidirectionalPipe<'a> {
    /// Total bytes of shared memory needed for a given `ring_size`.
    pub const fn required_size(ring_size: u64) -> u64 {
        2 * (HEADER_SIZE + ring_size)
    }

    /// Create a pipe endpoint over a shared memory region.
    ///
    /// Caller must ensure the region is zero-initialized before the first side
    /// is constructed, and that exactly one `Side::A` and one `Side::B` are
    /// created per region.
    pub fn new(region: &'a SharedMemoryRegion, ring_size: u64, side: Side) -> Self {
        assert!(region.len() >= Self::required_size(ring_size));
        let base = region.as_ptr();

        let header_a = unsafe { &*(base as *const RingHeader) };
        let data_a = unsafe { base.add(HEADER_SIZE as usize) };
        let header_b = unsafe { &*(data_a.add(ring_size as usize) as *const RingHeader) };
        let data_b = unsafe { data_a.add(ring_size as usize + HEADER_SIZE as usize) };

        let (writer_header, writer_data, reader_header, reader_data) = match side {
            Side::A => (header_a, data_a, header_b, data_b),
            Side::B => (header_b, data_b, header_a, data_a),
        };

        let writer = RingProducer::new(writer_header, unsafe {
            RingData::new(writer_data, ring_size)
        });
        let reader = RingConsumer::new(reader_header, unsafe {
            RingData::new(reader_data, ring_size)
        });
        Self { writer, reader }
    }

    /// Split into independent read and write halves (like `TcpStream::split`).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{Read, Write};

    #[test]
    fn required_size_matches_layout() {
        let size = BidirectionalPipe::required_size(64);
        assert_eq!(size, 2 * (16 + 64));
    }

    #[test]
    fn round_trip_a_to_b() {
        const RING: u64 = 64;
        let size = BidirectionalPipe::required_size(RING) as usize;
        let mut mem = [0u8; 2 * (16 + 64)];
        assert_eq!(mem.len(), size);

        let region = unsafe { SharedMemoryRegion::from_raw(mem.as_mut_ptr(), size as u64) };
        let mut a = BidirectionalPipe::new(&region, RING, Side::A);
        let mut b = BidirectionalPipe::new(&region, RING, Side::B);

        assert_eq!(a.write(b"ping").unwrap(), 4);
        let mut out = [0u8; 4];
        assert_eq!(b.read(&mut out).unwrap(), 4);
        assert_eq!(&out, b"ping");
    }

    #[test]
    fn round_trip_b_to_a() {
        const RING: u64 = 32;
        let size = BidirectionalPipe::required_size(RING) as usize;
        let mut mem = [0u8; 2 * (16 + 32)];
        let region = unsafe { SharedMemoryRegion::from_raw(mem.as_mut_ptr(), size as u64) };
        let mut a = BidirectionalPipe::new(&region, RING, Side::A);
        let mut b = BidirectionalPipe::new(&region, RING, Side::B);

        assert_eq!(b.write(b"pong!!").unwrap(), 6);
        let mut out = [0u8; 6];
        assert_eq!(a.read(&mut out).unwrap(), 6);
        assert_eq!(&out, b"pong!!");
    }

    #[test]
    fn both_directions_independent() {
        const RING: u64 = 32;
        let size = BidirectionalPipe::required_size(RING) as usize;
        let mut mem = [0u8; 2 * (16 + 32)];
        let region = unsafe { SharedMemoryRegion::from_raw(mem.as_mut_ptr(), size as u64) };
        let mut a = BidirectionalPipe::new(&region, RING, Side::A);
        let mut b = BidirectionalPipe::new(&region, RING, Side::B);

        a.write(b"hello").unwrap();
        b.write(b"world").unwrap();

        let mut from_a = [0u8; 5];
        let mut from_b = [0u8; 5];
        b.read(&mut from_a).unwrap();
        a.read(&mut from_b).unwrap();
        assert_eq!(&from_a, b"hello");
        assert_eq!(&from_b, b"world");
    }
}
