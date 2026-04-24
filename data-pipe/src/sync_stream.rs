use arca_pipe::{BidirectionalPipe, PipeError, Read, Write};
use crate::dataframe::{DataFrameHeader, FrameType};

#[derive(Debug)]
pub enum StreamError {
    WriteClosed,
    ConnectionReset,
}

pub struct SyncStream<'a> {
    pub conn_id: u32,
    pipe: BidirectionalPipe<'a>,
    write_closed: bool,
    read_closed: bool,
}

impl<'a> SyncStream<'a> {
    pub fn from_pipe(conn_id: u32, pipe: BidirectionalPipe<'a>) -> Self {
        Self { conn_id, pipe, write_closed: false, read_closed: false }
    }

    pub fn send(&mut self, buf: &[u8]) -> Result<usize, StreamError> {
        if self.write_closed {
            return Err(StreamError::WriteClosed);
        }
        write_all(&mut self.pipe, DataFrameHeader::new(FrameType::Data, buf.len() as u32).as_bytes());
        write_all(&mut self.pipe, buf);
        Ok(buf.len())
    }

    pub fn recv(&mut self, buf: &mut [u8]) -> Result<usize, StreamError> {
        if self.read_closed {
            return Ok(0);
        }
        let mut header_bytes = [0u8; core::mem::size_of::<DataFrameHeader>()];
        read_exact(&mut self.pipe, &mut header_bytes);
        let frame_type = FrameType::from_u32(u32::from_le_bytes(header_bytes[0..4].try_into().unwrap()));
        let payload_len = u32::from_le_bytes(header_bytes[4..8].try_into().unwrap());

        match frame_type {
            None => {
                self.read_closed = true;
                write_all(&mut self.pipe, DataFrameHeader::new(FrameType::Rst, 0).as_bytes());
                self.write_closed = true;
                Err(StreamError::ConnectionReset)
            }
            Some(FrameType::Data) => {
                read_exact(&mut self.pipe, &mut buf[..payload_len as usize]);
                Ok(payload_len as usize)
            }
            Some(FrameType::Fin) => {
                self.read_closed = true;
                Ok(0)
            }
            Some(FrameType::Rst) => {
                self.read_closed = true;
                self.write_closed = true;
                Err(StreamError::ConnectionReset)
            }
        }
    }

    pub fn shutdown(&mut self) -> Result<(), StreamError> {
        if self.write_closed {
            return Ok(());
        }
        write_all(&mut self.pipe, DataFrameHeader::new(FrameType::Fin, 0).as_bytes());
        self.write_closed = true;
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.write_closed && self.read_closed
    }

    /// True when both FINs have been exchanged and the other side has consumed
    /// all outgoing bytes, meaning shared memory is safe to free.
    pub fn ready_to_free(&self) -> bool {
        self.is_closed() && self.pipe.outgoing_bytes_remaining() == 0
    }
}

fn write_all<W: Write>(pipe: &mut W, mut src: &[u8]) {
    while !src.is_empty() {
        match pipe.write(src) {
            Ok(n) => src = &src[n..],
            Err(PipeError::WouldBlock) => {}
        }
    }
}

fn read_exact<R: Read>(pipe: &mut R, mut dst: &mut [u8]) {
    while !dst.is_empty() {
        match pipe.read(dst) {
            Ok(n) => dst = &mut dst[n..],
            Err(PipeError::WouldBlock) => {}
        }
    }
}
