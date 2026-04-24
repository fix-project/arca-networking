use arca_pipe::{BidirectionalPipe, PipeError};
use arca_pipe::Read as PipeRead;
use arca_pipe::Write as PipeWrite;
use crate::dataframe::{DataFrameHeader, FrameType};

#[derive(Debug)]
pub enum StreamError {
    WriteClosed,
    ConnectionReset,
}

pub struct AsyncStream<'a> {
    pub conn_id: u32,
    pipe: BidirectionalPipe<'a>,
    write_closed: bool,
    read_closed: bool,
}

impl<'a> AsyncStream<'a> {
    pub fn from_pipe(conn_id: u32, pipe: BidirectionalPipe<'a>) -> Self {
        Self { conn_id, pipe, write_closed: false, read_closed: false }
    }

    pub async fn send(&mut self, buf: &[u8]) -> Result<usize, StreamError> {
        if self.write_closed {
            return Err(StreamError::WriteClosed);
        }
        let header = DataFrameHeader::new(FrameType::Data, buf.len() as u32);
        write_all(&mut self.pipe, header.as_bytes()).await;
        write_all(&mut self.pipe, buf).await;
        Ok(buf.len())
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, StreamError> {
        if self.read_closed {
            return Ok(0);
        }
        let mut header_bytes = [0u8; core::mem::size_of::<DataFrameHeader>()];
        read_exact(&mut self.pipe, &mut header_bytes).await;
        let frame_type = FrameType::from_u32(u32::from_le_bytes(header_bytes[0..4].try_into().unwrap()));
        let payload_len = u32::from_le_bytes(header_bytes[4..8].try_into().unwrap());

        match frame_type {
            None => {
                self.read_closed = true;
                write_all(&mut self.pipe, DataFrameHeader::new(FrameType::Rst, 0).as_bytes()).await;
                self.write_closed = true;
                Err(StreamError::ConnectionReset)
            }
            Some(FrameType::Data) => {
                let len = payload_len as usize;
                read_exact(&mut self.pipe, &mut buf[..len]).await;
                Ok(len)
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

    pub async fn shutdown(&mut self) -> Result<(), StreamError> {
        if self.write_closed {
            return Ok(());
        }
        write_all(&mut self.pipe, DataFrameHeader::new(FrameType::Fin, 0).as_bytes()).await;
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

async fn write_all<W: PipeWrite>(pipe: &mut W, buf: &[u8]) {
    let mut remaining = buf;
    while !remaining.is_empty() {
        match pipe.write(remaining) {
            Ok(n) => remaining = &remaining[n..],
            Err(PipeError::WouldBlock) => yield_now().await,
        }
    }
}

async fn read_exact<R: PipeRead>(pipe: &mut R, buf: &mut [u8]) {
    let mut filled = 0;
    while filled < buf.len() {
        match pipe.read(&mut buf[filled..]) {
            Ok(n) => filled += n,
            Err(PipeError::WouldBlock) => yield_now().await,
        }
    }
}

async fn yield_now() {
    let mut yielded = false;
    core::future::poll_fn(|cx| {
        if yielded { return core::task::Poll::Ready(()); }
        yielded = true;
        cx.waker().wake_by_ref();
        core::task::Poll::Pending
    }).await
}
