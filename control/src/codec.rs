//! Read and write [`ControlFrame`] values on an `arca-pipe` byte stream.
//!
//! Header layout matches [`crate::protocol`].

use arca_pipe::{PipeError, Read, Write};

use crate::{ControlFrame, MessageType, MAX_FRAME_PAYLOAD};

/// `message_type` (1) + `payload_len` (2) + `request_id` (4).
pub const HEADER_LEN: usize = 7;

pub fn write_frame<T: Write>(transport: &mut T, frame: &ControlFrame) {
    let mut header = [0u8; HEADER_LEN];
    header[0] = frame.message_type as u8;
    header[1..3].copy_from_slice(&frame.payload_len.to_le_bytes());
    header[3..7].copy_from_slice(&frame.request_id.to_le_bytes());

    write_all(transport, &header);
    write_all(transport, frame.payload());
}

pub fn read_frame<T: Read>(transport: &mut T) -> ControlFrame {
    let mut header = [0u8; HEADER_LEN];
    read_exact(transport, &mut header);

    let message_type = MessageType::from_u8(header[0]).expect("unknown message type");
    let payload_len = u16::from_le_bytes([header[1], header[2]]) as usize;
    assert!(payload_len <= MAX_FRAME_PAYLOAD, "payload_len too large");

    let request_id = u32::from_le_bytes([header[3], header[4], header[5], header[6]]);

    let mut frame = ControlFrame {
        message_type,
        request_id,
        payload_len: payload_len as u16,
        payload: [0u8; MAX_FRAME_PAYLOAD],
    };
    read_exact(transport, &mut frame.payload[..payload_len]);
    frame
}

fn write_all<T: Write>(transport: &mut T, mut src: &[u8]) {
    while !src.is_empty() {
        match transport.write(src) {
            Ok(0) => panic!("write returned 0"),
            Ok(n) => src = &src[n..],
            Err(PipeError::WouldBlock) => {}
        }
    }
}

fn read_exact<T: Read>(transport: &mut T, mut dst: &mut [u8]) {
    while !dst.is_empty() {
        match transport.read(dst) {
            Ok(0) => panic!("read returned 0"),
            Ok(n) => dst = &mut dst[n..],
            Err(PipeError::WouldBlock) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConnectionReady, DataPipeInfo, Endpoint, ListenerReady, MessageType, MAX_FRAME_PAYLOAD,
    };

    /// In-memory bytes for tests: `write` appends, `read` consumes from the front.
    struct MemPipe<const N: usize> {
        buf: [u8; N],
        len: usize,
        pos: usize,
    }

    impl<const N: usize> MemPipe<N> {
        fn new() -> Self {
            Self {
                buf: [0u8; N],
                len: 0,
                pos: 0,
            }
        }
    }

    impl<const N: usize> Write for MemPipe<N> {
        fn write(&mut self, src: &[u8]) -> Result<usize, PipeError> {
            let n = src.len();
            assert!(self.len + n <= N, "MemPipe full");
            self.buf[self.len..self.len + n].copy_from_slice(src);
            self.len += n;
            Ok(n)
        }
    }

    impl<const N: usize> Read for MemPipe<N> {
        fn read(&mut self, dst: &mut [u8]) -> Result<usize, PipeError> {
            if self.pos >= self.len {
                return Err(PipeError::WouldBlock);
            }
            let n = core::cmp::min(dst.len(), self.len - self.pos);
            dst[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    #[test]
    fn write_read_round_trip_empty_payload() {
        let mut pipe = MemPipe::<256>::new();
        let frame = ControlFrame::new(MessageType::ListenRequest, 7, &[]);
        write_frame(&mut pipe, &frame);

        let got = read_frame(&mut pipe);
        assert_eq!(got.message_type, MessageType::ListenRequest);
        assert_eq!(got.request_id, 7);
        assert_eq!(got.payload_len, 0);
    }

    #[test]
    fn write_read_round_trip_endpoint_payload() {
        let ep = Endpoint::new([127, 0, 0, 1], 8080);
        let mut pl = [0u8; MAX_FRAME_PAYLOAD];
        let n = ep.encode(&mut pl);
        let frame = ControlFrame::new(MessageType::ConnectRequest, 99, &pl[..n]);

        let mut pipe = MemPipe::<256>::new();
        write_frame(&mut pipe, &frame);
        let got = read_frame(&mut pipe);

        assert_eq!(got.message_type, MessageType::ConnectRequest);
        assert_eq!(got.request_id, 99);
        let back = Endpoint::decode(got.payload());
        assert_eq!(back, ep);
    }

    #[test]
    fn write_read_round_trip_listener_ready_payload() {
        let lr = ListenerReady {
            listener_id: 0xdead_beef,
        };
        let mut pl = [0u8; 4];
        lr.encode(&mut pl);
        let frame = ControlFrame::new(MessageType::ListenOk, 1, &pl);

        let mut pipe = MemPipe::<128>::new();
        write_frame(&mut pipe, &frame);
        let got = read_frame(&mut pipe);
        assert_eq!(got.message_type, MessageType::ListenOk);
        assert_eq!(ListenerReady::decode(got.payload()).listener_id, lr.listener_id);
    }

    #[test]
    fn write_read_round_trip_connection_ready_payload() {
        let ready = ConnectionReady {
            listener_id: 0,
            connection_id: 42,
            pipe: DataPipeInfo::new(7, 128),
        };
        let mut pl = [0u8; 20];
        ready.encode(&mut pl);
        let frame = ControlFrame::new(MessageType::ConnectOk, 100, &pl);

        let mut pipe = MemPipe::<256>::new();
        write_frame(&mut pipe, &frame);
        let got = read_frame(&mut pipe);
        assert_eq!(ConnectionReady::decode(got.payload()), ready);
    }

    #[test]
    fn write_read_two_frames_in_order() {
        let mut pipe = MemPipe::<1024>::new();
        let a = ControlFrame::new(MessageType::ListenRequest, 1, &[1, 2, 3]);
        let b = ControlFrame::new(MessageType::ConnectRequest, 2, &[]);
        write_frame(&mut pipe, &a);
        write_frame(&mut pipe, &b);

        let got_a = read_frame(&mut pipe);
        assert_eq!(got_a.request_id, 1);
        assert_eq!(got_a.payload(), &[1, 2, 3]);

        let got_b = read_frame(&mut pipe);
        assert_eq!(got_b.message_type, MessageType::ConnectRequest);
        assert_eq!(got_b.request_id, 2);
        assert_eq!(got_b.payload_len, 0);
    }

    #[test]
    fn write_read_max_sized_payload() {
        let payload: [u8; MAX_FRAME_PAYLOAD] = core::array::from_fn(|i| i as u8);
        let frame = ControlFrame::new(MessageType::IncomingConnection, 0x1234_5678, &payload);

        let mut pipe = MemPipe::<{ HEADER_LEN + MAX_FRAME_PAYLOAD + 32 }>::new();
        write_frame(&mut pipe, &frame);
        let got = read_frame(&mut pipe);
        assert_eq!(got.payload_len as usize, MAX_FRAME_PAYLOAD);
        assert_eq!(got.payload(), payload.as_slice());
    }
}
