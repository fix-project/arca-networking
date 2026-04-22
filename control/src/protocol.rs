//! Protocol types and payload encodings for control messages.
//!
//! Single-byte message kinds plus small fixed payload structs.
use arca_pipe::BidirectionalPipe;

pub const MAX_FRAME_PAYLOAD: usize = 256;

/// Minimal set of message kinds for current scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    ListenRequest = 1,
    ListenOk = 2,
    ConnectRequest = 3,
    ConnectOk = 4,
    IncomingConnection = 5,
}

impl MessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::ListenRequest),
            2 => Some(Self::ListenOk),
            3 => Some(Self::ConnectRequest),
            4 => Some(Self::ConnectOk),
            5 => Some(Self::IncomingConnection),
            _ => None,
        }
    }
}

/// Wire frame:
/// - byte 0: message type
/// - bytes 1..3: payload length (u16 LE)
/// - bytes 3..7: request_id (u32 LE)
/// - bytes 7..: payload bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlFrame {
    pub message_type: MessageType,
    pub request_id: u32,
    pub payload_len: u16,
    pub payload: [u8; MAX_FRAME_PAYLOAD],
}

impl ControlFrame {
    pub fn new(message_type: MessageType, request_id: u32, payload: &[u8]) -> Self {
        assert!(payload.len() <= MAX_FRAME_PAYLOAD, "payload too large");

        let mut out = Self {
            message_type,
            request_id,
            payload_len: payload.len() as u16,
            payload: [0u8; MAX_FRAME_PAYLOAD],
        };
        out.payload[..payload.len()].copy_from_slice(payload);
        out
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload[..self.payload_len as usize]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Endpoint {
    /// IPv4 address as 4 octets
    pub host: [u8; 4],
    pub port: u16,
}

impl Endpoint {
    pub fn new(host: [u8; 4], port: u16) -> Self {
        Self { host, port }
    }

    pub fn encode(&self, out: &mut [u8; MAX_FRAME_PAYLOAD]) -> usize {
        // Payload layout: [ipv4: 4 bytes][port: u16 LE]
        out[..4].copy_from_slice(&self.host);
        out[4..6].copy_from_slice(&self.port.to_le_bytes());
        6
    }

    pub fn decode(payload: &[u8]) -> Self {
        assert!(payload.len() == 6, "endpoint payload must be 6 bytes");
        let host = [payload[0], payload[1], payload[2], payload[3]];
        let port = u16::from_le_bytes([payload[4], payload[5]]);
        Endpoint::new(host, port)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenerReady {
    pub listener_id: u32,
}

impl ListenerReady {
    pub fn encode(&self, out: &mut [u8; 4]) {
        out.copy_from_slice(&self.listener_id.to_le_bytes());
    }

    pub fn decode(payload: &[u8]) -> Self {
        assert!(payload.len() == 4, "listener-ready payload must be 4 bytes");
        Self {
            listener_id: u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]),
        }
    }
}

/// Pipe metadata needed by Arca to attach to a newly created data pipe.
///
/// On the wire we only send `pipe_id` and `ring_size`. Total shared memory
/// size matches [`BidirectionalPipe::required_size`] — derive it instead of
/// duplicating another `u64` in the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataPipeInfo {
    /// Opaque pipe id/handle understood by both sides.
    pub pipe_id: u32,
    /// Ring capacity in bytes for each direction (A->B and B->A), same as `BidirectionalPipe::new`.
    pub ring_size: u64,
}

impl DataPipeInfo {
    pub fn new(pipe_id: u32, ring_size: u64) -> Self {
        Self { pipe_id, ring_size }
    }

    pub fn shm_len(self) -> u64 {
        BidirectionalPipe::required_size(self.ring_size)
    }

    /// Fixed layout: `pipe_id` (4) then `ring_size` (8), little-endian.
    pub fn encode(&self, out: &mut [u8; 12]) {
        out[..4].copy_from_slice(&self.pipe_id.to_le_bytes());
        out[4..12].copy_from_slice(&self.ring_size.to_le_bytes());
    }

    pub fn decode(payload: &[u8]) -> Self {
        assert!(payload.len() == 12, "data pipe info payload must be 12 bytes");
        Self {
            pipe_id: u32::from_le_bytes(payload[0..4].try_into().unwrap()),
            ring_size: u64::from_le_bytes(payload[4..12].try_into().unwrap()),
        }
    }
}

/// Unified payload for "connection established and data pipe ready".
///
/// Use this for:
/// - `ConnectOk` (outbound connection succeeded)
/// - `IncomingConnection` (inbound connection accepted)
///
/// `listener_id` is the listener that accepted the connection, or `0` when the
/// connection is outbound and there is no listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionReady {
    pub listener_id: u32,
    pub connection_id: u32,
    pub pipe: DataPipeInfo,
}

impl ConnectionReady {
    pub fn encode(&self, out: &mut [u8; 20]) {
        out[..4].copy_from_slice(&self.listener_id.to_le_bytes());
        out[4..8].copy_from_slice(&self.connection_id.to_le_bytes());
        let mut pipe_buf = [0u8; 12];
        self.pipe.encode(&mut pipe_buf);
        out[8..20].copy_from_slice(&pipe_buf);
    }

    pub fn decode(payload: &[u8]) -> Self {
        assert!(payload.len() == 20, "connection ready payload must be 20 bytes");
        Self {
            listener_id: u32::from_le_bytes(payload[0..4].try_into().unwrap()),
            connection_id: u32::from_le_bytes(payload[4..8].try_into().unwrap()),
            pipe: DataPipeInfo::decode(&payload[8..20]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_type_from_u8_round_trip() {
        assert_eq!(MessageType::from_u8(MessageType::ListenRequest as u8), Some(MessageType::ListenRequest));
        assert_eq!(MessageType::from_u8(MessageType::ListenOk as u8), Some(MessageType::ListenOk));
        assert_eq!(MessageType::from_u8(MessageType::ConnectRequest as u8), Some(MessageType::ConnectRequest));
        assert_eq!(MessageType::from_u8(MessageType::ConnectOk as u8), Some(MessageType::ConnectOk));
        assert_eq!(MessageType::from_u8(MessageType::IncomingConnection as u8), Some(MessageType::IncomingConnection));
    }

    #[test]
    fn message_type_from_u8_unknown() {
        assert_eq!(MessageType::from_u8(0), None);
        assert_eq!(MessageType::from_u8(6), None);
    }

    #[test]
    fn listener_ready_round_trip() {
        let lr = ListenerReady { listener_id: 12345 };
        let mut buf = [0u8; 4];
        lr.encode(&mut buf);
        assert_eq!(ListenerReady::decode(&buf), lr);
    }

    #[test]
    fn data_pipe_info_round_trip() {
        let info = DataPipeInfo::new(99, 1024);
        let mut buf = [0u8; 12];
        info.encode(&mut buf);
        assert_eq!(DataPipeInfo::decode(&buf), info);
    }

    #[test]
    fn data_pipe_info_shm_len_matches_bidirectional_pipe() {
        let ring_size = 64u64;
        let info = DataPipeInfo::new(1, ring_size);
        assert_eq!(info.shm_len(), BidirectionalPipe::required_size(ring_size));
    }

    #[test]
    fn control_frame_payload_slice() {
        let frame = ControlFrame::new(MessageType::ListenRequest, 0, &[10, 20]);
        assert_eq!(frame.payload(), &[10, 20]);
    }

    #[test]
    fn endpoint_round_trip() {
        let ep = Endpoint::new([192, 168, 1, 10], 443);
        let mut buf = [0u8; MAX_FRAME_PAYLOAD];
        let n = ep.encode(&mut buf);
        assert_eq!(n, 6);
        assert_eq!(Endpoint::decode(&buf[..n]), ep);
    }

    #[test]
    fn connection_ready_round_trip() {
        let ready = ConnectionReady {
            listener_id: 3,
            connection_id: 9,
            pipe: DataPipeInfo::new(100, 64),
        };
        let mut buf = [0u8; 20];
        ready.encode(&mut buf);
        assert_eq!(ConnectionReady::decode(&buf), ready);
    }
}
