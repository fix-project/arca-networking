//! Wire protocol for the **single control pipe** between Arca and the Linux monitor.
//!
//! Every control message is a [`ControlFrame`]: a fixed 7-byte header followed
//! by a small payload. The format is intentionally tiny and field-positional
//! so it's easy to read on the wire when debugging.
//!
//! ```text
//! offset  size  field
//! ------  ----  -----------------------------------------
//!  0      1     message_type (u8, see MessageType)
//!  1      2     payload_len  (u16 little-endian, bytes)
//!  3      4     request_id   (u32 little-endian)
//!  7      ..    payload      (payload_len bytes)
//! ```
//!
//! Payloads themselves are also fixed-layout little-endian structs. They
//! never contain pointers or string lengths — just numeric fields and IPv4
//! address octets — so an engineer staring at a hex dump can read them.
//!
//! The frame is the *only* thing that flows across the control pipe. Per-
//! connection bytestreams flow on **separate** shared-memory data pipes,
//! whose location is communicated to Arca via [`DataPipeInfo`] inside the
//! `ConnectOk` / `IncomingConnection` payloads.

use arca_pipe::BidirectionalPipe;

/// Maximum payload bytes after the 7-byte header.
///
/// Sized comfortably above today's largest payload (`ConnectionReady`, 20 B)
/// so we have headroom to add fields without bumping a version byte.
pub const MAX_FRAME_PAYLOAD: usize = 256;

/// Catalog of message kinds carried on the control pipe.
///
/// Keep this list **small** and add new variants only when there's a real
/// need. Each variant is a single byte on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Arca → Linux: please `bind`+`listen` on the given [`Endpoint`].
    ListenRequest = 1,
    /// Linux → Arca: listener is ready; payload is [`ListenerReady`].
    ListenOk = 2,
    /// Arca → Linux: please `connect` outbound to the given [`Endpoint`].
    ConnectRequest = 3,
    /// Linux → Arca: outbound connect succeeded; payload is [`ConnectionReady`].
    ConnectOk = 4,
    /// Linux → Arca: reply to [`MessageType::AcceptRequest`]; payload is
    /// [`ConnectionReady`] with `listener_id` set.
    IncomingConnection = 5,
    /// Linux → Arca: bind/listen failed; payload is [`ErrPayload`].
    ListenErr = 6,
    /// Linux → Arca: outbound connect failed; payload is [`ErrPayload`].
    ConnectErr = 7,
    /// Arca → Linux: block until the next inbound TCP connection on this
    /// listener; payload is [`AcceptListenerId`] (4 B).
    ///
    /// The matching reply is [`MessageType::IncomingConnection`] with the same
    /// `request_id`.
    AcceptRequest = 8,
}

impl MessageType {
    /// Reverse of `as u8`, returning `None` for unknown bytes so the codec
    /// can reject garbage instead of silently misinterpreting it.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::ListenRequest),
            2 => Some(Self::ListenOk),
            3 => Some(Self::ConnectRequest),
            4 => Some(Self::ConnectOk),
            5 => Some(Self::IncomingConnection),
            6 => Some(Self::ListenErr),
            7 => Some(Self::ConnectErr),
            8 => Some(Self::AcceptRequest),
            _ => None,
        }
    }
}

/// One control message in memory (header + inline payload buffer).
///
/// The `payload` array is fixed-size so `ControlFrame` is `Copy` and lives
/// happily in `no_std`. Only the first `payload_len` bytes are valid; use
/// [`ControlFrame::payload`] to get the live slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlFrame {
    pub message_type: MessageType,
    pub request_id: u32,
    pub payload_len: u16,
    pub payload: [u8; MAX_FRAME_PAYLOAD],
}

impl ControlFrame {
    /// Build a frame, copying `payload` into the inline buffer.
    ///
    /// Panics if `payload.len() > MAX_FRAME_PAYLOAD`. Callers always own the
    /// payload, so this is a programming bug, not a runtime input error.
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

    /// The valid prefix of `self.payload`.
    pub fn payload(&self) -> &[u8] {
        &self.payload[..self.payload_len as usize]
    }
}

/// IPv4 host + port. Six bytes on the wire: 4 octets then `port` LE.
///
/// IPv6 isn't modeled yet; when we need it we'll add a sibling type or a
/// length-prefixed address field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Endpoint {
    pub host: [u8; 4],
    pub port: u16,
}

impl Endpoint {
    pub fn new(host: [u8; 4], port: u16) -> Self {
        Self { host, port }
    }

    /// Encode into the start of `out`; returns bytes written (always 6).
    pub fn encode(&self, out: &mut [u8; MAX_FRAME_PAYLOAD]) -> usize {
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

/// Payload of [`MessageType::ListenOk`]. Just the listener handle.
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

/// Payload of [`MessageType::AcceptRequest`]: which listener to `accept` on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptListenerId {
    pub listener_id: u32,
}

impl AcceptListenerId {
    pub fn encode(&self, out: &mut [u8; 4]) {
        out.copy_from_slice(&self.listener_id.to_le_bytes());
    }

    pub fn decode(payload: &[u8]) -> Self {
        assert!(payload.len() == 4, "accept-listener payload must be 4 bytes");
        Self {
            listener_id: u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]),
        }
    }
}

/// Payload of [`MessageType::ListenErr`] / [`MessageType::ConnectErr`].
///
/// `code` is the Linux `errno` when available, else `1` for "unknown".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrPayload {
    pub code: u32,
}

impl ErrPayload {
    pub fn encode(&self, out: &mut [u8; 4]) {
        out.copy_from_slice(&self.code.to_le_bytes());
    }

    pub fn decode(payload: &[u8]) -> Self {
        assert!(payload.len() == 4, "err payload must be 4 bytes");
        Self {
            code: u32::from_le_bytes(payload[0..4].try_into().unwrap()),
        }
    }
}

/// How Arca finds the per-connection data pipe.
///
/// Layout on the wire (12 bytes): `pipe_id` (u32 LE) then `ring_size` (u64 LE).
/// Total shared-memory size for the `BidirectionalPipe` is derived from
/// `ring_size`; we don't ship a redundant `len` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataPipeInfo {
    /// Opaque handle agreed by both sides (e.g., index into a SHM table).
    pub pipe_id: u64,
    /// Per-direction ring capacity in bytes (same value passed to
    /// [`BidirectionalPipe::new`]).
    pub ring_size: u64,
}

impl DataPipeInfo {
    pub fn new(pipe_id: u64, ring_size: u64) -> Self {
        Self { pipe_id, ring_size }
    }

    /// Total shared-memory bytes the receiver must map for this pipe.
    pub fn shm_len(self) -> u64 {
        BidirectionalPipe::required_size(self.ring_size)
    }

    pub fn encode(&self, out: &mut [u8; 16]) {
        out[..8].copy_from_slice(&self.pipe_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.ring_size.to_le_bytes());
    }

    pub fn decode(payload: &[u8]) -> Self {
        assert!(payload.len() == 16, "data pipe info payload must be 16 bytes");
        Self {
            pipe_id: u64::from_le_bytes(payload[0..8].try_into().unwrap()),
            ring_size: u64::from_le_bytes(payload[8..16].try_into().unwrap()),
        }
    }
}

/// Payload of [`MessageType::ConnectOk`] (outbound), [`MessageType::IncomingConnection`]
/// (reply to [`MessageType::AcceptRequest`]). Same fields, same layout —
/// the message kind is what tells you which is which.
///
/// `listener_id == 0` means "outbound connection, no listener was involved".
/// Real listeners always get `listener_id >= 1` from the monitor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionReady {
    pub listener_id: u32,
    pub connection_id: u32,
    pub pipe: DataPipeInfo,
}

impl ConnectionReady {
    pub fn encode(&self, out: &mut [u8; 24]) {
        out[..4].copy_from_slice(&self.listener_id.to_le_bytes());
        out[4..8].copy_from_slice(&self.connection_id.to_le_bytes());
        let mut pipe_buf = [0u8; 16];
        self.pipe.encode(&mut pipe_buf);
        out[8..24].copy_from_slice(&pipe_buf);
    }

    pub fn decode(payload: &[u8]) -> Self {
        assert!(payload.len() == 24, "connection ready payload must be 24 bytes");
        Self {
            listener_id: u32::from_le_bytes(payload[0..4].try_into().unwrap()),
            connection_id: u32::from_le_bytes(payload[4..8].try_into().unwrap()),
            pipe: DataPipeInfo::decode(&payload[8..24]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_type_from_u8_round_trip() {
        for mt in [
            MessageType::ListenRequest,
            MessageType::ListenOk,
            MessageType::ConnectRequest,
            MessageType::ConnectOk,
            MessageType::IncomingConnection,
            MessageType::ListenErr,
            MessageType::ConnectErr,
            MessageType::AcceptRequest,
        ] {
            assert_eq!(MessageType::from_u8(mt as u8), Some(mt));
        }
    }

    #[test]
    fn message_type_from_u8_unknown() {
        assert_eq!(MessageType::from_u8(0), None);
        assert_eq!(MessageType::from_u8(9), None);
        assert_eq!(MessageType::from_u8(255), None);
    }

    #[test]
    fn err_payload_round_trip() {
        let e = ErrPayload { code: 42 };
        let mut b = [0u8; 4];
        e.encode(&mut b);
        assert_eq!(ErrPayload::decode(&b), e);
    }

    #[test]
    fn listener_ready_round_trip() {
        let lr = ListenerReady { listener_id: 12345 };
        let mut buf = [0u8; 4];
        lr.encode(&mut buf);
        assert_eq!(ListenerReady::decode(&buf), lr);
    }

    #[test]
    fn accept_listener_id_round_trip() {
        let a = AcceptListenerId { listener_id: 0x00ab_cd01 };
        let mut buf = [0u8; 4];
        a.encode(&mut buf);
        assert_eq!(AcceptListenerId::decode(&buf), a);
    }

    #[test]
    fn data_pipe_info_round_trip() {
        let info = DataPipeInfo::new(99, 1024);
        let mut buf = [0u8; 16];
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
        let mut buf = [0u8; 24];
        ready.encode(&mut buf);
        assert_eq!(ConnectionReady::decode(&buf), ready);
    }
}
