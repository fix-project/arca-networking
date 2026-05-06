//! Arca-side wrappers over the control pipe.
//!
//! Designed to *feel* like `std::net` for callers:
//!
//! ```ignore
//! let listener = session.bind(Endpoint::new([0, 0, 0, 0], 8080))?;
//! let stream   = session.connect(Endpoint::new([8, 8, 8, 8], 443))?;
//! let inbound  = session.accept(&listener)?;
//! ```
//!
//! The objects we hand back ([`ArcaTcpListener`], [`ArcaTcpStream`]) are
//! lightweight handles — just IDs and a [`DataPipeInfo`]. They deliberately
//! don't implement `Read`/`Write` themselves; the per-connection bytestream
//! is wired up by the data-pipe layer (the rings live in their own SHM
//! region, separate from the control pipe).
//!
//! `no_std` constraints: the event stash is a fixed-size inline array. Sized
//! to 8 deep, which is plenty for "single listener, occasionally request
//! something while a connection arrives" workloads. Overflow returns an
//! error instead of silently dropping events.

use arca_pipe::{Read, Write};

use crate::{
    read_frame, write_frame, CodecError, ConnectionReady, ControlFrame, DataPipeInfo, Endpoint,
    ErrPayload, ListenerReady, MessageType, MAX_FRAME_PAYLOAD,
};

/// Depth of the inline queue for `IncomingConnection` events that arrive
/// while we're waiting for a request reply.
const PENDING_INCOMING_CAPACITY: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArcaError {
    Codec(CodecError),
    /// Linux returned `ListenErr` with the given errno-like code.
    ListenFailed { code: u32 },
    /// Linux returned `ConnectErr` with the given errno-like code.
    ConnectFailed { code: u32 },
    /// Got a frame we weren't expecting — protocol bug or out-of-sync state.
    UnexpectedReply(MessageType),
    /// Reply came back with a request_id we didn't issue (or already consumed).
    UnexpectedRequestId { expected: u32, got: u32 },
    /// Saw too many `IncomingConnection` events stack up before any `accept`
    /// drained them. Increase `PENDING_INCOMING_CAPACITY` or call `accept`
    /// more often.
    PendingIncomingOverflow,
}

impl From<CodecError> for ArcaError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

/// Handle to a listener Linux is holding open for us. POD on purpose —
/// hand it to `accept` to wait for new connections on this listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArcaTcpListener {
    listener_id: u32,
}

impl ArcaTcpListener {
    pub fn id(&self) -> u32 {
        self.listener_id
    }
}

/// Handle to one accepted/connected TCP session.
///
/// `listener_id == 0` means "outbound connection, no listener". The actual
/// per-direction bytestream is in the data pipe described by `pipe`; this
/// struct is just the metadata Arca needs to attach to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArcaTcpStream {
    listener_id: u32,
    connection_id: u32,
    pipe: DataPipeInfo,
}

impl ArcaTcpStream {
    pub fn connection_id(&self) -> u32 {
        self.connection_id
    }

    /// `0` for outbound `connect`, otherwise the listener that produced this
    /// stream via `accept`.
    pub fn listener_id(&self) -> u32 {
        self.listener_id
    }

    /// Where the per-connection data pipe lives in shared memory.
    pub fn pipe(&self) -> DataPipeInfo {
        self.pipe
    }

    pub fn is_inbound(&self) -> bool {
        self.listener_id != 0
    }
}

/// Owner of the **single** control pipe on the Arca side.
///
/// Maintains:
/// - the next `request_id` to assign,
/// - a small queue of `IncomingConnection` events that arrived while we were
///   waiting on the reply to a different request.
pub struct ArcaSession<'a, T: Read + Write> {
    transport: &'a mut T,
    next_request_id: u32,
    pending_incoming: [Option<ConnectionReady>; PENDING_INCOMING_CAPACITY],
    pending_count: usize,
}

impl<'a, T: Read + Write> ArcaSession<'a, T> {
    pub fn new(transport: &'a mut T) -> Self {
        Self {
            transport,
            next_request_id: 1,
            pending_incoming: [None; PENDING_INCOMING_CAPACITY],
            pending_count: 0,
        }
    }

    fn alloc_request_id(&mut self) -> u32 {
        let id = self.next_request_id;
        // wrapping_add is fine: `0` is reserved for "unsolicited" in the
        // monitor's IncomingConnection events, but we never check `req_id`
        // on those, so even if we wrap into `0` for a real request the
        // correlation logic uses the message_type+id pair.
        self.next_request_id = self.next_request_id.wrapping_add(1);
        id
    }

    /// Ask Linux to bind+listen on `ep`. Returns a handle on success.
    pub fn bind(&mut self, ep: Endpoint) -> Result<ArcaTcpListener, ArcaError> {
        let rid = self.alloc_request_id();
        let mut pl = [0u8; MAX_FRAME_PAYLOAD];
        let n = ep.encode(&mut pl);
        write_frame(
            self.transport,
            &ControlFrame::new(MessageType::ListenRequest, rid, &pl[..n]),
        )?;

        let reply = self.read_reply_for(rid)?;
        match reply.message_type {
            MessageType::ListenOk => Ok(ArcaTcpListener {
                listener_id: ListenerReady::decode(reply.payload()).listener_id,
            }),
            MessageType::ListenErr => Err(ArcaError::ListenFailed {
                code: ErrPayload::decode(reply.payload()).code,
            }),
            other => Err(ArcaError::UnexpectedReply(other)),
        }
    }

    /// Ask Linux to connect outbound to `ep`. Returns a handle on success.
    pub fn connect(&mut self, ep: Endpoint) -> Result<ArcaTcpStream, ArcaError> {
        let rid = self.alloc_request_id();
        let mut pl = [0u8; MAX_FRAME_PAYLOAD];
        let n = ep.encode(&mut pl);
        write_frame(
            self.transport,
            &ControlFrame::new(MessageType::ConnectRequest, rid, &pl[..n]),
        )?;

        let reply = self.read_reply_for(rid)?;
        match reply.message_type {
            MessageType::ConnectOk => {
                let ready = ConnectionReady::decode(reply.payload());
                Ok(ArcaTcpStream {
                    listener_id: ready.listener_id,
                    connection_id: ready.connection_id,
                    pipe: ready.pipe,
                })
            }
            MessageType::ConnectErr => Err(ArcaError::ConnectFailed {
                code: ErrPayload::decode(reply.payload()).code,
            }),
            other => Err(ArcaError::UnexpectedReply(other)),
        }
    }

    /// Block until the next `IncomingConnection` for `listener` arrives.
    ///
    /// Events for *other* listeners that show up first are stashed and
    /// returned by future `accept` calls for those listeners.
    pub fn accept(&mut self, listener: &ArcaTcpListener) -> Result<ArcaTcpStream, ArcaError> {
        if let Some(ready) = self.take_pending_for(listener.listener_id) {
            return Ok(stream_from_ready(ready));
        }

        loop {
            let frame = read_frame(self.transport)?;
            match frame.message_type {
                MessageType::IncomingConnection => {
                    let ready = ConnectionReady::decode(frame.payload());
                    if ready.listener_id == listener.listener_id {
                        return Ok(stream_from_ready(ready));
                    }
                    self.stash_incoming(ready)?;
                }
                other => return Err(ArcaError::UnexpectedReply(other)),
            }
        }
    }

    /// Read the reply for `expected_rid`, stashing any `IncomingConnection`
    /// frames that arrive in the meantime so they can be returned via
    /// `accept` later.
    fn read_reply_for(&mut self, expected_rid: u32) -> Result<ControlFrame, ArcaError> {
        loop {
            let frame = read_frame(self.transport)?;
            match frame.message_type {
                MessageType::IncomingConnection => {
                    let ready = ConnectionReady::decode(frame.payload());
                    self.stash_incoming(ready)?;
                }
                MessageType::ListenOk
                | MessageType::ListenErr
                | MessageType::ConnectOk
                | MessageType::ConnectErr => {
                    if frame.request_id != expected_rid {
                        return Err(ArcaError::UnexpectedRequestId {
                            expected: expected_rid,
                            got: frame.request_id,
                        });
                    }
                    return Ok(frame);
                }
                other => return Err(ArcaError::UnexpectedReply(other)),
            }
        }
    }

    fn stash_incoming(&mut self, ready: ConnectionReady) -> Result<(), ArcaError> {
        if self.pending_count >= PENDING_INCOMING_CAPACITY {
            return Err(ArcaError::PendingIncomingOverflow);
        }
        // Find first empty slot. Linear because the stash is tiny (8).
        for slot in self.pending_incoming.iter_mut() {
            if slot.is_none() {
                *slot = Some(ready);
                self.pending_count += 1;
                return Ok(());
            }
        }
        // Unreachable given pending_count <= CAPACITY, but be safe:
        Err(ArcaError::PendingIncomingOverflow)
    }

    fn take_pending_for(&mut self, listener_id: u32) -> Option<ConnectionReady> {
        for slot in self.pending_incoming.iter_mut() {
            if let Some(ready) = slot {
                if ready.listener_id == listener_id {
                    let taken = *ready;
                    *slot = None;
                    self.pending_count -= 1;
                    return Some(taken);
                }
            }
        }
        None
    }
}

fn stream_from_ready(ready: ConnectionReady) -> ArcaTcpStream {
    ArcaTcpStream {
        listener_id: ready.listener_id,
        connection_id: ready.connection_id,
        pipe: ready.pipe,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataPipeInfo, MessageType};
    use arca_pipe::PipeError;

    /// In-memory transport for tests: writes append, reads pop from front.
    struct MemTransport {
        outbox: [u8; 1024],
        outbox_len: usize,
        inbox: [u8; 1024],
        inbox_len: usize,
        inbox_pos: usize,
    }

    impl MemTransport {
        fn new() -> Self {
            Self {
                outbox: [0u8; 1024],
                outbox_len: 0,
                inbox: [0u8; 1024],
                inbox_len: 0,
                inbox_pos: 0,
            }
        }

        fn push_inbound(&mut self, frame: &ControlFrame) {
            // Encode the frame into the inbox via a tiny adapter writer.
            struct InboxWriter<'a>(&'a mut MemTransport);
            impl Write for InboxWriter<'_> {
                fn write(&mut self, src: &[u8]) -> Result<usize, PipeError> {
                    let end = self.0.inbox_len + src.len();
                    assert!(end <= self.0.inbox.len(), "inbox overflow");
                    self.0.inbox[self.0.inbox_len..end].copy_from_slice(src);
                    self.0.inbox_len = end;
                    Ok(src.len())
                }
            }
            let mut w = InboxWriter(self);
            write_frame(&mut w, frame).unwrap();
        }

        fn outbox_slice(&self) -> &[u8] {
            &self.outbox[..self.outbox_len]
        }
    }

    impl Write for MemTransport {
        fn write(&mut self, src: &[u8]) -> Result<usize, PipeError> {
            let end = self.outbox_len + src.len();
            assert!(end <= self.outbox.len(), "outbox overflow");
            self.outbox[self.outbox_len..end].copy_from_slice(src);
            self.outbox_len = end;
            Ok(src.len())
        }
    }

    impl Read for MemTransport {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, PipeError> {
            if self.inbox_pos >= self.inbox_len {
                return Err(PipeError::WouldBlock);
            }
            let n = core::cmp::min(buf.len(), self.inbox_len - self.inbox_pos);
            buf[..n].copy_from_slice(&self.inbox[self.inbox_pos..self.inbox_pos + n]);
            self.inbox_pos += n;
            Ok(n)
        }
    }

    fn listen_ok(rid: u32, listener_id: u32) -> ControlFrame {
        let mut pl = [0u8; 4];
        ListenerReady { listener_id }.encode(&mut pl);
        ControlFrame::new(MessageType::ListenOk, rid, &pl)
    }

    fn connect_ok(rid: u32, ready: ConnectionReady) -> ControlFrame {
        let mut pl = [0u8; 20];
        ready.encode(&mut pl);
        ControlFrame::new(MessageType::ConnectOk, rid, &pl)
    }

    fn incoming(ready: ConnectionReady) -> ControlFrame {
        let mut pl = [0u8; 20];
        ready.encode(&mut pl);
        ControlFrame::new(MessageType::IncomingConnection, 0, &pl)
    }

    #[test]
    fn bind_returns_listener_handle() {
        let mut t = MemTransport::new();
        // Pre-populate the reply Linux will send back.
        t.push_inbound(&listen_ok(1, 7));

        let listener = {
            let mut s = ArcaSession::new(&mut t);
            s.bind(Endpoint::new([127, 0, 0, 1], 8080)).unwrap()
        };
        assert_eq!(listener.id(), 7);

        // The session should have written one ListenRequest frame.
        let mut reader = SliceReader {
            data: t.outbox_slice(),
            pos: 0,
        };
        let req = read_frame(&mut reader).unwrap();
        assert_eq!(req.message_type, MessageType::ListenRequest);
        assert_eq!(req.request_id, 1);
    }

    #[test]
    fn connect_returns_stream_handle() {
        let ready = ConnectionReady {
            listener_id: 0,
            connection_id: 17,
            pipe: DataPipeInfo::new(17, 256),
        };
        let mut t = MemTransport::new();
        t.push_inbound(&connect_ok(1, ready));

        let stream = {
            let mut s = ArcaSession::new(&mut t);
            s.connect(Endpoint::new([8, 8, 8, 8], 443)).unwrap()
        };
        assert_eq!(stream.connection_id(), 17);
        assert!(!stream.is_inbound());
        assert_eq!(stream.pipe(), DataPipeInfo::new(17, 256));
    }

    #[test]
    fn accept_pulls_from_stash_when_event_arrived_during_other_request() {
        let listener_ready = ConnectionReady {
            listener_id: 5,
            connection_id: 99,
            pipe: DataPipeInfo::new(99, 64),
        };
        let outbound_ready = ConnectionReady {
            listener_id: 0,
            connection_id: 100,
            pipe: DataPipeInfo::new(100, 64),
        };

        let mut t = MemTransport::new();
        // Linux sends an IncomingConnection event *first*, then the ConnectOk
        // for our outbound request. ArcaSession should stash the event,
        // deliver the reply, then return the event from accept().
        t.push_inbound(&incoming(listener_ready));
        t.push_inbound(&connect_ok(1, outbound_ready));

        let mut s = ArcaSession::new(&mut t);
        let connected = s.connect(Endpoint::new([1, 1, 1, 1], 80)).unwrap();
        assert_eq!(connected.connection_id(), 100);

        // The IncomingConnection should now be retrievable via accept().
        let listener = ArcaTcpListener { listener_id: 5 };
        let inbound = s.accept(&listener).unwrap();
        assert_eq!(inbound.connection_id(), 99);
        assert!(inbound.is_inbound());
    }

    #[test]
    fn accept_skips_events_for_other_listeners_into_stash() {
        let other = ConnectionReady {
            listener_id: 7,
            connection_id: 1,
            pipe: DataPipeInfo::new(1, 64),
        };
        let ours = ConnectionReady {
            listener_id: 5,
            connection_id: 2,
            pipe: DataPipeInfo::new(2, 64),
        };
        let mut t = MemTransport::new();
        t.push_inbound(&incoming(other));
        t.push_inbound(&incoming(ours));

        let mut s = ArcaSession::new(&mut t);
        let listener = ArcaTcpListener { listener_id: 5 };
        let inbound = s.accept(&listener).unwrap();
        assert_eq!(inbound.connection_id(), 2);

        // The first event is still pending for listener 7.
        let other_listener = ArcaTcpListener { listener_id: 7 };
        let other_inbound = s.accept(&other_listener).unwrap();
        assert_eq!(other_inbound.connection_id(), 1);
    }

    #[test]
    fn listen_failure_propagates_errno() {
        let mut t = MemTransport::new();
        let mut pl = [0u8; 4];
        ErrPayload { code: 98 }.encode(&mut pl);
        t.push_inbound(&ControlFrame::new(MessageType::ListenErr, 1, &pl));

        let mut s = ArcaSession::new(&mut t);
        let err = s.bind(Endpoint::new([0, 0, 0, 0], 1)).unwrap_err();
        assert_eq!(err, ArcaError::ListenFailed { code: 98 });
    }

    #[test]
    fn pending_incoming_overflow_is_reported() {
        let mut t = MemTransport::new();
        // Fill the inbox with 9 events for a different listener; on connect
        // we'll try to stash all 9 and overflow on the 9th.
        for cid in 1..=9 {
            t.push_inbound(&incoming(ConnectionReady {
                listener_id: 7,
                connection_id: cid,
                pipe: DataPipeInfo::new(cid, 64),
            }));
        }
        // Final reply is a ConnectOk so the loop would otherwise exit
        // cleanly, but overflow should fire first.
        t.push_inbound(&connect_ok(
            1,
            ConnectionReady {
                listener_id: 0,
                connection_id: 100,
                pipe: DataPipeInfo::new(100, 64),
            },
        ));

        let mut s = ArcaSession::new(&mut t);
        let err = s.connect(Endpoint::new([1, 2, 3, 4], 80)).unwrap_err();
        assert_eq!(err, ArcaError::PendingIncomingOverflow);
    }

    struct SliceReader<'a> {
        data: &'a [u8],
        pos: usize,
    }

    impl<'a> Read for SliceReader<'a> {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, PipeError> {
            if self.pos >= self.data.len() {
                return Err(PipeError::WouldBlock);
            }
            let n = core::cmp::min(buf.len(), self.data.len() - self.pos);
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }
}
