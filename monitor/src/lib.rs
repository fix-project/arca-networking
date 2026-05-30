//! Linux-side **monitor**: holds the kernel sockets and translates Arca's
//! control-protocol requests into real `TcpListener` / `TcpStream` actions.
//!
//! Architecture in one paragraph:
//!
//! - The monitor owns one [`TcpListener`] per `ListenerReady` it has handed
//!   out, and one [`TcpStream`] per live connection.
//! - Listeners are kept **non-blocking** so [`Monitor::poll_accepts`] can probe
//!   the kernel when Arca has queued an accept wait, without wedging the I/O
//!   thread when no TCP peer is ready yet.
//! - Connection streams are left in their default mode — the *byte pump*
//!   (the data-pipe layer) decides blocking vs non-blocking based on its own
//!   scheduling needs.
//!
//! Driving the protocol is one of:
//! - [`Monitor::dispatch_request`] — `Listen` / `Connect` only (for tests and
//!   custom drivers); [`MessageType::AcceptRequest`] is handled in
//!   [`Monitor::pump_once`] / [`Monitor::serve_one`].
//! - [`Monitor::pump_once`] — non-blocking kernel accepts + try read every
//!   fully received Arca→Linux frame on `transport`.
//! - [`Monitor::serve_one`] — spins until a full Arca→Linux frame exists, then
//!   dispatches it (uses [`std::thread::yield_now`] while waiting).

mod relay;

pub use relay::{pipe_to_tcp, tcp_to_pipe};

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};

use arca_control::{
    write_frame, AcceptListenerId, CodecError, ConnectionReady, ControlFrame, DataPipeInfo,
    Endpoint, ErrPayload, FrameReadBuf, ListenerReady, MessageType,
};
use arca_pipe::{Read, Write};

#[derive(Debug)]
pub enum MonitorError {
    Io(io::Error),
    Codec(CodecError),
    UnexpectedRequest(MessageType),
    /// Accept referenced an unknown `listener_id`.
    UnknownListener(u32),
}

impl From<io::Error> for MonitorError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<CodecError> for MonitorError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

fn io_err_code(e: &io::Error) -> u32 {
    // Linux `errno` when available, else `1` to mean "unknown error".
    e.raw_os_error().map(|x| x as u32).unwrap_or(1)
}

/// Linux-side state machine.
pub struct Monitor {
    next_listener_id: u32,
    next_connection_id: u32,
    default_ring_size: u64,
    listeners: HashMap<u32, TcpListener>,
    connections: HashMap<u32, TcpStream>,
    /// For each listener, FIFO of Arca `request_id`s waiting for a kernel `accept`.
    pending_accepts: HashMap<u32, VecDeque<u32>>,
    control_rx: FrameReadBuf,
}

impl Monitor {
    pub fn new(default_ring_size: u64) -> Self {
        Self {
            // 0 is reserved as "no listener" sentinel for outbound connects.
            next_listener_id: 1,
            next_connection_id: 1,
            default_ring_size,
            listeners: HashMap::new(),
            connections: HashMap::new(),
            pending_accepts: HashMap::new(),
            control_rx: FrameReadBuf::new(),
        }
    }

    /// Translate one Arca → Linux request frame into the reply we owe Arca.
    ///
    /// Handles only [`MessageType::ListenRequest`] and
    /// [`MessageType::ConnectRequest`]. [`MessageType::AcceptRequest`] is
    /// handled in [`Monitor::handle_control_frame`].
    pub fn dispatch_request(&mut self, frame: ControlFrame) -> Result<ControlFrame, MonitorError> {
        let rid = frame.request_id;
        match frame.message_type {
            MessageType::ListenRequest => {
                let ep = Endpoint::decode(frame.payload());
                let addr = SocketAddr::from((Ipv4Addr::from(ep.host), ep.port));
                match TcpListener::bind(addr) {
                    Ok(listener) => {
                        listener.set_nonblocking(true)?;
                        let id = self.alloc_listener_id();
                        self.listeners.insert(id, listener);
                        let mut pl = [0u8; 4];
                        ListenerReady { listener_id: id }.encode(&mut pl);
                        Ok(ControlFrame::new(MessageType::ListenOk, rid, &pl))
                    }
                    Err(e) => Ok(err_frame(MessageType::ListenErr, rid, io_err_code(&e))),
                }
            }
            MessageType::ConnectRequest => {
                let ep = Endpoint::decode(frame.payload());
                let addr = SocketAddr::from((Ipv4Addr::from(ep.host), ep.port));
                // Blocks until the kernel handshake completes — “connect waits”.
                match TcpStream::connect(addr) {
                    Ok(stream) => {
                        let id = self.alloc_connection_id();
                        self.connections.insert(id, stream);
                        Ok(ready_frame(
                            MessageType::ConnectOk,
                            rid,
                            ConnectionReady {
                                listener_id: 0,
                                connection_id: id,
                                // TODO: allocate real SHM region of size
                                // BidirectionalPipe::required_size(self.default_ring_size)
                                // and pass the returned handle here instead of `id as u64`.
                                pipe: DataPipeInfo::new(id as u64, self.default_ring_size),
                            },
                        ))
                    }
                    Err(e) => Ok(err_frame(MessageType::ConnectErr, rid, io_err_code(&e))),
                }
            }
            MessageType::AcceptRequest => Err(MonitorError::UnexpectedRequest(
                MessageType::AcceptRequest,
            )),
            other => Err(MonitorError::UnexpectedRequest(other)),
        }
    }

    /// Try pairing pending Arca `AcceptRequest`s with kernel `accept` results,
    /// writing one [`MessageType::IncomingConnection`] per successful accept
    /// (each carrying the Arca-issued `request_id`).
    pub fn poll_accepts<T: Write>(&mut self, transport: &mut T) -> Result<usize, MonitorError> {
        use std::io::ErrorKind;
        let mut written = 0usize;
        let lids: Vec<u32> = self
            .pending_accepts
            .iter()
            .filter(|(_, q)| !q.is_empty())
            .map(|(k, _)| *k)
            .collect();
        for lid in lids {
            let Some(listener) = self.listeners.get_mut(&lid) else {
                continue;
            };
            match listener.accept() {
                Ok((stream, _)) => {
                    let Some(rid) = self
                        .pending_accepts
                        .get_mut(&lid)
                        .and_then(|q| q.pop_front())
                    else {
                        continue;
                    };
                    let cid = self.alloc_connection_id();
                    self.connections.insert(cid, stream);
                    let fr = ready_frame(
                        MessageType::IncomingConnection,
                        rid,
                        ConnectionReady {
                            listener_id: lid,
                            connection_id: cid,
                            // TODO: allocate real SHM region of size
                            // BidirectionalPipe::required_size(self.default_ring_size)
                            // and pass the returned handle here instead of `cid as u64`.
                            pipe: DataPipeInfo::new(cid as u64, self.default_ring_size),
                        },
                    );
                    write_frame(transport, &fr)?;
                    written += 1;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(_) => {}
            }
        }
        Ok(written)
    }

    /// Non-blocking progress: poll kernel accepts, then read and handle every
    /// fully received Arca→Linux frame currently available on `transport`.
    pub fn pump_once<T: Read + Write>(&mut self, transport: &mut T) -> Result<(), MonitorError> {
        self.poll_accepts(transport)?;
        while let Some(frame) = self.control_rx.try_read_frame(transport)? {
            self.handle_control_frame(transport, frame)?;
        }
        Ok(())
    }

    fn handle_control_frame<T: Write>(
        &mut self,
        transport: &mut T,
        frame: ControlFrame,
    ) -> Result<(), MonitorError> {
        match frame.message_type {
            MessageType::AcceptRequest => {
                let lid = AcceptListenerId::decode(frame.payload()).listener_id;
                if !self.listeners.contains_key(&lid) {
                    return Err(MonitorError::UnknownListener(lid));
                }
                self.pending_accepts
                    .entry(lid)
                    .or_default()
                    .push_back(frame.request_id);
                self.poll_accepts(transport)?;
                Ok(())
            }
            _ => {
                let reply = self.dispatch_request(frame)?;
                write_frame(transport, &reply)?;
                self.poll_accepts(transport)?;
                Ok(())
            }
        }
    }

    /// Read and dispatch one Arca→Linux frame. Spins with
    /// [`std::thread::yield_now`] until the incremental decoder can produce a
    /// full frame (transport keeps returning [`arca_pipe::PipeError::WouldBlock`]).
    pub fn serve_one<T: Read + Write>(&mut self, transport: &mut T) -> Result<(), MonitorError> {
        loop {
            self.poll_accepts(transport)?;
            if let Some(frame) = self.control_rx.try_read_frame(transport)? {
                return self.handle_control_frame(transport, frame);
            }
            std::thread::yield_now();
        }
    }

    /// Borrow a live connection's `TcpStream` for the byte pump.
    pub fn connection(&mut self, id: u32) -> Option<&mut TcpStream> {
        self.connections.get_mut(&id)
    }

    /// Borrow a live listener (mostly for tests / introspection).
    pub fn listener(&self, id: u32) -> Option<&TcpListener> {
        self.listeners.get(&id)
    }

    fn alloc_listener_id(&mut self) -> u32 {
        let id = self.next_listener_id;
        self.next_listener_id = self.next_listener_id.wrapping_add(1);
        if self.next_listener_id == 0 {
            self.next_listener_id = 1;
        }
        id
    }

    fn alloc_connection_id(&mut self) -> u32 {
        let id = self.next_connection_id;
        self.next_connection_id = self.next_connection_id.wrapping_add(1);
        if self.next_connection_id == 0 {
            self.next_connection_id = 1;
        }
        id
    }
}

#[cfg(test)]
impl Monitor {
    pub(crate) fn test_enqueue_accept(&mut self, listener_id: u32, rid: u32) {
        self.pending_accepts
            .entry(listener_id)
            .or_default()
            .push_back(rid);
    }
}

fn err_frame(kind: MessageType, rid: u32, code: u32) -> ControlFrame {
    let mut pl = [0u8; 4];
    ErrPayload { code }.encode(&mut pl);
    ControlFrame::new(kind, rid, &pl)
}

fn ready_frame(kind: MessageType, rid: u32, ready: ConnectionReady) -> ControlFrame {
    let mut pl = [0u8; 24];
    ready.encode(&mut pl);
    ControlFrame::new(kind, rid, &pl)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arca_control::read_frame;
    use arca_pipe::{PipeError, Read as PipeRead, Write as PipeWrite};
    use std::io::{Read as IoRead, Write as IoWrite};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    struct VecWriter(Vec<u8>);

    impl PipeWrite for VecWriter {
        fn write(&mut self, buf: &[u8]) -> Result<usize, PipeError> {
            self.0.extend_from_slice(buf);
            Ok(buf.len())
        }
    }

    /// In-memory control pipe: pops inbound bytes, collects outbound writes.
    struct QueuePipe {
        inbound: std::collections::VecDeque<u8>,
        outbound: Vec<u8>,
    }

    impl PipeRead for QueuePipe {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, PipeError> {
            if self.inbound.is_empty() {
                return Err(PipeError::WouldBlock);
            }
            let n = std::cmp::min(buf.len(), self.inbound.len());
            for i in 0..n {
                buf[i] = self.inbound.pop_front().unwrap();
            }
            Ok(n)
        }
    }

    impl PipeWrite for QueuePipe {
        fn write(&mut self, buf: &[u8]) -> Result<usize, PipeError> {
            self.outbound.extend_from_slice(buf);
            Ok(buf.len())
        }
    }

    #[test]
    fn pump_once_reads_accept_request_then_tcp_pairs_request_id() {
        use arca_control::{write_frame, AcceptListenerId, ControlFrame};
        let mut m = Monitor::new(64);
        let bind_ep = Endpoint::new([127, 0, 0, 1], 0);
        let mut pl = [0u8; arca_control::MAX_FRAME_PAYLOAD];
        let n = bind_ep.encode(&mut pl);
        let reply = m
            .dispatch_request(ControlFrame::new(
                MessageType::ListenRequest,
                1,
                &pl[..n],
            ))
            .unwrap();
        let lid = ListenerReady::decode(reply.payload()).listener_id;

        let mut pay = [0u8; 4];
        AcceptListenerId { listener_id: lid }.encode(&mut pay);
        let acc = ControlFrame::new(MessageType::AcceptRequest, 77, &pay);
        let mut enc = VecWriter(Vec::new());
        write_frame(&mut enc, &acc).unwrap();

        let mut transport = QueuePipe {
            inbound: std::collections::VecDeque::from(enc.0),
            outbound: Vec::new(),
        };
        m.pump_once(&mut transport).unwrap();

        let port = m
            .listener(lid)
            .expect("listener")
            .local_addr()
            .unwrap()
            .port();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            let _ = TcpStream::connect(("127.0.0.1", port));
        });
        thread::sleep(Duration::from_millis(60));

        let mut w = VecWriter(Vec::new());
        assert_eq!(m.poll_accepts(&mut w).unwrap(), 1);

        struct FrameSliceReader<'a> {
            data: &'a [u8],
            pos: usize,
        }
        impl PipeRead for FrameSliceReader<'_> {
            fn read(&mut self, buf: &mut [u8]) -> Result<usize, PipeError> {
                if self.pos >= self.data.len() {
                    return Err(PipeError::WouldBlock);
                }
                let n = std::cmp::min(buf.len(), self.data.len() - self.pos);
                buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
                self.pos += n;
                Ok(n)
            }
        }
        let mut r = FrameSliceReader {
            data: &w.0,
            pos: 0,
        };
        let fr = read_frame(&mut r).unwrap();
        assert_eq!(fr.message_type, MessageType::IncomingConnection);
        assert_eq!(fr.request_id, 77);
        let ready = ConnectionReady::decode(fr.payload());
        assert_eq!(ready.listener_id, lid);
    }

    #[test]
    fn listen_dispatch_binds() {
        let mut m = Monitor::new(64);
        let ep = Endpoint::new([127, 0, 0, 1], 0);
        let mut pl = [0u8; arca_control::MAX_FRAME_PAYLOAD];
        let n = ep.encode(&mut pl);
        let req = ControlFrame::new(MessageType::ListenRequest, 7, &pl[..n]);
        let reply = m.dispatch_request(req).unwrap();
        assert_eq!(reply.message_type, MessageType::ListenOk);
        assert_eq!(reply.request_id, 7);
        let lr = ListenerReady::decode(reply.payload());
        assert!(m.listeners.contains_key(&lr.listener_id));
    }

    #[test]
    fn connect_dispatch_reaches_listener() {
        let (tx, rx) = mpsc::channel::<u16>();
        let server = thread::spawn(move || {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            l.set_nonblocking(false).unwrap();
            let port = l.local_addr().unwrap().port();
            tx.send(port).unwrap();
            let (mut s, _) = l.accept().unwrap();
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"ping");
            s.write_all(b"pong").unwrap();
        });

        let port = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let mut m = Monitor::new(64);
        let ep = Endpoint::new([127, 0, 0, 1], port);
        let mut pl = [0u8; arca_control::MAX_FRAME_PAYLOAD];
        let n = ep.encode(&mut pl);
        let req = ControlFrame::new(MessageType::ConnectRequest, 1, &pl[..n]);
        let reply = m.dispatch_request(req).unwrap();
        assert_eq!(reply.message_type, MessageType::ConnectOk);
        let ready = ConnectionReady::decode(reply.payload());
        let stream = m.connection(ready.connection_id).unwrap();
        let mut owned = stream.try_clone().unwrap();
        owned.write_all(b"ping").unwrap();
        let mut out = [0u8; 4];
        owned.read_exact(&mut out).unwrap();
        assert_eq!(&out, b"pong");

        server.join().unwrap();
    }

    #[test]
    fn incoming_after_client_connects() {
        let mut m = Monitor::new(64);
        let bind_ep = Endpoint::new([127, 0, 0, 1], 0);
        let mut pl = [0u8; arca_control::MAX_FRAME_PAYLOAD];
        let n = bind_ep.encode(&mut pl);
        let req = ControlFrame::new(MessageType::ListenRequest, 1, &pl[..n]);
        let reply = m.dispatch_request(req).unwrap();
        let lid = ListenerReady::decode(reply.payload()).listener_id;
        let port = m.listeners.get(&lid).unwrap().local_addr().unwrap().port();
        m.test_enqueue_accept(lid, 42);

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            let _ = TcpStream::connect(("127.0.0.1", port));
        });

        thread::sleep(Duration::from_millis(50));
        let mut w = VecWriter(Vec::new());
        let written = m.poll_accepts(&mut w).unwrap();
        assert_eq!(written, 1);

        struct SliceReader<'a> {
            data: &'a [u8],
            pos: usize,
        }

        impl PipeRead for SliceReader<'_> {
            fn read(&mut self, buf: &mut [u8]) -> Result<usize, PipeError> {
                if self.pos >= self.data.len() {
                    return Err(PipeError::WouldBlock);
                }
                let n = std::cmp::min(buf.len(), self.data.len() - self.pos);
                buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
                self.pos += n;
                Ok(n)
            }
        }

        let mut r = SliceReader {
            data: &w.0,
            pos: 0,
        };
        let ev = read_frame(&mut r).unwrap();
        assert_eq!(ev.message_type, MessageType::IncomingConnection);
        assert_eq!(ev.request_id, 42);
        let ready = ConnectionReady::decode(ev.payload());
        assert_eq!(ready.listener_id, lid);
    }

    #[test]
    fn tcp_to_pipe_reads_from_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            s.write_all(b"hi").unwrap();
        });

        let mut tcp = TcpStream::connect(("127.0.0.1", port)).unwrap();
        thread::sleep(Duration::from_millis(20));
        let mut out = VecWriter(Vec::new());
        let n = relay::tcp_to_pipe(&mut tcp, &mut out).unwrap();
        assert_eq!(n, 2);
        assert_eq!(out.0, b"hi");
        server.join().unwrap();
    }
}
