//! Linux-side **monitor**: holds the kernel sockets and translates Arca's
//! control-protocol requests into real `TcpListener` / `TcpStream` actions.
//!
//! Architecture in one paragraph:
//!
//! - The monitor owns one [`TcpListener`] per `ListenerReady` it has handed
//!   out, and one [`TcpStream`] per live connection.
//! - Listeners are kept **non-blocking** so we can `accept` opportunistically
//!   inside [`Monitor::poll_incoming`] without hanging the I/O thread.
//! - Connection streams are left in their default mode — the *byte pump*
//!   (the data-pipe layer) decides blocking vs non-blocking based on its own
//!   scheduling needs.
//!
//! Driving the protocol is one of:
//! - [`Monitor::dispatch_request`] — pure function: in: request frame,
//!   out: reply frame. Useful in tests and for callers who want to do their
//!   own framing.
//! - [`Monitor::serve_one`] — wired up against a control pipe transport:
//!   read one frame, dispatch, write the reply. Plus [`Monitor::flush_events`]
//!   to drain pending `IncomingConnection` events. Use these in a loop on
//!   the single I/O thread.

mod relay;

pub use relay::{pipe_to_tcp, tcp_to_pipe};

use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};

use arca_control::{
    read_frame, write_frame, CodecError, ConnectionReady, ControlFrame, DataPipeInfo, Endpoint,
    ErrPayload, ListenerReady, MessageType,
};
use arca_pipe::{Read, Write};

#[derive(Debug)]
pub enum MonitorError {
    Io(io::Error),
    Codec(CodecError),
    UnexpectedRequest(MessageType),
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
        }
    }

    /// Translate one Arca → Linux request frame into the reply we owe Arca.
    ///
    /// This is a pure-ish function over `&mut self` (state changes are
    /// inserts into the listener/connection maps). It does **no** I/O on the
    /// control pipe — see [`Monitor::serve_one`] for the wired-up version.
    pub fn dispatch_request(&mut self, frame: ControlFrame) -> Result<ControlFrame, MonitorError> {
        let rid = frame.request_id;
        match frame.message_type {
            MessageType::ListenRequest => {
                let ep = Endpoint::decode(frame.payload());
                let addr = SocketAddr::from((Ipv4Addr::from(ep.host), ep.port));
                match TcpListener::bind(addr) {
                    Ok(listener) => {
                        // Non-blocking on the listener so poll_incoming can
                        // be called from the single I/O thread.
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
                match TcpStream::connect(addr) {
                    Ok(stream) => {
                        // Leave blocking-mode alone; that's the byte-pump's
                        // call. We just hand the stream off.
                        let id = self.alloc_connection_id();
                        self.connections.insert(id, stream);
                        Ok(ready_frame(
                            MessageType::ConnectOk,
                            rid,
                            ConnectionReady {
                                listener_id: 0,
                                connection_id: id,
                                pipe: DataPipeInfo::new(id, self.default_ring_size),
                            },
                        ))
                    }
                    Err(e) => Ok(err_frame(MessageType::ConnectErr, rid, io_err_code(&e))),
                }
            }
            other => Err(MonitorError::UnexpectedRequest(other)),
        }
    }

    /// Non-blocking sweep across all listeners. For each pending accept,
    /// return one [`MessageType::IncomingConnection`] frame Arca should
    /// receive.
    ///
    /// Returns frames in arrival order. Caller is responsible for actually
    /// writing them onto the control pipe (see [`Monitor::flush_events`]).
    pub fn poll_incoming(&mut self) -> Vec<ControlFrame> {
        use std::io::ErrorKind;
        let mut out = Vec::new();
        let ids: Vec<u32> = self.listeners.keys().copied().collect();
        for lid in ids {
            let Some(listener) = self.listeners.get_mut(&lid) else {
                continue;
            };
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let cid = self.next_connection_id;
                        self.next_connection_id = self.next_connection_id.wrapping_add(1);
                        if self.next_connection_id == 0 {
                            self.next_connection_id = 1;
                        }
                        self.connections.insert(cid, stream);
                        out.push(ready_frame(
                            MessageType::IncomingConnection,
                            // Unsolicited events use request_id 0.
                            0,
                            ConnectionReady {
                                listener_id: lid,
                                connection_id: cid,
                                pipe: DataPipeInfo::new(cid, self.default_ring_size),
                            },
                        ));
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    // A different error on a single listener shouldn't kill
                    // the whole sweep — drop it for now and move on.
                    Err(_) => break,
                }
            }
        }
        out
    }

    /// Read one request from `transport`, dispatch it, write the reply back.
    ///
    /// Blocks until a full request frame arrives. Use this in the body of
    /// the I/O thread loop after [`Monitor::flush_events`].
    pub fn serve_one<T: Read + Write>(&mut self, transport: &mut T) -> Result<(), MonitorError> {
        let frame = read_frame(transport)?;
        let reply = self.dispatch_request(frame)?;
        write_frame(transport, &reply)?;
        Ok(())
    }

    /// Drain pending `IncomingConnection` events to the control pipe.
    pub fn flush_events<T: Write>(&mut self, transport: &mut T) -> Result<usize, MonitorError> {
        let events = self.poll_incoming();
        let n = events.len();
        for f in events {
            write_frame(transport, &f)?;
        }
        Ok(n)
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
        // Keep the "0 means no listener" sentinel safe across wrap-around.
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

fn err_frame(kind: MessageType, rid: u32, code: u32) -> ControlFrame {
    let mut pl = [0u8; 4];
    ErrPayload { code }.encode(&mut pl);
    ControlFrame::new(kind, rid, &pl)
}

fn ready_frame(kind: MessageType, rid: u32, ready: ConnectionReady) -> ControlFrame {
    let mut pl = [0u8; 20];
    ready.encode(&mut pl);
    ControlFrame::new(kind, rid, &pl)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arca_pipe::{PipeError, Write as PipeWrite};
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

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            let _ = TcpStream::connect(("127.0.0.1", port));
        });

        thread::sleep(Duration::from_millis(50));
        let evs = m.poll_incoming();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].message_type, MessageType::IncomingConnection);
        let ready = ConnectionReady::decode(evs[0].payload());
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
