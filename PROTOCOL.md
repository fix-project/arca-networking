# Arca ↔ Linux Control Protocol

This is the design doc for the **control protocol** that lets Arca ask Linux
to do TCP-like things (`connect`, `listen`, `accept`) without Arca having to
implement TCP itself.

It pairs with two sibling pieces of work:

| Piece | Owner | Crate | What it does |
|------|-------|-------|--------------|
| **Bidirectional pipe** | Greg | `arca-pipe` | Lock-free SPSC ring buffers in shared memory. Provides `Read` + `Write` byte streams. The transport everything else rides on. |
| **Control protocol** *(this doc)* | Majd | `arca-control` (no_std) + `arca-monitor` (std) | Single shared "control pipe" carrying request/reply messages for session management. |
| **Data protocol** | Luna | (TBD) | Per-connection bytestream wrapper that feels like `std::net::TcpStream`. Sits on top of a per-session bidirectional pipe; the monitor pumps bytes between that pipe and the kernel socket. |

---

## 1. Mental model

```
                             ┌─────────────────────────────────────┐
                             │           Linux user-space          │
   ┌────────────┐            │                                     │
   │            │ ─control──►│  Monitor (arca-monitor)             │
   │            │            │   • single I/O thread               │
   │            │ ◄──reply───│   • owns kernel TcpListeners        │
   │   Arca     │            │   • owns kernel TcpStreams          │
   │ (no_std)   │            │                                     │
   │            │ ◄──data────│           ↕                         │
   │            │   …per     │  Linux kernel networking stack       │
   │            │ ──conn.───►│                                     │
   └────────────┘            └─────────────────────────────────────┘
```

- **Arca never speaks TCP.** It calls `bind` / `connect` / `accept` on the
  control pipe, and Linux runs the real socket calls.
- **Linux owns sockets.** Whatever leaves the NIC is plain kernel TCP.
- **One control pipe, many data pipes.** The control pipe is statically
  allocated (one per Arca instance). Each accepted/connected session gets
  its **own** bidirectional data pipe, allocated dynamically by the monitor.

---

## 2. Pipes and shared memory

Both control and data pipes are instances of `arca_pipe::BidirectionalPipe`,
which is two single-producer/single-consumer rings packed into one shared
memory region. The pipe layer is a raw byte stream — **no framing**. Higher
layers add their own framing.

| Pipe | Lifetime | Carries |
|------|----------|---------|
| **Control pipe** | Static (1 per Arca instance) | Framed control messages (this doc). |
| **Data pipe** | Dynamic (1 per session) | Raw application bytes for one TCP session. |

Where does the SHM region come from? Out of scope for this doc — assume both
sides have a way to map the same physical memory (hypervisor mapping, POSIX
`shm_open`, etc.). The control pipe ships a `DataPipeInfo { pipe_id,
ring_size }` to Arca; both sides interpret `pipe_id` against an agreed-upon
table to find the right region.

---

## 3. Wire format

A control message is one **frame**. All multi-byte integers are
**little-endian**.

```
offset  size  field
------  ----  ------------------------------------------
 0      1     message_type   (u8, see catalog below)
 1      2     payload_len    (u16, bytes after header)
 3      4     request_id     (u32, correlation token)
 7      ..    payload        (payload_len bytes, fixed layout per message_type)
```

- **`message_type`** — single byte, one of the variants in the catalog.
- **`payload_len`** — caps at `MAX_FRAME_PAYLOAD` (currently 256). Any frame
  with a larger length is rejected as malformed.
- **`request_id`** — Arca-assigned for requests; copied back in the reply.
  `0` is reserved for unsolicited Linux→Arca events (`IncomingConnection`).

The framing is intentionally tiny so a hex dump on the pipe is human
readable. We don't have a magic byte or version field yet — when we add
the first backwards-incompatible change we'll bump the protocol with a
new message type or an extra header byte.

---

## 4. Message catalog

| Code | Name | Direction | Payload | Notes |
|------|------|-----------|---------|-------|
| 1 | `ListenRequest` | Arca → Linux | `Endpoint` (6 B) | "Bind+listen on this address." |
| 2 | `ListenOk` | Linux → Arca | `ListenerReady` (4 B) | Reply to `ListenRequest`. |
| 3 | `ConnectRequest` | Arca → Linux | `Endpoint` (6 B) | "Connect outbound to this address." |
| 4 | `ConnectOk` | Linux → Arca | `ConnectionReady` (20 B) | Reply to `ConnectRequest`. `listener_id == 0`. |
| 5 | `IncomingConnection` | Linux → Arca | `ConnectionReady` (20 B) | **Unsolicited.** A peer connected to one of our listeners. `request_id == 0`. |
| 6 | `ListenErr` | Linux → Arca | `ErrPayload` (4 B) | Reply to `ListenRequest`; `code` is the `errno` if available. |
| 7 | `ConnectErr` | Linux → Arca | `ErrPayload` (4 B) | Reply to `ConnectRequest`; `code` is the `errno` if available. |

### Payload layouts

All fields little-endian, fixed offsets, no padding.

**`Endpoint` (6 B)** — IPv4 only for now.
```
0..4   host  (4 bytes, network-order octets)
4..6   port  (u16)
```

**`ListenerReady` (4 B)**
```
0..4   listener_id  (u32, allocated by the monitor)
```

**`ErrPayload` (4 B)**
```
0..4   code  (u32, Linux errno or 1 for "unknown")
```

**`DataPipeInfo` (12 B)** — shared by both `ConnectOk` and `IncomingConnection`.
```
0..4   pipe_id     (u32, opaque handle agreed by both sides)
4..12  ring_size   (u64, per-direction ring capacity in bytes)
```
The total shared-memory size for this pipe is
`BidirectionalPipe::required_size(ring_size)` — derived, not transmitted.

**`ConnectionReady` (20 B)**
```
0..4   listener_id    (u32, 0 == outbound connection)
4..8   connection_id  (u32, monitor-allocated)
8..20  data_pipe_info (12 B, layout above)
```

---

## 5. Sequence diagrams

### Outbound connect

```
   Arca                                    Monitor
    │  ConnectRequest{rid=N, ep}            │
    ├──────────────────────────────────────►│  TcpStream::connect(ep)
    │                                       │  (kernel handshake)
    │  ConnectOk{rid=N, ConnectionReady}    │
    │◄──────────────────────────────────────┤
    │                                       │
    ▼                                       ▼
  ArcaTcpStream                       monitor.connection(id) is live
```

If `connect` fails the monitor replies with `ConnectErr{rid=N, errno}` and
no connection is allocated.

### Listen + accept (inbound)

```
   Arca                                    Monitor
    │  ListenRequest{rid=N, ep}             │
    ├──────────────────────────────────────►│  TcpListener::bind(ep)
    │                                       │  set_nonblocking(true)
    │  ListenOk{rid=N, listener_id=L}       │
    │◄──────────────────────────────────────┤
    │                                       │
    │   ───────── time passes ─────────     │
    │                                       │   loop: poll_incoming()
    │                                       │     listener.accept() -> stream
    │                                       │     allocate connection_id
    │  IncomingConnection{rid=0,            │
    │      listener_id=L, conn_id=C, pipe}  │
    │◄──────────────────────────────────────┤
    │                                       │
    ▼                                       ▼
   accept(&listener) returns ArcaTcpStream
```

A few subtleties worth stating clearly:

- `IncomingConnection` is **unsolicited** — Arca didn't issue a request, so
  it carries `request_id == 0`. Correlation is by `listener_id` instead.
- The monitor's `accept` runs in non-blocking mode inside a single I/O
  thread (`poll_incoming`); Arca doesn't need to poll itself.
- If an `IncomingConnection` arrives while Arca is mid-request-reply on
  some other operation, the Arca-side library **stashes it** in a tiny
  fixed-size queue and delivers it on a later `accept` call. See §7.

---

## 6. Identifiers

There are **four** numbers in a typical frame, and they're easy to mix up
because three of them look like little integers and they often appear in
the same payload. Each one answers a different question.

| Number | Lives in | Answers the question | Allocated by |
|--------|----------|----------------------|--------------|
| `message_type` | header byte 0 | *What kind of operation is this frame?* (e.g., "a connect request", "an incoming connection event") | Fixed by the protocol — values `1..=7`. |
| `request_id` | header bytes 3..7 | *Which Arca→Linux conversation does this frame belong to?* | Arca, before it sends a request. Linux echoes it on the reply. |
| `listener_id` | payload | *Which specific kernel `TcpListener`?* | Monitor, on `bind`. |
| `connection_id` | payload | *Which specific live TCP session?* | Monitor, on connect or accept. |

The first one says **what** we're doing. The other three say **which thing**
we're doing it to / about. `message_type` is the same byte for every
`ListenRequest` Arca ever sends (always `1`); the others are fresh per
listener / per connection / per conversation.

### "Solicited" vs "unsolicited"

- **Solicited frame**: a reply to a request Arca made. The reply's
  `request_id` is copied verbatim from the request, so Arca can tell which
  question this frame is answering. Examples: `ListenOk`, `ConnectOk`,
  `ListenErr`, `ConnectErr`.
- **Unsolicited frame**: Linux talking first. Arca didn't ask. The only
  one today is `IncomingConnection` — a peer just opened a TCP connection
  to one of our listeners. There's no matching request, so we set
  `request_id = 0` as a flag meaning "this is an event, not a reply."
  Correlation is by `listener_id` instead.

### Worked example: full lifecycle of one listener with one inbound peer

```
Arca → Linux:  ListenRequest   rid=42   payload: 0.0.0.0:8080
Linux → Arca:  ListenOk        rid=42   payload: listener_id=1
                                ^^^^^^ same as request — "this is the reply to 42"

   ... time passes, someone opens a TCP socket to port 8080 ...

Linux → Arca:  IncomingConnection  rid=0   payload: listener_id=1, connection_id=7, pipe…
                                   ^^^^^ "no question being answered" — it's an event
                                         listener_id=1  → "for the listener you bound"
                                         connection_id=7 → "the new session is named 7"
```

And the outbound case:

```
Arca → Linux:  ConnectRequest  rid=43   payload: 8.8.8.8:443
Linux → Arca:  ConnectOk       rid=43   payload: listener_id=0, connection_id=8, pipe…
                                                       ^ 0 means "no listener, this was outbound"
```

### Reserved values

| ID | Reserved value | Meaning |
|----|----------------|---------|
| `request_id` | `0` | This frame is an unsolicited event, not a reply. |
| `listener_id` | `0` | "No listener" — used inside `ConnectOk` payloads since outbound `connect` has no associated listener. |
| `connection_id` | `0` | Unused; reserved as a "no connection" sentinel for future error/teardown payloads. |

`pipe_id` (inside `DataPipeInfo`) is currently always equal to
`connection_id`. They're separate fields so we can decouple them later
if a SHM allocator hands out pipe regions independently of connection
identity.

All allocators wrap on `u32::MAX` and skip back to `1` so the reserved
`0` is preserved. Real production code should reuse IDs of closed
listeners/connections (out of scope for the current iteration — see §9).

---

## 7. Correlation, in-flight ops, and the event stash

The control pipe carries **two kinds of traffic on one byte stream**:

- request/reply pairs (Arca asks, Linux answers), and
- unsolicited events (`IncomingConnection`, pushed by Linux whenever a peer connects).

Concretely the control pipe is a `BidirectionalPipe`, which is two
one-way ring buffers in shared memory — one for each direction. Below
we only care about the **Linux → Arca** direction, since that's the
one carrying both replies *and* events. Bytes leave the monitor in the
order it writes them, and arrive at Arca in the same order (it's a FIFO
ring). The monitor has no idea what Arca is currently waiting on.

So this timeline:

```
t=1   peer A connects to our listener         monitor writes IncomingConnection(conn=7) into the ring
t=2   peer B connects to our listener         monitor writes IncomingConnection(conn=8) into the ring
t=3   Arca calls connect(8.8.8.8:443)         Arca writes ConnectRequest(rid=43) into the OTHER direction
t=4   monitor receives that request,
      handshakes outbound, replies            monitor writes ConnectOk(rid=43) into the ring
```

leaves the Linux→Arca ring looking like this when Arca's `connect()`
finally gets around to reading from it:

```
   front of FIFO  ───────────────────────────────────────►  back of FIFO
   (Arca reads next)                                    (most recently written)

   ┌────────────────────────────┐ ┌────────────────────────────┐ ┌──────────────────────┐
   │ IncomingConnection         │ │ IncomingConnection         │ │ ConnectOk            │
   │   listener=1, conn=7       │ │   listener=1, conn=8       │ │   rid=43             │
   │   rid=0  (unsolicited)     │ │   rid=0  (unsolicited)     │ │                      │
   └────────────────────────────┘ └────────────────────────────┘ └──────────────────────┘
        ▲
        │
        Arca's next read() pulls bytes from here.
        It's *not* the ConnectOk — that's two frames behind.
```

Arca was waiting for `ConnectOk(rid=43)`, but the bytes the ring hands
back first are the two `IncomingConnection` frames the monitor wrote
earlier. We can't ask the ring "skip those and give me the next reply" —
it's a plain FIFO. So Arca has to read the two events out of the ring
before the bytes for `ConnectOk` are even reachable. That's the reading
half of the problem.

The other half: once we've read those event frames, **what do we do with
them?** Each one carries data that we can't reconstruct later:

- `connection_id` was just assigned by the monitor when the kernel
  `accept()` returned. There's no way to re-derive it on the Arca side.
- `pipe.pipe_id` / `pipe.ring_size` are handles to a specific shared
  memory region the monitor allocated for *this* connection.

If we drop the bytes and just remember "an event happened," when `accept()`
is later called there's nothing to hand back — the new connection on Linux
is orphaned. So the payloads have to live somewhere until the application
asks for them. That somewhere is the **event stash**.

### How the stash works

The Arca-side library keeps a small fixed-size queue of `IncomingConnection`
payloads:

- During `bind` / `connect`: when waiting for a reply, any
  `IncomingConnection` that arrives first is moved into the stash and the
  read continues until the matching `request_id` reply shows up.
- During `accept(&listener)`: the stash is checked **first** for an entry
  whose `listener_id` matches. If found, return it without touching the
  wire. Otherwise read from the wire; if a frame arrives that is for some
  *other* listener, stash it and keep reading until the right one comes.

The stash is currently sized at **8 events**. If it overflows, the library
returns `ArcaError::PendingIncomingOverflow` rather than dropping events
silently — losing an event would mean leaking a connection on the monitor
side. "8 connections landing with zero `accept` calls in between" is
already outside the simple-protocol scope; if it becomes real the size
either bumps or the queue moves into a heap-allocated structure.

### One outstanding control request at a time

For now the protocol assumes **at most one outstanding control request
per pipe** (no pipelined `connect`s, etc.). The stash is purely about
unsolicited events stepping in front of a reply, not about multiple
concurrent replies. Pipelined / async control ops are out of scope (§9);
the existing `request_id` field is what we'd use when we get there.

---

## 8. Linux-side I/O thread

The monitor is currently single-threaded. The intended drive loop is:

```rust
loop {
    monitor.flush_events(&mut control_pipe)?;     // push any IncomingConnection events
    monitor.serve_one(&mut control_pipe)?;        // read one request, reply
    // Eventually: also pump bytes between live TcpStreams and per-session pipes.
}
```

`flush_events` is non-blocking (`accept` is set non-blocking on every
listener). `serve_one` *does* block — the codec spins on `WouldBlock` on
the read side. That's fine for the simple version: Arca's request/reply
pattern means the monitor only sits in `serve_one` while Arca is
expecting a reply from it.

The monitor explicitly **does not** force connection streams non-blocking.
That's a policy decision for the byte-pump layer (Luna) — the control
protocol just hands off the `TcpStream`.

### Comparison with `io_uring`

`io_uring`'s submission queue + completion queue model is exactly what we
have here at a tiny scale: the **control pipe = SQ+CQ for session
management**, with `request_id` as the user-data field. The data pipes are
the bulk-transfer side, like `io_uring` shared rings. We don't currently
support multi-shot or chained ops — when the monitor goes async, the
shape will likely converge a bit further toward `io_uring`.

---

## 9. Out of scope (future work)

In rough priority order, things this iteration intentionally doesn't do:

1. **Connection close / half-close.** Real apps need `shutdown` and EOF
   propagation. Easiest extension: add `CloseRequest{conn_id}`/`CloseOk`
   plus an unsolicited `PeerClosed{conn_id, direction}` event. For now,
   teardown is "drop the data pipe and forget about the connection."
2. **Listener teardown.** Same idea: `CloseListenerRequest{listener_id}`.
3. **IPv6.** `Endpoint` is fixed at 4 octets. When IPv6 lands, either
   add a sibling type or change `Endpoint` to a length-prefixed form
   (which would be the first wire-incompatible change).
4. **Async / pipelined control ops.** Today: one in-flight request per
   pipe. Tomorrow: many. The `request_id` is already there for it.
5. **Backpressure on the control pipe.** The codec spins on `WouldBlock`.
   That's fine when traffic is low; under load we want a proper readiness
   mechanism (epoll-style on Linux side, signaling primitive on Arca).
6. **ID reuse / cleanup.** Listener and connection IDs leak monotonically
   today.
7. **Linux→Arca data-pipe allocator integration.** The `pipe_id` field
   is currently just `connection_id`; real allocation needs a SHM
   manager that hands out distinct pipe regions.
8. **Shared file mapping** (notes file): "ask Linux to open `path`,
   return a pointer/length into shared memory." Same control/data split
   as TCP, different verb. Easy follow-on once the existing path is
   solid.

---

## 10. Where the code lives

```
arca-networking/
├── pipe/                  # arca-pipe (Greg)        — no_std bidirectional pipe
├── control/               # arca-control (Majd)     — no_std control protocol
│   └── src/
│       ├── protocol.rs    #   wire types + payload encodings
│       ├── codec.rs       #   read_frame / write_frame
│       ├── arca_side.rs   #   ArcaSession, ArcaTcpListener, ArcaTcpStream
│       └── lib.rs
├── monitor/               # arca-monitor (Majd)     — std, Linux-side driver
│   ├── src/
│   │   ├── lib.rs         #   Monitor, dispatch_request, serve_one, flush_events
│   │   └── relay.rs       #   tcp_to_pipe / pipe_to_tcp helpers (will graduate to Luna's data crate)
│   └── tests/end_to_end.rs
└── PROTOCOL.md            # this file
```

The Arca-facing public surface is intentionally small:

```rust
use arca_control::{ArcaSession, Endpoint};

let mut sess = ArcaSession::new(&mut control_pipe);

let listener = sess.bind(Endpoint::new([0, 0, 0, 0], 8080))?;
let inbound  = sess.accept(&listener)?;        // ArcaTcpStream

let outbound = sess.connect(Endpoint::new([8, 8, 8, 8], 443))?; // ArcaTcpStream

// inbound.pipe()  ──► DataPipeInfo { pipe_id, ring_size }
// hand off to Luna's data-pipe wrapper for read/write.
```

Everything beyond returning the `ArcaTcpStream` handle (i.e., actually
moving bytes through the per-session data pipe) is the data-protocol
layer's job.
