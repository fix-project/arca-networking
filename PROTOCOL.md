# Arca ↔ Linux Control Protocol

This doc specifies the **control protocol**: framed messages on one
**control pipe** between Arca and a **monitor** in Linux so Arca can call
`connect`, `listen`, and `accept` without implementing TCP. It defines the
wire format, message types, identifiers, data-pipe lifecycle at session
setup, and how `arca-control` (Arca) and `arca-monitor` (Linux) implement
the exchange.

All Linux→Arca control traffic is **replies**: each frame echoes Arca’s
`request_id`. Bind/listen uses `ListenRequest` and `ListenOk` or `ListenErr`.
Outbound connect uses `ConnectRequest` and `ConnectOk` or `ConnectErr`; the
monitor waits on `TcpStream::connect` until the handshake completes.
Inbound connections use `AcceptRequest` (message type 8): Arca waits for
`IncomingConnection` (success) or `AcceptErr` (unknown listener or kernel
`accept` failed), each sent **only** in reply to that request with the
same `request_id`. The monitor keeps a pending-accept queue per
listener, calls kernel `accept` only when a wait exists, and drives the pipe
with `poll_accepts`, `pump_once`, `serve_one`, and `FrameReadBuf` when the
transport is non-blocking. `ArcaSession` matches replies by `request_id`
only (no secondary stash for out-of-order events).

Related pieces in the stack:

| Piece               | Crate                                       | Role                                                                 |
| ------------------- | ------------------------------------------- | -------------------------------------------------------------------- |
| Bidirectional pipe  | `arca-pipe`                                 | Shared-memory rings; `Read` + `Write` byte streams.                 |
| Control protocol    | `arca-control`, `arca-monitor` *(this doc)* | Framed messages on one control pipe for listen, connect, accept.     |
| Data path           | *(not this doc)*                            | Per-session byte stream on a bidirectional pipe; monitor relays I/O. |

---

## 1. Mental model

```
                         ┌────────────────────────────────────┐
                         │       Linux user-space             │
   ┌────────────┐        │                                    │
   │            │control │  Monitor (arca-monitor)            │
   │            ├───────►│  owns listeners / TCP streams      │
   │   Arca     │◄──────┤                                    │
   │ (no_std)   │ reply │             ↕                      │
   │            │ data  │  Linux kernel networking           │
   └────────────┘        └────────────────────────────────────┘
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

| Pipe           | Lifetime                       | Created by               | Carries                                   |
| -------------- | ------------------------------ | ------------------------ | ----------------------------------------- |
| Control pipe   | Static (1 per Arca instance)   | Bootstrap (out of scope) | Framed control messages (this doc).      |
| Data pipe      | Dynamic (1 per session)        | Monitor, on demand       | Raw application bytes for one TCP session. |

The control pipe is assumed to exist before any frame on it is sent — both
Arca and the monitor know about its SHM region at boot. *Data* pipes are
the ones the protocol actually creates and tears down at runtime, so they
deserve a precise lifecycle.

### Data-pipe lifecycle

A new data pipe is needed at exactly **two moments** — and these are
exactly the two moments the monitor is about to send a `ConnectionReady`
payload:

- *Outbound*: a `ConnectRequest` succeeded. The kernel handshake is done
  and the monitor is about to reply with `ConnectOk`.
- *Inbound*: Arca has sent `AcceptRequest` for a listener, the monitor has
  queued that wait, and a non-blocking kernel `accept` on that listener
  returned a fresh socket. The monitor is about to reply with
  `IncomingConnection` carrying the same `request_id` as that
  `AcceptRequest`.

In both cases the lifecycle is the same five steps:

```
1. Trigger    Monitor decides a new session needs a pipe (one of the two
              moments above).

2. Allocate   Monitor:
                 • picks a connection_id        (monotonic, §6)
                 • picks a pipe_id              (today: == connection_id)
                 • allocates an SHM region of
                   BidirectionalPipe::required_size(ring_size) bytes
                 • registers (pipe_id → region) in the SHM table
              No bytes are written into the rings yet.

3. Inform     Monitor encodes the just-allocated handles into a
              ConnectionReady{ listener_id, connection_id, pipe }
              and ships it inside ConnectOk (outbound) or
              IncomingConnection (inbound), always echoing Arca's `request_id`
              on those replies.

4. Attach     Arca decodes ConnectionReady, looks up pipe_id in the same
              SHM table, and constructs its half of the pipe:
                BidirectionalPipe::new(region, ring_size, Side::A)
              The monitor's I/O thread already holds Side::B.

5. Pump       Per-session data path: move bytes between the rings and the
              kernel `TcpStream`. The control protocol's job for this session
              is done until teardown.
```

**Ordering matters.** The SHM region must be allocated and registered
**before** the `ConnectionReady` frame is written. Otherwise Arca can
decode the payload, look up `pipe_id`, and find nothing — or worse, find
a stale region. The current single-threaded monitor enforces this
naturally (allocate → encode → write happen in that order on one stack).
A future multi-threaded monitor must keep the same happens-before edge.

**How Arca resolves `pipe_id` to a real SHM region** is intentionally not
specified by the protocol. Both sides share an external registry — an
SHM name table, a hypervisor handle table, whatever the platform offers —
keyed by `pipe_id`. The protocol just gives each region a stable name.

### Teardown

Currently undefined — see §9. When a session ends, nothing reclaims the
SHM region. Real production needs `CloseRequest{conn_id}` /
`PeerClosed{conn_id}` plus a "release `pipe_id`" step on both sides;
until those land, sessions leak.

### Status of the current implementation

Step 2 (Allocate) is **stubbed**: `Monitor::dispatch_request` and
`Monitor::poll_accepts` write `DataPipeInfo::new(connection_id, default_ring_size)` into the reply without allocating SHM; `pipe_id` is just `connection_id` as a placeholder. Step 4 (Attach) is part of the data-protocol layer and is not exercised by the control crate’s tests yet. Once a real SHM allocator exists, `pipe_id` can diverge from `connection_id`; nothing else on the wire changes for that alone.

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
- **`request_id`** — Arca-assigned for **every** Arca→Linux request
  (including `AcceptRequest`); Linux echoes the same value on the matching
  reply. There are no Linux-initiated control frames: everything the monitor
  writes on the Arca→Linux ring is a **response** to something Arca asked.

The framing is intentionally tiny so a hex dump on the pipe is human
readable. We don't have a magic byte or version field yet — when we add
the first backwards-incompatible change we'll bump the protocol with a
new message type or an extra header byte.

---

## 4. Message catalog

| Code | Name                 | Direction   | Payload                 | Notes |
| ---- | -------------------- | ----------- | ----------------------- | ----- |
| 1    | `ListenRequest`      | Arca → Linux | `Endpoint` (6 B)      | Bind and listen. |
| 2    | `ListenOk`           | Linux → Arca | `ListenerReady` (4 B) | Reply to `ListenRequest`. |
| 3    | `ConnectRequest`     | Arca → Linux | `Endpoint` (6 B)      | Outbound connect. |
| 4    | `ConnectOk`          | Linux → Arca | `ConnectionReady` (20 B) | Reply to `ConnectRequest`. `listener_id == 0`. |
| 5    | `IncomingConnection` | Linux → Arca | `ConnectionReady` (20 B) | Reply to `AcceptRequest`. `listener_id != 0`. Same `request_id` as the wait. |
| 6    | `ListenErr`          | Linux → Arca | `ErrPayload` (4 B)    | Reply to `ListenRequest`. |
| 7    | `ConnectErr`         | Linux → Arca | `ErrPayload` (4 B)    | Reply to `ConnectRequest`. |
| 8    | `AcceptRequest`      | Arca → Linux | `AcceptListenerId` (4 B) | Wait for next inbound on this `listener_id` (see §5). |
| 9    | `AcceptErr`          | Linux → Arca | `ErrPayload` (4 B)    | Reply to `AcceptRequest` when the monitor can't fulfil it (unknown listener, kernel `accept` failed). `code` is errno-like; `9` (EBADF) for unknown listener. |

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

**`AcceptListenerId` (4 B)**
```
0..4   listener_id  (u32, must be a live listener from `ListenOk`)
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
    │                                       │  allocate connection_id
    │                                       │  allocate SHM region, pick pipe_id   ← data pipe born here (§2)
    │  ConnectOk{rid=N, ConnectionReady}    │
    │◄──────────────────────────────────────┤
    │  attach to SHM via pipe_id            │
    │   (Side::A)                           │   (monitor already holds Side::B)
    │                                       │
    ▼                                       ▼
  ArcaTcpStream                       monitor.connection(id) is live
```

If `connect` fails the monitor replies with `ConnectErr{rid=N, errno}` and
no connection or SHM region is allocated.

### Listen + accept (inbound)

```
   Arca                                    Monitor
    │  ListenRequest{rid=N, ep}             │
    ├──────────────────────────────────────►│  TcpListener::bind(ep)
    │                                       │  set_nonblocking(true)
    │  ListenOk{rid=N, listener_id=L}       │   (no data pipe yet — listeners
    │◄──────────────────────────────────────┤    don't carry application bytes)
    │                                       │
    │  AcceptRequest{rid=M, listener L}     │   enqueue (L, M); accept waits
    ├──────────────────────────────────────►│   until a kernel accept pairs it
    │                                       │
    │   ───────── time passes ─────────     │   poll_accepts(): non-blocking
    │                                       │   accept(); on success → reply
    │                                       │   allocate connection_id, SHM…  ← data pipe born here (§2)
    │  IncomingConnection{rid=M,            │
    │      listener_id=L, conn_id=C, pipe}    │
    │◄──────────────────────────────────────┤
    │  attach to SHM via pipe_id (Side::A)  │
    │                                       │
    ▼                                       ▼
   accept(&listener) returns ArcaTcpStream
```

Subtleties:

- **Listen** returns immediately (`ListenOk` / `ListenErr`).
- **Connect** (`ConnectRequest`) blocks in the monitor until the kernel
  connect completes — *connect waits*.
- **Accept** (`AcceptRequest`) blocks on the Arca side until the matching
  `IncomingConnection` reply arrives; the monitor does **not** perform a
  kernel `accept` unless there is a pending `request_id` for that listener
  (so stray inbound TCP connections are not turned into sessions with no
  Arca wait).
- The monitor services the control pipe with **`poll_accepts`**
  (try kernel `accept` for each listener that has a FIFO of pending Arca
  waits) plus **`pump_once` / `serve_one`** (read Arca→Linux frames with a
  small incremental decoder on **non-blocking** transports). `serve_one`
  yields the CPU while waiting for a full frame.

---

## 6. Identifiers

There are **four** numbers in a typical frame, and they're easy to mix up
because three of them look like little integers and they often appear in
the same payload. Each one answers a different question.

| Number          | Lives in         | Meaning                                      | Allocated by                          |
| --------------- | ---------------- | -------------------------------------------- | ------------------------------------- |
| `message_type`  | header byte 0    | What kind of frame (see catalog).            | Protocol (`1`–`8`).                   |
| `request_id`    | header bytes 3–7 | Which request–reply pair (Arca sets).        | Arca; echoed on replies.              |
| `listener_id`   | payload          | Which `TcpListener`.                    | Monitor on bind.                      |
| `connection_id` | payload          | Which live TCP session.                 | Monitor on connect or accept.         |

The first one says **what** we're doing. The other three say **which thing**
we're doing it to / about. `message_type` is the same byte for every
`ListenRequest` Arca ever sends (always `1`); the others are fresh per
listener / per connection / per conversation.

### Correlation

Every Linux→Arca control frame is a **reply**: its `request_id` copies the
Arca-issued token from the matching request (`ListenRequest`,
`ConnectRequest`, or `AcceptRequest`). There is no parallel “event” channel
and **no stash** on the Arca library side — `IncomingConnection` is not
delivered ahead of an `AcceptRequest`.

### Worked example: full lifecycle of one listener with one inbound peer

Inbound:

```text
Arca → Linux:  ListenRequest   rid=42   payload: 0.0.0.0:8080
Linux → Arca:  ListenOk        rid=42   payload: listener_id=1

Arca → Linux:  AcceptRequest   rid=50   payload: listener_id=1

   ... time passes, someone opens a TCP socket to port 8080 ...

Linux → Arca:  IncomingConnection  rid=50   payload: listener_id=1, connection_id=7, pipe…
              (rid matches AcceptRequest)
```

Outbound:

```text
Arca → Linux:  ConnectRequest  rid=43   payload: 8.8.8.8:443
Linux → Arca:  ConnectOk       rid=43   payload: listener_id=0, connection_id=8, pipe…
                                listener_id 0 = outbound connect (no listener)
```

### Reserved values (partial)

| Field           | Reserved | Meaning |
| --------------- | -------- | ------- |
| `listener_id`   | `0`      | No listener — used in `ConnectOk` for outbound connects. |
| `connection_id` | `0`      | Reserved for future “no connection” / error payloads.   |

`request_id == 0` is not used by the protocol today (Arca allocates monotonically from `1`). It remains available as a sentinel if we add monitor-pushed exceptions later.

`pipe_id` (inside `DataPipeInfo`) is currently always equal to
`connection_id`. They're separate fields so we can decouple them later
if a SHM allocator hands out pipe regions independently of connection
identity.

All allocators wrap on `u32::MAX` and skip back to `1` so the reserved
`0` is preserved. Real production code should reuse IDs of closed
listeners/connections (out of scope for the current iteration — see §9).

---

## 7. Multiple Arca threads, ordering, and waiting on the ring

The Linux→Arca direction of the control pipe is still a **FIFO** byte
stream: frames arrive in the order the monitor writes them. **Every**
frame is a *reply*, so its `request_id` tells you which outstanding Arca
request it completes. There is **no** secondary stash queue for
out-of-order arrivals.

### Accept before kernel `accept`

Because `IncomingConnection` is only emitted after an `AcceptRequest`,
the monitor cannot push inbound sessions “ahead of” unrelated replies.
For example, while Arca waits on `ConnectOk(rid=43)`, there is no longer a
scenario where two `IncomingConnection` events sit in front of that reply in
the ring without matching `AcceptRequest`s.

### Pipelined control ops

Several Arca threads may each block in `bind`, `connect`, or `accept` with
distinct `request_id`s. The **completion order** is whatever the monitor
produces; the Arca library reads frames strictly in FIFO order. If thread A
is waiting for `rid=2` but the next frame on the wire is `ConnectOk` for
`rid=7`, that is a **protocol / scheduling bug** (you need a single
reader/demux, or you must guarantee the monitor completes requests in the
same order Arca expects). The reference `ArcaSession` implementation
therefore **errors** on a mismatched `request_id` while waiting for a
specific reply.

### “Peek” and CPU yield

On **non-blocking** transports the codecs spin when a read or write returns
`WouldBlock`; they call `core::hint::spin_loop` so other hardware threads can
make progress. The monitor’s `serve_one` similarly calls `std::thread::yield_now`
while waiting for the incremental decoder to fill a complete frame. That is
the cooperative wait for the control pipe driver, without a separate buffer
of undelivered frames on Arca.

---

## 8. Linux-side I/O thread

The monitor is currently single-threaded. The intended drive loop is:

```rust
loop {
    monitor.pump_once(&mut control_pipe)?;
    // Eventually: pump bytes between live TcpStreams and per-session pipes.
}
```

`pump_once` is **non-blocking** on the transport: it runs `poll_accepts`
(a non-blocking kernel `accept` for listeners that have pending Arca
`AcceptRequest` IDs) and then drains every fully received Arca→Linux frame
from an internal reassembly buffer.

`Monitor::serve_one` spins (with `yield_now`) until one complete Arca→Linux
frame is available — useful when the caller prefers a blocking API.

`dispatch_request` handles only `ListenRequest` and `ConnectRequest`; the
latter **blocks** on `TcpStream::connect` until the kernel handshake
finishes (connect waits).

The monitor explicitly **does not** force connection streams non-blocking.
That's a policy decision for the byte-pump layer — the control
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
4. **Backpressure on the control pipe.** The codec spins on `WouldBlock`.
   That's fine when traffic is low; under load we want a proper readiness
   mechanism (epoll-style on Linux side, signaling primitive on Arca).
5. **ID reuse / cleanup.** Listener and connection IDs leak monotonically
   today.
6. **Linux→Arca data-pipe allocator integration.** The `pipe_id` field
   is currently just `connection_id`; real allocation needs a SHM
   manager that hands out distinct pipe regions.
7. **Shared file mapping** (notes file): "ask Linux to open `path`,
   return a pointer/length into shared memory." Same control/data split
   as TCP, different verb. Easy follow-on once the existing path is
   solid.

---

## 10. Where the code lives

```
arca-networking/
├── pipe/                  # arca-pipe — bidirectional pipe
├── control/               # arca-control — wire types, codec, ArcaSession
│   └── src/
│       ├── protocol.rs    #   wire types + payload encodings
│       ├── codec.rs       #   read_frame / write_frame / FrameReadBuf
│       ├── arca_side.rs   #   ArcaSession, ArcaTcpListener, ArcaTcpStream
│       └── lib.rs
├── monitor/               # arca-monitor — Linux-side driver
│   ├── src/
│   │   ├── lib.rs         #   Monitor, dispatch_request, pump_once, serve_one, poll_accepts
│   │   └── relay.rs       #   tcp_to_pipe / pipe_to_tcp helpers
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
// then attach your per-session pipe / byte layer for read/write.
```

Everything beyond returning the `ArcaTcpStream` handle (i.e., actually
moving bytes through the per-session data pipe) is the data-protocol
layer's job.
