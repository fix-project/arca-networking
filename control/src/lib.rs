//! Arca ↔ Linux **control protocol**.
//!
//! Layout of this crate:
//!
//! - [`protocol`]: the on-wire types — frame header, message catalog, payload
//!   structs ([`ControlFrame`], [`MessageType`], [`Endpoint`], …).
//! - Codec: [`read_frame`] / [`write_frame`] move frames over any
//!   `arca_pipe::Read`/`Write` byte transport (typically the dedicated
//!   control pipe, an `arca_pipe::BidirectionalPipe`).
//! - Arca side: [`ArcaSession`] owns the control pipe and exposes
//!   [`ArcaSession::bind`] / [`ArcaSession::connect`] / [`ArcaSession::accept`]
//!   — built to *feel* like `std::net::{TcpListener, TcpStream}`. The
//!   returned [`ArcaTcpListener`] / [`ArcaTcpStream`] are lightweight
//!   handles; per-connection bytestreams live in **separate** data pipes.
//!
//! `no_std` throughout. The Linux-side counterpart lives in `arca-monitor`.

#![no_std]

mod arca_side;
mod codec;
pub mod protocol;

pub use arca_side::{ArcaError, ArcaSession, ArcaTcpListener, ArcaTcpStream};
pub use codec::{read_frame, write_frame, CodecError, HEADER_LEN};
pub use protocol::*;
