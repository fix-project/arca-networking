//! Crate entrypoint for the control protocol layer.
//!
//! - `protocol`: message types and payloads ([`ControlFrame`], [`MessageType`], …)
//! - Framing on `arca-pipe`: [`read_frame`], [`write_frame`]

#![no_std]

mod codec;
pub mod protocol;

pub use codec::{read_frame, write_frame, HEADER_LEN};
pub use protocol::*;
