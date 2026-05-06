//! Best-effort byte pump between a [`TcpStream`](std::net::TcpStream) and `arca-pipe` endpoints.

use arca_pipe::{PipeError, Read, Write};
use std::io::{Read as IoRead, Write as IoWrite};
use std::net::TcpStream;

/// Read from TCP (non-blocking friendly) and write into the pipe.
pub fn tcp_to_pipe(tcp: &mut TcpStream, pipe: &mut impl Write) -> std::io::Result<usize> {
    let mut buf = [0u8; 4096];
    match tcp.read(&mut buf) {
        Ok(0) => Ok(0),
        Ok(n) => {
            let mut off = 0usize;
            while off < n {
                match pipe.write(&buf[off..n]) {
                    Ok(0) => return Ok(off),
                    Ok(m) => off += m,
                    Err(PipeError::WouldBlock) => {}
                }
            }
            Ok(n)
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(0),
        Err(e) => Err(e),
    }
}

/// Read from the pipe and write to TCP.
pub fn pipe_to_tcp(tcp: &mut TcpStream, pipe: &mut impl Read) -> std::io::Result<usize> {
    let mut buf = [0u8; 4096];
    match pipe.read(&mut buf) {
        Ok(0) => Ok(0),
        Ok(n) => {
            tcp.write_all(&buf[..n])?;
            Ok(n)
        }
        Err(PipeError::WouldBlock) => Ok(0),
    }
}
