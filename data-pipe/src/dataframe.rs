#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FrameType {
    Data = 0,
    Fin  = 1,
    Rst  = 2,
}

impl FrameType {
    pub fn from_u32(v: u32) -> Option<Self> {
        match v {
            0 => Some(Self::Data),
            1 => Some(Self::Fin),
            2 => Some(Self::Rst),
            _ => None,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DataFrameHeader {
    pub frame_type: FrameType,
    pub payload_len: u32,
}

impl DataFrameHeader {
    pub fn new(frame_type: FrameType, payload_len: u32) -> Self {
        Self { frame_type, payload_len }
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            core::slice::from_raw_parts(
                self as *const Self as *const u8,
                core::mem::size_of::<Self>(),
            )
        }
    }
}
