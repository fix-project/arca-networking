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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_type_from_u32_round_trip() {
        assert_eq!(FrameType::from_u32(FrameType::Data as u32), Some(FrameType::Data));
        assert_eq!(FrameType::from_u32(FrameType::Fin  as u32), Some(FrameType::Fin));
        assert_eq!(FrameType::from_u32(FrameType::Rst  as u32), Some(FrameType::Rst));
    }

    #[test]
    fn frame_type_from_u32_unknown() {
        assert_eq!(FrameType::from_u32(3), None);
        assert_eq!(FrameType::from_u32(u32::MAX), None);
    }

    #[test]
    fn data_frame_header_as_bytes_is_8_bytes() {
        let h = DataFrameHeader::new(FrameType::Data, 0);
        assert_eq!(h.as_bytes().len(), 8);
    }

    #[test]
    fn data_frame_header_as_bytes_encodes_fields() {
        let h = DataFrameHeader::new(FrameType::Fin, 1234);
        let bytes = h.as_bytes();
        let raw_type = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let payload_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        assert_eq!(FrameType::from_u32(raw_type), Some(FrameType::Fin));
        assert_eq!(payload_len, 1234);
    }
}
