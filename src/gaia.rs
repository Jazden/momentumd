#![allow(dead_code)]

pub const MAGIC: [u8; 2] = [0xFF, 0x03];
pub const VENDOR_SENNHEISER: u16 = 0x0495;
pub const VENDOR_QUALCOMM: u16 = 0x000A;

pub const CMD_REGISTER_NOTIFICATION: u16 = 0x0007;

pub const CMD_SET_EQ_PRESETS: u16 = 0x1002;
pub const CMD_GET_EQ_PRESETS: u16 = 0x1003;

pub const CMD_SET_BASS_BOOST: u16 = 0x1008;
pub const CMD_GET_BASS_BOOST: u16 = 0x1009;

pub const CMD_SET_WEAR_DETECTION: u16 = 0x1200;
pub const CMD_GET_WEAR_DETECTION: u16 = 0x1201;

pub const CMD_SET_AUTO_POWER_OFF: u16 = 0x1202;
pub const CMD_GET_AUTO_POWER_OFF: u16 = 0x1203;

pub const CMD_SET_TRANSPARENCY: u16 = 0x1A02;
pub const CMD_GET_TRANSPARENCY: u16 = 0x1A03;

pub const CMD_SET_ANC_STATUS: u16 = 0x1A04;
pub const CMD_GET_ANC_STATUS: u16 = 0x1A05;

pub const CMD_SET_SIDETONE: u16 = 0x1A06;
pub const CMD_GET_SIDETONE: u16 = 0x1A07;

pub const FEATURE_USER_EQ: u8 = 8;
pub const FEATURE_TRANSPARENCY: u8 = 12;
pub const FEATURE_ANC: u8 = 13;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GaiaPacket {
    pub vendor: u16,
    pub command: u16,
    pub payload: Vec<u8>,
}

impl GaiaPacket {
    pub fn new(command: u16, payload: Vec<u8>) -> Self {
        Self {
            vendor: VENDOR_SENNHEISER,
            command,
            payload,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.payload.len());
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&(self.payload.len() as u16).to_be_bytes());
        buf.extend_from_slice(&self.vendor.to_be_bytes());
        buf.extend_from_slice(&self.command.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn expected_response(command: u16) -> u16 {
        command | 0x0100
    }
}

pub fn parse_packets(buffer: &[u8]) -> (Vec<GaiaPacket>, usize) {
    let mut packets = Vec::new();
    let mut offset = 0;

    while buffer.len() - offset >= 8 {
        if buffer[offset..offset + 2] != MAGIC {
            // Find next magic byte
            if let Some(pos) = buffer[offset + 1..].windows(2).position(|w| w == MAGIC) {
                offset += 1 + pos;
                continue;
            } else {
                return (packets, buffer.len());
            }
        }

        let payload_len = u16::from_be_bytes([buffer[offset + 2], buffer[offset + 3]]) as usize;
        let total = 8 + payload_len;
        if buffer.len() - offset < total {
            break;
        }

        let vendor = u16::from_be_bytes([buffer[offset + 4], buffer[offset + 5]]);
        let command = u16::from_be_bytes([buffer[offset + 6], buffer[offset + 7]]);
        let payload = buffer[offset + 8..offset + total].to_vec();

        packets.push(GaiaPacket {
            vendor,
            command,
            payload,
        });
        offset += total;
    }

    (packets, offset)
}
