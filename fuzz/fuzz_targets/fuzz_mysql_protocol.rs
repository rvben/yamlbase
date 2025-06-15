#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Test MySQL packet parsing
    if data.len() >= 4 {
        // MySQL packets have a 4-byte header
        let length = u32::from_le_bytes([data[0], data[1], data[2], 0]) as usize;
        let _sequence = data[3];
        
        if data.len() >= 4 + length {
            let packet_data = &data[4..4 + length];
            
            // Try to parse as different packet types
            if !packet_data.is_empty() {
                match packet_data[0] {
                    0x00 => {
                        // OK packet
                        let mut pos = 1;
                        if pos < packet_data.len() {
                            // Read length-encoded integer (affected rows)
                            let _ = read_lenenc_int(&packet_data[pos..]);
                        }
                    }
                    0xff => {
                        // Error packet
                        if packet_data.len() >= 3 {
                            let _error_code = u16::from_le_bytes([packet_data[1], packet_data[2]]);
                        }
                    }
                    0xfe if packet_data.len() < 9 => {
                        // EOF packet
                        if packet_data.len() >= 5 {
                            let _warnings = u16::from_le_bytes([packet_data[1], packet_data[2]]);
                            let _status = u16::from_le_bytes([packet_data[3], packet_data[4]]);
                        }
                    }
                    _ => {
                        // Could be a result row or other packet type
                        // Try to read as length-encoded strings
                        let mut pos = 0;
                        while pos < packet_data.len() {
                            match read_lenenc_string(&packet_data[pos..]) {
                                Some((_, bytes_read)) => pos += bytes_read,
                                None => break,
                            }
                        }
                    }
                }
            }
        }
    }
});

fn read_lenenc_int(data: &[u8]) -> Option<(u64, usize)> {
    if data.is_empty() {
        return None;
    }
    
    match data[0] {
        0..=250 => Some((data[0] as u64, 1)),
        0xfb => Some((0, 1)), // NULL
        0xfc => {
            if data.len() >= 3 {
                Some((u16::from_le_bytes([data[1], data[2]]) as u64, 3))
            } else {
                None
            }
        }
        0xfd => {
            if data.len() >= 4 {
                Some((u32::from_le_bytes([data[1], data[2], data[3], 0]) as u64, 4))
            } else {
                None
            }
        }
        0xfe => {
            if data.len() >= 9 {
                Some((u64::from_le_bytes([
                    data[1], data[2], data[3], data[4],
                    data[5], data[6], data[7], data[8],
                ]), 9))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn read_lenenc_string(data: &[u8]) -> Option<(&[u8], usize)> {
    let (len, bytes_read) = read_lenenc_int(data)?;
    let total_len = bytes_read + len as usize;
    
    if data.len() >= total_len {
        Some((&data[bytes_read..total_len], total_len))
    } else {
        None
    }
}