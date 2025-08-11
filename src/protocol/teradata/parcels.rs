use bytes::{Buf, BufMut, BytesMut};
use std::io;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ParcelKind {
    // Request parcels
    RunRequest = 1,
    PrepareRequest = 2,
    ExecuteRequest = 3,
    FetchRequest = 4,
    AbortRequest = 5,
    EndRequest = 6,

    // Response parcels
    SuccessParcel = 8,
    RecordParcel = 10,
    EndStatementParcel = 11,
    EndRequestParcel = 12,
    ErrorParcel = 13,
    StatementInfoParcel = 14,
    DataInfoParcel = 15,

    // Control parcels
    LogonRequest = 100,
    LogoffRequest = 101,
    AuthenticationOk = 102,
    AuthenticationFailed = 103,
}

impl ParcelKind {
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::RunRequest),
            2 => Some(Self::PrepareRequest),
            3 => Some(Self::ExecuteRequest),
            4 => Some(Self::FetchRequest),
            5 => Some(Self::AbortRequest),
            6 => Some(Self::EndRequest),
            8 => Some(Self::SuccessParcel),
            10 => Some(Self::RecordParcel),
            11 => Some(Self::EndStatementParcel),
            12 => Some(Self::EndRequestParcel),
            13 => Some(Self::ErrorParcel),
            14 => Some(Self::StatementInfoParcel),
            15 => Some(Self::DataInfoParcel),
            100 => Some(Self::LogonRequest),
            101 => Some(Self::LogoffRequest),
            102 => Some(Self::AuthenticationOk),
            103 => Some(Self::AuthenticationFailed),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Parcel {
    pub kind: ParcelKind,
    pub data: Vec<u8>,
}

impl Parcel {
    pub fn new(kind: ParcelKind, data: Vec<u8>) -> Self {
        Self { kind, data }
    }

    pub fn logon_request(username: &str, password: &str, database: &str) -> Self {
        let mut data = BytesMut::new();

        // Version info
        data.put_u16(1); // Protocol version

        // Username
        data.put_u16(username.len() as u16);
        data.put_slice(username.as_bytes());

        // Password
        data.put_u16(password.len() as u16);
        data.put_slice(password.as_bytes());

        // Database
        data.put_u16(database.len() as u16);
        data.put_slice(database.as_bytes());

        // Session charset (UTF8)
        data.put_u16(4);
        data.put_slice(b"UTF8");

        Self::new(ParcelKind::LogonRequest, data.to_vec())
    }

    pub fn logoff_request() -> Self {
        Self::new(ParcelKind::LogoffRequest, vec![])
    }

    pub fn run_request(sql: &str) -> Self {
        let mut data = BytesMut::new();

        // Statement info
        data.put_u16(1); // Statement number
        data.put_u16(0); // Options

        // SQL text
        data.put_u32(sql.len() as u32);
        data.put_slice(sql.as_bytes());

        Self::new(ParcelKind::RunRequest, data.to_vec())
    }

    pub fn success_parcel(activity_count: u32) -> Self {
        let mut data = BytesMut::new();
        data.put_u32(activity_count);
        Self::new(ParcelKind::SuccessParcel, data.to_vec())
    }

    pub fn error_parcel(code: u16, message: &str) -> Self {
        let mut data = BytesMut::new();
        data.put_u16(code);
        data.put_u16(message.len() as u16);
        data.put_slice(message.as_bytes());
        Self::new(ParcelKind::ErrorParcel, data.to_vec())
    }

    pub fn end_statement_parcel() -> Self {
        Self::new(ParcelKind::EndStatementParcel, vec![])
    }

    pub fn end_request_parcel() -> Self {
        Self::new(ParcelKind::EndRequestParcel, vec![])
    }

    pub fn record_parcel(row_data: &[u8]) -> Self {
        Self::new(ParcelKind::RecordParcel, row_data.to_vec())
    }

    pub fn data_info_parcel(column_count: u16, columns: &[(String, String)]) -> Self {
        let mut data = BytesMut::new();
        data.put_u16(column_count);

        for (name, type_name) in columns {
            // Column name
            data.put_u16(name.len() as u16);
            data.put_slice(name.as_bytes());

            // Type name
            data.put_u16(type_name.len() as u16);
            data.put_slice(type_name.as_bytes());

            // Additional metadata (simplified)
            data.put_u16(0); // Nullable
            data.put_u16(255); // Max length
        }

        Self::new(ParcelKind::DataInfoParcel, data.to_vec())
    }

    pub fn encode(&self) -> BytesMut {
        let mut buffer = BytesMut::new();

        // Parcel header
        buffer.put_u16(self.kind as u16);
        buffer.put_u32((self.data.len() + 6) as u32); // Total length including header

        // Parcel data
        buffer.put_slice(&self.data);

        buffer
    }

    pub fn decode(buffer: &mut BytesMut) -> io::Result<Option<Self>> {
        if buffer.len() < 6 {
            return Ok(None); // Not enough data for header
        }

        // Peek at the header without consuming
        let kind_value = u16::from_be_bytes([buffer[0], buffer[1]]);
        let length = u32::from_be_bytes([buffer[2], buffer[3], buffer[4], buffer[5]]) as usize;

        if buffer.len() < length {
            return Ok(None); // Not enough data for complete parcel
        }

        // Now consume the data
        buffer.advance(6); // Skip header
        let data_length = length - 6;
        let data = buffer.split_to(data_length).to_vec();

        let kind = ParcelKind::from_u16(kind_value)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Unknown parcel kind"))?;

        Ok(Some(Parcel::new(kind, data)))
    }
}

pub struct Message {
    pub parcels: Vec<Parcel>,
}

impl Message {
    pub fn new(parcels: Vec<Parcel>) -> Self {
        Self { parcels }
    }

    pub fn single(parcel: Parcel) -> Self {
        Self::new(vec![parcel])
    }

    pub fn encode(&self) -> BytesMut {
        let mut buffer = BytesMut::new();

        // Message header
        buffer.put_u32(0x00000001); // Message kind (Request/Response)
        buffer.put_u16(self.parcels.len() as u16); // Parcel count

        // Encode all parcels
        for parcel in &self.parcels {
            buffer.extend_from_slice(&parcel.encode());
        }

        // Add total message length at the beginning
        let total_length = buffer.len() + 4;
        let mut final_buffer = BytesMut::new();
        final_buffer.put_u32(total_length as u32);
        final_buffer.extend_from_slice(&buffer);

        final_buffer
    }

    pub fn decode(buffer: &mut BytesMut) -> io::Result<Option<Self>> {
        if buffer.len() < 4 {
            return Ok(None);
        }

        // Peek at message length
        let message_length =
            u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;

        if buffer.len() < message_length {
            return Ok(None); // Not enough data
        }

        // Consume message length
        buffer.advance(4);

        // Read message header
        if buffer.len() < 6 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid message header",
            ));
        }

        let _message_kind = buffer.get_u32();
        let parcel_count = buffer.get_u16();

        // Read all parcels
        let mut parcels = Vec::new();
        for _ in 0..parcel_count {
            match Parcel::decode(buffer)? {
                Some(parcel) => parcels.push(parcel),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Incomplete parcel",
                    ));
                }
            }
        }

        Ok(Some(Message::new(parcels)))
    }
}
