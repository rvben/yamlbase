use super::parcels::{Message, Parcel, ParcelKind};

pub struct TeradataAuth {
    username: String,
    password: String,
}

impl TeradataAuth {
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
    
    pub fn validate_credentials(&self, username: &str, password: &str) -> bool {
        self.username == username && self.password == password
    }
    
    pub fn handle_logon(&self, parcel: &Parcel) -> anyhow::Result<Message> {
        // Parse logon request from parcel data
        let mut cursor = 0;
        let data = &parcel.data;
        
        // Skip version info
        if data.len() < 2 {
            return Ok(Message::single(
                Parcel::error_parcel(3001, "Invalid logon request")
            ));
        }
        cursor += 2;
        
        // Read username
        if data.len() < cursor + 2 {
            return Ok(Message::single(
                Parcel::error_parcel(3002, "Missing username")
            ));
        }
        let username_len = u16::from_be_bytes([data[cursor], data[cursor + 1]]) as usize;
        cursor += 2;
        
        if data.len() < cursor + username_len {
            return Ok(Message::single(
                Parcel::error_parcel(3003, "Invalid username length")
            ));
        }
        let username = String::from_utf8_lossy(&data[cursor..cursor + username_len]).to_string();
        cursor += username_len;
        
        // Read password
        if data.len() < cursor + 2 {
            return Ok(Message::single(
                Parcel::error_parcel(3004, "Missing password")
            ));
        }
        let password_len = u16::from_be_bytes([data[cursor], data[cursor + 1]]) as usize;
        cursor += 2;
        
        if data.len() < cursor + password_len {
            return Ok(Message::single(
                Parcel::error_parcel(3005, "Invalid password length")
            ));
        }
        let password = String::from_utf8_lossy(&data[cursor..cursor + password_len]).to_string();
        
        // Validate credentials
        if self.validate_credentials(&username, &password) {
            Ok(Message::new(vec![
                Parcel::new(ParcelKind::AuthenticationOk, vec![]),
                Parcel::success_parcel(1),
                Parcel::end_statement_parcel(),
                Parcel::end_request_parcel(),
            ]))
        } else {
            Ok(Message::single(
                Parcel::new(ParcelKind::AuthenticationFailed, 
                    format!("Invalid credentials for user: {}", username).into_bytes())
            ))
        }
    }
    
    pub fn handle_logoff(&self) -> Message {
        Message::new(vec![
            Parcel::success_parcel(1),
            Parcel::end_statement_parcel(),
            Parcel::end_request_parcel(),
        ])
    }
}