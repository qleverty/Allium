mod reliable_service;
mod allium_client;
mod allium_server;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use allium_client::AlliumClient;
use allium_server::AlliumServer;

// ============== PACKET TYPE ==============

pub const AUDIO: u8 = 0x00;
pub const VIDEO: u8 = 0x01;

use reliable_service::{ReliableService, RELIABLE_DATA, RELIABLE_ACK, RELIABLE_FRAGMENTED};

// ============== PACKET BUILDER ==============

pub struct PacketBuilder;

impl PacketBuilder {
    pub fn build_text(message: &str) -> Vec<u8> {
        let data = message.as_bytes();
        let length = data.len() as u32;
        
        let mut packet = Vec::with_capacity(5 + data.len());
        packet.push(0xFF);
        packet.push((length >> 24) as u8);
        packet.push(((length >> 16) & 0xFF) as u8);
        packet.push(((length >> 8) & 0xFF) as u8);
        packet.push((length & 0xFF) as u8);
        packet.extend_from_slice(data);
        
        packet
    }
}

// ============== PACKET PARSER ==============

pub struct PacketParser;

impl PacketParser {
    pub fn parse(packet: &[u8]) -> Result<Vec<u8>, String> {
        if packet.len() < 5 {
            return Err("Packet too short".to_string());
        }
        
        let length = ((packet[1] as u32) << 24)
            | ((packet[2] as u32) << 16)
            | ((packet[3] as u32) << 8)
            | (packet[4] as u32);
        
        let mut payload = vec![0u8; length as usize];
        payload.copy_from_slice(&packet[5..5 + length as usize]);
        
        Ok(payload)
    }
}

// ============== LOCAL PARTICIPANT ==============

#[derive(Debug, Clone)]
pub struct LocalParticipant {
    pub id: Option<i32>,
    pub name: String,
}

impl LocalParticipant {
    pub fn new(name: String) -> Self {
        Self { id: None, name }
    }
}

// ============== REMOTE PARTICIPANT ==============

#[derive(Debug, Clone)]
pub struct RemoteParticipant {
    pub id: i32,
    pub name: String,
}

impl RemoteParticipant {
    pub fn new(id: i32, name: String) -> Self {
        Self { id, name }
    }
}

// ============== MAIN ==============

#[tokio::main]
async fn main() {
    let mut server = AlliumServer::new("Alex".to_string(), None);
    tokio::spawn(async move {
        let _ = server.start_async().await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    let mut c1 = AlliumClient::new("Nika".to_string(), None, None);
    c1.connect_async().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    let mut c2 = AlliumClient::new("Bob".to_string(), None, None);
    c2.connect_async().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    c1.send_reliable("Hello everyone!").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    c2.send_reliable("Hi Nika!").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
	
    let big = "X".repeat(3500);
    let preview = format!("{}...{}", &big[..20], &big[big.len()-20..]);
    println!("[TEST] Nika sends fragmented ({} bytes): {}", big.len(), preview);
    c1.send_reliable(&big).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    println!("[TEST] Nika sends ordered after fragmented: \"ping\"");
    c1.send_reliable("ping").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    c1.disconnect_async().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    c2.send_reliable("Where did Nika go?").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    
    c2.disconnect_async().await.unwrap();
}
