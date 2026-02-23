use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use crate::reliable_service::{ReliableService, RELIABLE_DATA, RELIABLE_ACK, RELIABLE_FRAGMENTED};
use crate::{LocalParticipant, RemoteParticipant, PacketBuilder, PacketParser};

pub struct AlliumClient {
    pub local: LocalParticipant,
    pub remotes: Arc<Mutex<HashMap<i32, RemoteParticipant>>>,
    
    host: String,
    port: u16,
    udp: Option<Arc<UdpSocket>>,
    server_ep: Option<SocketAddr>,
    running: Arc<Mutex<bool>>,
    reliable: Option<Arc<Mutex<ReliableService>>>,
}

impl AlliumClient {
    pub fn new(name: String, host: Option<String>, port: Option<u16>) -> Self {
        Self {
            local: LocalParticipant::new(name),
            remotes: Arc::new(Mutex::new(HashMap::new())),
            host: host.unwrap_or_else(|| "localhost".to_string()),
            port: port.unwrap_or(4242),
            udp: None,
            server_ep: None,
            running: Arc::new(Mutex::new(false)),
            reliable: None,
        }
    }
    
	pub async fn connect_async(&mut self) -> Result<(), Box<dyn std::error::Error>> {
		let host = if self.host == "localhost" {
			"127.0.0.1"
		} else {
			&self.host
		};
		
		let server_ep = tokio::net::lookup_host(format!("{}:{}", host, self.port))
			.await?
			.next()
			.ok_or("Failed to resolve host")?;

		let bind_addr = if server_ep.is_ipv6() {
			"[::]:0"
		} else {
			"0.0.0.0:0"
		};
		let udp = UdpSocket::bind(bind_addr).await?;
		let udp = Arc::new(udp);
		
		let reliable = Arc::new(Mutex::new(ReliableService::new(server_ep, udp.clone())));
		
		let packet = PacketBuilder::build_text(&format!("HELLO:{}", self.local.name));
		udp.send_to(&packet, server_ep).await?;
		
		println!("[CLIENT] {} connecting to {}", self.local.name, server_ep);
		
		self.server_ep = Some(server_ep);
		self.udp = Some(udp.clone());
		self.reliable = Some(reliable.clone());
		*self.running.lock().await = true;
		
		let running_clone = self.running.clone();
		let remotes_clone = self.remotes.clone();
		let mut local_clone = self.local.clone();
		let reliable_clone = reliable.clone();
		
		tokio::spawn(async move {
			Self::receive_loop(udp.clone(), running_clone, remotes_clone, &mut local_clone, reliable_clone).await;
		});
		
		let running_clone = self.running.clone();
		tokio::spawn(async move {
			while *running_clone.lock().await {
				tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
				let mut rs = reliable.lock().await;
				if let Err(e) = rs.tick().await {
					eprintln!("[CLIENT] Reliable error: {}", e);
					break;
				}
			}
		});
		
		Ok(())
	}
    
    pub async fn send_message_async(&self, msg: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let (Some(udp), Some(server_ep)) = (&self.udp, &self.server_ep) {
            let packet = PacketBuilder::build_text(&format!("MSG:{}", msg));
            udp.send_to(&packet, server_ep).await?;
        }
        Ok(())
    }
    
    pub async fn send_reliable(&self, msg: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(reliable) = &self.reliable {
            let payload = format!("MSG:{}", msg).into_bytes();
            reliable.lock().await.send(payload).await?;
        }
        Ok(())
    }
    
    async fn receive_loop(
        udp: Arc<UdpSocket>,
        running: Arc<Mutex<bool>>,
        remotes: Arc<Mutex<HashMap<i32, RemoteParticipant>>>,
        local: &mut LocalParticipant,
        reliable: Arc<Mutex<ReliableService>>,
    ) {
        let mut buf = [0u8; 65535];
        
        while *running.lock().await {
            match udp.recv_from(&mut buf).await {
                Ok((len, _addr)) => {
                    if len < 1 { continue; }
                    
                    let packet_type = buf[0];
                    
                    if packet_type == RELIABLE_DATA || packet_type == RELIABLE_ACK || packet_type == RELIABLE_FRAGMENTED {
                        let mut rs = reliable.lock().await;
						if let Ok(payloads) = rs.handle_incoming(&buf[..len]).await {
							for payload in payloads {
								let msg = String::from_utf8_lossy(&payload).to_string();
								Self::handle_message(msg, local, &remotes).await;
							}
						}
                    } else {
                        if let Ok(payload) = PacketParser::parse(&buf[..len]) {
							let msg = String::from_utf8_lossy(&payload).to_string();
							Self::handle_message(msg, local, &remotes).await;
						}
                    }
                }
                Err(_) => {
                    if *running.lock().await {
                        break;
                    }
                }
            }
        }
    }
    
    async fn handle_message(
        msg: String,
        local: &mut LocalParticipant,
        remotes: &Arc<Mutex<HashMap<i32, RemoteParticipant>>>,
    ) {
        let parts: Vec<&str> = msg.splitn(2, ':').collect();
        if parts.len() < 2 {
            return;
        }
        
        match parts[0] {
            "WELCOME" => {
                if let Ok(id) = parts[1].parse::<i32>() {
                    local.id = Some(id);
                    println!("[CLIENT] Got ID: {}", id);
                }
            }
            "JOIN" => {
                let jp: Vec<&str> = parts[1].splitn(2, ':').collect();
                if jp.len() == 2 {
                    if let Ok(id) = jp[0].parse::<i32>() {
                        if Some(id) != local.id {
                            let mut remotes_lock = remotes.lock().await;
                            remotes_lock.insert(id, RemoteParticipant::new(id, jp[1].to_string()));
                            println!("[CLIENT] {} joined", jp[1]);
                        }
                    }
                }
            }
            "LEFT" => {
                if let Ok(left_id) = parts[1].parse::<i32>() {
                    let mut remotes_lock = remotes.lock().await;
                    if let Some(r) = remotes_lock.remove(&left_id) {
                        println!("[CLIENT] {} left", r.name);
                    }
                }
            }
            "MSG" => {
                let mp: Vec<&str> = parts[1].splitn(2, ':').collect();
                if mp.len() == 2 {
                    if let Ok(from) = mp[0].parse::<i32>() {
                        let remotes_lock = remotes.lock().await;
                        let name = remotes_lock
                            .get(&from)
                            .map(|r| r.name.as_str())
                            .unwrap_or("Unknown");
                        println!("[CLIENT] {}: {}", name, mp[1]);
                    }
                }
            }
            _ => {}
        }
    }
    
    pub async fn disconnect_async(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(reliable) = &self.reliable {
            let payload = "BYE".as_bytes().to_vec();
            reliable.lock().await.send(payload).await.map_err(|e| e.to_string())?;
            
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        
        *self.running.lock().await = false;
        Ok(())
    }
}
