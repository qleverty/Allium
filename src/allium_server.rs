use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use crate::reliable_service::{ReliableService, RELIABLE_DATA, RELIABLE_ACK, RELIABLE_FRAGMENTED};
use crate::{LocalParticipant, RemoteParticipant, PacketBuilder, PacketParser, AUDIO, VIDEO};

pub struct AlliumServer {
    pub local: LocalParticipant,
    pub remotes: Arc<Mutex<HashMap<i32, RemoteParticipant>>>,
    
    addr_to_id: Arc<Mutex<HashMap<String, i32>>>,
    id_to_addr: Arc<Mutex<HashMap<i32, SocketAddr>>>,
    port: u16,
    next_id: Arc<Mutex<i32>>,
    udp: Option<Arc<UdpSocket>>,
    running: Arc<Mutex<bool>>,
    reliable_connections: Arc<Mutex<HashMap<i32, ReliableService>>>,
}

impl AlliumServer {
    pub fn new(name: String, port: Option<u16>) -> Self {
        let mut local = LocalParticipant::new(name);
        local.id = Some(0);
        
        Self {
            local,
            remotes: Arc::new(Mutex::new(HashMap::new())),
            addr_to_id: Arc::new(Mutex::new(HashMap::new())),
            id_to_addr: Arc::new(Mutex::new(HashMap::new())),
            port: port.unwrap_or(4242),
            next_id: Arc::new(Mutex::new(1)),
            udp: None,
            running: Arc::new(Mutex::new(false)),
            reliable_connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    pub async fn start_async(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let udp = UdpSocket::bind(format!("0.0.0.0:{}", self.port)).await?;
        let udp = Arc::new(udp);
        println!("[SERVER] '{}' started on {}", self.local.name, self.port);
        
        self.udp = Some(udp.clone());
        *self.running.lock().await = true;
        
        let running = self.running.clone();
        let remotes = self.remotes.clone();
        let addr_to_id = self.addr_to_id.clone();
        let id_to_addr = self.id_to_addr.clone();
        let next_id = self.next_id.clone();
        let local = self.local.clone();
        let reliable_connections = self.reliable_connections.clone();
        
        let mut buf = [0u8; 2048];
        
        loop {
            tokio::select! {
                Ok((len, addr)) = udp.recv_from(&mut buf) => {
                    if len < 1 { continue; }
                    
                    let packet_type = buf[0];
					
					if packet_type == AUDIO {
						// audio
					}
					
					else if packet_type == VIDEO {
						// video
					}
                    
                    else if packet_type == RELIABLE_DATA || packet_type == RELIABLE_ACK || packet_type == RELIABLE_FRAGMENTED {
                        let key = addr.to_string();
                        let addr_to_id_lock = addr_to_id.lock().await;
                        
                        if let Some(&client_id) = addr_to_id_lock.get(&key) {
                            drop(addr_to_id_lock);
                            
							let mut reliable_lock = reliable_connections.lock().await;
							if let Some(rs) = reliable_lock.get_mut(&client_id) {
								if let Ok(payloads) = rs.handle_incoming(&buf[..len]).await {
									drop(reliable_lock);
									
									for payload in payloads {
										let msg = String::from_utf8_lossy(&payload).to_string();
										
										let mut reliable_lock = reliable_connections.lock().await;
										let should_disconnect = Self::handle_reliable_message_locked(
											client_id,
											msg,
											&remotes,
											&mut *reliable_lock,
										).await;
										drop(reliable_lock);
										
										if should_disconnect {
											remotes.lock().await.remove(&client_id);
											addr_to_id.lock().await.remove(&key);
											id_to_addr.lock().await.remove(&client_id);
											reliable_connections.lock().await.remove(&client_id);
											break;
										}
									}
								}
							}
                        }
                    } else {
                        if let Ok(payload) = PacketParser::parse(&buf[..len]) {
							let msg = String::from_utf8_lossy(&payload).to_string();
							Self::handle_message(
								msg,
								addr,
								&udp,
								&remotes,
								&addr_to_id,
								&id_to_addr,
								&next_id,
								&local,
								&reliable_connections,
							)
							.await;
						}
                    }
                }
                
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(50)) => {
                    let mut reliable_lock = reliable_connections.lock().await;
                    let mut dead_clients = Vec::new();
                    
                    for (client_id, rs) in reliable_lock.iter_mut() {
                        if let Err(_) = rs.tick().await {
                            dead_clients.push(*client_id);
                        }
                    }
                    
                    for client_id in dead_clients {
                        reliable_lock.remove(&client_id);
                        
                        remotes.lock().await.remove(&client_id);
                        id_to_addr.lock().await.remove(&client_id);
                        
                        let mut addr_to_id_lock = addr_to_id.lock().await;
                        addr_to_id_lock.retain(|_, &mut id| id != client_id);
                        drop(addr_to_id_lock);
                        
                        eprintln!("[SERVER] Client {} reliable timeout", client_id);
                    }
                }
            }
            
            if !*running.lock().await {
                break;
            }
        }
        
        Ok(())
    }
    
    async fn handle_message(
        msg: String,
        addr: SocketAddr,
        udp: &Arc<UdpSocket>,
        remotes: &Arc<Mutex<HashMap<i32, RemoteParticipant>>>,
        addr_to_id: &Arc<Mutex<HashMap<String, i32>>>,
        id_to_addr: &Arc<Mutex<HashMap<i32, SocketAddr>>>,
        next_id: &Arc<Mutex<i32>>,
        local: &LocalParticipant,
        reliable_connections: &Arc<Mutex<HashMap<i32, ReliableService>>>,
    ) {
        let parts: Vec<&str> = msg.splitn(2, ':').collect();
        let key = addr.to_string();
        
        match parts[0] {
            "HELLO" if parts.len() == 2 => {
                println!("[SERVER] Received HELLO from {} at {}", parts[1], addr);
                
                let mut next_id_lock = next_id.lock().await;
                let id = *next_id_lock;
                *next_id_lock += 1;
                drop(next_id_lock);
                
                let name = parts[1].to_string();
                
                remotes.lock().await.insert(id, RemoteParticipant::new(id, name.clone()));
                addr_to_id.lock().await.insert(key, id);
                id_to_addr.lock().await.insert(id, addr);
                
                let rs = ReliableService::new(addr, udp.clone());
                reliable_connections.lock().await.insert(id, rs);
                
                let packet = PacketBuilder::build_text(&format!("WELCOME:{}", id));
                let _ = udp.send_to(&packet, addr).await;
                println!("[SERVER] Sent WELCOME:{} to {}", id, addr);
                
                Self::broadcast(
                    &PacketBuilder::build_text(&format!("JOIN:{}:{}", id, name)),
                    udp,
                    id_to_addr,
                    Some(id),
                )
                .await;
                
                let packet = PacketBuilder::build_text(&format!(
                    "JOIN:{}:{}",
                    local.id.unwrap(),
                    local.name
                ));
                let _ = udp.send_to(&packet, addr).await;
                
                let remotes_lock = remotes.lock().await;
                for (oid, other) in remotes_lock.iter() {
                    if *oid != id {
                        let packet = PacketBuilder::build_text(&format!("JOIN:{}:{}", oid, other.name));
                        let _ = udp.send_to(&packet, addr).await;
                    }
                }
                
                println!("[SERVER] {} joined", name);
            }
            "MSG" if parts.len() == 2 => {
                let addr_to_id_lock = addr_to_id.lock().await;
                if let Some(&from) = addr_to_id_lock.get(&key) {
                    drop(addr_to_id_lock);
                    
                    let remotes_lock = remotes.lock().await;
                    if let Some(remote) = remotes_lock.get(&from) {
                        println!("[SERVER] {}: {}", remote.name, parts[1]);
                    }
                    drop(remotes_lock);
                    
                    Self::broadcast(
                        &PacketBuilder::build_text(&format!("MSG:{}:{}", from, parts[1])),
                        udp,
                        id_to_addr,
                        Some(from),
                    )
                    .await;
                }
            }
            "BYE" => {
                let addr_to_id_lock = addr_to_id.lock().await;
                if let Some(&cid) = addr_to_id_lock.get(&key) {
                    drop(addr_to_id_lock);
                    
                    let remotes_lock = remotes.lock().await;
                    if let Some(remote) = remotes_lock.get(&cid) {
                        println!("[SERVER] {} left", remote.name);
                    }
                    drop(remotes_lock);
                    
                    remotes.lock().await.remove(&cid);
                    addr_to_id.lock().await.remove(&key);
                    id_to_addr.lock().await.remove(&cid);
                    reliable_connections.lock().await.remove(&cid);
                    
                    Self::broadcast(
                        &PacketBuilder::build_text(&format!("LEFT:{}", cid)),
                        udp,
                        id_to_addr,
                        None,
                    )
                    .await;
                }
            }
            _ => {}
        }
    }
    
    async fn handle_reliable_message_locked(
        from_id: i32,
        msg: String,
        remotes: &Arc<Mutex<HashMap<i32, RemoteParticipant>>>,
        reliable_lock: &mut HashMap<i32, ReliableService>,
    ) -> bool {
        let parts: Vec<&str> = msg.splitn(2, ':').collect();
        
        if parts[0] == "MSG" && parts.len() == 2 {
            let remotes_lock = remotes.lock().await;
            if let Some(sender) = remotes_lock.get(&from_id) {
                println!("[SERVER] {}: {}", sender.name, parts[1]);
            }
            drop(remotes_lock);
            
            let payload = format!("MSG:{}:{}", from_id, parts[1]).into_bytes();
            
            for (client_id, rs) in reliable_lock.iter_mut() {
                if *client_id != from_id {
                    let _ = rs.send(payload.clone()).await;
                }
            }
            false
        } else if parts[0] == "BYE" {
            let remotes_lock = remotes.lock().await;
            if let Some(remote) = remotes_lock.get(&from_id) {
                println!("[SERVER] {} left", remote.name);
            }
            drop(remotes_lock);
            
            let payload = format!("LEFT:{}", from_id).into_bytes();
            for (client_id, rs) in reliable_lock.iter_mut() {
                if *client_id != from_id {
                    let _ = rs.send(payload.clone()).await;
                }
            }
            
            true
        } else {
            false
        }
    }
    
    async fn broadcast(
        packet: &[u8],
        udp: &Arc<UdpSocket>,
        id_to_addr: &Arc<Mutex<HashMap<i32, SocketAddr>>>,
        exclude: Option<i32>,
    ) {
        let id_to_addr_lock = id_to_addr.lock().await;
        for (id, addr) in id_to_addr_lock.iter() {
            if Some(*id) != exclude {
                let _ = udp.send_to(packet, addr).await;
            }
        }
    }
    
    pub async fn stop(&self) {
        *self.running.lock().await = false;
    }
}
