use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

// ============== PACKET TYPE ==============

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum PacketType {
    TextMessage = 0,
}

// ============== PACKET BUILDER ==============

pub struct PacketBuilder;

impl PacketBuilder {
    pub fn build_text(message: &str) -> Vec<u8> {
        let data = message.as_bytes();
        let length = data.len() as u32;
        
        let mut packet = Vec::with_capacity(5 + data.len());
        packet.push(PacketType::TextMessage as u8);
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
    pub fn parse(packet: &[u8]) -> Result<(PacketType, Vec<u8>), String> {
        if packet.len() < 5 {
            return Err("Packet too short".to_string());
        }
        
        let length = ((packet[1] as u32) << 24)
            | ((packet[2] as u32) << 16)
            | ((packet[3] as u32) << 8)
            | (packet[4] as u32);
        
        let mut payload = vec![0u8; length as usize];
        payload.copy_from_slice(&packet[5..5 + length as usize]);
        
        Ok((PacketType::TextMessage, payload))
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

// ============== CLIENT ==============

pub struct AlliumClient {
    pub local: LocalParticipant,
    pub remotes: Arc<Mutex<HashMap<i32, RemoteParticipant>>>,
    
    host: String,
    port: u16,
    udp: Option<Arc<UdpSocket>>,
    server_ep: Option<SocketAddr>,
    running: Arc<Mutex<bool>>,
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
        }
    }
    
	pub async fn connect_async(&mut self) -> Result<(), Box<dyn std::error::Error>> {
		// Force IPv4 for localhost to match server
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
		
		let packet = PacketBuilder::build_text(&format!("HELLO:{}", self.local.name));
		udp.send_to(&packet, server_ep).await?;
		
		println!("[CLIENT] {} connecting to {}", self.local.name, server_ep);
		
		self.server_ep = Some(server_ep);
		self.udp = Some(Arc::new(udp));
		*self.running.lock().await = true;
		
		// Start receive loop
		let udp_clone = self.udp.as_ref().unwrap().clone();
		let running_clone = self.running.clone();
		let remotes_clone = self.remotes.clone();
		let mut local_clone = self.local.clone();
		
		tokio::spawn(async move {
			Self::receive_loop(udp_clone, running_clone, remotes_clone, &mut local_clone).await;
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
    
    async fn receive_loop(
        udp: Arc<UdpSocket>,
        running: Arc<Mutex<bool>>,
        remotes: Arc<Mutex<HashMap<i32, RemoteParticipant>>>,
        local: &mut LocalParticipant,
    ) {
        let mut buf = [0u8; 2048];
        
        while *running.lock().await {
            match udp.recv_from(&mut buf).await {
                Ok((len, _addr)) => {
                    if let Ok((packet_type, payload)) = PacketParser::parse(&buf[..len]) {
                        if packet_type == PacketType::TextMessage {
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
        if let (Some(udp), Some(server_ep)) = (&self.udp, &self.server_ep) {
            let packet = PacketBuilder::build_text("BYE");
            udp.send_to(&packet, server_ep).await?;
            *self.running.lock().await = false;
        }
        Ok(())
    }
}

// ============== SERVER ==============

pub struct AlliumServer {
    pub local: LocalParticipant,
    pub remotes: Arc<Mutex<HashMap<i32, RemoteParticipant>>>,
    
    addr_to_id: Arc<Mutex<HashMap<String, i32>>>,
    id_to_addr: Arc<Mutex<HashMap<i32, SocketAddr>>>,
    port: u16,
    next_id: Arc<Mutex<i32>>,
    udp: Option<Arc<UdpSocket>>,
    running: Arc<Mutex<bool>>,
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
        }
    }
    
    pub async fn start_async(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let udp = UdpSocket::bind(format!("0.0.0.0:{}", self.port)).await?;
        println!("[SERVER] '{}' started on {}", self.local.name, self.port);
        
        self.udp = Some(Arc::new(udp));
        *self.running.lock().await = true;
        
        let udp_clone = self.udp.as_ref().unwrap().clone();
        let running_clone = self.running.clone();
        let remotes_clone = self.remotes.clone();
        let addr_to_id_clone = self.addr_to_id.clone();
        let id_to_addr_clone = self.id_to_addr.clone();
        let next_id_clone = self.next_id.clone();
        let local_clone = self.local.clone();
        
        let mut buf = [0u8; 2048];
        
        while *running_clone.lock().await {
            match udp_clone.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    if let Ok((packet_type, payload)) = PacketParser::parse(&buf[..len]) {
                        if packet_type == PacketType::TextMessage {
                            let msg = String::from_utf8_lossy(&payload).to_string();
                            Self::handle_message(
                                msg,
                                addr,
                                &udp_clone,
                                &remotes_clone,
                                &addr_to_id_clone,
                                &id_to_addr_clone,
                                &next_id_clone,
                                &local_clone,
                            )
                            .await;
                        }
                    }
                }
                Err(_) => {
                    if *running_clone.lock().await {
                        break;
                    }
                }
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
                
                // Send WELCOME
                let packet = PacketBuilder::build_text(&format!("WELCOME:{}", id));
                let _ = udp.send_to(&packet, addr).await;
                println!("[SERVER] Sent WELCOME:{} to {}", id, addr);
                
                // Broadcast JOIN
                Self::broadcast(
                    &PacketBuilder::build_text(&format!("JOIN:{}:{}", id, name)),
                    udp,
                    id_to_addr,
                    Some(id),
                )
                .await;
                
                // Send server JOIN
                let packet = PacketBuilder::build_text(&format!(
                    "JOIN:{}:{}",
                    local.id.unwrap(),
                    local.name
                ));
                let _ = udp.send_to(&packet, addr).await;
                
                // Send all other clients
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
    
    c1.send_message_async("Hello everyone!").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    c2.send_message_async("Hi Nika!").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    c1.disconnect_async().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    c2.send_message_async("Where did Nika go?").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    
    c2.disconnect_async().await.unwrap();
}