use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;

pub const RELIABLE_DATA: u8 = 0x10;
pub const RELIABLE_ACK: u8 = 0x11;
pub const RELIABLE_FRAGMENTED: u8 = 0x12;

const MAX_FRAGMENT_SIZE: usize = 1200;
const FRAGMENT_ASSEMBLY_TIMEOUT_SEC: u64 = 60;

struct PartialMessage {
    total_fragments: u16,
    received_fragments: HashMap<u16, Vec<u8>>,
    first_received_at: Instant,
}

pub struct ReliableService {
    remote_addr: SocketAddr,
    udp: Arc<UdpSocket>,
    config: ReliableConfig,
    outgoing: HashMap<u32, PendingPacket>,
    next_seq: u32,
    next_order_id: u32,
    received_seqs: HashMap<u32, Instant>,
    next_expected_order_id: u32,
    out_of_order_buffer: HashMap<u32, Vec<u8>>,
    last_activity: Instant,
	fragment_assembler: HashMap<u32, PartialMessage>,
	pending_acks: Vec<u32>,
}

struct PendingPacket {
    data: Vec<u8>,
    #[allow(dead_code)]
    first_sent_at: Instant,
    last_sent_at: Instant,
    attempts: u32,
}

pub struct ReliableConfig {
    pub retransmit_timeout_ms: u64,
    pub max_attempts: u32,
    pub old_seq_cleanup_secs: u64,
    // TODO: Add dynamic RTT calculation based on ACK timing
    // TODO: Add max_packet_size for automatic fragmentation
    // TODO: Add congestion control based on packet loss rate
}

impl Default for ReliableConfig {
    fn default() -> Self {
        Self {
            retransmit_timeout_ms: 100,
            max_attempts: 3,
            old_seq_cleanup_secs: 30,
        }
    }
}

impl ReliableService {
    pub fn new(remote_addr: SocketAddr, udp: Arc<UdpSocket>) -> Self {
        Self::with_config(remote_addr, udp, ReliableConfig::default())
    }

	pub fn with_config(remote_addr: SocketAddr, udp: Arc<UdpSocket>, config: ReliableConfig) -> Self {
		Self {
			remote_addr,
			udp,
			config,
			outgoing: HashMap::new(),
			next_seq: 0,
			next_order_id: 0,
			received_seqs: HashMap::new(),
			next_expected_order_id: 0,
			out_of_order_buffer: HashMap::new(),
			last_activity: Instant::now(),
			fragment_assembler: HashMap::new(),
			pending_acks: Vec::new(),
		}
	}

	pub async fn send(&mut self, payload: Vec<u8>) -> Result<u32, String> {
		let order_id = self.next_order_id;
		self.next_order_id = self.next_order_id.wrapping_add(1);
		
		if payload.len() <= MAX_FRAGMENT_SIZE {
			self.send_single(order_id, payload).await?;
		} else {
			self.send_fragmented(order_id, payload).await?;
		}
		
		Ok(order_id)
	}
	
	async fn send_single(&mut self, order_id: u32, payload: Vec<u8>) -> Result<(), String> {
		let seq = self.next_seq;
		self.next_seq = self.next_seq.wrapping_add(1);
		
		let mut packet = Vec::with_capacity(9 + payload.len());
		packet.push(RELIABLE_DATA);
		packet.extend_from_slice(&seq.to_be_bytes());
		packet.extend_from_slice(&order_id.to_be_bytes());
		packet.extend_from_slice(&payload);
		
		self.udp.send_to(&packet, self.remote_addr).await.map_err(|e| e.to_string())?;
		
		let now = Instant::now();
		self.outgoing.insert(seq, PendingPacket {
			data: packet,
			first_sent_at: now,
			last_sent_at: now,
			attempts: 1,
		});
		
		Ok(())
	}
	
	async fn send_fragmented(&mut self, order_id: u32, payload: Vec<u8>) -> Result<(), String> {
		let payload_len = payload.len();
		let total_fragments = ((payload_len + MAX_FRAGMENT_SIZE - 1) / MAX_FRAGMENT_SIZE) as u16;
		
		for fragment_idx in 0..total_fragments {
			let seq = self.next_seq;
			self.next_seq = self.next_seq.wrapping_add(1);
			
			let start = (fragment_idx as usize) * MAX_FRAGMENT_SIZE;
			let end = std::cmp::min(start + MAX_FRAGMENT_SIZE, payload_len);
			let fragment_data = &payload[start..end];
			
			let mut packet = Vec::with_capacity(13 + fragment_data.len());
			packet.push(RELIABLE_FRAGMENTED);
			packet.extend_from_slice(&seq.to_be_bytes());
			packet.extend_from_slice(&order_id.to_be_bytes());
			packet.extend_from_slice(&total_fragments.to_be_bytes());
			packet.extend_from_slice(&fragment_idx.to_be_bytes());
			packet.extend_from_slice(fragment_data);
			
			self.udp.send_to(&packet, self.remote_addr).await.map_err(|e| e.to_string())?;
			
			let now = Instant::now();
			self.outgoing.insert(seq, PendingPacket {
				data: packet,
				first_sent_at: now,
				last_sent_at: now,
				attempts: 1,
			});
		}
		
		Ok(())
	}
	
async fn enqueue_ack(&mut self, seq: u32) -> Result<(), String> {
    self.pending_acks.push(seq);
    if self.pending_acks.len() >= 300 {
        self.flush_acks().await?;
    }
    Ok(())
}

pub async fn handle_incoming(&mut self, packet: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    self.last_activity = Instant::now();

    if packet.len() < 5 {
        return Err("packet too short".to_string());
    }

    let packet_type = packet[0];
    let seq = u32::from_be_bytes([packet[1], packet[2], packet[3], packet[4]]);

    match packet_type {
        RELIABLE_DATA => {
            if packet.len() < 9 {
                return Err("ReliableOrdered packet too short".to_string());
            }
			
            let cutoff = Instant::now() - Duration::from_secs(self.config.old_seq_cleanup_secs);
            self.received_seqs.retain(|_, time| *time > cutoff);

            if self.received_seqs.contains_key(&seq) {
                self.enqueue_ack(seq).await?;
                return Ok(Vec::new());
            }

            self.received_seqs.insert(seq, Instant::now());
            self.enqueue_ack(seq).await?;

            let order_id = u32::from_be_bytes([packet[5], packet[6], packet[7], packet[8]]);
            let payload = packet[9..].to_vec();

            if order_id < self.next_expected_order_id {
                println!("[RELIABLE] Ignoring old order_id={}, expected={}", 
                    order_id, self.next_expected_order_id);
                return Ok(Vec::new());
            } else if order_id > self.next_expected_order_id {
                println!("[RELIABLE] Buffering order_id={}, expected={}", 
                    order_id, self.next_expected_order_id);
                self.out_of_order_buffer.insert(order_id, payload);
                return Ok(Vec::new());
            }

            let mut result = Vec::new();
            result.push(payload);
            self.next_expected_order_id = self.next_expected_order_id.wrapping_add(1);

			while let Some(payload) = self.out_of_order_buffer.remove(&self.next_expected_order_id) {
				result.push(payload);
				self.next_expected_order_id = self.next_expected_order_id.wrapping_add(1);
			}

            println!("[RELIABLE] Delivered {} ordered messages", result.len());
            Ok(result)
        }
		
		RELIABLE_FRAGMENTED => {
			if packet.len() < 13 {
				return Err("ReliableFragmented packet too short".to_string());
			}
			
			let cutoff = Instant::now() - Duration::from_secs(self.config.old_seq_cleanup_secs);
			self.received_seqs.retain(|_, time| *time > cutoff);
			
			if self.received_seqs.contains_key(&seq) {
				self.enqueue_ack(seq).await?;
				return Ok(Vec::new());
			}

			self.received_seqs.insert(seq, Instant::now());
			self.enqueue_ack(seq).await?;
			
			let order_id = u32::from_be_bytes([packet[5], packet[6], packet[7], packet[8]]);
			let total_fragments = u16::from_be_bytes([packet[9], packet[10]]);
			let fragment_idx = u16::from_be_bytes([packet[11], packet[12]]);
			let fragment_data = packet[13..].to_vec();
			
			let partial = self.fragment_assembler
				.entry(order_id)
				.or_insert_with(|| PartialMessage {
					total_fragments,
					received_fragments: HashMap::new(),
					first_received_at: Instant::now(),
				});
			
			partial.received_fragments.insert(fragment_idx, fragment_data);
			
			let expected = partial.total_fragments;
			if partial.received_fragments.len() == expected as usize {
				let mut complete_payload = Vec::new();
				for idx in 0..expected {
					if let Some(frag) = partial.received_fragments.get(&idx) {
						complete_payload.extend_from_slice(frag);
					}
				}
				
				self.fragment_assembler.remove(&order_id);
				
				if order_id < self.next_expected_order_id {
					println!("[RELIABLE] Ignoring old fragmented order_id={}", order_id);
					return Ok(Vec::new());
				} else if order_id > self.next_expected_order_id {
					println!("[RELIABLE] Buffering fragmented order_id={}", order_id);
					self.out_of_order_buffer.insert(order_id, complete_payload);
					return Ok(Vec::new());
				}
				
				let mut result = Vec::new();
				result.push(complete_payload);
				self.next_expected_order_id = self.next_expected_order_id.wrapping_add(1);
				
				while let Some(payload) = self.out_of_order_buffer.remove(&self.next_expected_order_id) {
					result.push(payload);
					self.next_expected_order_id = self.next_expected_order_id.wrapping_add(1);
				}
				
				println!("[RELIABLE] Delivered {} ordered messages (including fragmented)", result.len());
				Ok(result)
			} else {
				Ok(Vec::new())
			}
		}
		
		RELIABLE_ACK => {
			let count = (packet.len() - 1) / 4;
			for i in 0..count {
				let o = 1 + i * 4;
				let s = u32::from_be_bytes([packet[o], packet[o+1], packet[o+2], packet[o+3]]);
				if self.outgoing.remove(&s).is_some() {
					println!("[RELIABLE] ACK seq={} from {}", s, self.remote_addr);
				}
			}
			Ok(Vec::new())
		}
        _ => Err("unknown reliable packet type".to_string())
    }
}
	
	async fn flush_acks(&mut self) -> Result<(), String> {
		if self.pending_acks.is_empty() { return Ok(()); }
		let mut packet = vec![RELIABLE_ACK];
		for seq in self.pending_acks.drain(..) {
			packet.extend_from_slice(&seq.to_be_bytes());
		}
		self.udp.send_to(&packet, self.remote_addr).await.map_err(|e| e.to_string())?;
		println!("[RELIABLE] Sent ACK ({} seqs) to {}", (packet.len() - 1) / 4, self.remote_addr);
		Ok(())
	}

	pub async fn tick(&mut self) -> Result<(), String> {
		let now = Instant::now();
		let timeout = Duration::from_millis(self.config.retransmit_timeout_ms);
		
		self.flush_acks().await?;

		let cutoff = now - Duration::from_secs(self.config.old_seq_cleanup_secs);
		self.received_seqs.retain(|_, time| *time > cutoff);

		let mut failed_seqs = Vec::new();

		let to_resend: Vec<(u32, Vec<u8>)> = self.outgoing
			.iter_mut()
			.filter(|(_, p)| now - p.last_sent_at >= timeout)
			.filter_map(|(&seq, p)| {
				if p.attempts >= self.config.max_attempts {
					failed_seqs.push(seq);
					return None;
				}
				p.last_sent_at = now;
				p.attempts += 1;
				Some((seq, p.data.clone()))
			})
			.collect();

		for (_, data) in &to_resend {
			self.udp.send_to(data, self.remote_addr).await.map_err(|e| e.to_string())?;
		}

		for seq in failed_seqs {
			self.outgoing.remove(&seq);
			return Err(format!("packet {} failed after {} attempts", seq, self.config.max_attempts));
		}

		self.out_of_order_buffer.retain(|order_id, _| {
			*order_id < self.next_expected_order_id + 1000
		});
		
		self.fragment_assembler.retain(|order_id, partial| {
			let age = now.duration_since(partial.first_received_at).as_secs();
			if age >= FRAGMENT_ASSEMBLY_TIMEOUT_SEC {
				println!("[RELIABLE] Dropping incomplete fragmented message order_id={} (timeout)", order_id);
				false
			} else {
				true
			}
		});

		Ok(())
	}

    #[allow(dead_code)]
    pub fn last_activity(&self) -> Instant {
        self.last_activity
    }

    #[allow(dead_code)]
    pub fn stats(&self) -> (usize, usize) {
        (self.outgoing.len(), self.received_seqs.len())
    }
}