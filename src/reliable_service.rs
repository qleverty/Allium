use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;

pub struct ReliableService {
    remote_addr: SocketAddr,
    udp: Arc<UdpSocket>,
    config: ReliableConfig,
    outgoing: HashMap<u32, PendingPacket>,
    next_seq: u32,
    received_seqs: HashMap<u32, Instant>,
    last_activity: Instant,
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
            received_seqs: HashMap::new(),
            last_activity: Instant::now(),
        }
    }

    pub async fn send(&mut self, payload: Vec<u8>) -> Result<u32, String> {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);

        let mut packet = Vec::with_capacity(5 + payload.len());
        packet.push(0x10); // ReliableData
        packet.extend_from_slice(&seq.to_be_bytes());
        packet.extend_from_slice(&payload);

        self.udp.send_to(&packet, self.remote_addr).await.map_err(|e| e.to_string())?;

        let now = Instant::now();
        self.outgoing.insert(seq, PendingPacket {
            data: packet,
            first_sent_at: now,
            last_sent_at: now,
            attempts: 1,
        });

        Ok(seq)
    }

    pub async fn handle_incoming(&mut self, packet: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.last_activity = Instant::now();

        if packet.len() < 5 {
            return Err("packet too short".to_string());
        }

        let packet_type = packet[0];
        let seq = u32::from_be_bytes([packet[1], packet[2], packet[3], packet[4]]);

        match packet_type {
            0x10 => {
                let cutoff = Instant::now() - Duration::from_secs(self.config.old_seq_cleanup_secs);
                self.received_seqs.retain(|_, time| *time > cutoff);

                if self.received_seqs.contains_key(&seq) {
                    self.send_ack(seq).await?;
                    return Ok(None);
                }

                self.received_seqs.insert(seq, Instant::now());
                self.send_ack(seq).await?;

                Ok(Some(packet[5..].to_vec()))
            }
            0x11 => {
                if self.outgoing.remove(&seq).is_some() {
                    println!("[RELIABLE] Received ACK seq={} from {}", seq, self.remote_addr);
                }
                Ok(None)
            }
            _ => Err("unknown reliable packet type".to_string())
        }
    }

    async fn send_ack(&self, seq: u32) -> Result<(), String> {
        let mut packet = vec![0x11]; // ReliableAck
        packet.extend_from_slice(&seq.to_be_bytes());
        self.udp.send_to(&packet, self.remote_addr).await.map_err(|e| e.to_string())?;
        println!("[RELIABLE] Sent ACK seq={} to {}", seq, self.remote_addr);
        Ok(())
    }

    pub async fn tick(&mut self) -> Result<(), String> {
        let now = Instant::now();
        let timeout = Duration::from_millis(self.config.retransmit_timeout_ms);

        // Cleanup old received seqs (fix memory leak when client is silent)
        let cutoff = now - Duration::from_secs(self.config.old_seq_cleanup_secs);
        self.received_seqs.retain(|_, time| *time > cutoff);

        let mut failed_seqs = Vec::new();

        for (seq, pending) in &mut self.outgoing {
            if now - pending.last_sent_at < timeout {
                continue;
            }

            if pending.attempts >= self.config.max_attempts {
                failed_seqs.push(*seq);
                continue;
            }

            self.udp.send_to(&pending.data, self.remote_addr).await.map_err(|e| e.to_string())?;
            pending.last_sent_at = now;
            pending.attempts += 1;
        }

        for seq in failed_seqs {
            self.outgoing.remove(&seq);
            return Err(format!("packet {} failed after {} attempts", seq, self.config.max_attempts));
        }

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