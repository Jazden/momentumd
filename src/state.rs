use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub mac: String,
    pub name: String,
    pub connected: bool,
    pub battery: Option<u8>,
    pub codec: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioSettings {
    pub anc_enabled: bool,
    pub transparency: u8,
    pub bass_boost: bool,
    pub sidetone: u8,
    pub wear_detection: bool,
}

impl Default for AudioSettings {
    fn default() -> Self {
        Self {
            anc_enabled: true,
            transparency: 50,
            bass_boost: false,
            sidetone: 1,
            wear_detection: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairedDevice {
    pub index: u8,
    pub name: String,
    pub connected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullStatus {
    pub status: String,
    pub connected: bool,
    pub device: DeviceInfo,
    pub settings: AudioSettings,
    pub paired_devices: Vec<PairedDevice>,
    pub channel: Option<u8>,
    pub timestamp: f64,
}

pub struct StateCache {
    pub device: DeviceInfo,
    pub settings: AudioSettings,
    pub paired_devices: Vec<PairedDevice>,
    pub channel: Option<u8>,
    pub last_updated: f64,
}

impl StateCache {
    pub fn new() -> Self {
        Self {
            device: DeviceInfo {
                mac: String::new(),
                name: "Sennheiser Momentum".to_string(),
                connected: false,
                battery: None,
                codec: None,
            },
            settings: AudioSettings::default(),
            paired_devices: Vec::new(),
            channel: None,
            last_updated: 0.0,
        }
    }

    pub fn to_full_status(&self) -> FullStatus {
        FullStatus {
            status: "ok".to_string(),
            connected: self.device.connected,
            device: self.device.clone(),
            settings: self.settings.clone(),
            paired_devices: self.paired_devices.clone(),
            channel: self.channel,
            timestamp: self.last_updated,
        }
    }
}

pub type SharedCache = Arc<RwLock<StateCache>>;

pub fn now_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}
