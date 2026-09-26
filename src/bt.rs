use anyhow::{anyhow, Result};
use bluer::rfcomm::{SocketAddr, Stream};
use bluer::{Address, Device, Session};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::gaia::*;
use crate::state::{now_secs, PairedDevice, SharedCache};

pub const COMMON_CHANNELS: [u8; 6] = [2, 1, 14, 15, 11, 12];

pub struct GaiaClient {
    stream: Stream,
    buffer: Vec<u8>,
}

impl GaiaClient {
    pub async fn connect(target: Address, channel: u8) -> Result<Self> {
        let sa = SocketAddr::new(target, channel);
        let stream = Stream::connect(sa).await?;
        Ok(Self {
            stream,
            buffer: Vec::with_capacity(1024),
        })
    }

    pub async fn exchange(
        &mut self,
        command: u16,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<GaiaPacket> {
        let pkt = GaiaPacket::new(command, payload.to_vec());
        self.stream.write_all(&pkt.to_bytes()).await?;

        let expected_cmd = GaiaPacket::expected_response(command);
        let expected_ack = command | 0x0180;

        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            // Check if packet already in buffer
            let (packets, consumed) = parse_packets(&self.buffer);
            if consumed > 0 {
                self.buffer.drain(0..consumed);
            }

            for p in packets {
                if p.vendor == VENDOR_SENNHEISER && (p.command == expected_cmd || p.command == expected_ack) {
                    return Ok(p);
                }
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(anyhow!("timeout waiting for response to command {:#06x}", command));
            }

            let mut tmp = [0u8; 512];
            tokio::select! {
                res = self.stream.read(&mut tmp) => {
                    let n = res?;
                    if n == 0 {
                        return Err(anyhow!("RFCOMM connection closed"));
                    }
                    self.buffer.extend_from_slice(&tmp[..n]);
                }
                _ = tokio::time::sleep(remaining) => {
                    return Err(anyhow!("timeout waiting for response to command {:#06x}", command));
                }
            }
        }
    }

    pub async fn read_incoming(&mut self) -> Result<Vec<GaiaPacket>> {
        let mut tmp = [0u8; 512];
        let n = self.stream.read(&mut tmp).await?;
        if n == 0 {
            return Err(anyhow!("RFCOMM connection closed"));
        }
        self.buffer.extend_from_slice(&tmp[..n]);
        let (packets, consumed) = parse_packets(&self.buffer);
        if consumed > 0 {
            self.buffer.drain(0..consumed);
        }
        Ok(packets)
    }

    pub async fn send(&mut self, command: u16, payload: &[u8]) -> Result<()> {
        let pkt = GaiaPacket::new(command, payload.to_vec());
        self.stream.write_all(&pkt.to_bytes()).await?;
        Ok(())
    }

    pub async fn exchange_raw(
        &mut self,
        vendor: u16,
        command: u16,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Vec<GaiaPacket>> {
        let pkt = GaiaPacket {
            vendor,
            command,
            payload: payload.to_vec(),
        };
        self.stream.write_all(&pkt.to_bytes()).await?;

        let deadline = tokio::time::Instant::now() + timeout;
        let mut responses = Vec::new();

        loop {
            let (packets, consumed) = parse_packets(&self.buffer);
            if consumed > 0 {
                self.buffer.drain(0..consumed);
            }

            for p in packets {
                let is_match = p.command == (command | 0x0100)
                    || p.command == (command | 0x0180)
                    || p.command == (command | 0x8000)
                    || p.command == (command | 0x8100)
                    || (p.command & 0x00FF) == (command & 0x00FF)
                    || p.command == command;
                if is_match {
                    responses.push(p);
                }
            }

            if !responses.is_empty() {
                // Wait briefly (60ms) for any multi-part packets to arrive
                let mut tmp = [0u8; 512];
                if let Ok(Ok(n)) = tokio::time::timeout(Duration::from_millis(60), self.stream.read(&mut tmp)).await {
                    if n > 0 {
                        self.buffer.extend_from_slice(&tmp[..n]);
                        let (more, c2) = parse_packets(&self.buffer);
                        if c2 > 0 {
                            self.buffer.drain(0..c2);
                        }
                        responses.extend(more);
                    }
                }
                return Ok(responses);
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(anyhow!("timeout waiting for response to command {:#06x}", command));
            }

            let mut tmp = [0u8; 512];
            tokio::select! {
                res = self.stream.read(&mut tmp) => {
                    let n = res?;
                    if n == 0 {
                        return Err(anyhow!("RFCOMM connection closed"));
                    }
                    self.buffer.extend_from_slice(&tmp[..n]);
                }
                _ = tokio::time::sleep(remaining) => {
                    return Err(anyhow!("timeout waiting for response to command {:#06x}", command));
                }
            }
        }
    }
}

pub async fn query_paired_devices(client: &mut GaiaClient) -> Result<Vec<PairedDevice>> {
    let resp = client.exchange(CMD_GET_PAIRED_DEVICE_COUNT, &[], Duration::from_millis(1500)).await?;
    let count = if resp.payload.len() >= 2 {
        ((resp.payload[0] as usize) << 8) | (resp.payload[1] as usize)
    } else if let Some(&c) = resp.payload.first() {
        c as usize
    } else {
        0
    };

    let mut devices = Vec::new();
    let check_limit = count.max(1).min(8);
    for i in 0..check_limit {
        if let Ok(entry) = client.exchange(CMD_GET_PAIRED_DEVICE_INFO, &[i as u8], Duration::from_millis(1000)).await {
            let p = entry.payload;
            if p.len() >= 3 && p[0] != 0xFF {
                let idx = p[0];
                let connected = p[2] != 0;
                let name_bytes = if p.len() > 3 {
                    let raw = &p[3..];
                    if let Some(pos) = raw.iter().position(|&b| b == 0) {
                        &raw[..pos]
                    } else {
                        raw
                    }
                } else {
                    &[]
                };
                let name = String::from_utf8_lossy(name_bytes).trim().to_string();
                devices.push(PairedDevice {
                    index: idx,
                    name,
                    connected,
                });
            }
        }
    }
    Ok(devices)
}

pub async fn find_momentum_device(session: &Session) -> Result<Option<Device>> {
    let adapter = session.default_adapter().await?;
    for addr in adapter.device_addresses().await? {
        let dev = adapter.device(addr)?;
        let name = dev.name().await?.unwrap_or_default().to_lowercase();
        let alias = dev.alias().await.unwrap_or_default().to_lowercase();
        let uuids = dev.uuids().await?.unwrap_or_default();

        let is_momentum = name.contains("momentum")
            || alias.contains("momentum")
            || name.contains("accentum")
            || alias.contains("accentum")
            || uuids.iter().any(|u| {
                let s = u.to_string();
                s.contains("deca-fade") || s.contains("a2129ff3")
            });

        if is_momentum {
            return Ok(Some(dev));
        }
    }
    Ok(None)
}

pub fn format_codec(raw: &str) -> String {
    match raw.trim().to_lowercase().as_str() {
        "aptx_hd" => "aptX HD".to_string(),
        "aptx_adaptive" => "aptX Adaptive".to_string(),
        "aptx_ll" => "aptX LL".to_string(),
        "aptx" => "aptX".to_string(),
        "ldac" => "LDAC".to_string(),
        "aac" => "AAC".to_string(),
        "sbc" => "SBC".to_string(),
        "sbc_xq" => "SBC-XQ".to_string(),
        "lc3" => "LC3".to_string(),
        "opus" => "Opus".to_string(),
        other => other.replace('_', " ").to_uppercase(),
    }
}

pub fn detect_audio_codec(mac: &str) -> Option<String> {
    if mac.is_empty() {
        return None;
    }
    let mac_under = mac.replace(':', "_").to_lowercase();
    let mac_colon = mac.to_lowercase();

    // 1. Try pactl list sinks
    if let Ok(output) = std::process::Command::new("pactl")
        .args(["list", "sinks"])
        .output()
    {
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let mut current_sink_match = false;
            for line in stdout.lines() {
                let trimmed = line.trim();
                if trimmed.starts_with("Name:") {
                    let lower = trimmed.to_lowercase();
                    current_sink_match = lower.contains(&mac_under) || lower.contains(&mac_colon);
                } else if current_sink_match && trimmed.contains("api.bluez5.codec") {
                    if let Some(idx) = trimmed.find('=') {
                        let val = trimmed[idx + 1..].trim().trim_matches('"');
                        return Some(format_codec(val));
                    }
                }
            }
        }
    }

    // 2. Try pw-dump fallback
    if let Ok(output) = std::process::Command::new("pw-dump").output() {
        if output.status.success() {
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
                if let Some(arr) = val.as_array() {
                    for item in arr {
                        let props = &item["info"]["props"];
                        if let Some(addr) = props["api.bluez5.address"].as_str() {
                            if addr.eq_ignore_ascii_case(mac) {
                                if let Some(codec) = props["api.bluez5.codec"].as_str() {
                                    return Some(format_codec(codec));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    None
}

#[allow(dead_code)]
pub enum DeviceRequest {
    SetAnc(bool, tokio::sync::oneshot::Sender<Result<bool>>),
    SetTransparency(u8, tokio::sync::oneshot::Sender<Result<u8>>),
    SetBassBoost(bool, tokio::sync::oneshot::Sender<Result<bool>>),
    SetSidetone(u8, tokio::sync::oneshot::Sender<Result<u8>>),
    SetWearDetection(bool, tokio::sync::oneshot::Sender<Result<bool>>),
    GetDevices(tokio::sync::oneshot::Sender<Result<Vec<PairedDevice>>>),
    ConnectDevice(u8, tokio::sync::oneshot::Sender<Result<()>>),
    DisconnectDevice(u8, tokio::sync::oneshot::Sender<Result<()>>),
    Raw(
        u16,
        u16,
        Vec<u8>,
        Duration,
        tokio::sync::oneshot::Sender<Result<Vec<GaiaPacket>>>,
    ),
}

pub async fn run_bt_loop(
    cache: SharedCache,
    mut req_rx: mpsc::Receiver<DeviceRequest>,
) {
    let session = match Session::new().await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to create BlueZ session: {}", e);
            return;
        }
    };

    loop {
        // Step 1: Check if device is connected in BlueZ
        let device = match find_momentum_device(&session).await {
            Ok(Some(d)) => d,
            _ => {
                {
                    let mut c = cache.write().await;
                    c.device.connected = false;
                    c.device.codec = None;
                    c.paired_devices.clear();
                    c.channel = None;
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        let is_connected = device.is_connected().await.unwrap_or(false);
        let addr = device.address();
        let name = device.alias().await.unwrap_or_else(|_| "MOMENTUM".into());

        if !is_connected {
            {
                let mut c = cache.write().await;
                c.device.connected = false;
                c.device.mac = addr.to_string();
                c.device.name = name;
                c.device.codec = None;
                c.paired_devices.clear();
                c.channel = None;
            }
            while let Ok(req) = req_rx.try_recv() {
                match req {
                    DeviceRequest::SetAnc(_, reply) => { let _ = reply.send(Err(anyhow!("device not connected"))); }
                    DeviceRequest::SetTransparency(_, reply) => { let _ = reply.send(Err(anyhow!("device not connected"))); }
                    DeviceRequest::SetBassBoost(_, reply) => { let _ = reply.send(Err(anyhow!("device not connected"))); }
                    DeviceRequest::SetSidetone(_, reply) => { let _ = reply.send(Err(anyhow!("device not connected"))); }
                    DeviceRequest::SetWearDetection(_, reply) => { let _ = reply.send(Err(anyhow!("device not connected"))); }
                    DeviceRequest::GetDevices(reply) => { let _ = reply.send(Err(anyhow!("device not connected"))); }
                    DeviceRequest::ConnectDevice(_, reply) => { let _ = reply.send(Err(anyhow!("device not connected"))); }
                    DeviceRequest::DisconnectDevice(_, reply) => { let _ = reply.send(Err(anyhow!("device not connected"))); }
                    DeviceRequest::Raw(_, _, _, _, reply) => { let _ = reply.send(Err(anyhow!("device not connected"))); }
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        // Try reading native battery
        let battery = device.battery_percentage().await.ok().flatten();

        tracing::info!(address=%addr, name=%name, "Sennheiser Momentum connected in BlueZ, establishing RFCOMM");

        // Step 2: Connect RFCOMM channel
        let mut client_opt = None;
        let mut connected_channel = None;

        for &ch in &COMMON_CHANNELS {
            tracing::debug!("Trying RFCOMM channel {}", ch);
            match tokio::time::timeout(Duration::from_secs(2), GaiaClient::connect(addr, ch)).await {
                Ok(Ok(mut client)) => {
                    // Verify with GET_ANC_STATUS
                    if let Ok(resp) = client.exchange(CMD_GET_ANC_STATUS, &[], Duration::from_millis(1500)).await {
                        tracing::info!("GAIA protocol verified on channel {}", ch);
                        let anc_on = resp.payload.first().copied().unwrap_or(1) != 0;
                        {
                            let mut c = cache.write().await;
                            c.settings.anc_enabled = anc_on;
                        }
                        client_opt = Some(client);
                        connected_channel = Some(ch);
                        break;
                    }
                }
                _ => continue,
            }
        }

        let mut client = match client_opt {
            Some(c) => c,
            None => {
                tracing::warn!("Failed to establish GAIA on RFCOMM channels, retrying in 3s");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        // Step 3: Register notifications
        for feat in [FEATURE_USER_EQ, FEATURE_TRANSPARENCY, FEATURE_ANC, FEATURE_PAIRED_DEVICES] {
            let _ = client.send(CMD_REGISTER_NOTIFICATION, &[feat]).await;
        }

        // Initial sync of settings
        if let Ok(resp) = client.exchange(CMD_GET_TRANSPARENCY, &[], Duration::from_millis(1000)).await {
            if let Some(&val) = resp.payload.first() {
                let mut c = cache.write().await;
                c.settings.transparency = val.min(100);
            }
        }
        if let Ok(resp) = client.exchange(CMD_GET_BASS_BOOST, &[], Duration::from_millis(1000)).await {
            if let Some(&val) = resp.payload.first() {
                let mut c = cache.write().await;
                c.settings.bass_boost = val != 0;
            }
        }
        if let Ok(resp) = client.exchange(CMD_GET_SIDETONE, &[], Duration::from_millis(1000)).await {
            if let Some(&val) = resp.payload.first() {
                let mut c = cache.write().await;
                c.settings.sidetone = val;
            }
        }

        let initial_devs = query_paired_devices(&mut client).await.unwrap_or_default();
        let codec = detect_audio_codec(&addr.to_string());
        {
            let mut c = cache.write().await;
            c.device.connected = true;
            c.device.mac = addr.to_string();
            c.device.name = name.clone();
            c.device.battery = battery;
            c.device.codec = codec;
            c.paired_devices = initial_devs;
            c.channel = connected_channel;
            c.last_updated = now_secs();
        }

        tracing::info!("Momentum connection active and cache initialized!");

        // Step 4: Active connection event loop
        let mut poll_timer = tokio::time::interval(Duration::from_secs(5));
        poll_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        'active: loop {
            tokio::select! {
                _ = poll_timer.tick() => {
                    // Check BlueZ battery & audio codec
                    let bat_opt = device.battery_percentage().await.ok().flatten();
                    let codec_opt = detect_audio_codec(&addr.to_string());
                    let devs_res = query_paired_devices(&mut client).await;

                    match devs_res {
                        Ok(devs) => {
                            let mut c = cache.write().await;
                            if let Some(bat) = bat_opt {
                                c.device.battery = Some(bat);
                            }
                            if codec_opt.is_some() {
                                c.device.codec = codec_opt;
                            }
                            c.paired_devices = devs;
                            c.last_updated = now_secs();
                        }
                        Err(e) => {
                            let err_str = e.to_string();
                            if err_str.contains("os error") || err_str.contains("closed") || err_str.contains("Broken pipe") {
                                tracing::warn!("Periodic poll connection error: {}, disconnecting", e);
                                break 'active;
                            }
                        }
                    }
                }

                req = req_rx.recv() => {
                    match req {
                        Some(DeviceRequest::SetAnc(enabled, reply)) => {
                            let val = if enabled { 1u8 } else { 0u8 };
                            let res = client.exchange(CMD_SET_ANC_STATUS, &[val], Duration::from_millis(1500)).await;
                            match res {
                                Ok(_) => {
                                    {
                                        let mut c = cache.write().await;
                                        c.settings.anc_enabled = enabled;
                                        c.last_updated = now_secs();
                                    }
                                    let _ = reply.send(Ok(enabled));
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                    break 'active;
                                }
                            }
                        }
                        Some(DeviceRequest::SetTransparency(pct, reply)) => {
                            let clamped = pct.min(100);
                            let res = client.exchange(CMD_SET_TRANSPARENCY, &[clamped], Duration::from_millis(1500)).await;
                            match res {
                                Ok(_) => {
                                    {
                                        let mut c = cache.write().await;
                                        c.settings.transparency = clamped;
                                        c.last_updated = now_secs();
                                    }
                                    let _ = reply.send(Ok(clamped));
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                    break 'active;
                                }
                            }
                        }
                        Some(DeviceRequest::SetBassBoost(enabled, reply)) => {
                            let val = if enabled { 1u8 } else { 0u8 };
                            let res = client.exchange(CMD_SET_BASS_BOOST, &[val], Duration::from_millis(1500)).await;
                            match res {
                                Ok(_) => {
                                    {
                                        let mut c = cache.write().await;
                                        c.settings.bass_boost = enabled;
                                        c.last_updated = now_secs();
                                    }
                                    let _ = reply.send(Ok(enabled));
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                    break 'active;
                                }
                            }
                        }
                        Some(DeviceRequest::SetSidetone(lvl, reply)) => {
                            let res = client.exchange(CMD_SET_SIDETONE, &[lvl], Duration::from_millis(1500)).await;
                            match res {
                                Ok(_) => {
                                    {
                                        let mut c = cache.write().await;
                                        c.settings.sidetone = lvl;
                                        c.last_updated = now_secs();
                                    }
                                    let _ = reply.send(Ok(lvl));
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                    break 'active;
                                }
                            }
                        }
                        Some(DeviceRequest::SetWearDetection(enabled, reply)) => {
                            let val = if enabled { 1u8 } else { 0u8 };
                            let res = client.exchange(CMD_SET_WEAR_DETECTION, &[val], Duration::from_millis(1500)).await;
                            match res {
                                Ok(_) => {
                                    {
                                        let mut c = cache.write().await;
                                        c.settings.wear_detection = enabled;
                                        c.last_updated = now_secs();
                                    }
                                    let _ = reply.send(Ok(enabled));
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                    break 'active;
                                }
                            }
                        }
                        Some(DeviceRequest::GetDevices(reply)) => {
                            let res = query_paired_devices(&mut client).await;
                            match res {
                                Ok(devs) => {
                                    {
                                        let mut c = cache.write().await;
                                        c.paired_devices = devs.clone();
                                        c.last_updated = now_secs();
                                    }
                                    let _ = reply.send(Ok(devs));
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(anyhow!("{}", e)));
                                    break 'active;
                                }
                            }
                        }
                        Some(DeviceRequest::ConnectDevice(idx, reply)) => {
                            let res = client.exchange(CMD_CONNECT_DEVICE, &[idx], Duration::from_millis(2000)).await;
                            match res {
                                Ok(_) => {
                                    tokio::time::sleep(Duration::from_millis(300)).await;
                                    if let Ok(devs) = query_paired_devices(&mut client).await {
                                        let mut c = cache.write().await;
                                        c.paired_devices = devs;
                                        c.last_updated = now_secs();
                                    }
                                    let _ = reply.send(Ok(()));
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                    break 'active;
                                }
                            }
                        }
                        Some(DeviceRequest::DisconnectDevice(idx, reply)) => {
                            let res = client.exchange(CMD_DISCONNECT_DEVICE, &[idx], Duration::from_millis(2000)).await;
                            match res {
                                Ok(_) => {
                                    tokio::time::sleep(Duration::from_millis(300)).await;
                                    if let Ok(devs) = query_paired_devices(&mut client).await {
                                        let mut c = cache.write().await;
                                        c.paired_devices = devs;
                                        c.last_updated = now_secs();
                                    }
                                    let _ = reply.send(Ok(()));
                                }
                                Err(e) => {
                                    let _ = reply.send(Err(e));
                                    break 'active;
                                }
                            }
                        }
                        Some(DeviceRequest::Raw(vendor, command, payload, timeout, reply)) => {
                            let res = client.exchange_raw(vendor, command, &payload, timeout).await;
                            let is_disconnect = res.is_err();
                            let _ = reply.send(res);
                            if is_disconnect {
                                break 'active;
                            }
                        }
                        None => break 'active,
                    }
                }

                // Handle live incoming notifications (e.g. user touches cup, changes volume, slides transparency)
                inc = client.read_incoming() => {
                    match inc {
                        Ok(packets) => {
                            for p in packets {
                                if p.command == 0x1B03 || p.command == 0x1A03 {
                                    if let Some(&val) = p.payload.first() {
                                        let mut c = cache.write().await;
                                        c.settings.transparency = val.min(100);
                                        c.last_updated = now_secs();
                                    }
                                } else if p.command == 0x1B05 || p.command == 0x1A05 {
                                    if let Some(&val) = p.payload.first() {
                                        let mut c = cache.write().await;
                                        c.settings.anc_enabled = val != 0;
                                        c.last_updated = now_secs();
                                    }
                                } else if p.command == 0x1109 || p.command == 0x1009 {
                                    if let Some(&val) = p.payload.first() {
                                        let mut c = cache.write().await;
                                        c.settings.bass_boost = val != 0;
                                        c.last_updated = now_secs();
                                    }
                                } else if p.command == 0x1B14 || p.command == 0x1501 || p.command == 0x1504 {
                                    if let Ok(devs) = query_paired_devices(&mut client).await {
                                        let mut c = cache.write().await;
                                        c.paired_devices = devs;
                                        c.last_updated = now_secs();
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("RFCOMM read error: {}, disconnecting", e);
                            break 'active;
                        }
                    }
                }
            }
        }

        {
            let mut c = cache.write().await;
            c.device.connected = false;
            c.paired_devices.clear();
            c.channel = None;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
