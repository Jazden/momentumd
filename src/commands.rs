use std::time::Duration;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::bt::DeviceRequest;
use crate::gaia::VENDOR_SENNHEISER;
use crate::state::SharedCache;

pub struct CommandHandler {
    cache: SharedCache,
    tx: mpsc::Sender<DeviceRequest>,
}

impl CommandHandler {
    pub fn new(cache: SharedCache, tx: mpsc::Sender<DeviceRequest>) -> Self {
        Self { cache, tx }
    }

    pub async fn handle_line(&self, line: &str) -> Value {
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            return json!({"status": "error", "message": "empty command"});
        }

        // Also check if line is a JSON object like {"cmd": "status"}
        if line.trim().starts_with('{') {
            if let Ok(v) = serde_json::from_str::<Value>(line) {
                if let Some(cmd) = v.get("cmd").and_then(|c| c.as_str()) {
                    let mut args = Vec::new();
                    if let Some(a) = v.get("args").and_then(|a| a.as_array()) {
                        for item in a {
                            if let Some(s) = item.as_str() {
                                args.push(s);
                            }
                        }
                    }
                    return self.dispatch(cmd, &args).await;
                }
            }
        }

        self.dispatch(parts[0], &parts[1..]).await
    }

    pub async fn dispatch(&self, cmd: &str, args: &[&str]) -> Value {
        let cmd_clean = cmd.to_lowercase();
        let is_query = match cmd_clean.as_str() {
            "status" | "info" | "get-battery" | "battery" | "devices" | "get-devices" => true,
            "get-anc" | "anc" | "get-transparency" | "transparency" | "get-bass-boost"
            | "bass-boost" | "bass" | "get-sidetone" | "sidetone" => args.is_empty(),
            _ => false,
        };

        if !is_query {
            let c = self.cache.read().await;
            if !c.device.connected {
                return json!({"status": "error", "message": "device not connected"});
            }
        }

        match cmd_clean.as_str() {
            "status" | "info" => {
                let c = self.cache.read().await;
                serde_json::to_value(c.to_full_status()).unwrap_or_else(|_| json!({"status": "error"}))
            }

            "get-anc" | "anc" if args.is_empty() => {
                let c = self.cache.read().await;
                json!({
                    "status": "ok",
                    "anc_enabled": c.settings.anc_enabled,
                    "mode": if c.settings.anc_enabled { "active" } else { "off" }
                })
            }

            "set-anc" | "anc" => {
                if args.is_empty() {
                    return json!({"status": "error", "message": "usage: set-anc <true|false|active|off>"});
                }
                let target = match args[0].to_lowercase().as_str() {
                    "true" | "1" | "on" | "active" | "anc" => true,
                    "false" | "0" | "off" => false,
                    _ => return json!({"status": "error", "message": "invalid anc state"}),
                };

                let (reply_tx, reply_rx) = oneshot::channel();
                if self.tx.send(DeviceRequest::SetAnc(target, reply_tx)).await.is_err() {
                    return json!({"status": "error", "message": "device connection offline"});
                }

                match reply_rx.await {
                    Ok(Ok(val)) => json!({"status": "ok", "anc_enabled": val}),
                    Ok(Err(e)) => json!({"status": "error", "message": e.to_string()}),
                    Err(_) => json!({"status": "error", "message": "internal channel error"}),
                }
            }

            "cycle-anc" | "cycle" | "cycle-noise" => {
                let curr_anc = {
                    let c = self.cache.read().await;
                    c.settings.anc_enabled
                };
                let next_anc = !curr_anc;

                let (reply_tx, reply_rx) = oneshot::channel();
                if self.tx.send(DeviceRequest::SetAnc(next_anc, reply_tx)).await.is_err() {
                    return json!({"status": "error", "message": "device connection offline"});
                }

                match reply_rx.await {
                    Ok(Ok(val)) => json!({"status": "ok", "anc_enabled": val, "cycled": true}),
                    Ok(Err(e)) => json!({"status": "error", "message": e.to_string()}),
                    Err(_) => json!({"status": "error", "message": "internal channel error"}),
                }
            }

            "get-transparency" | "transparency" if args.is_empty() => {
                let c = self.cache.read().await;
                json!({"status": "ok", "transparency": c.settings.transparency})
            }

            "set-transparency" | "transparency" => {
                if args.is_empty() {
                    return json!({"status": "error", "message": "usage: set-transparency <0-100>"});
                }
                let val: u8 = match args[0].parse() {
                    Ok(n) => n,
                    Err(_) => return json!({"status": "error", "message": "invalid transparency value (0-100)"}),
                };

                let (reply_tx, reply_rx) = oneshot::channel();
                if self.tx.send(DeviceRequest::SetTransparency(val, reply_tx)).await.is_err() {
                    return json!({"status": "error", "message": "device connection offline"});
                }

                match reply_rx.await {
                    Ok(Ok(v)) => json!({"status": "ok", "transparency": v}),
                    Ok(Err(e)) => json!({"status": "error", "message": e.to_string()}),
                    Err(_) => json!({"status": "error", "message": "internal channel error"}),
                }
            }

            "get-bass-boost" | "bass-boost" | "bass" if args.is_empty() => {
                let c = self.cache.read().await;
                json!({"status": "ok", "bass_boost": c.settings.bass_boost})
            }

            "set-bass-boost" | "bass-boost" | "bass" => {
                if args.is_empty() {
                    return json!({"status": "error", "message": "usage: set-bass-boost <true|false|on|off>"});
                }
                let target = match args[0].to_lowercase().as_str() {
                    "true" | "1" | "on" => true,
                    "false" | "0" | "off" => false,
                    _ => return json!({"status": "error", "message": "invalid bass boost state"}),
                };

                let (reply_tx, reply_rx) = oneshot::channel();
                if self.tx.send(DeviceRequest::SetBassBoost(target, reply_tx)).await.is_err() {
                    return json!({"status": "error", "message": "device connection offline"});
                }

                match reply_rx.await {
                    Ok(Ok(val)) => json!({"status": "ok", "bass_boost": val}),
                    Ok(Err(e)) => json!({"status": "error", "message": e.to_string()}),
                    Err(_) => json!({"status": "error", "message": "internal channel error"}),
                }
            }

            "get-battery" | "battery" => {
                let c = self.cache.read().await;
                json!({
                    "status": "ok",
                    "battery": c.device.battery,
                    "connected": c.device.connected
                })
            }

            "get-sidetone" | "sidetone" if args.is_empty() => {
                let c = self.cache.read().await;
                json!({"status": "ok", "sidetone": c.settings.sidetone})
            }

            "set-sidetone" | "sidetone" => {
                if args.is_empty() {
                    return json!({"status": "error", "message": "usage: set-sidetone <0-100>"});
                }
                let val: u8 = match args[0].parse() {
                    Ok(n) => n,
                    Err(_) => return json!({"status": "error", "message": "invalid sidetone value"}),
                };

                let (reply_tx, reply_rx) = oneshot::channel();
                if self.tx.send(DeviceRequest::SetSidetone(val, reply_tx)).await.is_err() {
                    return json!({"status": "error", "message": "device connection offline"});
                }

                match reply_rx.await {
                    Ok(Ok(v)) => json!({"status": "ok", "sidetone": v}),
                    Ok(Err(e)) => json!({"status": "error", "message": e.to_string()}),
                    Err(_) => json!({"status": "error", "message": "internal channel error"}),
                }
            }

            "get-devices" | "devices" => {
                let (reply_tx, reply_rx) = oneshot::channel();
                if self.tx.send(DeviceRequest::GetDevices(reply_tx)).await.is_err() {
                    let c = self.cache.read().await;
                    return json!({
                        "status": "ok",
                        "devices": c.paired_devices
                    });
                }
                match reply_rx.await {
                    Ok(Ok(devs)) => json!({"status": "ok", "devices": devs}),
                    Ok(Err(_)) => {
                        let c = self.cache.read().await;
                        json!({"status": "ok", "devices": c.paired_devices})
                    }
                    Err(_) => {
                        let c = self.cache.read().await;
                        json!({"status": "ok", "devices": c.paired_devices})
                    }
                }
            }

            "connect-device" | "switch-device" | "switch" => {
                if args.is_empty() {
                    return json!({"status": "error", "message": "usage: switch-device <index|name>"});
                }
                let target = args.join(" ").to_lowercase();
                let index_opt: Option<u8> = if let Ok(idx) = target.parse::<u8>() {
                    Some(idx)
                } else {
                    let c = self.cache.read().await;
                    c.paired_devices.iter().find(|d| d.name.to_lowercase().contains(&target)).map(|d| d.index)
                };

                let index = match index_opt {
                    Some(idx) => idx,
                    None => return json!({"status": "error", "message": format!("device '{}' not found in paired list", target)}),
                };

                let (reply_tx, reply_rx) = oneshot::channel();
                if self.tx.send(DeviceRequest::ConnectDevice(index, reply_tx)).await.is_err() {
                    return json!({"status": "error", "message": "device connection offline"});
                }

                match reply_rx.await {
                    Ok(Ok(_)) => json!({"status": "ok", "action": "connect", "device_index": index}),
                    Ok(Err(e)) => json!({"status": "error", "message": e.to_string()}),
                    Err(_) => json!({"status": "error", "message": "internal channel error"}),
                }
            }

            "disconnect-device" => {
                if args.is_empty() {
                    return json!({"status": "error", "message": "usage: disconnect-device <index|name>"});
                }
                let target = args.join(" ").to_lowercase();
                let index_opt: Option<u8> = if let Ok(idx) = target.parse::<u8>() {
                    Some(idx)
                } else {
                    let c = self.cache.read().await;
                    c.paired_devices.iter().find(|d| d.name.to_lowercase().contains(&target)).map(|d| d.index)
                };

                let index = match index_opt {
                    Some(idx) => idx,
                    None => return json!({"status": "error", "message": format!("device '{}' not found in paired list", target)}),
                };

                let (reply_tx, reply_rx) = oneshot::channel();
                if self.tx.send(DeviceRequest::DisconnectDevice(index, reply_tx)).await.is_err() {
                    return json!({"status": "error", "message": "device connection offline"});
                }

                match reply_rx.await {
                    Ok(Ok(_)) => json!({"status": "ok", "action": "disconnect", "device_index": index}),
                    Ok(Err(e)) => json!({"status": "error", "message": e.to_string()}),
                    Err(_) => json!({"status": "error", "message": "internal channel error"}),
                }
            }

            "raw" => {
                if args.is_empty() {
                    return json!({
                        "status": "error",
                        "message": "usage: raw [vendor_hex] <command_hex> [payload_hex] [timeout_ms]"
                    });
                }
                let mut vendor = VENDOR_SENNHEISER;
                let mut cmd_str = args[0];
                let mut payload_idx = 1;

                if args.len() >= 2 {
                    if let (Some(v), Some(_)) = (parse_hex_u16(args[0]), parse_hex_u16(args[1])) {
                        if v == 0x0495 || v == 0x000A || v == 0x001B || v < 0x0100 {
                            vendor = v;
                            cmd_str = args[1];
                            payload_idx = 2;
                        }
                    }
                }

                let command = match parse_hex_u16(cmd_str) {
                    Some(c) => c,
                    None => return json!({"status": "error", "message": format!("invalid command hex: {}", cmd_str)}),
                };

                let mut payload = Vec::new();
                let mut timeout = Duration::from_millis(1500);

                if args.len() > payload_idx {
                    let p_str = args[payload_idx];
                    if p_str.is_empty() || p_str == "-" || p_str == "none" {
                        payload = Vec::new();
                    } else if let Some(bytes) = hex_decode(p_str) {
                        payload = bytes;
                    } else if let Ok(ms) = p_str.parse::<u64>() {
                        timeout = Duration::from_millis(ms);
                    } else {
                        return json!({"status": "error", "message": "invalid payload hex"});
                    }
                }
                if args.len() > payload_idx + 1 {
                    if let Ok(ms) = args[payload_idx + 1].parse::<u64>() {
                        timeout = Duration::from_millis(ms);
                    }
                }

                let (reply_tx, reply_rx) = oneshot::channel();
                if self.tx.send(DeviceRequest::Raw(vendor, command, payload, timeout, reply_tx)).await.is_err() {
                    return json!({"status": "error", "message": "device connection offline"});
                }

                match reply_rx.await {
                    Ok(Ok(packets)) => {
                        let pkts_json: Vec<Value> = packets.iter().map(|p| {
                            json!({
                                "vendor": format!("{:#06x}", p.vendor),
                                "command": format!("{:#06x}", p.command),
                                "payload_hex": hex_encode(&p.payload),
                                "payload_bytes": p.payload,
                                "payload_ascii": String::from_utf8_lossy(&p.payload).to_string(),
                            })
                        }).collect();
                        json!({
                            "status": "ok",
                            "count": pkts_json.len(),
                            "packets": pkts_json,
                        })
                    }
                    Ok(Err(e)) => json!({"status": "error", "message": e.to_string()}),
                    Err(_) => json!({"status": "error", "message": "internal channel error"}),
                }
            }

            _ => json!({"status": "error", "message": format!("unknown command: {}", cmd)}),
        }
    }
}

fn parse_hex_u16(s: &str) -> Option<u16> {
    let clean = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    u16::from_str_radix(clean, 16).ok()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let clean = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    if clean.is_empty() {
        return Some(Vec::new());
    }
    if clean.len() % 2 != 0 {
        return None;
    }
    (0..clean.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&clean[i..i + 2], 16).ok())
        .collect()
}

