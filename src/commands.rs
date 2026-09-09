use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::bt::DeviceRequest;
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
        match cmd.to_lowercase().as_str() {
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

            _ => json!({"status": "error", "message": format!("unknown command: {}", cmd)}),
        }
    }
}
