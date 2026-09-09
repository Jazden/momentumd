# `momentumd`

**Ultra-fast persistent connection daemon and CLI for Sennheiser Momentum headphones on Linux.**

Supports the **Sennheiser Momentum series** (Momentum 4 Wireless, Momentum 3, Momentum True Wireless, Accentum) using the Qualcomm GAIA v3 protocol over Bluetooth Classic RFCOMM.

Maintains a persistent connection to eliminate the ~1.5s cold-connection penalty, executing ANC toggles and transparency shifts in **~20ms** with **< 1 MB RAM** and **0% idle CPU**.

---

## ⚡ Highlights

- **Near-Instant (~20ms) Controls**: Holds the RFCOMM control channel open, switching noise control and sound profiles instantaneously.
- **Ultra-Lightweight Footprint**:
  - **RAM**: Under **1 MB** RSS (~600 KB typical).
  - **CPU**: **0.0%** usage when idle (kernel `epoll` driven).
  - **Single Binary**: Stripped, LTO-optimized binary functions as both the background daemon and the lightning-fast CLI client.
- **Full GAIA v3 Support**:
  - Continuous Transparency Balance (0% Max ANC to 100% Max Transparency)
  - Active Noise Cancellation (ANC) on/off
  - Bass Boost on/off
  - Sidetone monitor volume
  - Smart Pause (Wear Detection)
  - Live touchpad gesture notification streaming
- **UNIX Domain Socket API**: Exposes `${XDG_RUNTIME_DIR}/momentum.sock` for desktop widgets (Noctalia, Waybar, Polybar) and scripts.

---

## 🚀 Installation

### 1. Build and Install Binary

Ensure build dependencies are installed (e.g., `libdbus-1-dev` / `dbus`, `pkg-config`).

```bash
git clone https://github.com/Jazden/momentumd.git
cd momentumd
cargo build --release
cp target/release/momentumd ~/.cargo/bin/momentumd
```

### 2. Enable & Start Systemd User Service

```bash
mkdir -p ~/.config/systemd/user
cp momentumd.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now momentumd
```

Verify service status:
```bash
systemctl --user status momentumd
```

---

## 💻 CLI Usage

The `momentumd` binary or the `./momentum` bash wrapper communicates with the running daemon:

```bash
# Query full device status in < 5ms
momentumd status

# Check or set ANC
momentumd get-anc
momentumd set-anc active
momentumd set-anc off
momentumd cycle-anc

# Check or adjust transparency balance (0 to 100)
momentumd get-transparency
momentumd set-transparency 75

# Check or toggle Bass Boost
momentumd get-bass-boost
momentumd set-bass-boost on
momentumd set-bass-boost off

# Battery percentage
momentumd battery
```

Using the `./momentum` wrapper:
```bash
./momentum status
./momentum anc cycle
./momentum transparency 50
./momentum bass on
./momentum battery
```

---

## 🔌 Socket Protocol

The daemon listens on `${XDG_RUNTIME_DIR:-/tmp}/momentum.sock` (mode `0700`).

```bash
# Line-based queries
echo "status" | nc -U "$XDG_RUNTIME_DIR/momentum.sock"
echo "set-anc active" | nc -U "$XDG_RUNTIME_DIR/momentum.sock"
echo "set-transparency 50" | nc -U "$XDG_RUNTIME_DIR/momentum.sock"
echo "cycle-anc" | nc -U "$XDG_RUNTIME_DIR/momentum.sock"

# JSON-based queries
echo '{"cmd": "status"}' | nc -U "$XDG_RUNTIME_DIR/momentum.sock"
```

### Status Output Example

```json
{
  "status": "ok",
  "connected": true,
  "device": {
    "mac": "80:C3:BA:4A:CF:82",
    "name": "MOMENTUM 4",
    "connected": true,
    "battery": 100,
    "codec": null
  },
  "settings": {
    "anc_enabled": true,
    "transparency": 50,
    "bass_boost": true,
    "sidetone": 1,
    "wear_detection": true
  },
  "channel": 2,
  "timestamp": 1788972855.123
}
```

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
