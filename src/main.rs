use anyhow::Result;
use clap::{Parser, Subcommand};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, RwLock};

mod bt;
mod commands;
mod gaia;
mod state;

use commands::CommandHandler;
use state::{SharedCache, StateCache};

#[derive(Parser)]
#[command(name = "momentumd", author = "Jazden", version = "0.1.0", about = "Daemon and CLI for Sennheiser Momentum headphones on Linux")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Run in daemon server mode (default if no subcommand is given)
    #[arg(short, long)]
    daemon: bool,

    /// Socket path override
    #[arg(short, long)]
    socket: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the persistent background daemon
    Daemon,
    /// Query full device status
    Status,
    /// Get current ANC mode
    GetAnc,
    /// Set ANC state (active/on or off)
    SetAnc {
        mode: String,
    },
    /// Cycle through ANC modes
    CycleAnc,
    /// Get transparency level (0-100)
    GetTransparency,
    /// Set transparency level (0-100)
    SetTransparency {
        level: u8,
    },
    /// Get Bass Boost status
    GetBassBoost,
    /// Set Bass Boost (on/off)
    SetBassBoost {
        enabled: String,
    },
    /// Get battery level
    Battery,
}

fn get_socket_path(override_path: Option<PathBuf>) -> PathBuf {
    if let Some(p) = override_path {
        return p;
    }
    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(runtime_dir).join("momentum.sock")
    } else {
        PathBuf::from("/tmp").join(format!("momentum_{}.sock", unsafe { libc::getuid() }))
    }
}

async fn run_client(sock_path: PathBuf, cmd_str: String) -> Result<()> {
    let mut stream = match UnixStream::connect(&sock_path).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: Could not connect to momentumd daemon at {}: {}", sock_path.display(), e);
            eprintln!("Is the daemon running? Start it with: systemctl --user start momentumd");
            std::process::exit(1);
        }
    };

    stream.write_all(format!("{}\n", cmd_str).as_bytes()).await?;
    stream.shutdown().await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    print!("{}", line);
    Ok(())
}

async fn run_daemon(sock_path: PathBuf) -> Result<()> {
    // Clean up old socket if it exists
    if sock_path.exists() {
        let _ = std::fs::remove_file(&sock_path);
    }

    let listener = UnixListener::bind(&sock_path)?;
    // Set 0700 permissions
    let _ = std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o700));

    tracing::info!("momentumd listening on {}", sock_path.display());

    let cache: SharedCache = Arc::new(RwLock::new(StateCache::new()));
    let (req_tx, req_rx) = mpsc::channel(32);

    // Spawn Bluetooth connection & notification worker
    let bt_cache = Arc::clone(&cache);
    tokio::spawn(async move {
        bt::run_bt_loop(bt_cache, req_rx).await;
    });

    let handler = Arc::new(CommandHandler::new(Arc::clone(&cache), req_tx));

    // Handle Ctrl+C / SIGTERM
    let sock_to_clean = sock_path.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = std::fs::remove_file(sock_to_clean);
        std::process::exit(0);
    });

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let h = Arc::clone(&handler);
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.split();
                    let mut buf_reader = BufReader::new(reader);
                    let mut line = String::new();
                    if buf_reader.read_line(&mut line).await.is_ok() {
                        let resp = h.handle_line(&line).await;
                        let out = format!("{}\n", serde_json::to_string(&resp).unwrap_or_default());
                        let _ = writer.write_all(out.as_bytes()).await;
                    }
                });
            }
            Err(e) => {
                tracing::error!("Socket accept error: {}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let sock_path = get_socket_path(cli.socket);

    match cli.command {
        None | Some(Commands::Daemon) => {
            run_daemon(sock_path).await?;
        }
        Some(Commands::Status) => {
            run_client(sock_path, "status".to_string()).await?;
        }
        Some(Commands::GetAnc) => {
            run_client(sock_path, "get-anc".to_string()).await?;
        }
        Some(Commands::SetAnc { mode }) => {
            run_client(sock_path, format!("set-anc {}", mode)).await?;
        }
        Some(Commands::CycleAnc) => {
            run_client(sock_path, "cycle-anc".to_string()).await?;
        }
        Some(Commands::GetTransparency) => {
            run_client(sock_path, "get-transparency".to_string()).await?;
        }
        Some(Commands::SetTransparency { level }) => {
            run_client(sock_path, format!("set-transparency {}", level)).await?;
        }
        Some(Commands::GetBassBoost) => {
            run_client(sock_path, "get-bass-boost".to_string()).await?;
        }
        Some(Commands::SetBassBoost { enabled }) => {
            run_client(sock_path, format!("set-bass-boost {}", enabled)).await?;
        }
        Some(Commands::Battery) => {
            run_client(sock_path, "battery".to_string()).await?;
        }
    }

    Ok(())
}
