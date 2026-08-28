use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{interval, sleep_until, Instant, MissedTickBehavior};
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, AUTHORIZATION};
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use url::Url;

use super::protocol::{ProtocolError, RemoteCommand, WireMessage};

const ACCESS_KEY_MIN_LENGTH: usize = 32;
const ACCESS_KEY_MAX_LENGTH: usize = 256;
const RELAY_COMMAND_CAPACITY: usize = 128;
const RELAY_EVENT_CAPACITY: usize = 256;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RelayEvent {
    Connected,
    Disconnected,
    Command(RemoteCommand),
    ProtocolError(String),
}

#[derive(Debug, Error)]
pub(crate) enum RelayError {
    #[error("Relay URL 无效: {0}")]
    InvalidUrl(String),
    #[error("公网 Relay 必须使用 wss://")]
    InsecureUrl,
    #[error("Access Key 格式无效")]
    InvalidAccessKey,
    #[error("Relay 当前未连接")]
    NotConnected,
    #[error("Relay 连接已关闭")]
    Closed,
    #[error("Relay WebSocket 错误: {0}")]
    WebSocket(String),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
}

struct RelayConfig {
    url: Url,
    access_key: String,
}

enum RelayCommand {
    Send {
        message: WireMessage,
        result: oneshot::Sender<Result<(), RelayError>>,
    },
    Shutdown {
        complete: oneshot::Sender<()>,
    },
}

enum ConnectionOutcome {
    Reconnect,
    Shutdown(oneshot::Sender<()>),
    Stop,
}

enum ReconnectWaitOutcome {
    Retry,
    Shutdown(oneshot::Sender<()>),
    Stop,
}

#[derive(Clone)]
pub(crate) struct RelayClient {
    commands: mpsc::Sender<RelayCommand>,
    events: broadcast::Sender<RelayEvent>,
    connected: watch::Receiver<bool>,
}

impl RelayClient {
    /// 创建 Relay 客户端，并在后台持续连接和自动重连。
    pub(crate) fn start(url: &str, access_key: String) -> Result<Self, RelayError> {
        let config = RelayConfig {
            url: validate_relay_url(url)?,
            access_key: validate_access_key(access_key)?,
        };
        let (command_tx, command_rx) = mpsc::channel(RELAY_COMMAND_CAPACITY);
        let (event_tx, _) = broadcast::channel(RELAY_EVENT_CAPACITY);
        let (connected_tx, connected_rx) = watch::channel(false);

        tokio::spawn(run_relay(
            config,
            command_rx,
            event_tx.clone(),
            connected_tx,
        ));

        Ok(Self {
            commands: command_tx,
            events: event_tx,
            connected: connected_rx,
        })
    }

    /// 返回 Relay WebSocket 当前是否已经建立。
    pub(crate) fn is_connected(&self) -> bool {
        *self.connected.borrow()
    }

    /// 订阅连接状态、协议错误和经过白名单校验的手机端命令。
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<RelayEvent> {
        self.events.subscribe()
    }

    /// 向手机端发送状态快照、任务详情或请求结果。
    pub(crate) async fn send(&self, message: WireMessage) -> Result<(), RelayError> {
        if !self.is_connected() {
            return Err(RelayError::NotConnected);
        }
        let (result_tx, result_rx) = oneshot::channel();
        self.commands
            .send(RelayCommand::Send {
                message,
                result: result_tx,
            })
            .await
            .map_err(|_| RelayError::Closed)?;
        result_rx.await.map_err(|_| RelayError::Closed)?
    }

    /// 停止自动重连，并关闭当前 Relay WebSocket。
    pub(crate) async fn shutdown(self) {
        let (complete_tx, complete_rx) = oneshot::channel();
        if self
            .commands
            .send(RelayCommand::Shutdown {
                complete: complete_tx,
            })
            .await
            .is_ok()
        {
            let _ = complete_rx.await;
        }
    }
}

async fn run_relay(
    config: RelayConfig,
    mut commands: mpsc::Receiver<RelayCommand>,
    events: broadcast::Sender<RelayEvent>,
    connected: watch::Sender<bool>,
) {
    let mut reconnect_attempt = 0_u32;

    loop {
        match connect_relay(&config).await {
            Ok(socket) => {
                reconnect_attempt = 0;
                let _ = connected.send(true);
                let _ = events.send(RelayEvent::Connected);
                match run_connected(socket, &mut commands, &events).await {
                    ConnectionOutcome::Reconnect => {
                        let _ = connected.send(false);
                        let _ = events.send(RelayEvent::Disconnected);
                    }
                    ConnectionOutcome::Shutdown(complete) => {
                        let _ = connected.send(false);
                        let _ = events.send(RelayEvent::Disconnected);
                        let _ = complete.send(());
                        return;
                    }
                    ConnectionOutcome::Stop => {
                        let _ = connected.send(false);
                        let _ = events.send(RelayEvent::Disconnected);
                        return;
                    }
                }
            }
            Err(error) => {
                log::debug!("Relay connection attempt failed: {error}");
                let _ = connected.send(false);
            }
        }

        reconnect_attempt = reconnect_attempt.saturating_add(1);
        match wait_before_reconnect(reconnect_delay(reconnect_attempt), &mut commands).await {
            ReconnectWaitOutcome::Retry => {}
            ReconnectWaitOutcome::Shutdown(complete) => {
                let _ = complete.send(());
                return;
            }
            ReconnectWaitOutcome::Stop => return,
        }
    }
}

async fn connect_relay(
    config: &RelayConfig,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    RelayError,
> {
    let mut request = config
        .url
        .as_str()
        .into_client_request()
        .map_err(|error| RelayError::WebSocket(error.to_string()))?;
    let mut authorization = HeaderValue::from_str(&format!("Bearer {}", config.access_key))
        .map_err(|_| RelayError::InvalidAccessKey)?;
    authorization.set_sensitive(true);
    request.headers_mut().insert(AUTHORIZATION, authorization);

    let mut websocket_config = WebSocketConfig::default();
    websocket_config.max_message_size = Some(MAX_MESSAGE_BYTES);
    websocket_config.max_frame_size = Some(MAX_MESSAGE_BYTES);
    let (socket, _) = connect_async_with_config(request, Some(websocket_config), false)
        .await
        .map_err(|error| RelayError::WebSocket(error.to_string()))?;
    Ok(socket)
}

async fn run_connected<S>(
    mut socket: tokio_tungstenite::WebSocketStream<S>,
    commands: &mut mpsc::Receiver<RelayCommand>,
    events: &broadcast::Sender<RelayEvent>,
) -> ConnectionOutcome
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut heartbeat = interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(RelayCommand::Send { message, result }) => {
                        let send_result = match message.encode() {
                            Ok(encoded) => socket
                                .send(Message::Text(encoded.into()))
                                .await
                                .map_err(|error| RelayError::WebSocket(error.to_string())),
                            Err(error) => Err(RelayError::Protocol(error)),
                        };
                        let reconnect = matches!(send_result, Err(RelayError::WebSocket(_)));
                        let _ = result.send(send_result);
                        if reconnect {
                            return ConnectionOutcome::Reconnect;
                        }
                    }
                    Some(RelayCommand::Shutdown { complete }) => {
                        let _ = socket.close(None).await;
                        return ConnectionOutcome::Shutdown(complete);
                    }
                    None => {
                        let _ = socket.close(None).await;
                        return ConnectionOutcome::Stop;
                    }
                }
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => match RemoteCommand::decode(&text) {
                        Ok(command) => {
                            let _ = events.send(RelayEvent::Command(command));
                        }
                        Err(error) => {
                            let _ = events.send(RelayEvent::ProtocolError(error.to_string()));
                        }
                    },
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            return ConnectionOutcome::Reconnect;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                        return ConnectionOutcome::Reconnect;
                    }
                    Some(Ok(_)) => {
                        let _ = events.send(RelayEvent::ProtocolError(
                            "Relay 仅允许文本 JSON 消息".to_string(),
                        ));
                    }
                }
            }
            _ = heartbeat.tick() => {
                if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                    return ConnectionOutcome::Reconnect;
                }
            }
        }
    }
}

async fn wait_before_reconnect(
    delay: Duration,
    commands: &mut mpsc::Receiver<RelayCommand>,
) -> ReconnectWaitOutcome {
    let deadline = sleep_until(Instant::now() + delay);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => return ReconnectWaitOutcome::Retry,
            command = commands.recv() => match command {
                Some(RelayCommand::Send { result, .. }) => {
                    let _ = result.send(Err(RelayError::NotConnected));
                }
                Some(RelayCommand::Shutdown { complete }) => {
                    return ReconnectWaitOutcome::Shutdown(complete);
                }
                None => return ReconnectWaitOutcome::Stop,
            }
        }
    }
}

fn validate_relay_url(value: &str) -> Result<Url, RelayError> {
    let url = Url::parse(value).map_err(|error| RelayError::InvalidUrl(error.to_string()))?;
    if url.username() != "" || url.password().is_some() || url.fragment().is_some() {
        return Err(RelayError::InvalidUrl(
            "URL 不能包含凭据或 fragment".to_string(),
        ));
    }
    if url.path() != "/ws/agent" || url.query().is_some() {
        return Err(RelayError::InvalidUrl(
            "URL 路径必须是 /ws/agent，且不能包含 query".to_string(),
        ));
    }
    match url.scheme() {
        "wss" => Ok(url),
        "ws" if is_loopback_host(&url) => Ok(url),
        "ws" => Err(RelayError::InsecureUrl),
        _ => Err(RelayError::InvalidUrl(
            "URL 必须使用 wss://，本机开发可使用 ws://".to_string(),
        )),
    }
}

fn validate_access_key(access_key: String) -> Result<String, RelayError> {
    if !(ACCESS_KEY_MIN_LENGTH..=ACCESS_KEY_MAX_LENGTH).contains(&access_key.len()) {
        return Err(RelayError::InvalidAccessKey);
    }
    Ok(access_key)
}

fn is_loopback_host(url: &Url) -> bool {
    matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
}

fn reconnect_delay(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(5);
    let base_millis = 1_000_u64.saturating_mul(1_u64 << exponent);
    let jitter_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::from(duration.subsec_millis()) % 500)
        .unwrap_or(0);
    Duration::from_millis(base_millis.saturating_add(jitter_millis)).min(MAX_RECONNECT_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_tls_except_for_loopback_development() {
        assert!(validate_relay_url("wss://relay.example.com/ws/agent").is_ok());
        assert!(validate_relay_url("ws://127.0.0.1:3000/ws/agent").is_ok());
        assert!(matches!(
            validate_relay_url("ws://relay.example.com/ws/agent"),
            Err(RelayError::InsecureUrl)
        ));
        assert!(validate_relay_url("wss://relay.example.com/other").is_err());
        assert!(validate_relay_url("wss://user:secret@relay.example.com/ws/agent").is_err());
    }

    #[test]
    fn validates_access_key_length_without_exposing_value() {
        assert!(validate_access_key("x".repeat(ACCESS_KEY_MIN_LENGTH)).is_ok());
        assert!(matches!(
            validate_access_key("short".to_string()),
            Err(RelayError::InvalidAccessKey)
        ));
    }

    #[test]
    fn reconnect_delay_is_bounded() {
        assert!(reconnect_delay(1) >= Duration::from_secs(1));
        assert!(reconnect_delay(100) <= MAX_RECONNECT_DELAY);
    }
}
