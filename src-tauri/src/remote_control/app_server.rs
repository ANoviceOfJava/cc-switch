use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::timeout;

use crate::codex_config::codex_cli_candidates;

// Long histories and a cold model request may legitimately take longer than the default.
const APP_SERVER_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const APP_SERVER_CHANNEL_CAPACITY: usize = 128;
const APP_SERVER_EVENT_CAPACITY: usize = 512;
#[cfg(target_os = "windows")]
const CREATE_NO_WINDOW: u32 = 0x08000000;

type PendingResponse = oneshot::Sender<Result<Value, AppServerError>>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AppServerEvent {
    Notification {
        method: String,
        params: Value,
    },
    ServerRequest {
        id: Value,
        method: String,
        params: Value,
    },
    ProtocolError(String),
    Exited,
}

#[derive(Debug, Error)]
pub(crate) enum AppServerError {
    #[error("未找到可用的 Codex CLI")]
    CliUnavailable,
    #[error("无法启动 Codex App Server: {0}")]
    Spawn(String),
    #[error("Codex App Server 连接已关闭")]
    Closed,
    #[error("Codex App Server 请求超时: {0}")]
    Timeout(String),
    #[error("Codex App Server 请求失败 ({code}): {message}")]
    Remote {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("Codex App Server 协议错误: {0}")]
    Protocol(String),
    #[error("Codex App Server I/O 错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("无法序列化 Codex App Server 消息: {0}")]
    Serialize(#[from] serde_json::Error),
}

enum AppServerCommand {
    Request {
        method: String,
        params: Value,
        response: PendingResponse,
    },
    Notify {
        method: String,
        params: Value,
        result: oneshot::Sender<Result<(), AppServerError>>,
    },
    Respond {
        id: Value,
        result: Result<Value, RpcResponseError>,
        sent: oneshot::Sender<Result<(), AppServerError>>,
    },
    Shutdown {
        complete: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
enum IncomingMessage {
    Response {
        id: u64,
        result: Result<Value, RpcResponseError>,
    },
    Notification {
        method: String,
        params: Value,
    },
    ServerRequest {
        id: Value,
        method: String,
        params: Value,
    },
}

#[derive(Debug)]
pub(crate) struct RpcResponseError {
    pub(crate) code: i64,
    pub(crate) message: String,
    pub(crate) data: Option<Value>,
}

#[derive(Clone)]
pub(crate) struct AppServerClient {
    commands: mpsc::Sender<AppServerCommand>,
    events: broadcast::Sender<AppServerEvent>,
}

impl AppServerClient {
    /// 启动 Codex App Server，并完成 `initialize -> initialized` 握手。
    pub(crate) async fn start() -> Result<Self, AppServerError> {
        let mut last_error = None;

        for candidate in codex_cli_candidates() {
            match Self::start_candidate(&candidate).await {
                Ok(client) => return Ok(client),
                Err(error) => {
                    log::debug!(
                        "Codex App Server candidate {} was unavailable: {error}",
                        candidate.display()
                    );
                    last_error = Some(error);
                }
            }
        }

        Err(last_error.unwrap_or(AppServerError::CliUnavailable))
    }

    /// 发送 App Server 请求，并等待对应请求 ID 的响应。
    pub(crate) async fn request(
        &self,
        method: impl Into<String>,
        params: Value,
    ) -> Result<Value, AppServerError> {
        let method = method.into();
        let (response_tx, response_rx) = oneshot::channel();
        self.commands
            .send(AppServerCommand::Request {
                method: method.clone(),
                params,
                response: response_tx,
            })
            .await
            .map_err(|_| AppServerError::Closed)?;

        timeout(APP_SERVER_REQUEST_TIMEOUT, response_rx)
            .await
            .map_err(|_| AppServerError::Timeout(method))?
            .map_err(|_| AppServerError::Closed)?
    }

    /// 发送不需要响应的 App Server 通知。
    pub(crate) async fn notify(
        &self,
        method: impl Into<String>,
        params: Value,
    ) -> Result<(), AppServerError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.commands
            .send(AppServerCommand::Notify {
                method: method.into(),
                params,
                result: result_tx,
            })
            .await
            .map_err(|_| AppServerError::Closed)?;
        result_rx.await.map_err(|_| AppServerError::Closed)?
    }

    /// 响应 App Server 主动发起的审批或用户输入请求。
    pub(crate) async fn respond(
        &self,
        id: Value,
        result: Result<Value, RpcResponseError>,
    ) -> Result<(), AppServerError> {
        if !is_valid_request_id(&id) {
            return Err(AppServerError::Protocol(
                "服务端请求 ID 必须是字符串或非负整数".to_string(),
            ));
        }

        let (sent_tx, sent_rx) = oneshot::channel();
        self.commands
            .send(AppServerCommand::Respond {
                id,
                result,
                sent: sent_tx,
            })
            .await
            .map_err(|_| AppServerError::Closed)?;
        sent_rx.await.map_err(|_| AppServerError::Closed)?
    }

    /// 订阅 App Server 通知和服务端主动请求。
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<AppServerEvent> {
        self.events.subscribe()
    }

    /// 关闭 App Server，并等待子进程完成回收。
    pub(crate) async fn shutdown(&self) {
        let (complete_tx, complete_rx) = oneshot::channel();
        if self
            .commands
            .send(AppServerCommand::Shutdown {
                complete: complete_tx,
            })
            .await
            .is_ok()
        {
            let _ = complete_rx.await;
        }
    }

    async fn start_candidate(candidate: &Path) -> Result<Self, AppServerError> {
        let mut child = app_server_command(candidate)
            .spawn()
            .map_err(|error| AppServerError::Spawn(error.to_string()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| AppServerError::Spawn("Codex App Server 未提供标准输入".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppServerError::Spawn("Codex App Server 未提供标准输出".to_string()))?;
        let (command_tx, command_rx) = mpsc::channel(APP_SERVER_CHANNEL_CAPACITY);
        let (event_tx, _) = broadcast::channel(APP_SERVER_EVENT_CAPACITY);
        let client = Self {
            commands: command_tx,
            events: event_tx.clone(),
        };

        tokio::spawn(run_app_server(child, stdin, stdout, command_rx, event_tx));

        if let Err(error) = client
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "cc_switch_remote",
                        "title": "CC Switch Remote",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    // `thread/resume.excludeTurns` keeps remote sends independent of a
                    // potentially large local transcript and requires this opt-in.
                    "capabilities": {
                        "experimentalApi": true
                    }
                }),
            )
            .await
        {
            client.shutdown().await;
            return Err(error);
        }
        client.notify("initialized", json!({})).await?;

        Ok(client)
    }
}

fn app_server_command(candidate: &Path) -> Command {
    let mut command = Command::new(candidate);
    command
        .arg("app-server")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    #[cfg(target_os = "windows")]
    {
        command.creation_flags(CREATE_NO_WINDOW);
    }

    command
}

async fn run_app_server(
    mut child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    mut commands: mpsc::Receiver<AppServerCommand>,
    events: broadcast::Sender<AppServerEvent>,
) {
    let mut writer = BufWriter::new(stdin);
    let mut lines = BufReader::new(stdout).lines();
    let mut pending = HashMap::new();
    let mut next_request_id = 1_u64;
    let mut shutdown_complete = None;

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    break;
                };
                let command = match command {
                    AppServerCommand::Shutdown { complete } => {
                        shutdown_complete = Some(complete);
                        break;
                    }
                    command => command,
                };
                if handle_command(
                    command,
                    &mut writer,
                    &mut pending,
                    &mut next_request_id,
                ).await {
                    break;
                }
            }
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => handle_server_line(&line, &mut pending, &events),
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }

    for (_, response) in pending.drain() {
        let _ = response.send(Err(AppServerError::Closed));
    }
    let _ = writer.shutdown().await;
    let _ = child.kill().await;
    let _ = child.wait().await;
    let _ = events.send(AppServerEvent::Exited);
    if let Some(complete) = shutdown_complete {
        let _ = complete.send(());
    }
}

async fn handle_command(
    command: AppServerCommand,
    writer: &mut BufWriter<ChildStdin>,
    pending: &mut HashMap<u64, PendingResponse>,
    next_request_id: &mut u64,
) -> bool {
    match command {
        AppServerCommand::Request {
            method,
            params,
            response,
        } => {
            let id = *next_request_id;
            *next_request_id = next_request_id.saturating_add(1);
            let message = json!({ "method": method, "id": id, "params": params });
            match write_message(writer, &message).await {
                Ok(()) => {
                    pending.insert(id, response);
                    false
                }
                Err(error) => {
                    let _ = response.send(Err(error));
                    true
                }
            }
        }
        AppServerCommand::Notify {
            method,
            params,
            result,
        } => {
            let message = json!({ "method": method, "params": params });
            let send_result = write_message(writer, &message).await;
            let failed = send_result.is_err();
            let _ = result.send(send_result);
            failed
        }
        AppServerCommand::Respond { id, result, sent } => {
            let message = match result {
                Ok(result) => json!({ "id": id, "result": result }),
                Err(error) => json!({
                    "id": id,
                    "error": {
                        "code": error.code,
                        "message": error.message,
                        "data": error.data
                    }
                }),
            };
            let send_result = write_message(writer, &message).await;
            let failed = send_result.is_err();
            let _ = sent.send(send_result);
            failed
        }
        AppServerCommand::Shutdown { .. } => unreachable!("shutdown is handled by the actor loop"),
    }
}

async fn write_message(
    writer: &mut BufWriter<ChildStdin>,
    message: &Value,
) -> Result<(), AppServerError> {
    let mut encoded = serde_json::to_vec(message)?;
    encoded.push(b'\n');
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

fn handle_server_line(
    line: &str,
    pending: &mut HashMap<u64, PendingResponse>,
    events: &broadcast::Sender<AppServerEvent>,
) {
    let parsed = serde_json::from_str(line)
        .map_err(|_| "收到无法解析的 JSONL 消息".to_string())
        .and_then(classify_incoming);

    match parsed {
        Ok(IncomingMessage::Response { id, result }) => {
            let Some(response) = pending.remove(&id) else {
                let _ = events.send(AppServerEvent::ProtocolError(format!(
                    "收到未知请求 ID 的响应: {id}"
                )));
                return;
            };
            let result = result.map_err(|error| AppServerError::Remote {
                code: error.code,
                message: error.message,
                data: error.data,
            });
            let _ = response.send(result);
        }
        Ok(IncomingMessage::Notification { method, params }) => {
            let _ = events.send(AppServerEvent::Notification { method, params });
        }
        Ok(IncomingMessage::ServerRequest { id, method, params }) => {
            let _ = events.send(AppServerEvent::ServerRequest { id, method, params });
        }
        Err(error) => {
            let _ = events.send(AppServerEvent::ProtocolError(error));
        }
    }
}

fn classify_incoming(message: Value) -> Result<IncomingMessage, String> {
    let object = message
        .as_object()
        .ok_or_else(|| "消息必须是 JSON 对象".to_string())?;
    let method = object.get("method").and_then(Value::as_str);
    let id = object.get("id");

    match (method, id) {
        (Some(method), Some(id)) if is_valid_request_id(id) => Ok(IncomingMessage::ServerRequest {
            id: id.clone(),
            method: method.to_string(),
            params: object.get("params").cloned().unwrap_or_else(|| json!({})),
        }),
        (Some(method), None) => Ok(IncomingMessage::Notification {
            method: method.to_string(),
            params: object.get("params").cloned().unwrap_or_else(|| json!({})),
        }),
        (None, Some(id)) => {
            let id = id
                .as_u64()
                .ok_or_else(|| "客户端响应 ID 必须是非负整数".to_string())?;
            let result = if let Some(error) = object.get("error") {
                Err(parse_response_error(error)?)
            } else if let Some(result) = object.get("result") {
                Ok(result.clone())
            } else {
                return Err("响应缺少 result 或 error".to_string());
            };
            Ok(IncomingMessage::Response { id, result })
        }
        _ => Err("消息缺少有效的 method 或 id".to_string()),
    }
}

fn parse_response_error(error: &Value) -> Result<RpcResponseError, String> {
    let object = error
        .as_object()
        .ok_or_else(|| "error 必须是 JSON 对象".to_string())?;
    let code = object
        .get("code")
        .and_then(Value::as_i64)
        .ok_or_else(|| "error.code 必须是整数".to_string())?;
    let message = object
        .get("message")
        .and_then(Value::as_str)
        .ok_or_else(|| "error.message 必须是字符串".to_string())?;

    Ok(RpcResponseError {
        code,
        message: message.to_string(),
        data: object.get("data").cloned(),
    })
}

fn is_valid_request_id(id: &Value) -> bool {
    id.is_string() || id.as_u64().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_response_notification_and_server_request() {
        let response = classify_incoming(json!({ "id": 7, "result": { "ok": true } }))
            .expect("classify response");
        assert!(matches!(response, IncomingMessage::Response { id: 7, .. }));

        let notification = classify_incoming(json!({
            "method": "turn/started",
            "params": { "threadId": "thread-1" }
        }))
        .expect("classify notification");
        assert!(matches!(
            notification,
            IncomingMessage::Notification { ref method, .. } if method == "turn/started"
        ));

        let request = classify_incoming(json!({
            "id": "approval-1",
            "method": "item/commandExecution/requestApproval",
            "params": { "threadId": "thread-1" }
        }))
        .expect("classify server request");
        assert!(matches!(
            request,
            IncomingMessage::ServerRequest { ref method, .. }
                if method == "item/commandExecution/requestApproval"
        ));
    }

    #[test]
    fn classifies_remote_error_without_losing_data() {
        let response = classify_incoming(json!({
            "id": 9,
            "error": {
                "code": -32001,
                "message": "Server overloaded; retry later.",
                "data": { "retry": true }
            }
        }))
        .expect("classify error response");

        let IncomingMessage::Response {
            result: Err(error), ..
        } = response
        else {
            panic!("expected error response");
        };
        assert_eq!(error.code, -32001);
        assert_eq!(error.data, Some(json!({ "retry": true })));
    }

    #[test]
    fn rejects_invalid_message_shapes() {
        assert!(classify_incoming(json!([])).is_err());
        assert!(classify_incoming(json!({ "id": -1, "result": {} })).is_err());
        assert!(classify_incoming(json!({ "id": 1 })).is_err());
        assert!(!is_valid_request_id(&Value::Null));
    }
}
