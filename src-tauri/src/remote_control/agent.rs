use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::app_server::{AppServerClient, AppServerError, AppServerEvent};
use super::desktop_control::{self, DesktopControlError};
use super::project_state::{
    assign_thread_to_project, load_codex_project_state, set_thread_pinned, CodexProject,
    CodexProjectState, ProjectStateError,
};
use super::protocol::{
    ApprovalDecision, KnownThreadRevision, ProtocolError, RemoteAttachment, RemoteCommand,
    WireMessage,
};
use super::relay_client::{RelayClient, RelayError, RelayEvent};
use crate::session_manager::providers::codex::{
    latest_message_timestamp, latest_thread_context_usage, latest_thread_execution_state,
    latest_thread_settings, load_message_attachments, load_messages, scan_sessions, session_roots,
    ThreadExecutionState,
};

const PAGE_SIZE: u32 = 100;
const MAX_PAGES: usize = 100;
const DETAIL_CHUNK_CHARS: usize = 180_000;
const STATUS_POLL_LIMIT: u32 = 50;
const THREAD_DETAIL_PAGE_SIZE: usize = 5;

#[derive(Debug, Error)]
pub(crate) enum RemoteAgentError {
    #[error(transparent)]
    AppServer(#[from] AppServerError),
    #[error(transparent)]
    Relay(#[from] RelayError),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    ProjectState(#[from] ProjectStateError),
    #[error("Codex App Server 返回的数据结构不兼容: {0}")]
    Incompatible(String),
    #[error("无法序列化远程状态: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("找不到项目: {0}")]
    ProjectNotFound(String),
    #[error("项目没有可用的本地目录: {0}")]
    ProjectDirectoryMissing(String),
    #[error("找不到任务: {0}")]
    ThreadNotFound(String),
    #[error("任务当前没有可中断的执行")]
    NoActiveTurn,
    #[error("审批请求已失效或不属于当前任务")]
    ApprovalNotFound,
    #[error("附件上传失败: {0}")]
    Attachment(String),
    #[error("附件文件操作失败: {0}")]
    Io(#[from] std::io::Error),
    #[error("附件数据不是有效 Base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("无法通过 Codex 桌面端发送消息: {0}")]
    DesktopControl(#[from] DesktopControlError),
}

pub(crate) struct RemoteControlAgent {
    shutdown: mpsc::Sender<oneshot::Sender<()>>,
    task: JoinHandle<()>,
    relay_status: RelayClient,
}

struct AgentRuntime {
    app_server: AppServerClient,
    relay: RelayClient,
    known_thread_ids: HashSet<String>,
    projects: HashMap<String, CodexProject>,
    thread_summaries: HashMap<String, ThreadSummaryDto>,
    thread_session_paths: HashMap<String, PathBuf>,
    thread_settings: HashMap<String, ThreadSettings>,
    thread_context_usage: HashMap<String, ContextUsageDto>,
    thread_reasoning_activity: HashMap<String, String>,
    attachment_dir: tempfile::TempDir,
    pending_attachment_uploads: HashMap<String, PendingAttachmentUpload>,
    uploaded_attachments: HashMap<String, UploadedAttachment>,
    remote_turn_thread_ids: HashSet<String>,
    app_server_restart_requested: bool,
    pending_approvals: HashMap<String, PendingApproval>,
    last_detail_thread_id: Option<String>,
    last_snapshot: Option<StateSnapshotDto>,
}

#[derive(Clone)]
struct ThreadSettings {
    model: Option<String>,
    effort: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct ContextUsageDto {
    used_tokens: u64,
    model_context_window: u64,
}

struct PendingApproval {
    app_server_request_id: Value,
    thread_id: String,
    title: String,
    detail: String,
}

struct PendingAttachmentUpload {
    attachment: UploadedAttachment,
    expected_size: u64,
    received_size: u64,
    next_index: u32,
    file: fs::File,
}

#[derive(Clone)]
struct UploadedAttachment {
    name: String,
    mime_type: String,
    size: u64,
    path: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page<T> {
    data: Vec<T>,
    next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawThread {
    id: String,
    name: Option<String>,
    preview: String,
    cwd: String,
    created_at: i64,
    updated_at: i64,
    status: ThreadStatusDto,
    #[serde(default)]
    turns: Vec<RawTurn>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTurn {
    id: String,
    status: TurnStatus,
    #[serde(default)]
    items: Vec<Value>,
    error: Option<Value>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
enum TurnStatus {
    Completed,
    Interrupted,
    Failed,
    InProgress,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawModel {
    id: String,
    display_name: String,
    hidden: bool,
    is_default: bool,
    default_reasoning_effort: String,
    supported_reasoning_efforts: Vec<RawReasoningEffort>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawReasoningEffort {
    reasoning_effort: String,
    description: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct StateSnapshotDto {
    revision: String,
    generated_at: i64,
    projects: Vec<CodexProject>,
    threads: Vec<ThreadSummaryDto>,
    models: Vec<ModelDto>,
    deleted_thread_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StateDeltaDto {
    revision: String,
    generated_at: i64,
    projects: Vec<CodexProject>,
    threads: Vec<ThreadSummaryDto>,
    thread_order: Vec<String>,
    models: Vec<ModelDto>,
    deleted_thread_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadSummaryDto {
    id: String,
    project_id: Option<String>,
    name: Option<String>,
    preview: String,
    cwd: String,
    created_at: i64,
    updated_at: i64,
    status: ThreadStatusDto,
    pinned: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
enum ThreadStatusDto {
    NotLoaded,
    Idle,
    SystemError,
    Active {
        #[serde(default, rename = "activeFlags")]
        active_flags: Vec<ThreadActiveFlag>,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum ThreadActiveFlag {
    WaitingOnApproval,
    WaitingOnUserInput,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelDto {
    id: String,
    display_name: String,
    hidden: bool,
    is_default: bool,
    default_reasoning_effort: Option<String>,
    supported_reasoning_efforts: Vec<ReasoningEffortDto>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReasoningEffortDto {
    reasoning_effort: String,
    description: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadDetailDto {
    revision: String,
    generated_at: i64,
    thread: ThreadSummaryDto,
    items: Vec<ConversationItemDto>,
    active_turn_id: Option<String>,
    selected_model: Option<String>,
    selected_reasoning_effort: Option<String>,
    context_usage: Option<ContextUsageDto>,
    before: usize,
    has_more_before: bool,
    next_before: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConversationItemDto {
    id: String,
    kind: ConversationKind,
    status: Option<ConversationStatus>,
    title: Option<String>,
    text: Option<String>,
    detail: Option<String>,
    created_at: Option<i64>,
    approval_request_id: Option<String>,
    approval_options: Vec<ApprovalOptionDto>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
enum ConversationKind {
    UserMessage,
    AgentMessage,
    Reasoning,
    Command,
    FileChange,
    ToolCall,
    Error,
    Approval,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
enum ConversationStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApprovalOptionDto {
    id: ApprovalDecisionDto,
    label: &'static str,
    tone: ApprovalTone,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
enum ApprovalDecisionDto {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
enum ApprovalTone {
    Primary,
    Neutral,
    Danger,
}

impl RemoteControlAgent {
    /// 启动 Codex App Server 和公网 Relay 后台代理。
    pub(crate) async fn start(
        relay_url: &str,
        access_key: String,
    ) -> Result<Self, RemoteAgentError> {
        let app_server = AppServerClient::start().await?;
        let relay = match RelayClient::start(relay_url, access_key) {
            Ok(relay) => relay,
            Err(error) => {
                app_server.shutdown().await;
                return Err(error.into());
            }
        };
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let attachment_dir = tempfile::Builder::new()
            .prefix("codex-remote-attachments-")
            .tempdir()?;
        let runtime = AgentRuntime {
            app_server,
            relay,
            known_thread_ids: HashSet::new(),
            projects: HashMap::new(),
            thread_summaries: HashMap::new(),
            thread_session_paths: HashMap::new(),
            thread_settings: HashMap::new(),
            thread_context_usage: HashMap::new(),
            thread_reasoning_activity: HashMap::new(),
            attachment_dir,
            pending_attachment_uploads: HashMap::new(),
            uploaded_attachments: HashMap::new(),
            remote_turn_thread_ids: HashSet::new(),
            app_server_restart_requested: false,
            pending_approvals: HashMap::new(),
            last_detail_thread_id: None,
            last_snapshot: None,
        };
        let relay_status = runtime.relay.clone();
        let task = tokio::spawn(runtime.run(shutdown_rx));

        Ok(Self {
            shutdown: shutdown_tx,
            task,
            relay_status,
        })
    }

    /// 返回 Agent 与公网 Relay 的实时连接状态。
    pub(crate) fn is_connected(&self) -> bool {
        self.relay_status.is_connected()
    }

    /// 返回 Agent 后台任务是否已经异常或正常退出。
    pub(crate) fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// 停止代理，并等待 Relay 与 App Server 子进程完成回收。
    pub(crate) async fn shutdown(self) {
        let (complete_tx, complete_rx) = oneshot::channel();
        if self.shutdown.send(complete_tx).await.is_ok() {
            let _ = complete_rx.await;
        }
        let _ = self.task.await;
    }
}

impl AgentRuntime {
    async fn run(mut self, mut shutdown: mpsc::Receiver<oneshot::Sender<()>>) {
        let mut app_events = self.app_server.subscribe();
        let mut relay_events = self.relay.subscribe();
        let mut status_poll = tokio::time::interval(Duration::from_secs(2));
        status_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = status_poll.tick() => {
                    if self.last_snapshot.is_some() {
                        if let Err(error) = self.refresh_desktop_thread_statuses().await {
                            log::debug!("Failed to refresh desktop thread statuses: {error}");
                        }
                    }
                }
                event = relay_events.recv() => match event {
                    Ok(RelayEvent::Command(command)) => {
                        if self.handle_remote_command(command).await.is_err() {
                            log::warn!("Remote command failed");
                        }
                    }
                    Ok(RelayEvent::ProtocolError(error)) => {
                        log::warn!("Rejected remote message: {error}");
                    }
                    Ok(RelayEvent::Connected | RelayEvent::Disconnected) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let _ = self.send_snapshot().await;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                event = app_events.recv() => match event {
                    Ok(AppServerEvent::Exited) => break,
                    Ok(event) => {
                        if self.handle_app_server_event(event).await.is_err() {
                            log::warn!("Codex App Server event handling failed");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let _ = self.refresh_current_view().await;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                complete = shutdown.recv() => {
                    if let Some(complete) = complete {
                        self.relay.shutdown().await;
                        self.app_server.shutdown().await;
                        let _ = complete.send(());
                    }
                    return;
                }
            }

            if self.app_server_restart_requested {
                match self.restart_app_server().await {
                    Ok(events) => {
                        app_events = events;
                        self.app_server_restart_requested = false;
                    }
                    Err(error) => {
                        log::warn!("Failed to restart Codex App Server after remote turn: {error}");
                    }
                }
            }
        }

        self.relay.shutdown().await;
        self.app_server.shutdown().await;
    }

    async fn handle_remote_command(
        &mut self,
        command: RemoteCommand,
    ) -> Result<(), RemoteAgentError> {
        let request_id = command_request_id(&command).map(ToOwned::to_owned);
        let result = match command {
            RemoteCommand::Sync => self.send_snapshot().await,
            RemoteCommand::SyncIncremental { known_threads } => {
                self.send_incremental_snapshot(known_threads).await
            }
            RemoteCommand::ReadThread {
                thread_id,
                before,
                limit,
                ..
            } => {
                self.send_thread_detail(&thread_id, before, limit).await
            }
            RemoteCommand::CreateThread {
                request_id,
                project_id,
            } => self.create_thread(&project_id, request_id).await,
            RemoteCommand::StartTurn {
                thread_id,
                text,
                model,
                effort,
                attachments,
                ..
            } => {
                self.start_turn(&thread_id, text, model, effort, attachments)
                    .await
            }
            RemoteCommand::StartAttachmentUpload {
                upload_id,
                name,
                mime_type,
                size,
                ..
            } => self.start_attachment_upload(upload_id, name, mime_type, size),
            RemoteCommand::UploadAttachmentChunk {
                upload_id,
                index,
                data,
            } => self.write_attachment_chunk(&upload_id, index, &data),
            RemoteCommand::FinishAttachmentUpload { upload_id, .. } => {
                self.finish_attachment_upload(&upload_id)
            }
            RemoteCommand::InterruptTurn {
                thread_id, turn_id, ..
            } => self.interrupt_turn(&thread_id, turn_id).await,
            RemoteCommand::RespondApproval {
                thread_id,
                approval_request_id,
                decision,
                ..
            } => {
                self.respond_approval(&thread_id, &approval_request_id, decision)
                    .await
            }
            RemoteCommand::SetThreadPinned {
                thread_id, pinned, ..
            } => self.set_thread_pinned(&thread_id, pinned).await,
        };

        if let Err(error) = &result {
            self.send_request_error(request_id, error).await;
        }
        result
    }

    async fn handle_app_server_event(
        &mut self,
        event: AppServerEvent,
    ) -> Result<(), RemoteAgentError> {
        match event {
            AppServerEvent::ServerRequest { id, method, params }
                if matches!(
                    method.as_str(),
                    "item/commandExecution/requestApproval" | "item/fileChange/requestApproval"
                ) =>
            {
                self.store_approval(id, &method, params)?;
                if let Some(thread_id) = self
                    .pending_approvals
                    .values()
                    .last()
                    .map(|approval| approval.thread_id.clone())
                {
                    self.send_execution_status(&thread_id, "approval", "等待你的确认")
                        .await?;
                }
                self.refresh_current_view().await?;
            }
            AppServerEvent::ServerRequest { method, .. } => {
                log::warn!("Unsupported Codex App Server request: {method}");
            }
            AppServerEvent::Notification { method, params } => {
                let thread_id = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                match method.as_str() {
                    "thread/tokenUsage/updated" => {
                        let Some(thread_id) = thread_id else {
                            return Ok(());
                        };
                        if let Some(context_usage) = context_usage_from_notification(&params) {
                            self.thread_context_usage
                                .insert(thread_id.clone(), context_usage);
                            self.send_current_thread_detail(&thread_id).await?;
                        }
                    }
                    "thread/status/changed" => {
                        let Some(thread_id) = thread_id else {
                            return Ok(());
                        };
                        let status: ThreadStatusDto = serde_json::from_value(
                            params.get("status").cloned().ok_or_else(|| {
                                RemoteAgentError::Incompatible(
                                    "thread/status/changed 缺少 status".to_string(),
                                )
                            })?,
                        )?;
                        if self.update_thread_status(&thread_id, status.clone()) {
                            match status {
                                ThreadStatusDto::Active { .. } => {
                                    self.send_execution_status(
                                        &thread_id,
                                        "thinking",
                                        "AI 正在处理",
                                    )
                                    .await?;
                                }
                                ThreadStatusDto::Idle => {
                                    self.send_execution_status(
                                        &thread_id,
                                        "completed",
                                        "任务已完成",
                                    )
                                    .await?;
                                }
                                ThreadStatusDto::SystemError => {
                                    self.send_execution_status(
                                        &thread_id,
                                        "failed",
                                        "任务执行失败",
                                    )
                                    .await?;
                                }
                                ThreadStatusDto::NotLoaded => {}
                            }
                            self.send_cached_snapshot().await?;
                            self.send_current_thread_detail(&thread_id).await?;
                        }
                    }
                    "thread/started" | "thread/deleted" | "thread/archived"
                    | "thread/unarchived" | "turn/started" | "turn/completed" => {
                        if method == "turn/started" {
                            if let Some(thread_id) = thread_id.as_deref() {
                                self.thread_reasoning_activity.remove(thread_id);
                                self.send_execution_status(thread_id, "thinking", "AI 正在思考")
                                    .await?;
                            }
                        }
                        if method == "turn/completed" {
                            if let Some(thread_id) = thread_id.as_deref() {
                                self.thread_reasoning_activity.remove(thread_id);
                                self.send_execution_status(thread_id, "completed", "任务已完成")
                                    .await?;
                                if self.remote_turn_thread_ids.remove(thread_id)
                                    && self.remote_turn_thread_ids.is_empty()
                                {
                                    self.app_server_restart_requested = true;
                                }
                            }
                        }
                        self.send_snapshot_silent().await?;
                        if let Some(thread_id) = thread_id {
                            self.send_current_thread_detail(&thread_id).await?;
                        }
                    }
                    "item/started" | "item/completed" => {
                        if let Some(thread_id) = thread_id {
                            if method == "item/started" {
                                let item_type = params
                                    .get("item")
                                    .and_then(|item| item.get("type"))
                                    .and_then(Value::as_str)
                                    .unwrap_or_default();
                                let (phase, message) = if item_type.contains("command")
                                    || item_type.contains("tool")
                                {
                                    ("tool", "正在执行工具")
                                } else if item_type.contains("reasoning") {
                                    ("thinking", "AI 正在思考")
                                } else {
                                    ("output", "AI 正在输出")
                                };
                                self.send_execution_status(&thread_id, phase, message)
                                    .await?;
                            }
                            self.send_current_thread_detail(&thread_id).await?;
                        }
                    }
                    "item/reasoning/summaryPartAdded" => {
                        if let Some(thread_id) = thread_id {
                            self.thread_reasoning_activity.remove(&thread_id);
                        }
                    }
                    "item/reasoning/summaryTextDelta" => {
                        if let Some(thread_id) = thread_id {
                            let delta = params
                                .get("delta")
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            let activity = self
                                .thread_reasoning_activity
                                .entry(thread_id.clone())
                                .or_default();
                            activity.push_str(delta);
                            let message = clean_activity_message(activity);
                            if !message.is_empty() {
                                self.send_execution_status(&thread_id, "thinking", &message)
                                    .await?;
                            }
                        }
                    }
                    _ => {}
                }
            }
            AppServerEvent::ProtocolError(error) => {
                log::warn!("Codex App Server protocol error: {error}");
            }
            AppServerEvent::Exited => unreachable!("exit is handled by the runtime loop"),
        }
        Ok(())
    }

    async fn send_snapshot(&mut self) -> Result<(), RemoteAgentError> {
        self.send_sync_progress("loading", 10, "正在读取电脑端项目和任务")
            .await?;
        self.send_snapshot_silent().await?;
        self.send_sync_progress("ready", 100, "同步完成").await?;
        Ok(())
    }

    async fn send_snapshot_silent(&mut self) -> Result<(), RemoteAgentError> {
        let snapshot = self.build_snapshot().await?;
        let payload = serde_json::to_value(&snapshot)?;
        self.relay
            .send(WireMessage::outbound(
                "state.snapshot",
                None,
                Some(payload),
            )?)
            .await?;
        Ok(())
    }

    async fn send_incremental_snapshot(
        &mut self,
        known_threads: Vec<KnownThreadRevision>,
    ) -> Result<(), RemoteAgentError> {
        let snapshot = self.build_snapshot().await?;
        let known_by_id = known_threads
            .into_iter()
            .map(|thread| (thread.id, thread.signature))
            .collect::<HashMap<_, _>>();
        let current_ids = snapshot
            .threads
            .iter()
            .map(|thread| thread.id.as_str())
            .collect::<HashSet<_>>();
        let mut deleted_thread_ids = snapshot.deleted_thread_ids.clone();
        deleted_thread_ids.extend(
            known_by_id
                .keys()
                .filter(|thread_id| !current_ids.contains(thread_id.as_str()))
                .cloned(),
        );
        deleted_thread_ids.sort_unstable();
        deleted_thread_ids.dedup();

        let mut changed_threads = Vec::new();
        for thread in &snapshot.threads {
            let signature = serde_json::to_string(thread)?;
            if known_by_id.get(&thread.id) != Some(&signature) {
                changed_threads.push(thread.clone());
            }
        }
        let delta = StateDeltaDto {
            revision: snapshot.revision,
            generated_at: snapshot.generated_at,
            projects: snapshot.projects,
            thread_order: snapshot
                .threads
                .iter()
                .map(|thread| thread.id.clone())
                .collect(),
            threads: changed_threads,
            models: snapshot.models,
            deleted_thread_ids,
        };
        self.relay
            .send(WireMessage::outbound(
                "state.delta",
                None,
                Some(serde_json::to_value(delta)?),
            )?)
            .await?;
        Ok(())
    }

    async fn send_cached_snapshot(&mut self) -> Result<(), RemoteAgentError> {
        let Some(snapshot) = self.last_snapshot.as_mut() else {
            return Ok(());
        };
        snapshot.revision = Uuid::new_v4().to_string();
        snapshot.generated_at = unix_time_millis();
        let payload = serde_json::to_value(snapshot)?;
        self.relay
            .send(WireMessage::outbound(
                "state.snapshot",
                None,
                Some(payload),
            )?)
            .await?;
        Ok(())
    }

    async fn send_sync_progress(
        &mut self,
        phase: &str,
        progress: u8,
        message: &str,
    ) -> Result<(), RemoteAgentError> {
        self.relay
            .send(WireMessage::outbound(
                "sync.progress",
                None,
                Some(json!({ "phase": phase, "progress": progress, "message": message })),
            )?)
            .await?;
        Ok(())
    }

    async fn build_snapshot(&mut self) -> Result<StateSnapshotDto, RemoteAgentError> {
        let project_state = load_codex_project_state()?;
        let raw_threads = self.list_all_threads().await?;
        let raw_models = self.list_all_models().await?;
        let mut snapshot = self.normalize_snapshot(project_state, raw_threads, raw_models)?;
        let mut latest_activity = HashMap::new();
        let mut thread_session_paths = HashMap::new();
        for session in scan_sessions() {
            let source_path = session.source_path.as_deref().map(PathBuf::from);
            let latest_message_time = source_path
                .as_deref()
                .and_then(|path| latest_message_timestamp(path));
            if let Some(path) = source_path {
                thread_session_paths.insert(session.session_id.clone(), path);
            }
            if let Some(time) = latest_message_time.or(session.last_active_at) {
                latest_activity.insert(session.session_id, time);
            }
        }
        let mut recent_thread_ids = latest_activity
            .iter()
            .map(|(thread_id, updated_at)| (thread_id.as_str(), *updated_at))
            .collect::<Vec<_>>();
        recent_thread_ids.sort_unstable_by(|left, right| right.1.cmp(&left.1));
        let local_execution_states = recent_thread_ids
            .into_iter()
            .take(STATUS_POLL_LIMIT as usize)
            .filter_map(|(thread_id, _)| {
                let state = thread_session_paths
                    .get(thread_id)
                    .and_then(|path| latest_thread_execution_state(path))?;
                Some((thread_id.to_string(), state))
            })
            .collect::<HashMap<_, _>>();
        for thread in &mut snapshot.threads {
            if let Some(updated_at) = latest_activity.get(&thread.id) {
                thread.updated_at = *updated_at;
            }
            if let Some(state) = local_execution_states.get(&thread.id) {
                thread.status = merge_thread_execution_state(&thread.status, *state);
            }
        }

        self.thread_session_paths = thread_session_paths;
        self.projects = snapshot
            .projects
            .iter()
            .cloned()
            .map(|project| (project.id.clone(), project))
            .collect();
        self.thread_summaries = snapshot
            .threads
            .iter()
            .cloned()
            .map(|thread| (thread.id.clone(), thread))
            .collect();
        self.last_snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }

    fn normalize_snapshot(
        &mut self,
        project_state: CodexProjectState,
        raw_threads: Vec<RawThread>,
        raw_models: Vec<RawModel>,
    ) -> Result<StateSnapshotDto, RemoteAgentError> {
        let project_ids: HashSet<&str> = project_state
            .projects
            .iter()
            .map(|project| project.id.as_str())
            .collect();
        let projectless_ids: HashSet<&str> = project_state
            .projectless_thread_ids
            .iter()
            .map(String::as_str)
            .collect();
        let dangling_ids: HashSet<&str> = project_state
            .dangling_thread_assignment_ids
            .iter()
            .map(String::as_str)
            .collect();
        let pinned_ids: HashSet<&str> = project_state
            .pinned_thread_ids
            .iter()
            .map(String::as_str)
            .collect();
        let mut threads = Vec::with_capacity(raw_threads.len());
        let mut unassigned_thread_count = 0_usize;

        for thread in raw_threads {
            if dangling_ids.contains(thread.id.as_str()) {
                continue;
            }
            let project_id = if let Some(assignment) = project_state
                .thread_project_assignments
                .get(thread.id.as_str())
            {
                if !project_ids.contains(assignment.project_id.as_str()) {
                    continue;
                }
                Some(assignment.project_id.clone())
            } else if projectless_ids.contains(thread.id.as_str()) {
                None
            } else {
                // App Server 还会列出 VS Code 等客户端创建、但 Codex 桌面端
                // 从未纳入项目状态的任务。它们不能靠 cwd 猜归属，也不应出现在
                // Remote 的桌面项目视图中。
                unassigned_thread_count = unassigned_thread_count.saturating_add(1);
                continue;
            };

            threads.push(ThreadSummaryDto {
                pinned: pinned_ids.contains(thread.id.as_str()),
                id: thread.id,
                project_id,
                name: thread.name,
                preview: thread.preview,
                cwd: thread.cwd,
                created_at: thread.created_at,
                updated_at: thread.updated_at,
                status: thread.status,
            });
        }

        let current_thread_ids: HashSet<String> =
            threads.iter().map(|thread| thread.id.clone()).collect();
        let mut deleted_thread_ids: Vec<String> = self
            .known_thread_ids
            .difference(&current_thread_ids)
            .cloned()
            .collect();
        deleted_thread_ids.sort_unstable();
        self.known_thread_ids = current_thread_ids;
        if unassigned_thread_count > 0 {
            log::debug!(
                "Skipped {unassigned_thread_count} App Server threads without desktop project assignments"
            );
        }

        let models = raw_models
            .into_iter()
            .map(|model| ModelDto {
                id: model.id,
                display_name: model.display_name,
                hidden: model.hidden,
                is_default: model.is_default,
                default_reasoning_effort: Some(model.default_reasoning_effort),
                supported_reasoning_efforts: model
                    .supported_reasoning_efforts
                    .into_iter()
                    .map(|effort| ReasoningEffortDto {
                        reasoning_effort: effort.reasoning_effort,
                        description: Some(effort.description),
                    })
                    .collect(),
            })
            .collect();

        Ok(StateSnapshotDto {
            revision: Uuid::new_v4().to_string(),
            generated_at: unix_time_millis(),
            projects: project_state.projects,
            threads,
            models,
            deleted_thread_ids,
        })
    }

    async fn list_all_threads(&self) -> Result<Vec<RawThread>, RemoteAgentError> {
        let mut all = Vec::new();
        let mut cursor = None;
        let mut seen_cursors = HashSet::new();

        for _ in 0..MAX_PAGES {
            let mut params = Map::from_iter([
                ("limit".to_string(), json!(PAGE_SIZE)),
                ("archived".to_string(), json!(false)),
            ]);
            if let Some(cursor) = &cursor {
                params.insert("cursor".to_string(), json!(cursor));
            }
            let page: Page<RawThread> = serde_json::from_value(
                self.app_server
                    .request("thread/list", Value::Object(params))
                    .await?,
            )?;
            all.extend(page.data);

            let Some(next_cursor) = page.next_cursor else {
                return Ok(all);
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(RemoteAgentError::Incompatible(
                    "thread/list 返回了重复 cursor".to_string(),
                ));
            }
            cursor = Some(next_cursor);
        }

        Err(RemoteAgentError::Incompatible(format!(
            "thread/list 超过 {MAX_PAGES} 页"
        )))
    }

    async fn list_recent_threads_for_status(&self) -> Result<Vec<RawThread>, RemoteAgentError> {
        let page: Page<RawThread> = serde_json::from_value(
            self.app_server
                .request(
                    "thread/list",
                    json!({
                        "limit": STATUS_POLL_LIMIT,
                        "archived": false,
                        "sortKey": "recency_at",
                        "sortDirection": "desc",
                        "useStateDbOnly": true,
                    }),
                )
                .await?,
        )?;
        Ok(page.data)
    }

    async fn refresh_desktop_thread_statuses(&mut self) -> Result<(), RemoteAgentError> {
        let recent_threads = self.list_recent_threads_for_status().await?;
        let states = recent_threads
            .into_iter()
            .filter_map(|thread| {
                let state = self
                    .thread_session_paths
                    .get(&thread.id)
                    .and_then(|path| latest_thread_execution_state(path))?;
                Some((thread.id, state))
            })
            .collect::<Vec<_>>();
        let mut changed = Vec::new();
        for (thread_id, state) in states {
            if self.update_thread_execution_state(&thread_id, state) {
                changed.push((thread_id, state));
            }
        }
        if changed.is_empty() {
            return Ok(());
        }

        for (thread_id, state) in &changed {
            let (phase, message) = match state {
                ThreadExecutionState::Active => ("thinking", "AI 正在处理"),
                ThreadExecutionState::Idle => ("completed", "任务已完成"),
            };
            self.send_execution_status(thread_id, phase, message)
                .await?;
        }
        self.send_cached_snapshot().await?;
        if let Some(thread_id) = self.last_detail_thread_id.clone() {
            if changed
                .iter()
                .any(|(changed_id, _)| changed_id == &thread_id)
            {
                self.send_thread_detail(&thread_id, 0, THREAD_DETAIL_PAGE_SIZE)
                    .await?;
            }
        }
        Ok(())
    }

    fn update_thread_execution_state(
        &mut self,
        thread_id: &str,
        state: ThreadExecutionState,
    ) -> bool {
        let Some(current) = self
            .thread_summaries
            .get(thread_id)
            .map(|thread| thread.status.clone())
        else {
            return false;
        };
        self.update_thread_status(thread_id, merge_thread_execution_state(&current, state))
    }

    fn update_thread_status(&mut self, thread_id: &str, status: ThreadStatusDto) -> bool {
        let mut changed = false;
        if let Some(thread) = self.thread_summaries.get_mut(thread_id) {
            if thread.status != status {
                thread.status = status.clone();
                changed = true;
            }
        }
        if let Some(thread) = self.last_snapshot.as_mut().and_then(|snapshot| {
            snapshot
                .threads
                .iter_mut()
                .find(|thread| thread.id == thread_id)
        }) {
            if thread.status != status {
                thread.status = status;
                changed = true;
            }
        }
        changed
    }

    async fn list_all_models(&self) -> Result<Vec<RawModel>, RemoteAgentError> {
        let mut all = Vec::new();
        let mut cursor = None;
        let mut seen_cursors = HashSet::new();

        for _ in 0..MAX_PAGES {
            let mut params = Map::from_iter([
                ("limit".to_string(), json!(PAGE_SIZE)),
                ("includeHidden".to_string(), json!(false)),
            ]);
            if let Some(cursor) = &cursor {
                params.insert("cursor".to_string(), json!(cursor));
            }
            let page: Page<RawModel> = serde_json::from_value(
                self.app_server
                    .request("model/list", Value::Object(params))
                    .await?,
            )?;
            all.extend(page.data);

            let Some(next_cursor) = page.next_cursor else {
                return Ok(all);
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(RemoteAgentError::Incompatible(
                    "model/list 返回了重复 cursor".to_string(),
                ));
            }
            cursor = Some(next_cursor);
        }

        Err(RemoteAgentError::Incompatible(format!(
            "model/list 超过 {MAX_PAGES} 页"
        )))
    }

    async fn send_thread_detail(
        &mut self,
        thread_id: &str,
        before: usize,
        limit: usize,
    ) -> Result<(), RemoteAgentError> {
        if !self.thread_summaries.contains_key(thread_id) {
            self.build_snapshot().await?;
        }
        let summary = self
            .thread_summaries
            .get(thread_id)
            .cloned()
            .ok_or_else(|| RemoteAgentError::ThreadNotFound(thread_id.to_string()))?;
        if let Some((items, local_model, local_effort, local_context_usage)) =
            load_local_thread_detail(thread_id)
        {
            let (items, has_more_before, next_before) =
                paginate_conversation_items(items, before, limit);
            let settings = self.thread_settings.get(thread_id);
            let detail = ThreadDetailDto {
                revision: Uuid::new_v4().to_string(),
                generated_at: unix_time_millis(),
                thread: summary.clone(),
                items,
                active_turn_id: None,
                selected_model: local_model
                    .or_else(|| settings.and_then(|settings| settings.model.clone())),
                selected_reasoning_effort: local_effort
                    .or_else(|| settings.and_then(|settings| settings.effort.clone())),
                context_usage: local_context_usage
                    .or_else(|| self.thread_context_usage.get(thread_id).copied()),
                before,
                has_more_before,
                next_before,
            };
            self.send_thread_detail_payload(thread_id, serde_json::to_value(detail)?)
                .await?;
            self.last_detail_thread_id = Some(thread_id.to_string());
            return Ok(());
        }
        let response = self
            .app_server
            .request(
                "thread/read",
                json!({ "threadId": thread_id, "includeTurns": true }),
            )
            .await?;
        let raw_thread: RawThread =
            serde_json::from_value(response.get("thread").cloned().ok_or_else(|| {
                RemoteAgentError::Incompatible("thread/read 缺少 thread".to_string())
            })?)?;
        if raw_thread.id != thread_id {
            return Err(RemoteAgentError::Incompatible(
                "thread/read 返回了错误的任务 ID".to_string(),
            ));
        }

        let active_turn_id = raw_thread
            .turns
            .iter()
            .find(|turn| matches!(turn.status, TurnStatus::InProgress))
            .map(|turn| turn.id.clone());
        let mut items = normalize_conversation_items(&raw_thread.turns);
        append_pending_approvals(&mut items, thread_id, &self.pending_approvals);
        let (items, has_more_before, next_before) =
            paginate_conversation_items(items, before, limit);
        let settings = self.thread_settings.get(thread_id);
        let detail = ThreadDetailDto {
            revision: Uuid::new_v4().to_string(),
            generated_at: unix_time_millis(),
            thread: ThreadSummaryDto {
                status: raw_thread.status,
                name: raw_thread.name,
                preview: raw_thread.preview,
                cwd: raw_thread.cwd,
                created_at: raw_thread.created_at,
                updated_at: raw_thread.updated_at,
                ..summary
            },
            items,
            active_turn_id,
            selected_model: settings.and_then(|settings| settings.model.clone()),
            selected_reasoning_effort: settings.and_then(|settings| settings.effort.clone()),
            context_usage: self.thread_context_usage.get(thread_id).copied(),
            before,
            has_more_before,
            next_before,
        };
        let payload = serde_json::to_value(detail)?;
        self.send_thread_detail_payload(thread_id, payload).await?;
        self.last_detail_thread_id = Some(thread_id.to_string());
        Ok(())
    }

    async fn send_thread_detail_payload(
        &mut self,
        thread_id: &str,
        payload: Value,
    ) -> Result<(), RemoteAgentError> {
        let encoded = serde_json::to_string(&payload)?;
        if encoded.chars().count() <= DETAIL_CHUNK_CHARS {
            self.relay
                .send(WireMessage::outbound("thread.detail", None, Some(payload))?)
                .await?;
            return Ok(());
        }

        let transfer_id = Uuid::new_v4().to_string();
        let chunks: Vec<String> = encoded
            .chars()
            .collect::<Vec<_>>()
            .chunks(DETAIL_CHUNK_CHARS)
            .map(|chunk| chunk.iter().collect())
            .collect();
        let total = chunks.len();
        self.relay
            .send(WireMessage::outbound(
                "thread.detail.start",
                None,
                Some(json!({ "transferId": transfer_id, "threadId": thread_id, "total": total })),
            )?)
            .await?;
        for (index, data) in chunks.into_iter().enumerate() {
            self.relay
                .send(WireMessage::outbound(
                    "thread.detail.chunk",
                    None,
                    Some(json!({
                        "transferId": transfer_id,
                        "threadId": thread_id,
                        "index": index,
                        "total": total,
                        "data": data,
                    })),
                )?)
                .await?;
        }
        self.relay
            .send(WireMessage::outbound(
                "thread.detail.end",
                None,
                Some(json!({ "transferId": transfer_id, "threadId": thread_id, "total": total })),
            )?)
            .await?;
        Ok(())
    }

    async fn send_execution_status(
        &mut self,
        thread_id: &str,
        phase: &str,
        message: &str,
    ) -> Result<(), RemoteAgentError> {
        self.relay
            .send(WireMessage::outbound(
                "execution.status",
                None,
                Some(json!({
                    "threadId": thread_id,
                    "phase": phase,
                    "message": message,
                    "updatedAt": unix_time_millis(),
                })),
            )?)
            .await?;
        Ok(())
    }

    async fn create_thread(
        &mut self,
        project_id: &str,
        request_id: Option<String>,
    ) -> Result<(), RemoteAgentError> {
        if !self.projects.contains_key(project_id) {
            self.build_snapshot().await?;
        }
        let project = self
            .projects
            .get(project_id)
            .ok_or_else(|| RemoteAgentError::ProjectNotFound(project_id.to_string()))?;
        let cwd = project
            .root_paths
            .first()
            .filter(|path| !path.is_empty())
            .ok_or_else(|| RemoteAgentError::ProjectDirectoryMissing(project_id.to_string()))?;
        let response = self
            .app_server
            .request("thread/start", json!({ "cwd": cwd }))
            .await?;
        let thread_id = response
            .pointer("/thread/id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                RemoteAgentError::Incompatible("thread/start 缺少 thread.id".to_string())
            })?;
        if let Err(error) = assign_thread_to_project(thread_id, project_id) {
            let _ = self
                .app_server
                .request("thread/delete", json!({ "threadId": thread_id }))
                .await;
            return Err(error.into());
        }
        self.send_snapshot_silent().await?;
        self.relay
            .send(WireMessage::outbound(
                "request.ack",
                request_id,
                Some(json!({
                    "action": "thread.created",
                    "threadId": thread_id,
                    "projectId": project_id,
                })),
            )?)
            .await?;
        Ok(())
    }

    async fn set_thread_pinned(
        &mut self,
        thread_id: &str,
        pinned: bool,
    ) -> Result<(), RemoteAgentError> {
        if !self.thread_summaries.contains_key(thread_id) {
            self.build_snapshot().await?;
        }
        if !self.thread_summaries.contains_key(thread_id) {
            return Err(RemoteAgentError::ThreadNotFound(thread_id.to_string()));
        }
        set_thread_pinned(thread_id, pinned)?;
        self.send_snapshot_silent().await
    }

    async fn start_turn(
        &mut self,
        thread_id: &str,
        text: String,
        model: Option<String>,
        effort: Option<String>,
        attachments: Vec<RemoteAttachment>,
    ) -> Result<(), RemoteAgentError> {
        self.send_execution_status(thread_id, "starting", "正在启动任务")
            .await?;
        if !self.thread_summaries.contains_key(thread_id) {
            self.build_snapshot().await?;
        }
        if !self.thread_summaries.contains_key(thread_id) {
            return Err(RemoteAgentError::ThreadNotFound(thread_id.to_string()));
        }

        let uploaded_attachments = attachments
            .iter()
            .map(|attachment| {
                let uploaded = self
                    .uploaded_attachments
                    .get(&attachment.upload_id)
                    .filter(|uploaded| {
                        uploaded.name == attachment.name
                            && uploaded.mime_type == attachment.mime_type
                            && uploaded.size == attachment.size
                    })
                    .cloned()
                    .ok_or_else(|| {
                        RemoteAgentError::Attachment(format!(
                            "附件 {} 尚未完成上传",
                            attachment.name
                        ))
                    })?;
                Ok((attachment.upload_id.clone(), uploaded))
            })
            .collect::<Result<Vec<_>, RemoteAgentError>>()?;
        // The response is not used here. Excluding persisted turns keeps long histories from
        // delaying the subsequent turn/start request or exceeding transport limits.
        let mut resume_params = Map::from_iter([
            ("threadId".to_string(), json!(thread_id)),
            ("excludeTurns".to_string(), json!(true)),
        ]);
        if let Some(model) = &model {
            resume_params.insert("model".to_string(), json!(model));
        }
        if let Err(error) = self
            .app_server
            .request("thread/resume", Value::Object(resume_params))
            .await
        {
            if !is_active_writer_conflict(&error) {
                return Err(error.into());
            }
            if !attachments.is_empty() {
                return Err(RemoteAgentError::Attachment(
                    "桌面端正在使用该任务时暂不支持通过手机发送附件".to_string(),
                ));
            }
            self.send_execution_status(thread_id, "starting", "正在交由 Codex 桌面端发送")
                .await?;
            let desktop_thread_id = thread_id.to_string();
            let desktop_text = text.clone();
            let desktop_text_for_send = desktop_text.clone();
            tokio::task::spawn_blocking(move || {
                desktop_control::send_text_to_desktop_thread(&desktop_thread_id, &desktop_text_for_send)
            })
            .await
            .map_err(|error| RemoteAgentError::Incompatible(format!("桌面端发送任务中断: {error}")))??;
            let mut confirmed = false;
            for _ in 0..10 {
                if load_local_thread_detail(thread_id).is_some_and(|(items, _, _, _)| {
                    items.iter().any(|item| {
                        matches!(item.kind, ConversationKind::UserMessage)
                            && item.text.as_deref().is_some_and(|value| value.trim() == desktop_text.trim())
                    })
                }) {
                    confirmed = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            if !confirmed {
                return Err(RemoteAgentError::DesktopControl(DesktopControlError(
                    "桌面端未确认消息已写入该会话".to_string(),
                )));
            }
            self.send_execution_status(thread_id, "output", "已发送至 Codex 桌面端")
                .await?;
            self.send_snapshot_silent().await?;
            self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE).await?;
            return Ok(());
        }
        self.remote_turn_thread_ids.insert(thread_id.to_string());

        let mut input = Vec::new();
        if !text.trim().is_empty() {
            input.push(json!({ "type": "text", "text": text }));
        }
        input.extend(uploaded_attachments.iter().map(|(_, attachment)| {
            let path = attachment.path.to_string_lossy();
            if attachment.mime_type.starts_with("image/") {
                json!({ "type": "localImage", "path": path })
            } else if attachment.mime_type.starts_with("audio/") {
                json!({ "type": "localAudio", "path": path })
            } else {
                json!({ "type": "mention", "name": attachment.name, "path": path })
            }
        }));
        let mut turn_params = Map::from_iter([
            ("threadId".to_string(), json!(thread_id)),
            ("input".to_string(), Value::Array(input)),
        ]);
        if let Some(model) = &model {
            turn_params.insert("model".to_string(), json!(model));
        }
        if let Some(effort) = &effort {
            turn_params.insert("effort".to_string(), json!(effort));
        }
        if let Err(error) = self
            .app_server
            .request("turn/start", Value::Object(turn_params))
            .await
        {
            self.remote_turn_thread_ids.remove(thread_id);
            if self.remote_turn_thread_ids.is_empty() {
                self.app_server_restart_requested = true;
            }
            return Err(error.into());
        }
        self.thread_settings
            .insert(thread_id.to_string(), ThreadSettings { model, effort });
        for (upload_id, _) in uploaded_attachments {
            self.uploaded_attachments.remove(&upload_id);
        }
        self.send_snapshot_silent().await?;
        self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE)
            .await
    }

    fn start_attachment_upload(
        &mut self,
        upload_id: String,
        name: String,
        mime_type: String,
        size: u64,
    ) -> Result<(), RemoteAgentError> {
        if self.pending_attachment_uploads.contains_key(&upload_id)
            || self.uploaded_attachments.contains_key(&upload_id)
        {
            return Err(RemoteAgentError::Attachment("附件上传 ID 重复".to_string()));
        }
        let safe_name = sanitize_attachment_name(&name);
        let path = self
            .attachment_dir
            .path()
            .join(format!("{}-{safe_name}", Uuid::new_v4().simple()));
        let file = fs::File::create(&path)?;
        self.pending_attachment_uploads.insert(
            upload_id,
            PendingAttachmentUpload {
                attachment: UploadedAttachment {
                    name,
                    mime_type,
                    size,
                    path,
                },
                expected_size: size,
                received_size: 0,
                next_index: 0,
                file,
            },
        );
        Ok(())
    }

    fn write_attachment_chunk(
        &mut self,
        upload_id: &str,
        index: u32,
        data: &str,
    ) -> Result<(), RemoteAgentError> {
        let upload = self
            .pending_attachment_uploads
            .get_mut(upload_id)
            .ok_or_else(|| RemoteAgentError::Attachment("找不到待上传附件".to_string()))?;
        if upload.next_index != index {
            return Err(RemoteAgentError::Attachment("附件分块顺序无效".to_string()));
        }
        let bytes = BASE64_STANDARD.decode(data)?;
        let next_size = upload.received_size.saturating_add(bytes.len() as u64);
        if next_size > upload.expected_size {
            return Err(RemoteAgentError::Attachment(
                "附件大小超过声明值".to_string(),
            ));
        }
        upload.file.write_all(&bytes)?;
        upload.received_size = next_size;
        upload.next_index += 1;
        Ok(())
    }

    fn finish_attachment_upload(&mut self, upload_id: &str) -> Result<(), RemoteAgentError> {
        let mut upload = self
            .pending_attachment_uploads
            .remove(upload_id)
            .ok_or_else(|| RemoteAgentError::Attachment("找不到待上传附件".to_string()))?;
        if upload.received_size != upload.expected_size {
            return Err(RemoteAgentError::Attachment("附件上传不完整".to_string()));
        }
        upload.file.flush()?;
        self.uploaded_attachments
            .insert(upload_id.to_string(), upload.attachment);
        Ok(())
    }

    async fn interrupt_turn(
        &mut self,
        thread_id: &str,
        turn_id: Option<String>,
    ) -> Result<(), RemoteAgentError> {
        let turn_id = match turn_id {
            Some(turn_id) => turn_id,
            None => self.find_active_turn_id(thread_id).await?,
        };
        self.app_server
            .request(
                "turn/interrupt",
                json!({ "threadId": thread_id, "turnId": turn_id }),
            )
            .await?;
        self.send_snapshot_silent().await?;
        self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE)
            .await
    }

    async fn find_active_turn_id(&self, thread_id: &str) -> Result<String, RemoteAgentError> {
        let response = self
            .app_server
            .request(
                "thread/read",
                json!({ "threadId": thread_id, "includeTurns": true }),
            )
            .await?;
        let thread: RawThread =
            serde_json::from_value(response.get("thread").cloned().ok_or_else(|| {
                RemoteAgentError::Incompatible("thread/read 缺少 thread".to_string())
            })?)?;
        thread
            .turns
            .into_iter()
            .find(|turn| matches!(turn.status, TurnStatus::InProgress))
            .map(|turn| turn.id)
            .ok_or(RemoteAgentError::NoActiveTurn)
    }

    async fn respond_approval(
        &mut self,
        thread_id: &str,
        approval_request_id: &str,
        decision: ApprovalDecision,
    ) -> Result<(), RemoteAgentError> {
        let approval = self
            .pending_approvals
            .remove(approval_request_id)
            .filter(|approval| approval.thread_id == thread_id)
            .ok_or(RemoteAgentError::ApprovalNotFound)?;
        self.app_server
            .respond(
                approval.app_server_request_id,
                Ok(json!({ "decision": approval_decision_value(decision) })),
            )
            .await?;
        self.send_snapshot_silent().await?;
        self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE)
            .await
    }

    fn store_approval(
        &mut self,
        request_id: Value,
        method: &str,
        params: Value,
    ) -> Result<(), RemoteAgentError> {
        let thread_id = params
            .get("threadId")
            .and_then(Value::as_str)
            .ok_or_else(|| RemoteAgentError::Incompatible("审批请求缺少 threadId".to_string()))?;
        let approval_id = Uuid::new_v4().to_string();
        let title = if method == "item/commandExecution/requestApproval" {
            "确认执行命令"
        } else {
            "确认修改文件"
        };
        self.pending_approvals.insert(
            approval_id,
            PendingApproval {
                app_server_request_id: request_id,
                thread_id: thread_id.to_string(),
                title: title.to_string(),
                detail: serde_json::to_string_pretty(&params)?,
            },
        );
        Ok(())
    }

    async fn refresh_current_view(&mut self) -> Result<(), RemoteAgentError> {
        self.send_snapshot_silent().await?;
        if let Some(thread_id) = self.last_detail_thread_id.clone() {
            self.send_thread_detail(&thread_id, 0, THREAD_DETAIL_PAGE_SIZE)
                .await?;
        }
        Ok(())
    }

    async fn send_current_thread_detail(
        &mut self,
        thread_id: &str,
    ) -> Result<(), RemoteAgentError> {
        if self.last_detail_thread_id.as_deref() == Some(thread_id) {
            self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE)
                .await?;
        }
        Ok(())
    }

    async fn send_request_error(&self, request_id: Option<String>, error: &RemoteAgentError) {
        if !self.relay.is_connected() {
            return;
        }
        let message = WireMessage::outbound(
            "request.error",
            request_id,
            Some(json!({ "message": error.to_string() })),
        );
        if let Ok(message) = message {
            let _ = self.relay.send(message).await;
        }
    }

    async fn restart_app_server(
        &mut self,
    ) -> Result<broadcast::Receiver<AppServerEvent>, AppServerError> {
        let replacement = AppServerClient::start().await?;
        let events = replacement.subscribe();
        self.app_server.shutdown().await;
        self.app_server = replacement;
        self.pending_approvals.clear();
        Ok(events)
    }
}

fn merge_thread_execution_state(
    current: &ThreadStatusDto,
    state: ThreadExecutionState,
) -> ThreadStatusDto {
    match state {
        ThreadExecutionState::Active => match current {
            ThreadStatusDto::Active { .. } => current.clone(),
            _ => ThreadStatusDto::Active {
                active_flags: Vec::new(),
            },
        },
        ThreadExecutionState::Idle => match current {
            ThreadStatusDto::Active { .. } => ThreadStatusDto::Idle,
            _ => current.clone(),
        },
    }
}

fn load_local_thread_detail(
    thread_id: &str,
) -> Option<(
    Vec<ConversationItemDto>,
    Option<String>,
    Option<String>,
    Option<ContextUsageDto>,
)> {
    let path = find_session_file(&session_roots(), thread_id)?;
    let (model, effort) = latest_thread_settings(&path);
    let context_usage =
        latest_thread_context_usage(&path).map(|(used_tokens, model_context_window)| {
            ContextUsageDto {
                used_tokens,
                model_context_window,
            }
        });
    let messages = load_messages(&path).ok()?;
    let message_attachments = load_message_attachments(&path).ok()?;
    let items = messages
        .into_iter()
        .zip(message_attachments)
        .enumerate()
        .filter_map(|(index, (message, detail))| {
            let kind = match message.role.as_str() {
                "user" => ConversationKind::UserMessage,
                "assistant" => ConversationKind::AgentMessage,
                "tool" => ConversationKind::ToolCall,
                _ => return None,
            };
            Some(ConversationItemDto {
                id: format!("local-{thread_id}-{index}"),
                kind,
                status: Some(ConversationStatus::Completed),
                title: None,
                text: Some(message.content),
                detail,
                created_at: message.ts,
                approval_request_id: None,
                approval_options: Vec::new(),
            })
        })
        .collect::<Vec<_>>();
    Some((items, model, effort, context_usage))
}

fn paginate_conversation_items(
    items: Vec<ConversationItemDto>,
    before: usize,
    limit: usize,
) -> (Vec<ConversationItemDto>, bool, Option<usize>) {
    let end = items.len().saturating_sub(before);
    let start = end.saturating_sub(limit);
    let page_length = end - start;
    let has_more_before = start > 0;
    let next_before = has_more_before.then_some(before.saturating_add(page_length));
    (items.into_iter().skip(start).take(page_length).collect(), has_more_before, next_before)
}

fn context_usage_from_notification(params: &Value) -> Option<ContextUsageDto> {
    let token_usage = params.get("tokenUsage")?;
    let used_tokens = token_usage
        .pointer("/last/totalTokens")
        .and_then(Value::as_u64)?;
    let model_context_window = token_usage
        .get("modelContextWindow")
        .and_then(Value::as_u64)?;
    (model_context_window > 0).then_some(ContextUsageDto {
        used_tokens,
        model_context_window,
    })
}

fn clean_activity_message(value: &str) -> String {
    value.trim().trim_matches('*').trim().to_string()
}

fn find_session_file(roots: &[PathBuf], thread_id: &str) -> Option<PathBuf> {
    roots
        .iter()
        .find_map(|root| find_session_file_in(root, thread_id))
}

fn find_session_file_in(path: &Path, thread_id: &str) -> Option<PathBuf> {
    let entries = fs::read_dir(path).ok()?;
    for entry in entries.flatten() {
        let entry_path = entry.path();
        if entry_path.is_dir() {
            if let Some(found) = find_session_file_in(&entry_path, thread_id) {
                return Some(found);
            }
        } else if entry_path.extension().and_then(|value| value.to_str()) == Some("jsonl")
            && entry_path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.contains(thread_id))
        {
            return Some(entry_path);
        }
    }
    None
}

fn command_request_id(command: &RemoteCommand) -> Option<&str> {
    match command {
        RemoteCommand::Sync
        | RemoteCommand::SyncIncremental { .. }
        | RemoteCommand::UploadAttachmentChunk { .. } => None,
        RemoteCommand::ReadThread { request_id, .. }
        | RemoteCommand::CreateThread { request_id, .. }
        | RemoteCommand::StartTurn { request_id, .. }
        | RemoteCommand::StartAttachmentUpload { request_id, .. }
        | RemoteCommand::FinishAttachmentUpload { request_id, .. }
        | RemoteCommand::InterruptTurn { request_id, .. }
        | RemoteCommand::RespondApproval { request_id, .. }
        | RemoteCommand::SetThreadPinned { request_id, .. } => request_id.as_deref(),
    }
}

fn is_active_writer_conflict(error: &AppServerError) -> bool {
    matches!(
        error,
        AppServerError::Remote { message, .. }
            if message.contains("already has an active writer")
    )
}

fn sanitize_attachment_name(name: &str) -> String {
    let file_name = name
        .rsplit(['/', '\\'])
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("attachment");
    let safe_name = file_name
        .chars()
        .map(|character| match character {
            character if character.is_alphanumeric() => character,
            '.' | '-' | '_' | ' ' => character,
            _ => '_',
        })
        .collect::<String>();
    if safe_name.is_empty() {
        "attachment".to_string()
    } else {
        safe_name
    }
}

fn normalize_conversation_items(turns: &[RawTurn]) -> Vec<ConversationItemDto> {
    let mut result = Vec::new();
    for turn in turns {
        for item in &turn.items {
            result.push(normalize_conversation_item(item, turn));
        }
        if let Some(error) = &turn.error {
            result.push(ConversationItemDto {
                id: format!("{}-error", turn.id),
                kind: ConversationKind::Error,
                status: Some(ConversationStatus::Failed),
                title: Some("执行失败".to_string()),
                text: value_text(error).or_else(|| Some("任务执行失败".to_string())),
                detail: pretty_value(error),
                created_at: turn.completed_at.or(turn.started_at),
                approval_request_id: None,
                approval_options: Vec::new(),
            });
        }
    }
    result
}

fn normalize_conversation_item(item: &Value, turn: &RawTurn) -> ConversationItemDto {
    let item_type = item
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let id = item
        .get("id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{}-{}", turn.id, Uuid::new_v4()));
    let turn_status = Some(conversation_status_from_turn(turn.status));
    let empty_approval = Vec::new();

    match item_type {
        "userMessage" => ConversationItemDto {
            id,
            kind: ConversationKind::UserMessage,
            status: turn_status,
            title: None,
            text: user_message_text(item.get("content")),
            detail: non_text_user_content(item.get("content")),
            created_at: turn.started_at,
            approval_request_id: None,
            approval_options: empty_approval,
        },
        "agentMessage" => ConversationItemDto {
            id,
            kind: ConversationKind::AgentMessage,
            status: turn_status,
            title: None,
            text: item
                .get("text")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            detail: None,
            created_at: turn.completed_at,
            approval_request_id: None,
            approval_options: empty_approval,
        },
        "reasoning" | "plan" => ConversationItemDto {
            id,
            kind: ConversationKind::Reasoning,
            status: turn_status,
            title: (item_type == "plan").then(|| "计划".to_string()),
            text: item
                .get("text")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| join_string_array(item.get("summary")))
                .or_else(|| join_string_array(item.get("content"))),
            detail: None,
            created_at: turn.started_at,
            approval_request_id: None,
            approval_options: empty_approval,
        },
        "commandExecution" => ConversationItemDto {
            id,
            kind: ConversationKind::Command,
            status: item_status(item).or(turn_status),
            title: item
                .get("command")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            text: None,
            detail: item
                .get("aggregatedOutput")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            created_at: turn.started_at,
            approval_request_id: None,
            approval_options: empty_approval,
        },
        "fileChange" => ConversationItemDto {
            id,
            kind: ConversationKind::FileChange,
            status: item_status(item).or(turn_status),
            title: Some("文件修改".to_string()),
            text: None,
            detail: item.get("changes").and_then(pretty_value),
            created_at: turn.started_at,
            approval_request_id: None,
            approval_options: empty_approval,
        },
        "mcpToolCall" | "dynamicToolCall" | "collabAgentToolCall" | "webSearch" => {
            ConversationItemDto {
                id,
                kind: ConversationKind::ToolCall,
                status: item_status(item).or(turn_status),
                title: tool_title(item, item_type),
                text: None,
                detail: pretty_value(item),
                created_at: turn.started_at,
                approval_request_id: None,
                approval_options: empty_approval,
            }
        }
        _ => ConversationItemDto {
            id,
            kind: ConversationKind::ToolCall,
            status: turn_status,
            title: Some(item_type.to_string()),
            text: value_text(item),
            detail: pretty_value(item),
            created_at: turn.started_at,
            approval_request_id: None,
            approval_options: empty_approval,
        },
    }
}

fn append_pending_approvals(
    items: &mut Vec<ConversationItemDto>,
    thread_id: &str,
    pending: &HashMap<String, PendingApproval>,
) {
    let mut approvals: Vec<(&String, &PendingApproval)> = pending
        .iter()
        .filter(|(_, approval)| approval.thread_id == thread_id)
        .collect();
    approvals.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

    for (approval_id, approval) in approvals {
        items.push(ConversationItemDto {
            id: format!("approval-{approval_id}"),
            kind: ConversationKind::Approval,
            status: Some(ConversationStatus::Pending),
            title: Some(approval.title.clone()),
            text: None,
            detail: Some(approval.detail.clone()),
            created_at: Some(unix_time_millis()),
            approval_request_id: Some(approval_id.clone()),
            approval_options: vec![
                ApprovalOptionDto {
                    id: ApprovalDecisionDto::Accept,
                    label: "允许",
                    tone: ApprovalTone::Primary,
                },
                ApprovalOptionDto {
                    id: ApprovalDecisionDto::AcceptForSession,
                    label: "本次会话始终允许",
                    tone: ApprovalTone::Neutral,
                },
                ApprovalOptionDto {
                    id: ApprovalDecisionDto::Decline,
                    label: "拒绝",
                    tone: ApprovalTone::Neutral,
                },
                ApprovalOptionDto {
                    id: ApprovalDecisionDto::Cancel,
                    label: "拒绝并中断",
                    tone: ApprovalTone::Danger,
                },
            ],
        });
    }
}

fn conversation_status_from_turn(status: TurnStatus) -> ConversationStatus {
    match status {
        TurnStatus::Completed => ConversationStatus::Completed,
        TurnStatus::Interrupted => ConversationStatus::Interrupted,
        TurnStatus::Failed => ConversationStatus::Failed,
        TurnStatus::InProgress => ConversationStatus::Running,
    }
}

fn item_status(item: &Value) -> Option<ConversationStatus> {
    match item.get("status").and_then(Value::as_str) {
        Some("inProgress") => Some(ConversationStatus::Running),
        Some("completed") => Some(ConversationStatus::Completed),
        Some("failed") => Some(ConversationStatus::Failed),
        Some("declined") => Some(ConversationStatus::Interrupted),
        _ => None,
    }
}

fn user_message_text(content: Option<&Value>) -> Option<String> {
    let text = content
        .and_then(Value::as_array)?
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

fn non_text_user_content(content: Option<&Value>) -> Option<String> {
    let items: Vec<Value> = content
        .and_then(Value::as_array)?
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) != Some("text"))
        .cloned()
        .collect();
    (!items.is_empty())
        .then(|| serde_json::to_string_pretty(&items).ok())
        .flatten()
}

fn join_string_array(value: Option<&Value>) -> Option<String> {
    let text = value
        .and_then(Value::as_array)?
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

fn tool_title(item: &Value, item_type: &str) -> Option<String> {
    if item_type == "webSearch" {
        return item
            .get("query")
            .and_then(Value::as_str)
            .map(|query| format!("网页搜索：{query}"));
    }
    let tool = item.get("tool").and_then(Value::as_str)?;
    let server = item
        .get("server")
        .or_else(|| item.get("namespace"))
        .and_then(Value::as_str);
    Some(match server {
        Some(server) => format!("{server} / {tool}"),
        None => tool.to_string(),
    })
}

fn value_text(value: &Value) -> Option<String> {
    value.as_str().map(ToOwned::to_owned).or_else(|| {
        value
            .get("message")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    })
}

fn pretty_value(value: &Value) -> Option<String> {
    serde_json::to_string_pretty(value).ok()
}

fn approval_decision_value(decision: ApprovalDecision) -> &'static str {
    match decision {
        ApprovalDecision::Accept => "accept",
        ApprovalDecision::AcceptForSession => "acceptForSession",
        ApprovalDecision::Decline => "decline",
        ApprovalDecision::Cancel => "cancel",
    }
}

fn unix_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    use super::*;

    #[test]
    fn normalizes_complete_conversation_without_dropping_unknown_items() {
        let turns = vec![RawTurn {
            id: "turn-1".to_string(),
            status: TurnStatus::Completed,
            items: vec![
                json!({
                    "id": "user-1",
                    "type": "userMessage",
                    "content": [
                        { "type": "text", "text": "检查项目" },
                        { "type": "localImage", "path": "C:/tmp/example.png" }
                    ]
                }),
                json!({
                    "id": "agent-1",
                    "type": "agentMessage",
                    "text": "已经完成。"
                }),
                json!({
                    "id": "future-1",
                    "type": "futureItem",
                    "value": 1
                }),
            ],
            error: None,
            started_at: Some(10),
            completed_at: Some(20),
        }];

        let items = normalize_conversation_items(&turns);

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].text.as_deref(), Some("检查项目"));
        assert!(items[0]
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("localImage")));
        assert!(matches!(items[2].kind, ConversationKind::ToolCall));
        assert!(items[2].detail.is_some());
    }

    #[test]
    fn maps_turn_and_item_statuses_to_mobile_protocol() {
        assert!(matches!(
            conversation_status_from_turn(TurnStatus::InProgress),
            ConversationStatus::Running
        ));
        assert!(matches!(
            item_status(&json!({ "status": "declined" })),
            Some(ConversationStatus::Interrupted)
        ));
    }

    #[test]
    fn parses_app_server_context_usage_notification() {
        let usage = context_usage_from_notification(&json!({
            "tokenUsage": {
                "last": { "totalTokens": 4096 },
                "modelContextWindow": 258400
            }
        }))
        .expect("context usage");

        assert_eq!(usage.used_tokens, 4096);
        assert_eq!(usage.model_context_window, 258400);
    }

    #[test]
    fn removes_reasoning_markdown_from_activity_message() {
        assert_eq!(
            clean_activity_message("**Running app-server help command**"),
            "Running app-server help command"
        );
    }

    #[test]
    fn sanitizes_attachment_file_name_without_losing_extension() {
        assert_eq!(
            sanitize_attachment_name("../需求:说明.pdf"),
            "需求_说明.pdf"
        );
    }

    #[test]
    fn merges_local_execution_state_without_losing_active_flags() {
        let waiting = ThreadStatusDto::Active {
            active_flags: vec![ThreadActiveFlag::WaitingOnApproval],
        };
        assert_eq!(
            merge_thread_execution_state(&waiting, ThreadExecutionState::Active),
            waiting
        );
        assert_eq!(
            merge_thread_execution_state(&waiting, ThreadExecutionState::Idle),
            ThreadStatusDto::Idle
        );
        assert_eq!(
            merge_thread_execution_state(&ThreadStatusDto::NotLoaded, ThreadExecutionState::Active),
            ThreadStatusDto::Active {
                active_flags: Vec::new(),
            }
        );
    }

    #[tokio::test]
    #[ignore = "requires the local Docker relay and a working Codex App Server"]
    async fn live_agent_relays_snapshot_and_thread_detail() {
        let access_key = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let agent = RemoteControlAgent::start("ws://127.0.0.1/ws/agent", access_key.clone())
            .await
            .expect("start live Remote Agent");

        timeout(Duration::from_secs(15), async {
            while !agent.is_connected() {
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("Agent did not connect to Relay");

        let session_response = reqwest::Client::new()
            .post("http://127.0.0.1/api/sessions")
            .json(&json!({ "accessKey": access_key }))
            .send()
            .await
            .expect("create web session");
        assert!(session_response.status().is_success());
        let session: Value = session_response.json().await.expect("decode web session");
        let session_token = session
            .get("sessionToken")
            .and_then(Value::as_str)
            .expect("session token");

        let (mut web, _) = tokio_tungstenite::connect_async(format!(
            "ws://127.0.0.1/ws/web?session={session_token}"
        ))
        .await
        .expect("connect web client");
        let status = next_live_message(&mut web, "agent.status").await;
        assert_eq!(status.pointer("/payload/online"), Some(&Value::Bool(true)));

        web.send(Message::Text(
            json!({
                "version": 1,
                "type": "sync.request",
                "requestId": Uuid::new_v4().to_string(),
                "payload": {}
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("request snapshot");
        let snapshot = next_live_message(&mut web, "state.snapshot").await;
        assert!(snapshot
            .pointer("/payload/projects")
            .and_then(Value::as_array)
            .is_some_and(|projects| !projects.is_empty()));
        assert!(snapshot
            .pointer("/payload/models")
            .and_then(Value::as_array)
            .is_some_and(|models| !models.is_empty()));

        if let Some(thread_id) = snapshot
            .pointer("/payload/threads/0/id")
            .and_then(Value::as_str)
        {
            web.send(Message::Text(
                json!({
                    "version": 1,
                    "type": "thread.read",
                    "requestId": Uuid::new_v4().to_string(),
                    "payload": { "threadId": thread_id }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("request thread detail");
            let detail = next_live_message(&mut web, "thread.detail").await;
            assert_eq!(
                detail.pointer("/payload/thread/id").and_then(Value::as_str),
                Some(thread_id)
            );
            assert!(detail
                .pointer("/payload/items")
                .and_then(Value::as_array)
                .is_some());
        }

        web.close(None).await.expect("close web client");
        agent.shutdown().await;
    }

    async fn next_live_message(
        socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        expected_type: &str,
    ) -> Value {
        timeout(Duration::from_secs(60), async {
            loop {
                let message = socket
                    .next()
                    .await
                    .expect("web socket closed")
                    .expect("receive web message");
                if let Message::Text(text) = message {
                    let value: Value = serde_json::from_str(&text).expect("decode web message");
                    if value.get("type").and_then(Value::as_str) == Some(expected_type) {
                        return value;
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {expected_type}"))
    }
}
