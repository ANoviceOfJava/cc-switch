use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::app_server::{AppServerClient, AppServerError, AppServerEvent};
use super::desktop_control;
use super::project_state::{
    assign_thread_to_project, load_codex_project_state, set_thread_pinned, CodexProject,
    CodexProjectState, ProjectStateError,
};
use super::protocol::{
    ApprovalDecision, KnownThreadRevision, ProtocolError, RemoteAttachment, RemoteCommand, RemoteSkill,
    WireMessage,
};
use super::relay_client::{RelayClient, RelayError, RelayEvent};
use crate::app_config::AppType;
use crate::provider::Provider;
use crate::services::{model_fetch, ProviderService};
use crate::session_manager::providers::codex::{
    latest_message_timestamp, latest_thread_context_usage, latest_thread_execution_state,
    latest_thread_settings, load_message_attachments, load_messages, scan_sessions, session_roots,
    ThreadExecutionState,
};
use crate::store::AppState;

const PAGE_SIZE: u32 = 100;
const MAX_PAGES: usize = 100;
const DETAIL_CHUNK_CHARS: usize = 180_000;
const STATUS_POLL_LIMIT: u32 = 50;
const THREAD_DETAIL_PAGE_SIZE: usize = 5;
const MODEL_CATALOG_TTL: Duration = Duration::from_secs(10 * 60);
const MODEL_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const MAX_MODELS_PER_PROVIDER: usize = 5;

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

}

pub(crate) struct RemoteControlAgent {
    shutdown: mpsc::Sender<oneshot::Sender<()>>,
    task: JoinHandle<()>,
    relay_status: RelayClient,
}

struct AgentRuntime {
    app_state: AppState,
    app_server: AppServerClient,
    relay: RelayClient,
    known_thread_ids: HashSet<String>,
    projects: HashMap<String, CodexProject>,
    thread_summaries: HashMap<String, ThreadSummaryDto>,
    fresh_thread_ids: HashSet<String>,
    thread_session_paths: HashMap<String, PathBuf>,
    thread_settings: HashMap<String, ThreadSettings>,
    thread_goals: HashMap<String, ThreadGoalDto>,
    thread_context_usage: HashMap<String, ContextUsageDto>,
    thread_reasoning_activity: HashMap<String, String>,
    attachment_dir: tempfile::TempDir,
    pending_attachment_uploads: HashMap<String, PendingAttachmentUpload>,
    uploaded_attachments: HashMap<String, UploadedAttachment>,
    remote_turn_thread_ids: HashSet<String>,
    app_server_restart_requested: bool,
    pending_approvals: HashMap<String, PendingApproval>,
    last_detail_thread_id: Option<String>,
    detail_turn_cache: Option<DetailTurnCache>,
    model_catalog_cache: Option<ModelCatalogCache>,
    last_snapshot: Option<StateSnapshotDto>,
    /// 桌面端自动化发送后、会话文件尚未落盘期间的乐观消息。
    pending_user_messages: HashMap<String, Vec<ConversationItemDto>>,
    /// 当前执行不允许同轮插入时，等待本轮完成后再发送的消息。
    queued_turn_messages: Vec<QueuedTurnMessage>,
}
#[derive(Clone)]
struct QueuedTurnMessage {
    id: String,
    thread_id: String,
    text: String,
    model: Option<String>,
    effort: Option<String>,
    approval_policy: Option<Value>,
    sandbox_policy: Option<Value>,
    collaboration_mode: Option<String>,
    skills: Vec<RemoteSkill>,
    attachments: Vec<RemoteAttachment>,
}

#[derive(Clone)]
struct ThreadSettings {
    model: Option<String>,
    effort: Option<String>,
    collaboration_mode: Option<String>,
}

struct DetailTurnCache {
    thread_id: String,
    updated_at: i64,
    turns: Vec<Vec<ConversationItemDto>>,
}

struct ModelCatalogCache {
    fetched_at: Instant,
    current_provider_id: Option<String>,
    models: Vec<ModelDto>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSkillListResponse {
    data: Vec<RawSkillListEntry>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSkillListEntry {
    skills: Vec<RawSkill>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSkill {
    name: String,
    description: String,
    enabled: bool,
    path: String,
    scope: String,
    short_description: Option<String>,
    interface: Option<RawSkillInterface>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSkillInterface {
    display_name: Option<String>,
    short_description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SkillDto {
    name: String,
    display_name: Option<String>,
    description: String,
    short_description: Option<String>,
    path: String,
    scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadGoalDto {
    objective: String,
    status: String,
    #[serde(default)]
    token_budget: Option<i64>,
    #[serde(default)]
    tokens_used: i64,
    #[serde(default)]
    time_used_seconds: i64,
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
    parent_thread_id: Option<String>,
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
    thread_signatures: Vec<KnownThreadRevision>,
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
    thread_signatures: Vec<KnownThreadRevision>,
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
    parent_thread_id: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_icon: Option<String>,
    #[serde(default)]
    current_provider: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReasoningEffortDto {
    reasoning_effort: String,
    description: Option<String>,
}


#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueuedMessageDto {
    id: String,
    text: String,
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
    collaboration_mode: Option<String>,
    goal: Option<ThreadGoalDto>,
    context_usage: Option<ContextUsageDto>,
    before: usize,
    has_more_before: bool,
    next_before: Option<usize>,
    queued_messages: Vec<QueuedMessageDto>,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
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
        app_state: AppState,
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
            app_state,
            app_server,
            relay,
            known_thread_ids: HashSet::new(),
            projects: HashMap::new(),
            thread_summaries: HashMap::new(),
            fresh_thread_ids: HashSet::new(),
            thread_session_paths: HashMap::new(),
            thread_settings: HashMap::new(),
            thread_goals: HashMap::new(),
            thread_context_usage: HashMap::new(),
            thread_reasoning_activity: HashMap::new(),
            attachment_dir,
            pending_attachment_uploads: HashMap::new(),
            uploaded_attachments: HashMap::new(),
            remote_turn_thread_ids: HashSet::new(),
            app_server_restart_requested: false,
            pending_approvals: HashMap::new(),
            last_detail_thread_id: None,
            detail_turn_cache: None,
            model_catalog_cache: None,
            last_snapshot: None,
            pending_user_messages: HashMap::new(),
            queued_turn_messages: Vec::new(),
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
                request_id: read_request_id,
                thread_id,
                before,
                limit,
            } => {
                self.send_thread_detail(&thread_id, before, limit, read_request_id).await
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
                approval_policy,
                sandbox_policy,
                collaboration_mode,
                skills,
                attachments,
                ..
            } => {
                self.start_turn(
                    &thread_id,
                    text,
                    model,
                    effort,
                    approval_policy,
                    sandbox_policy,
                    collaboration_mode,
                    skills,
                    attachments,
                )
                .await
            }
            RemoteCommand::QueueTurn {
                request_id: _,
                queue_id,
                thread_id,
                text,
                model,
                effort,
                approval_policy,
                sandbox_policy,
                collaboration_mode,
                skills,
                attachments,
            } => {
                self.queue_turn_message(QueuedTurnMessage {
                    id: queue_id,
                    thread_id,
                    text,
                    model,
                    effort,
                    approval_policy,
                    sandbox_policy,
                    collaboration_mode,
                    skills,
                    attachments,
                })
                .await
            }
            RemoteCommand::SteerQueuedTurn {
                thread_id,
                queue_id,
                ..
            } => self.steer_queued_turn(&thread_id, &queue_id).await,
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
            RemoteCommand::ListSkills {
                thread_id,
                project_id,
                force_reload,
                ..
            } => {
                self.list_skills(thread_id.as_deref(), project_id.as_deref(), force_reload)
                    .await
            }
            RemoteCommand::SetCollaborationMode {
                thread_id, mode, ..
            } => self.set_collaboration_mode(&thread_id, &mode).await,
            RemoteCommand::SetThreadGoal {
                thread_id,
                objective,
                token_budget,
                ..
            } => self.set_thread_goal(&thread_id, objective, token_budget).await,
            RemoteCommand::ClearThreadGoal { thread_id, .. } => {
                self.clear_thread_goal(&thread_id).await
            }
            RemoteCommand::SetThreadName {
                thread_id, name, ..
            } => self.set_thread_name(&thread_id, name).await,
            RemoteCommand::ArchiveThread { thread_id, .. } => {
                self.archive_thread(&thread_id).await
            }
            RemoteCommand::DeleteThread { thread_id, .. } => self.delete_thread(&thread_id).await,
            RemoteCommand::CompactThread { thread_id, .. } => {
                self.compact_thread(&thread_id).await
            }
            RemoteCommand::ListModels { request_id } => {
                self.send_model_catalog(request_id).await
            }
            RemoteCommand::SelectModel {
                request_id,
                provider_id,
                model_id,
            } => {
                self.select_provider_model(request_id, &provider_id, &model_id)
                    .await
            }
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
                    "thread/settings/updated" => {
                        let Some(thread_id) = thread_id else {
                            return Ok(());
                        };
                        let settings = params.get("threadSettings").ok_or_else(|| {
                            RemoteAgentError::Incompatible(
                                "thread/settings/updated 缺少 threadSettings".to_string(),
                            )
                        })?;
                        let current = self.thread_settings.entry(thread_id.clone()).or_insert(
                            ThreadSettings {
                                model: None,
                                effort: None,
                                collaboration_mode: None,
                            },
                        );
                        if let Some(model) = settings.get("model").and_then(Value::as_str) {
                            current.model = Some(model.to_string());
                        }
                        current.effort = settings
                            .get("effort")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        current.collaboration_mode = settings
                            .pointer("/collaborationMode/mode")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        self.send_current_thread_detail(&thread_id).await?;
                    }
                    "thread/goal/updated" => {
                        let Some(thread_id) = thread_id else {
                            return Ok(());
                        };
                        let goal: ThreadGoalDto = serde_json::from_value(
                            params.get("goal").cloned().ok_or_else(|| {
                                RemoteAgentError::Incompatible(
                                    "thread/goal/updated 缺少 goal".to_string(),
                                )
                            })?,
                        )?;
                        self.thread_goals.insert(thread_id.clone(), goal);
                        self.send_current_thread_detail(&thread_id).await?;
                    }
                    "thread/goal/cleared" => {
                        let Some(thread_id) = thread_id else {
                            return Ok(());
                        };
                        self.thread_goals.remove(&thread_id);
                        self.send_current_thread_detail(&thread_id).await?;
                    }
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
                                self.release_thread_writer(thread_id).await;
                                self.flush_queued_turn_message(thread_id).await?;
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
            let signature = thread_signature(thread);
            if known_by_id.get(&thread.id) != Some(&signature) {
                changed_threads.push(thread.clone());
            }
        }
        let thread_signatures = known_thread_revisions(&snapshot.threads);
        let delta = StateDeltaDto {
            revision: snapshot.revision,
            generated_at: snapshot.generated_at,
            projects: snapshot.projects,
            thread_signatures,
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
                parent_thread_id: thread.parent_thread_id,
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
                provider_id: None,
                provider_name: None,
                provider_icon: None,
                current_provider: false,
            })
            .collect();

        let thread_signatures = known_thread_revisions(&threads);
        Ok(StateSnapshotDto {
            revision: Uuid::new_v4().to_string(),
            generated_at: unix_time_millis(),
            projects: project_state.projects,
            threads,
            thread_signatures,
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
            if matches!(state, ThreadExecutionState::Idle) {
                self.release_thread_writer(thread_id).await;
            }
        }
        self.send_cached_snapshot().await?;
        if let Some(thread_id) = self.last_detail_thread_id.clone() {
            if changed
                .iter()
                .any(|(changed_id, _)| changed_id == &thread_id)
            {
                self.send_thread_detail(&thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
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

    async fn release_previous_detail_thread(&mut self, current_thread_id: &str) {
        let Some(previous) = self
            .last_detail_thread_id
            .clone()
            .filter(|thread_id| thread_id != current_thread_id)
        else {
            return;
        };
        if self.remote_turn_thread_ids.contains(&previous) {
            return;
        }
        self.last_detail_thread_id = None;
        let _ = self
            .app_server
            .request("thread/unsubscribe", json!({ "threadId": previous }))
            .await;
    }

    async fn release_thread_writer(&mut self, thread_id: &str) {
        if !self.remote_turn_thread_ids.remove(thread_id) {
            return;
        }
        match self
            .app_server
            .request("thread/unsubscribe", json!({ "threadId": thread_id }))
            .await
        {
            Ok(_) => {}
            Err(error) => {
                log::warn!(
                    "Failed to unsubscribe thread {thread_id}; restarting app-server to release writer lock: {error}"
                );
                self.app_server_restart_requested = true;
            }
        }
    }

    fn queued_messages_for(&self, thread_id: &str) -> Vec<QueuedMessageDto> {
        self.queued_turn_messages
            .iter()
            .filter(|message| message.thread_id == thread_id)
            .map(|message| QueuedMessageDto {
                id: message.id.clone(),
                text: message.text.clone(),
            })
            .collect()
    }


    async fn send_model_catalog(
        &mut self,
        request_id: Option<String>,
    ) -> Result<(), RemoteAgentError> {
        let now = Instant::now();
        let cached = self
            .model_catalog_cache
            .as_ref()
            .filter(|cache| now.duration_since(cache.fetched_at) < MODEL_CATALOG_TTL)
            .map(|cache| (cache.current_provider_id.clone(), cache.models.clone()));
        let (current_provider_id, models) = match cached {
            Some(cached) => cached,
            None => {
                let built = build_model_catalog(&self.app_state).await?;
                self.model_catalog_cache = Some(ModelCatalogCache {
                    fetched_at: now,
                    current_provider_id: built.0.clone(),
                    models: built.1.clone(),
                });
                built
            }
        };
        self.relay
            .send(WireMessage::outbound(
                "models.catalog",
                request_id,
                Some(json!({
                    "currentProviderId": current_provider_id,
                    "models": models,
                })),
            )?)
            .await?;
        Ok(())
    }

    async fn select_provider_model(
        &mut self,
        request_id: Option<String>,
        provider_id: &str,
        model_id: &str,
    ) -> Result<(), RemoteAgentError> {
        let mut provider = self
            .app_state
            .db
            .get_provider_by_id(provider_id, AppType::Codex.as_str())
            .map_err(|error| RemoteAgentError::Incompatible(error.to_string()))?
            .ok_or_else(|| RemoteAgentError::ProjectNotFound(provider_id.to_string()))?;
        let config = provider
            .settings_config
            .get("config")
            .and_then(Value::as_str)
            .ok_or_else(|| RemoteAgentError::Incompatible("供应商缺少 Codex config".to_string()))?;
        let updated = crate::codex_config::update_codex_toml_field(config, "model", model_id)
            .map_err(RemoteAgentError::Incompatible)?;
        let settings = provider
            .settings_config
            .as_object_mut()
            .ok_or_else(|| RemoteAgentError::Incompatible("供应商配置不是对象".to_string()))?;
        settings.insert("config".to_string(), Value::String(updated));
        self.app_state
            .db
            .save_provider(AppType::Codex.as_str(), &provider)
            .map_err(|error| RemoteAgentError::Incompatible(error.to_string()))?;
        ProviderService::switch(&self.app_state, AppType::Codex, provider_id)
            .map_err(|error| RemoteAgentError::Incompatible(error.to_string()))?;
        self.app_server_restart_requested = true;
        self.model_catalog_cache = None;
        self.relay
            .send(WireMessage::outbound(
                "model.selected",
                request_id,
                Some(json!({
                    "providerId": provider_id,
                    "modelId": model_id,
                })),
            )?)
            .await?;
        self.send_snapshot().await?;
        Ok(())
    }

    async fn send_thread_detail(
        &mut self,
        thread_id: &str,
        before: usize,
        limit: usize,
        request_id: Option<String>,
    ) -> Result<(), RemoteAgentError> {
        self.release_previous_detail_thread(thread_id).await;
        if !self.thread_summaries.contains_key(thread_id) {
            self.build_snapshot().await?;
        }
        let summary = self
            .thread_summaries
            .get(thread_id)
            .cloned()
            .unwrap_or_else(|| ThreadSummaryDto {
                pinned: false,
                id: thread_id.to_string(),
                project_id: None,
                name: None,
                preview: String::new(),
                cwd: String::new(),
                created_at: unix_time_millis(),
                updated_at: unix_time_millis(),
                status: ThreadStatusDto::NotLoaded,
                parent_thread_id: None,
            });
        if self.fresh_thread_ids.contains(thread_id) {
            let selected_model = self
                .thread_settings
                .get(thread_id)
                .and_then(|settings| settings.model.clone());
            let selected_reasoning_effort = self
                .thread_settings
                .get(thread_id)
                .and_then(|settings| settings.effort.clone());
            let collaboration_mode = self
                .thread_settings
                .get(thread_id)
                .and_then(|settings| settings.collaboration_mode.clone());
            let detail = ThreadDetailDto {
                revision: Uuid::new_v4().to_string(),
                generated_at: unix_time_millis(),
                thread: summary,
                items: self.merge_pending_user_messages(thread_id, Vec::new()),
                active_turn_id: None,
                selected_model,
                selected_reasoning_effort,
                collaboration_mode,
                goal: self.thread_goals.get(thread_id).cloned(),
                context_usage: self.thread_context_usage.get(thread_id).copied(),
                before,
                has_more_before: false,
                next_before: None,
                queued_messages: self.queued_messages_for(thread_id),
            };
            self.send_thread_detail_payload(thread_id, serde_json::to_value(detail)?, request_id.clone()).await?;
            self.last_detail_thread_id = Some(thread_id.to_string());
            return Ok(());
        }
        if before > 0 {
            if let Some((items, has_more_before, next_before)) =
                self.paginate_cached_detail(thread_id, summary.updated_at, before, limit)
            {
                let settings = self.thread_settings.get(thread_id);
                let detail = ThreadDetailDto {
                    revision: Uuid::new_v4().to_string(),
                    generated_at: unix_time_millis(),
                    thread: summary,
                    items,
                    active_turn_id: None,
                    selected_model: settings.and_then(|settings| settings.model.clone()),
                    selected_reasoning_effort: settings.and_then(|settings| settings.effort.clone()),
                    collaboration_mode: settings
                        .and_then(|settings| settings.collaboration_mode.clone()),
                    goal: self.thread_goals.get(thread_id).cloned(),
                    context_usage: self.thread_context_usage.get(thread_id).copied(),
                    before,
                    has_more_before,
                    next_before,
                    queued_messages: self.queued_messages_for(thread_id),
                };
                self.send_thread_detail_payload(
                    thread_id,
                    serde_json::to_value(detail)?,
                    request_id.clone(),
                )
                .await?;
                self.last_detail_thread_id = Some(thread_id.to_string());
                return Ok(());
            }
        }
        if let Some((items, local_model, local_effort, local_context_usage)) =
            load_local_thread_detail(thread_id)
        {
            let items = self.merge_pending_user_messages(thread_id, items);
            let turns = group_conversation_items_into_turns(items);
            self.cache_detail_turns(thread_id, summary.updated_at, turns.clone());
            let (items, has_more_before, next_before) =
                paginate_conversation_turns(&turns, before, limit);
            if let Some(usage) = local_context_usage {
                self.thread_context_usage.insert(thread_id.to_string(), usage);
            }
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
                collaboration_mode: settings
                    .and_then(|settings| settings.collaboration_mode.clone()),
                goal: self.thread_goals.get(thread_id).cloned(),
                context_usage: local_context_usage
                    .or_else(|| self.thread_context_usage.get(thread_id).copied()),
                before,
                has_more_before,
                next_before,
                queued_messages: self.queued_messages_for(thread_id),
            };
            self.send_thread_detail_payload(thread_id, serde_json::to_value(detail)?, request_id.clone())
                .await?;
            self.last_detail_thread_id = Some(thread_id.to_string());
            return Ok(());
        }
        let read_result = async {
            let params = json!({ "threadId": thread_id, "includeTurns": true });
            let mut result = Err(AppServerError::Closed);
            for attempt in 0..10 {
                match self.app_server.request("thread/read", params.clone()).await {
                    Ok(value) => {
                        result = Ok(value);
                        break;
                    }
                    Err(error) if is_thread_not_found_error(&error) && attempt < 9 => {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                    Err(error) => {
                        result = Err(error);
                        break;
                    }
                }
            }
            result
        }
        .await;
        let response = match read_result {
            Ok(response) => response,
            Err(error) if is_thread_not_found_error(&error) => {
                let items = self.merge_pending_user_messages(thread_id, Vec::new());
                let settings = self.thread_settings.get(thread_id);
                let detail = ThreadDetailDto {
                    revision: Uuid::new_v4().to_string(),
                    generated_at: unix_time_millis(),
                    thread: summary,
                    items,
                    active_turn_id: None,
                    selected_model: settings.and_then(|settings| settings.model.clone()),
                    selected_reasoning_effort: settings.and_then(|settings| settings.effort.clone()),
                    collaboration_mode: settings
                        .and_then(|settings| settings.collaboration_mode.clone()),
                    goal: self.thread_goals.get(thread_id).cloned(),
                    context_usage: self.thread_context_usage.get(thread_id).copied(),
                    before,
                    has_more_before: false,
                    next_before: None,
                        queued_messages: self.queued_messages_for(thread_id),
                };
                self.send_thread_detail_payload(thread_id, serde_json::to_value(detail)?, request_id.clone()).await?;
                self.last_detail_thread_id = Some(thread_id.to_string());
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
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
        items = self.merge_pending_user_messages(thread_id, items);
        append_pending_approvals(&mut items, thread_id, &self.pending_approvals);
        let turns = group_conversation_items_into_turns(items);
        self.cache_detail_turns(thread_id, summary.updated_at, turns.clone());
        let (items, has_more_before, next_before) =
            paginate_conversation_turns(&turns, before, limit);
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
            collaboration_mode: settings
                .and_then(|settings| settings.collaboration_mode.clone()),
            goal: self.thread_goals.get(thread_id).cloned(),
            context_usage: self.thread_context_usage.get(thread_id).copied(),
            before,
            has_more_before,
            next_before,
            queued_messages: self.queued_messages_for(thread_id),
        };
        let payload = serde_json::to_value(detail)?;
        self.send_thread_detail_payload(thread_id, payload, request_id.clone()).await?;
        self.last_detail_thread_id = Some(thread_id.to_string());
        Ok(())
    }

    /// 将桌面端尚未落盘的手机消息合并到会话详情，避免刷新时乐观气泡消失。
    fn merge_pending_user_messages(
        &mut self,
        thread_id: &str,
        mut items: Vec<ConversationItemDto>,
    ) -> Vec<ConversationItemDto> {
        let Some(mut pending) = self.pending_user_messages.remove(thread_id) else {
            return items;
        };
        let persisted_texts: HashSet<String> = items
            .iter()
            .filter(|item| matches!(item.kind, ConversationKind::UserMessage))
            .filter_map(|item| item.text.clone())
            .collect();
        pending.retain(|item| {
            item.text
                .as_ref()
                .is_none_or(|text| !persisted_texts.contains(text))
        });
        items.extend(pending.iter().cloned());
        if !pending.is_empty() {
            self.pending_user_messages.insert(thread_id.to_string(), pending);
        }
        items
    }

    async fn send_thread_detail_payload(
        &mut self,
        thread_id: &str,
        payload: Value,
        request_id: Option<String>,
    ) -> Result<(), RemoteAgentError> {
        let encoded = serde_json::to_string(&payload)?;
        if encoded.chars().count() <= DETAIL_CHUNK_CHARS {
            self.relay
                .send(WireMessage::outbound("thread.detail", request_id.clone(), Some(payload))?)
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
                request_id.clone(),
                Some(json!({ "transferId": transfer_id, "threadId": thread_id, "total": total })),
            )?)
            .await?;
        for (index, data) in chunks.into_iter().enumerate() {
            self.relay
                .send(WireMessage::outbound(
                    "thread.detail.chunk",
                    request_id.clone(),
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
                request_id.clone(),
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
        // 先把 thread/start 返回的任务写入内存索引，确保创建确认到达前后的
        // thread.read 能拿到稳定的摘要；thread/list 可能稍后才包含它。
        let thread_value = response.get("thread").cloned().unwrap_or(Value::Null);
        let now = unix_time_millis();
        self.thread_summaries.insert(
            thread_id.to_string(),
            ThreadSummaryDto {
                pinned: false,
                id: thread_id.to_string(),
                project_id: Some(project_id.to_string()),
                name: thread_value.get("name").and_then(Value::as_str).map(str::to_string),
                preview: thread_value
                    .get("preview")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                cwd: thread_value
                    .get("cwd")
                    .and_then(Value::as_str)
                    .unwrap_or(cwd)
                    .to_string(),
                created_at: thread_value
                    .get("createdAt")
                    .and_then(Value::as_i64)
                    .unwrap_or(now),
                updated_at: thread_value
                    .get("updatedAt")
                    .and_then(Value::as_i64)
                    .unwrap_or(now),
                status: thread_value
                    .get("status")
                    .cloned()
                    .and_then(|value| serde_json::from_value(value).ok())
                    .unwrap_or(ThreadStatusDto::NotLoaded),
                parent_thread_id: thread_value
                    .get("parentThreadId")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            },
        );
        self.fresh_thread_ids.insert(thread_id.to_string());
        // 先确认创建结果，避免 snapshot 较慢或失败时手机端一直停留在新对话页。
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
        if let Err(error) = self.send_snapshot_silent().await {
            log::warn!("Thread {thread_id} created but snapshot send failed: {error}");
        }
        // 远程 Agent 与桌面端 UI 使用不同的刷新通道；唤起新任务可让桌面端
        // 立即重新加载任务列表并显示刚创建的会话。
        let desktop_thread_id = thread_id.to_string();
        tokio::task::spawn_blocking(move || desktop_control::show_desktop_thread(&desktop_thread_id))
            .await
            .map_err(|error| RemoteAgentError::Incompatible(format!("桌面端刷新任务中断: {error}")))?
            .ok();
        Ok(())
    }

    async fn set_thread_pinned(
        &mut self,
        thread_id: &str,
        pinned: bool,
    ) -> Result<(), RemoteAgentError> {
        // 新建任务在 App Server 的 thread/start 成功后，可能还未及时出现在
        // thread/list 索引中。这里仍以 App Server 的 thread/resume 结果作为
        // 最终校验，否则手机端会在创建成功后立刻发送时被误判为不存在。
        if !self.thread_summaries.contains_key(thread_id) {
            let _ = self.build_snapshot().await;
        }
        set_thread_pinned(thread_id, pinned)?;
        self.send_snapshot_silent().await
    }

    async fn list_skills(
        &mut self,
        thread_id: Option<&str>,
        project_id: Option<&str>,
        force_reload: bool,
    ) -> Result<(), RemoteAgentError> {
        if self.thread_summaries.is_empty() {
            self.build_snapshot().await?;
        }
        let cwd = if let Some(thread_id) = thread_id {
            self.thread_summaries
                .get(thread_id)
                .map(|thread| thread.cwd.clone())
        } else if let Some(project_id) = project_id {
            self.projects
                .get(project_id)
                .and_then(|project| project.root_paths.first())
                .cloned()
        } else {
            None
        }
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RemoteAgentError::ProjectDirectoryMissing("skills".to_string()))?;

        let response: RawSkillListResponse = serde_json::from_value(
            self.app_server
                .request(
                    "skills/list",
                    json!({ "cwds": [cwd], "forceReload": force_reload }),
                )
                .await?,
        )?;
        let mut seen = HashSet::new();
        let mut skills = response
            .data
            .into_iter()
            .flat_map(|entry| entry.skills)
            .filter(|skill| skill.enabled)
            .filter_map(|skill| {
                let key = format!("{}:{}", skill.name, skill.path);
                seen.insert(key).then_some(SkillDto {
                    display_name: skill
                        .interface
                        .as_ref()
                        .and_then(|interface| interface.display_name.clone()),
                    short_description: skill
                        .interface
                        .as_ref()
                        .and_then(|interface| interface.short_description.clone())
                        .or(skill.short_description),
                    name: skill.name,
                    description: skill.description,
                    path: skill.path,
                    scope: skill.scope,
                })
            })
            .collect::<Vec<_>>();
        skills.sort_by(|left, right| {
            left.display_name
                .as_deref()
                .unwrap_or(&left.name)
                .to_lowercase()
                .cmp(&right.display_name.as_deref().unwrap_or(&right.name).to_lowercase())
        });
        self.relay
            .send(WireMessage::outbound(
                "skills.list",
                None,
                Some(json!({ "skills": skills })),
            )?)
            .await?;
        Ok(())
    }

    async fn set_collaboration_mode(
        &mut self,
        thread_id: &str,
        mode: &str,
    ) -> Result<(), RemoteAgentError> {
        if !self.thread_summaries.contains_key(thread_id) {
            self.build_snapshot().await?;
        }
        let model = self
            .thread_settings
            .get(thread_id)
            .and_then(|settings| settings.model.clone())
            .or_else(|| {
                self.last_snapshot
                    .as_ref()
                    .and_then(|snapshot| {
                        snapshot
                            .models
                            .iter()
                            .find(|model| model.is_default)
                            .or_else(|| snapshot.models.first())
                    })
                    .map(|model| model.id.clone())
            })
            .ok_or_else(|| {
                RemoteAgentError::Incompatible("计划模式缺少可用的模型".to_string())
            })?;
        let effort = self
            .thread_settings
            .get(thread_id)
            .and_then(|settings| settings.effort.clone());
        let mut settings = Map::new();
        settings.insert("model".to_string(), json!(model));
        if let Some(effort) = effort {
            settings.insert("reasoning_effort".to_string(), json!(effort));
        }
        self.app_server
            .request(
                "thread/settings/update",
                json!({
                    "threadId": thread_id,
                    "collaborationMode": {
                        "mode": mode,
                        "settings": Value::Object(settings),
                    },
                }),
            )
            .await?;
        let current = self
            .thread_settings
            .entry(thread_id.to_string())
            .or_insert(ThreadSettings {
                model: None,
                effort: None,
                collaboration_mode: None,
            });
        current.collaboration_mode = Some(mode.to_string());
        self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
            .await
    }

    async fn set_thread_goal(
        &mut self,
        thread_id: &str,
        objective: String,
        token_budget: Option<i64>,
    ) -> Result<(), RemoteAgentError> {
        if !self.thread_summaries.contains_key(thread_id) {
            self.build_snapshot().await?;
        }
        let response = self
            .app_server
            .request(
                "thread/goal/set",
                json!({
                    "threadId": thread_id,
                    "objective": objective,
                    "status": "active",
                    "tokenBudget": token_budget,
                }),
            )
            .await?;
        let goal: ThreadGoalDto = serde_json::from_value(
            response.get("goal").cloned().ok_or_else(|| {
                RemoteAgentError::Incompatible("thread/goal/set 缺少 goal".to_string())
            })?,
        )?;
        self.thread_goals.insert(thread_id.to_string(), goal);
        self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
            .await
    }

    async fn clear_thread_goal(&mut self, thread_id: &str) -> Result<(), RemoteAgentError> {
        self.app_server
            .request("thread/goal/clear", json!({ "threadId": thread_id }))
            .await?;
        self.thread_goals.remove(thread_id);
        self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
            .await
    }

    async fn set_thread_name(
        &mut self,
        thread_id: &str,
        name: String,
    ) -> Result<(), RemoteAgentError> {
        self.app_server
            .request(
                "thread/name/set",
                json!({ "threadId": thread_id, "name": name }),
            )
            .await?;
        if let Some(thread) = self.thread_summaries.get_mut(thread_id) {
            thread.name = Some(name);
            thread.updated_at = unix_time_millis();
        }
        self.send_snapshot_silent().await?;
        self.send_current_thread_detail(thread_id).await
    }

    async fn archive_thread(&mut self, thread_id: &str) -> Result<(), RemoteAgentError> {
        self.app_server
            .request("thread/archive", json!({ "threadId": thread_id }))
            .await?;
        self.thread_summaries.remove(thread_id);
        self.thread_session_paths.remove(thread_id);
        self.remote_turn_thread_ids.remove(thread_id);
        self.send_snapshot_silent().await
    }

    async fn delete_thread(&mut self, thread_id: &str) -> Result<(), RemoteAgentError> {
        self.app_server
            .request("thread/delete", json!({ "threadId": thread_id }))
            .await?;
        self.thread_summaries.remove(thread_id);
        self.thread_session_paths.remove(thread_id);
        self.remote_turn_thread_ids.remove(thread_id);
        self.send_snapshot_silent().await
    }

    async fn compact_thread(&mut self, thread_id: &str) -> Result<(), RemoteAgentError> {
        self.app_server
            .request("thread/compact/start", json!({ "threadId": thread_id }))
            .await?;
        self.send_execution_status(thread_id, "thinking", "正在压缩上下文")
            .await
    }

    async fn start_turn(
        &mut self,
        thread_id: &str,
        text: String,
        model: Option<String>,
        effort: Option<String>,
        approval_policy: Option<Value>,
        sandbox_policy: Option<Value>,
        collaboration_mode: Option<String>,
        skills: Vec<RemoteSkill>,
        attachments: Vec<RemoteAttachment>,
    ) -> Result<(), RemoteAgentError> {
        self.send_execution_status(thread_id, "starting", "正在启动任务")
            .await?;
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
        if let Some(summary) = self.thread_summaries.get(thread_id) {
            if !summary.cwd.trim().is_empty() {
                resume_params.insert("cwd".to_string(), json!(summary.cwd));
            }
        }
        // App Server 的索引可能在重启或迁移后暂时丢失，但 rollout 文件仍在。
        // 指定 path 可以直接从本地会话恢复，避免手机端继续使用旧任务时报
        // “thread not found”。
        if let Some(session_path) = find_session_file(&session_roots(), thread_id) {
            resume_params.insert(
                "path".to_string(),
                json!(session_path.to_string_lossy().to_string()),
            );
        }
        let is_fresh_thread = self.fresh_thread_ids.remove(thread_id);
        let resume_result = if is_fresh_thread {
            // thread/start 创建的空任务无需 resume；部分 App Server 版本会在
            // 空任务上尝试调用尚未实现的 list_turns。
            Ok(Value::Null)
        } else {
            let resume_params = Value::Object(resume_params);
            let mut result = Err(AppServerError::Closed);
            for attempt in 0..10 {
                match self
                    .app_server
                    .request("thread/resume", resume_params.clone())
                    .await
                {
                    Ok(value) => {
                        result = Ok(value);
                        break;
                    }
                    Err(error) if is_thread_not_found_error(&error) && attempt < 9 => {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                    Err(error) => {
                        result = Err(error);
                        break;
                    }
                }
            }
            result
        };
        // Resume may be rejected because the desktop owns the current turn. The
        // same app-server connection can still steer normal active turns without
        // requiring the desktop window to be focused or unlocked.
        match resume_result {
            Ok(_) => {}
            Err(error) if is_active_writer_conflict(&error) => {}
            Err(error) => return Err(error.into()),
        }
        self.remote_turn_thread_ids.insert(thread_id.to_string());

        let mut input = Vec::new();
        input.extend(skills.iter().map(|skill| {
            json!({ "type": "skill", "name": skill.name, "path": skill.path })
        }));
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
        if let Some(approval_policy) = approval_policy.clone() {
            turn_params.insert("approvalPolicy".to_string(), approval_policy);
        }
        if let Some(sandbox_policy) = sandbox_policy.clone() {
            turn_params.insert("sandboxPolicy".to_string(), sandbox_policy);
        }
        if let Some(mode) = collaboration_mode.as_deref() {
            let mode_model = model
                .clone()
                .or_else(|| {
                    self.thread_settings
                        .get(thread_id)
                        .and_then(|settings| settings.model.clone())
                })
                .or_else(|| {
                    self.last_snapshot
                        .as_ref()
                        .and_then(|snapshot| {
                            snapshot
                                .models
                                .iter()
                                .find(|model| model.is_default)
                                .or_else(|| snapshot.models.first())
                        })
                        .map(|model| model.id.clone())
                })
                .ok_or_else(|| {
                    RemoteAgentError::Incompatible(
                        "计划模式缺少可用的模型".to_string(),
                    )
                })?;
            let mut mode_settings = Map::new();
            mode_settings.insert("model".to_string(), json!(mode_model));
            if let Some(effort) = effort.clone() {
                mode_settings.insert("reasoning_effort".to_string(), json!(effort));
            }
            turn_params.insert(
                "collaborationMode".to_string(),
                json!({ "mode": mode, "settings": Value::Object(mode_settings) }),
            );
        }
        if let Err(error) = self
            .app_server
            .request("turn/start", Value::Object(turn_params))
            .await
        {
            self.release_thread_writer(thread_id).await;
            if is_active_writer_conflict(&error) || is_active_turn_not_steerable(&error) {
                self.queued_turn_messages.push(QueuedTurnMessage {
                    id: Uuid::new_v4().to_string(),
                    thread_id: thread_id.to_string(),
                    text: text.clone(),
                    model: model.clone(),
                    effort: effort.clone(),
                    approval_policy: approval_policy.clone(),
                    sandbox_policy: sandbox_policy.clone(),
                    collaboration_mode: collaboration_mode.clone(),
                    skills: skills.clone(),
                    attachments: attachments.clone(),
                });
                self.pending_user_messages
                    .entry(thread_id.to_string())
                    .or_default()
                    .push(ConversationItemDto {
                        id: format!("remote-pending-{}", Uuid::new_v4()),
                        kind: ConversationKind::UserMessage,
                        status: Some(ConversationStatus::Running),
                        title: None,
                        text: Some(text.clone()),
                        detail: None,
                        created_at: Some(unix_time_millis()),
                        approval_request_id: None,
                        approval_options: Vec::new(),
                    });
                self.send_execution_status(thread_id, "starting", "消息已排队，可从队列调整方向")
                    .await?;
                self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
                    .await?;
                return Ok(());
            }
            return Err(error.into());
        }
        let settings = self
            .thread_settings
            .entry(thread_id.to_string())
            .or_insert(ThreadSettings {
                model: None,
                effort: None,
                collaboration_mode: None,
            });
        if model.is_some() {
            settings.model = model;
        }
        if effort.is_some() {
            settings.effort = effort;
        }
        if collaboration_mode.is_some() {
            settings.collaboration_mode = collaboration_mode;
        }
        for (upload_id, _) in uploaded_attachments {
            self.uploaded_attachments.remove(&upload_id);
        }
        self.send_snapshot_silent().await?;
        self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
            .await
    }


    async fn queue_turn_message(&mut self, message: QueuedTurnMessage) -> Result<(), RemoteAgentError> {
        let thread_id = message.thread_id.clone();
        self.queued_turn_messages.push(message);
        self.send_execution_status(&thread_id, "starting", "消息已加入队列")
            .await?;
        self.send_thread_detail(&thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
            .await
    }

    async fn steer_queued_turn(
        &mut self,
        thread_id: &str,
        queue_id: &str,
    ) -> Result<(), RemoteAgentError> {
        let Some(index) = self
            .queued_turn_messages
            .iter()
            .position(|message| message.thread_id == thread_id && message.id == queue_id)
        else {
            return Err(RemoteAgentError::ThreadNotFound(queue_id.to_string()));
        };
        let message = self.queued_turn_messages[index].clone();
        let uploaded_attachments = message
            .attachments
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

        let mut input = Vec::new();
        input.extend(message.skills.iter().map(|skill| {
            json!({ "type": "skill", "name": skill.name, "path": skill.path })
        }));
        if !message.text.trim().is_empty() {
            input.push(json!({ "type": "text", "text": message.text }));
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
        let expected_turn_id = self.find_active_turn_id(thread_id).await?;
        self.app_server
            .request(
                "turn/steer",
                json!({
                    "threadId": thread_id,
                    "expectedTurnId": expected_turn_id,
                    "input": input,
                    "clientUserMessageId": message.id,
                }),
            )
            .await?;
        self.queued_turn_messages.remove(index);
        for (upload_id, _) in uploaded_attachments {
            self.uploaded_attachments.remove(&upload_id);
        }
        self.send_execution_status(thread_id, "thinking", "正在调整方向")
            .await?;
        self.send_snapshot_silent().await?;
        self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
            .await
    }


    async fn flush_queued_turn_message(&mut self, thread_id: &str) -> Result<(), RemoteAgentError> {
        let Some(index) = self.queued_turn_messages.iter().position(|message| message.thread_id == thread_id) else {
            return Ok(());
        };
        let message = self.queued_turn_messages.remove(index);
        self.start_turn(
            &message.thread_id,
            message.text,
            message.model,
            message.effort,
            message.approval_policy,
            message.sandbox_policy,
            message.collaboration_mode,
            message.skills,
            message.attachments,
        )
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
        self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
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
        self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
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
            self.send_thread_detail(&thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
                .await?;
        }
        Ok(())
    }

    async fn send_current_thread_detail(
        &mut self,
        thread_id: &str,
    ) -> Result<(), RemoteAgentError> {
        if self.last_detail_thread_id.as_deref() == Some(thread_id) {
            self.send_thread_detail(thread_id, 0, THREAD_DETAIL_PAGE_SIZE, None)
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
        self.detail_turn_cache = None;
        Ok(events)
    }

    fn cache_detail_turns(
        &mut self,
        thread_id: &str,
        updated_at: i64,
        turns: Vec<Vec<ConversationItemDto>>,
    ) {
        self.detail_turn_cache = Some(DetailTurnCache {
            thread_id: thread_id.to_string(),
            updated_at,
            turns,
        });
    }

    fn paginate_cached_detail(
        &self,
        thread_id: &str,
        updated_at: i64,
        before: usize,
        limit: usize,
    ) -> Option<(Vec<ConversationItemDto>, bool, Option<usize>)> {
        let cache = self.detail_turn_cache.as_ref()?;
        if cache.thread_id != thread_id || cache.updated_at != updated_at {
            return None;
        }
        Some(paginate_conversation_turns(&cache.turns, before, limit))
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

#[cfg(test)]
fn paginate_conversation_items(
    items: Vec<ConversationItemDto>,
    before: usize,
    limit: usize,
) -> (Vec<ConversationItemDto>, bool, Option<usize>) {
    let turns = group_conversation_items_into_turns(items);
    paginate_conversation_turns(&turns, before, limit)
}

fn paginate_conversation_turns(
    turns: &[Vec<ConversationItemDto>],
    before: usize,
    limit: usize,
) -> (Vec<ConversationItemDto>, bool, Option<usize>) {
    let end = turns.len().saturating_sub(before);
    let start = end.saturating_sub(limit);
    let page_length = end - start;
    let has_more_before = start > 0;
    let next_before = has_more_before.then_some(before.saturating_add(page_length));
    let page = turns
        .iter()
        .skip(start)
        .take(page_length)
        .flat_map(|turn| turn.iter().cloned())
        .collect();
    (page, has_more_before, next_before)
}

fn group_conversation_items_into_turns(
    items: Vec<ConversationItemDto>,
) -> Vec<Vec<ConversationItemDto>> {
    let mut turns: Vec<Vec<ConversationItemDto>> = Vec::new();
    for item in items {
        if turns.is_empty() || matches!(item.kind, ConversationKind::UserMessage) {
            turns.push(Vec::new());
        }
        if let Some(turn) = turns.last_mut() {
            turn.push(item);
        }
    }
    turns
}
async fn build_model_catalog(
    app_state: &AppState,
) -> Result<(Option<String>, Vec<ModelDto>), RemoteAgentError> {
    let providers = app_state
        .db
        .get_all_providers(AppType::Codex.as_str())
        .map_err(|error| RemoteAgentError::Incompatible(error.to_string()))?;
    let current_provider_id = app_state
        .db
        .get_current_provider(AppType::Codex.as_str())
        .map_err(|error| RemoteAgentError::Incompatible(error.to_string()))?;
    let mut groups = Vec::new();
    for (provider_id, provider) in providers {
        let (base_url, api_key) = provider.resolve_usage_credentials(&AppType::Codex);
        if base_url.trim().is_empty() || api_key.trim().is_empty() {
            continue;
        }
        let mut candidates = provider_model_candidates(&provider);
        if candidates.len() < MAX_MODELS_PER_PROVIDER {
            let is_full_url = provider
                .meta
                .as_ref()
                .and_then(|meta| meta.is_full_url)
                .unwrap_or(false);
            let api_format = provider
                .meta
                .as_ref()
                .and_then(|meta| meta.api_format.clone());
            if let Ok(fetched) = model_fetch::fetch_models(
                &base_url,
                &api_key,
                is_full_url,
                None,
                None,
                api_format.as_deref(),
                None,
            )
            .await
            {
                for model in fetched {
                    if !candidates.iter().any(|candidate| candidate == &model.id) {
                        candidates.push(model.id);
                    }
                    if candidates.len() >= MAX_MODELS_PER_PROVIDER {
                        break;
                    }
                }
            }
        }
        candidates.truncate(MAX_MODELS_PER_PROVIDER);
        if candidates.is_empty() {
            continue;
        }
        groups.push((
            provider_id.clone(),
            provider.name.clone(),
            provider.icon.clone(),
            base_url,
            api_key,
            provider
                .meta
                .as_ref()
                .and_then(|meta| meta.api_format.clone()),
            current_provider_id.as_deref() == Some(provider_id.as_str()),
            candidates,
        ));
    }

    let tasks = groups
        .into_iter()
        .map(
            |(
                provider_id,
                provider_name,
                provider_icon,
                base_url,
                api_key,
                api_format,
                current_provider,
                candidates,
            )| {
                tokio::spawn(async move {
                    let probes = candidates
                        .into_iter()
                        .map(|model_id| {
                            let base_url = base_url.clone();
                            let api_key = api_key.clone();
                            let api_format = api_format.clone();
                            tokio::spawn(async move {
                                let available = probe_provider_model(
                                    &base_url,
                                    &api_key,
                                    api_format.as_deref(),
                                    &model_id,
                                )
                                .await;
                                (model_id, available)
                            })
                        })
                        .collect::<Vec<_>>();
                    join_all(probes)
                        .await
                        .into_iter()
                        .filter_map(|result| result.ok())
                        .filter(|(_, available)| *available)
                        .map(|(model_id, _)| ModelDto {
                            id: model_id.clone(),
                            display_name: model_id,
                            hidden: false,
                            is_default: false,
                            default_reasoning_effort: None,
                            supported_reasoning_efforts: Vec::new(),
                            provider_id: Some(provider_id.clone()),
                            provider_name: Some(provider_name.clone()),
                            provider_icon: provider_icon.clone(),
                            current_provider,
                        })
                        .collect::<Vec<ModelDto>>()
                })
            },
        )
        .collect::<Vec<_>>();
    let results = join_all(tasks).await;
    let mut models = Vec::new();
    for result in results {
        match result {
            Ok(provider_models) => models.extend(provider_models),
            Err(error) => log::warn!("Model catalog probe task failed: {error}"),
        }
    }
    Ok((current_provider_id, models))
}

fn provider_model_candidates(provider: &Provider) -> Vec<String> {
    let mut candidates = Vec::new();
    if let Some(config) = provider
        .settings_config
        .get("config")
        .and_then(Value::as_str)
    {
        if let Ok(document) = config.parse::<toml_edit::DocumentMut>() {
            if let Some(model) = document.get("model").and_then(|value| value.as_str()) {
                let model = model.trim();
                if !model.is_empty() {
                    candidates.push(model.to_string());
                }
            }
        }
    }
    if let Some(models) = provider
        .settings_config
        .pointer("/modelCatalog/models")
        .and_then(Value::as_array)
    {
        for model in models {
            let id = model
                .get("model")
                .and_then(Value::as_str)
                .or_else(|| model.get("slug").and_then(Value::as_str))
                .or_else(|| model.get("id").and_then(Value::as_str));
            if let Some(id) = id.map(str::trim).filter(|id| !id.is_empty()) {
                if !candidates.iter().any(|candidate| candidate == id) {
                    candidates.push(id.to_string());
                }
            }
            if candidates.len() >= MAX_MODELS_PER_PROVIDER {
                break;
            }
        }
    }
    candidates
}

async fn probe_provider_model(
    base_url: &str,
    api_key: &str,
    api_format: Option<&str>,
    model_id: &str,
) -> bool {
    let chat_completions = matches!(
        api_format,
        Some("openai_chat" | "openai-completions" | "chat_completions")
    );
    let url = if chat_completions {
        chat_completions_endpoint(base_url)
    } else {
        responses_endpoint(base_url)
    };
    let body = if chat_completions {
        json!({
            "model": model_id,
            "messages": [{ "role": "user", "content": "Reply with OK." }],
            "max_tokens": 16,
            "stream": false,
        })
    } else {
        json!({
            "model": model_id,
            "input": "Reply with OK.",
            "max_output_tokens": 32,
            "stream": false,
            "store": false,
        })
    };
    let request = crate::proxy::http_client::get()
        .post(url)
        .timeout(MODEL_PROBE_TIMEOUT)
        .bearer_auth(api_key)
        .json(&body);
    let Ok(response) = request.send().await else {
        return false;
    };
    if !response.status().is_success() {
        return false;
    }
    let body = response.text().await.unwrap_or_default();
    response_contains_text(&body)
}

fn responses_endpoint(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.ends_with("/responses") {
        base.to_string()
    } else if base.ends_with("/v1") {
        format!("{base}/responses")
    } else {
        format!("{base}/v1/responses")
    }
}

fn chat_completions_endpoint(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.ends_with("/chat/completions") {
        base.to_string()
    } else if base.ends_with("/v1") {
        format!("{base}/chat/completions")
    } else {
        format!("{base}/v1/chat/completions")
    }
}

fn response_contains_text(body: &str) -> bool {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return false;
    }
    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return true;
    };
    if let Some(text) = value.get("output_text").and_then(Value::as_str) {
        if !text.trim().is_empty() {
            return true;
        }
    }
    if let Some(content) = value
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
    {
        if !content.trim().is_empty() {
            return true;
        }
    }
    if let Some(output) = value.get("output").and_then(Value::as_array) {
        return output.iter().any(|item| {
            item.get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.trim().is_empty())
                || item
                    .get("content")
                    .and_then(Value::as_array)
                    .is_some_and(|content| {
                        content.iter().any(|part| {
                            part.get("text")
                                .and_then(Value::as_str)
                                .is_some_and(|text| !text.trim().is_empty())
                        })
                    })
        });
    }
    false
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

fn known_thread_revisions(threads: &[ThreadSummaryDto]) -> Vec<KnownThreadRevision> {
    threads
        .iter()
        .map(|thread| KnownThreadRevision {
            id: thread.id.clone(),
            signature: thread_signature(thread),
        })
        .collect()
}

fn thread_signature(thread: &ThreadSummaryDto) -> String {
    serde_json::to_string(thread).unwrap_or_default()
}

fn command_request_id(command: &RemoteCommand) -> Option<&str> {
    match command {
        RemoteCommand::Sync
        | RemoteCommand::SyncIncremental { .. }
        | RemoteCommand::UploadAttachmentChunk { .. } => None,
        RemoteCommand::ReadThread { request_id, .. }
        | RemoteCommand::CreateThread { request_id, .. }
        | RemoteCommand::StartTurn { request_id, .. }
        | RemoteCommand::QueueTurn { request_id, .. }
        | RemoteCommand::SteerQueuedTurn { request_id, .. }
        | RemoteCommand::StartAttachmentUpload { request_id, .. }
        | RemoteCommand::FinishAttachmentUpload { request_id, .. }
        | RemoteCommand::InterruptTurn { request_id, .. }
        | RemoteCommand::RespondApproval { request_id, .. }
        | RemoteCommand::SetThreadPinned { request_id, .. }
        | RemoteCommand::ListSkills { request_id, .. }
        | RemoteCommand::SetCollaborationMode { request_id, .. }
        | RemoteCommand::SetThreadGoal { request_id, .. }
        | RemoteCommand::ClearThreadGoal { request_id, .. }
        | RemoteCommand::SetThreadName { request_id, .. }
        | RemoteCommand::ArchiveThread { request_id, .. }
        | RemoteCommand::DeleteThread { request_id, .. }
        | RemoteCommand::CompactThread { request_id, .. }
        | RemoteCommand::ListModels { request_id }
        | RemoteCommand::SelectModel { request_id, .. } => request_id.as_deref(),
    }
}

fn is_active_writer_conflict(error: &AppServerError) -> bool {
    matches!(
        error,
        AppServerError::Remote { message, .. }
            if message.contains("already has an active writer")
    )
}

fn is_active_turn_not_steerable(error: &AppServerError) -> bool {
    matches!(
        error,
        AppServerError::Remote { message, .. }
            if message.contains("active turn cannot accept")
                || message.contains("not steerable")
    )
}

fn is_thread_not_found_error(error: &AppServerError) -> bool {
    matches!(
        error,
        AppServerError::Remote { message, .. }
            if message.contains("找不到任务") || message.contains("thread not found")
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
    fn paginates_history_by_complete_user_agent_turns() {
        let item = |kind: ConversationKind, text: &str| ConversationItemDto {
            id: text.to_string(),
            kind,
            status: Some(ConversationStatus::Completed),
            title: None,
            text: Some(text.to_string()),
            detail: None,
            created_at: None,
            approval_request_id: None,
            approval_options: Vec::new(),
        };
        let items = vec![
            item(ConversationKind::UserMessage, "u1"),
            item(ConversationKind::AgentMessage, "a1"),
            item(ConversationKind::UserMessage, "u2"),
            item(ConversationKind::AgentMessage, "a2"),
            item(ConversationKind::UserMessage, "u3"),
            item(ConversationKind::AgentMessage, "a3"),
        ];

        let (latest, has_more, next_before) = paginate_conversation_items(items.clone(), 0, 2);
        assert!(has_more);
        assert_eq!(next_before, Some(2));
        assert_eq!(
            latest.iter().filter_map(|item| item.text.as_deref()).collect::<Vec<_>>(),
            vec!["u2", "a2", "u3", "a3"]
        );

        let (older, has_more, next_before) =
            paginate_conversation_items(items, next_before.expect("next cursor"), 2);
        assert!(!has_more);
        assert_eq!(next_before, None);
        assert_eq!(
            older.iter().filter_map(|item| item.text.as_deref()).collect::<Vec<_>>(),
            vec!["u1", "a1"]
        );
    }
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
        let app_state = AppState::new(std::sync::Arc::new(
            crate::database::Database::memory().expect("initialize in-memory database"),
        ));
        let agent = RemoteControlAgent::start(
            "ws://127.0.0.1/ws/agent",
            access_key.clone(),
            app_state,
        )
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
