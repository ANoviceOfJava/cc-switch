use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

const PROTOCOL_VERSION: u8 = 1;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const MAX_TYPE_LENGTH: usize = 100;
const MAX_REQUEST_ID_LENGTH: usize = 100;
const MAX_IDENTIFIER_LENGTH: usize = 512;
const MAX_TURN_TEXT_LENGTH: usize = 200_000;
const MAX_ATTACHMENT_SIZE: u64 = 20 * 1024 * 1024;
const MAX_ATTACHMENTS_PER_TURN: usize = 5;
const DEFAULT_THREAD_PAGE_LIMIT: usize = 5;
const MAX_THREAD_PAGE_LIMIT: usize = 20;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WireMessage {
    version: u8,
    #[serde(rename = "type")]
    message_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RemoteCommand {
    Sync,
    SyncIncremental {
        known_threads: Vec<KnownThreadRevision>,
    },
    ReadThread {
        request_id: Option<String>,
        thread_id: String,
        before: usize,
        limit: usize,
    },
    CreateThread {
        request_id: Option<String>,
        project_id: String,
    },
    StartTurn {
        request_id: Option<String>,
        thread_id: String,
        text: String,
        model: Option<String>,
        effort: Option<String>,
        attachments: Vec<RemoteAttachment>,
    },
    StartAttachmentUpload {
        request_id: Option<String>,
        upload_id: String,
        name: String,
        mime_type: String,
        size: u64,
    },
    UploadAttachmentChunk {
        upload_id: String,
        index: u32,
        data: String,
    },
    FinishAttachmentUpload {
        request_id: Option<String>,
        upload_id: String,
    },
    InterruptTurn {
        request_id: Option<String>,
        thread_id: String,
        turn_id: Option<String>,
    },
    RespondApproval {
        request_id: Option<String>,
        thread_id: String,
        approval_request_id: String,
        decision: ApprovalDecision,
    },
    SetThreadPinned {
        request_id: Option<String>,
        thread_id: String,
        pinned: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct KnownThreadRevision {
    pub(crate) id: String,
    pub(crate) signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RemoteAttachment {
    pub(crate) upload_id: String,
    pub(crate) name: String,
    pub(crate) mime_type: String,
    pub(crate) size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ApprovalDecision {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
}

#[derive(Debug, Error)]
pub(crate) enum ProtocolError {
    #[error("远程消息超过 1 MiB 限制")]
    MessageTooLarge,
    #[error("远程消息不是有效 JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("不支持的协议版本: {0}")]
    UnsupportedVersion(u8),
    #[error("远程消息类型无效")]
    InvalidMessageType,
    #[error("远程请求 ID 无效")]
    InvalidRequestId,
    #[error("不允许的远程命令: {0}")]
    CommandNotAllowed(String),
    #[error("远程命令参数无效: {0}")]
    InvalidPayload(String),
    #[error("不允许的电脑端消息: {0}")]
    OutboundMessageNotAllowed(String),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ThreadReadPayload {
    thread_id: String,
    #[serde(default)]
    before: usize,
    #[serde(default = "default_thread_page_limit")]
    limit: usize,
}

fn default_thread_page_limit() -> usize {
    DEFAULT_THREAD_PAGE_LIMIT
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateThreadPayload {
    project_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StartTurnPayload {
    thread_id: String,
    text: String,
    model: Option<String>,
    effort: Option<String>,
    #[serde(default)]
    attachments: Vec<RemoteAttachment>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StartAttachmentUploadPayload {
    upload_id: String,
    name: String,
    mime_type: String,
    size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AttachmentChunkPayload {
    upload_id: String,
    index: u32,
    data: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FinishAttachmentUploadPayload {
    upload_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InterruptTurnPayload {
    thread_id: String,
    turn_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApprovalPayload {
    thread_id: String,
    approval_request_id: String,
    decision: ApprovalDecision,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IncrementalSyncPayload {
    threads: Vec<KnownThreadRevision>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SetThreadPinnedPayload {
    thread_id: String,
    pinned: bool,
}

impl WireMessage {
    /// 创建由电脑端发往手机端的协议消息。
    pub(crate) fn outbound(
        message_type: impl Into<String>,
        request_id: Option<String>,
        payload: Option<Value>,
    ) -> Result<Self, ProtocolError> {
        let message = Self {
            version: PROTOCOL_VERSION,
            message_type: message_type.into(),
            request_id,
            payload,
        };
        message.validate_common()?;
        if !matches!(
            message.message_type.as_str(),
            "state.snapshot"
                | "state.delta"
                | "thread.detail"
                | "thread.detail.start"
                | "thread.detail.chunk"
                | "thread.detail.end"
                | "execution.status"
                | "sync.progress"
                | "request.error"
                | "request.ack"
        ) {
            return Err(ProtocolError::OutboundMessageNotAllowed(
                message.message_type.clone(),
            ));
        }
        Ok(message)
    }

    /// 编码协议消息，并执行与 Relay 相同的 1 MiB 上限检查。
    pub(crate) fn encode(&self) -> Result<String, ProtocolError> {
        self.validate_common()?;
        let encoded = serde_json::to_string(self)?;
        if encoded.len() > MAX_MESSAGE_BYTES {
            return Err(ProtocolError::MessageTooLarge);
        }
        Ok(encoded)
    }

    fn decode(input: &str) -> Result<Self, ProtocolError> {
        if input.len() > MAX_MESSAGE_BYTES {
            return Err(ProtocolError::MessageTooLarge);
        }
        let message: Self = serde_json::from_str(input)?;
        message.validate_common()?;
        Ok(message)
    }

    fn validate_common(&self) -> Result<(), ProtocolError> {
        if self.version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(self.version));
        }
        if self.message_type.is_empty() || self.message_type.len() > MAX_TYPE_LENGTH {
            return Err(ProtocolError::InvalidMessageType);
        }
        if self
            .request_id
            .as_ref()
            .is_some_and(|id| id.is_empty() || id.len() > MAX_REQUEST_ID_LENGTH)
        {
            return Err(ProtocolError::InvalidRequestId);
        }
        Ok(())
    }
}

impl RemoteCommand {
    /// 解码手机端命令，并在进入业务层前执行严格白名单和参数校验。
    pub(crate) fn decode(input: &str) -> Result<Self, ProtocolError> {
        let message = WireMessage::decode(input)?;
        let payload = message
            .payload
            .unwrap_or_else(|| Value::Object(Default::default()));

        match message.message_type.as_str() {
            "sync.request" => {
                ensure_empty_payload(&payload)?;
                Ok(Self::Sync)
            }
            "sync.incremental" => {
                let payload: IncrementalSyncPayload = parse_payload(payload)?;
                for thread in &payload.threads {
                    validate_identifier("threadId", &thread.id)?;
                    if thread.signature.is_empty() || thread.signature.len() > 8_192 {
                        return Err(ProtocolError::InvalidPayload(
                            "thread.signature".to_string(),
                        ));
                    }
                }
                Ok(Self::SyncIncremental {
                    known_threads: payload.threads,
                })
            }
            "thread.read" => {
                let payload: ThreadReadPayload = parse_payload(payload)?;
                validate_identifier("threadId", &payload.thread_id)?;
                if payload.limit == 0 || payload.limit > MAX_THREAD_PAGE_LIMIT {
                    return Err(ProtocolError::InvalidPayload("limit".to_string()));
                }
                Ok(Self::ReadThread {
                    request_id: message.request_id,
                    thread_id: payload.thread_id,
                    before: payload.before,
                    limit: payload.limit,
                })
            }
            "thread.create" => {
                let payload: CreateThreadPayload = parse_payload(payload)?;
                validate_identifier("projectId", &payload.project_id)?;
                Ok(Self::CreateThread {
                    request_id: message.request_id,
                    project_id: payload.project_id,
                })
            }
            "turn.start" => {
                let payload: StartTurnPayload = parse_payload(payload)?;
                validate_identifier("threadId", &payload.thread_id)?;
                if payload.attachments.len() > MAX_ATTACHMENTS_PER_TURN {
                    return Err(ProtocolError::InvalidPayload("attachments".to_string()));
                }
                for attachment in &payload.attachments {
                    validate_attachment(attachment)?;
                }
                if (payload.text.trim().is_empty() && payload.attachments.is_empty())
                    || payload.text.len() > MAX_TURN_TEXT_LENGTH
                {
                    return Err(ProtocolError::InvalidPayload("text".to_string()));
                }
                validate_optional_identifier("model", payload.model.as_deref())?;
                validate_optional_identifier("effort", payload.effort.as_deref())?;
                Ok(Self::StartTurn {
                    request_id: message.request_id,
                    thread_id: payload.thread_id,
                    text: payload.text,
                    model: payload.model,
                    effort: payload.effort,
                    attachments: payload.attachments,
                })
            }
            "attachment.start" => {
                let payload: StartAttachmentUploadPayload = parse_payload(payload)?;
                let attachment = RemoteAttachment {
                    upload_id: payload.upload_id,
                    name: payload.name,
                    mime_type: payload.mime_type,
                    size: payload.size,
                };
                validate_attachment(&attachment)?;
                Ok(Self::StartAttachmentUpload {
                    request_id: message.request_id,
                    upload_id: attachment.upload_id,
                    name: attachment.name,
                    mime_type: attachment.mime_type,
                    size: attachment.size,
                })
            }
            "attachment.chunk" => {
                let payload: AttachmentChunkPayload = parse_payload(payload)?;
                validate_identifier("uploadId", &payload.upload_id)?;
                if payload.data.is_empty() || payload.data.len() > 400_000 {
                    return Err(ProtocolError::InvalidPayload("attachment.data".to_string()));
                }
                Ok(Self::UploadAttachmentChunk {
                    upload_id: payload.upload_id,
                    index: payload.index,
                    data: payload.data,
                })
            }
            "attachment.end" => {
                let payload: FinishAttachmentUploadPayload = parse_payload(payload)?;
                validate_identifier("uploadId", &payload.upload_id)?;
                Ok(Self::FinishAttachmentUpload {
                    request_id: message.request_id,
                    upload_id: payload.upload_id,
                })
            }
            "turn.interrupt" => {
                let payload: InterruptTurnPayload = parse_payload(payload)?;
                validate_identifier("threadId", &payload.thread_id)?;
                validate_optional_identifier("turnId", payload.turn_id.as_deref())?;
                Ok(Self::InterruptTurn {
                    request_id: message.request_id,
                    thread_id: payload.thread_id,
                    turn_id: payload.turn_id,
                })
            }
            "approval.respond" => {
                let payload: ApprovalPayload = parse_payload(payload)?;
                validate_identifier("threadId", &payload.thread_id)?;
                validate_identifier("approvalRequestId", &payload.approval_request_id)?;
                Ok(Self::RespondApproval {
                    request_id: message.request_id,
                    thread_id: payload.thread_id,
                    approval_request_id: payload.approval_request_id,
                    decision: payload.decision,
                })
            }
            "thread.pin" => {
                let payload: SetThreadPinnedPayload = parse_payload(payload)?;
                validate_identifier("threadId", &payload.thread_id)?;
                Ok(Self::SetThreadPinned {
                    request_id: message.request_id,
                    thread_id: payload.thread_id,
                    pinned: payload.pinned,
                })
            }
            _ => Err(ProtocolError::CommandNotAllowed(message.message_type)),
        }
    }
}

fn parse_payload<T>(payload: Value) -> Result<T, ProtocolError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(payload)
        .map_err(|error| ProtocolError::InvalidPayload(error.to_string()))
}

fn ensure_empty_payload(payload: &Value) -> Result<(), ProtocolError> {
    if payload.as_object().is_some_and(serde_json::Map::is_empty) {
        return Ok(());
    }
    Err(ProtocolError::InvalidPayload(
        "sync.request 不接受参数".to_string(),
    ))
}

fn validate_identifier(name: &str, value: &str) -> Result<(), ProtocolError> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_LENGTH {
        return Err(ProtocolError::InvalidPayload(name.to_string()));
    }
    Ok(())
}

fn validate_optional_identifier(name: &str, value: Option<&str>) -> Result<(), ProtocolError> {
    if let Some(value) = value {
        validate_identifier(name, value)?;
    }
    Ok(())
}

fn validate_attachment(attachment: &RemoteAttachment) -> Result<(), ProtocolError> {
    validate_identifier("uploadId", &attachment.upload_id)?;
    if attachment.name.is_empty()
        || attachment.name.len() > 255
        || attachment.mime_type.len() > 128
        || attachment.size == 0
        || attachment.size > MAX_ATTACHMENT_SIZE
    {
        return Err(ProtocolError::InvalidPayload("attachment".to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn decodes_only_whitelisted_commands() {
        let command = RemoteCommand::decode(
            &json!({
                "version": 1,
                "type": "turn.start",
                "requestId": "request-1",
                "payload": {
                    "threadId": "thread-1",
                    "text": "继续",
                    "model": "gpt-5.6-terra",
                    "effort": "high"
                }
            })
            .to_string(),
        )
        .expect("decode command");

        assert!(matches!(
            command,
            RemoteCommand::StartTurn { ref thread_id, .. } if thread_id == "thread-1"
        ));
        assert!(matches!(
            RemoteCommand::decode(r#"{"version":1,"type":"shell.exec"}"#),
            Err(ProtocolError::CommandNotAllowed(_))
        ));

        let pin = RemoteCommand::decode(
            &json!({
                "version": 1,
                "type": "thread.pin",
                "payload": { "threadId": "thread-1", "pinned": true }
            })
            .to_string(),
        )
        .expect("decode pin command");
        assert!(matches!(
            pin,
            RemoteCommand::SetThreadPinned { ref thread_id, pinned: true, .. }
                if thread_id == "thread-1"
        ));
    }

    #[test]
    fn allows_thread_created_acknowledgement() {
        let message = WireMessage::outbound(
            "request.ack",
            Some("request-1".to_string()),
            Some(json!({
                "action": "thread.created",
                "threadId": "thread-1",
                "projectId": "project-1",
            })),
        )
        .expect("thread created acknowledgement");

        assert_eq!(message.message_type, "request.ack");
        assert_eq!(message.request_id.as_deref(), Some("request-1"));
    }

    #[test]
    fn decodes_chunked_attachment_upload_and_turn_reference() {
        let start = RemoteCommand::decode(
            &json!({
                "version": 1,
                "type": "attachment.start",
                "payload": {
                    "uploadId": "upload-1",
                    "name": "design.png",
                    "mimeType": "image/png",
                    "size": 128
                }
            })
            .to_string(),
        )
        .expect("decode attachment start");
        assert!(matches!(
            start,
            RemoteCommand::StartAttachmentUpload { size: 128, .. }
        ));

        let turn = RemoteCommand::decode(
            &json!({
                "version": 1,
                "type": "turn.start",
                "payload": {
                    "threadId": "thread-1",
                    "text": "查看附件",
                    "model": null,
                    "effort": null,
                    "attachments": [{
                        "uploadId": "upload-1",
                        "name": "design.png",
                        "mimeType": "image/png",
                        "size": 128
                    }]
                }
            })
            .to_string(),
        )
        .expect("decode turn attachment");
        assert!(
            matches!(turn, RemoteCommand::StartTurn { ref attachments, .. } if attachments.len() == 1)
        );
    }

    #[test]
    fn decodes_incremental_sync_revisions() {
        let command = RemoteCommand::decode(
            &json!({
                "version": 1,
                "type": "sync.incremental",
                "payload": {
                    "threads": [{ "id": "thread-1", "signature": "{thread}" }]
                }
            })
            .to_string(),
        )
        .expect("decode incremental sync");

        assert!(matches!(
            command,
            RemoteCommand::SyncIncremental { ref known_threads }
                if known_threads.len() == 1 && known_threads[0].id == "thread-1"
        ));
    }

    #[test]
    fn rejects_unknown_and_oversized_parameters() {
        let unknown = json!({
            "version": 1,
            "type": "thread.read",
            "payload": { "threadId": "thread-1", "command": "whoami" }
        })
        .to_string();
        assert!(matches!(
            RemoteCommand::decode(&unknown),
            Err(ProtocolError::InvalidPayload(_))
        ));

        let oversized = "x".repeat(MAX_IDENTIFIER_LENGTH + 1);
        let invalid = json!({
            "version": 1,
            "type": "thread.read",
            "payload": { "threadId": oversized }
        })
        .to_string();
        assert!(matches!(
            RemoteCommand::decode(&invalid),
            Err(ProtocolError::InvalidPayload(_))
        ));
    }

    #[test]
    fn encodes_only_agent_response_types() {
        let message =
            WireMessage::outbound("state.snapshot", None, Some(json!({ "revision": "1" })))
                .expect("create snapshot");
        assert!(message
            .encode()
            .expect("encode snapshot")
            .contains("state.snapshot"));
        assert!(matches!(
            WireMessage::outbound("turn.start", None, None),
            Err(ProtocolError::OutboundMessageNotAllowed(_))
        ));
    }
}
