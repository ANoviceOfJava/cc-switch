use std::process::Command;

#[derive(Debug)]
pub(crate) struct DesktopControlError(pub(crate) String);

impl std::fmt::Display for DesktopControlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for DesktopControlError {}

/// 唤起 Codex 桌面端显示指定任务，用于让桌面任务列表立即刷新新建任务。
pub(crate) fn show_desktop_thread(thread_id: &str) -> Result<(), DesktopControlError> {
    #[cfg(target_os = "windows")]
    {
        windows::show_desktop_thread(thread_id)
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = thread_id;
        Err(DesktopControlError(
            "当前系统不支持唤起 Codex 桌面端".to_string(),
        ))
    }
}

#[cfg(target_os = "windows")]
mod windows {
    use super::*;

    pub(super) fn show_desktop_thread(thread_id: &str) -> Result<(), DesktopControlError> {
        if !is_safe_thread_id(thread_id) {
            return Err(DesktopControlError("任务 ID 格式无效".to_string()));
        }
        open_desktop_thread(thread_id)
    }

    fn open_desktop_thread(thread_id: &str) -> Result<(), DesktopControlError> {
        let uri = format!("codex://threads/{thread_id}");
        let status = Command::new("cmd")
            .args(["/C", "start", "", &uri])
            .status()
            .map_err(|error| DesktopControlError(format!("无法唤起 Codex 桌面端: {error}")))?;
        if status.success() {
            Ok(())
        } else {
            Err(DesktopControlError("无法唤起 Codex 桌面端".to_string()))
        }
    }

    fn is_safe_thread_id(thread_id: &str) -> bool {
        !thread_id.is_empty()
            && thread_id
                .chars()
                .all(|character| character.is_ascii_hexdigit() || character == '-')
    }
}
