use std::process::Command;
use std::thread;
use std::time::Duration;

use arboard::Clipboard;

#[derive(Debug)]
pub(crate) struct DesktopControlError(pub(crate) String);

impl std::fmt::Display for DesktopControlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for DesktopControlError {}

/// 通过 Codex 桌面端当前持有的任务写入器发送纯文本消息。
///
/// 当独立 App Server 被桌面端的写入锁拒绝时，使用该路径保留原任务上下文。
pub(crate) fn send_text_to_desktop_thread(
    thread_id: &str,
    text: &str,
) -> Result<(), DesktopControlError> {
    #[cfg(target_os = "windows")]
    {
        windows::send_text_to_desktop_thread(thread_id, text)
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = (thread_id, text);
        Err(DesktopControlError(
            "当前系统不支持 Codex 桌面端自动发送".to_string(),
        ))
    }
}

#[cfg(target_os = "windows")]
mod windows {
    use super::*;
    use std::ffi::c_int;

    use windows_sys::Win32::Foundation::{HWND, LPARAM, RECT};
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::{
        keybd_event, mouse_event, KEYEVENTF_KEYUP, MOUSEEVENTF_LEFTDOWN, MOUSEEVENTF_LEFTUP,
        VK_CONTROL, VK_RETURN, VK_V,
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        EnumWindows, GetWindowRect, GetWindowTextW, IsWindowVisible, SetCursorPos,
        SetForegroundWindow, ShowWindow, SW_RESTORE,
    };

    const DEEP_LINK_WAIT: Duration = Duration::from_secs(2);
    const INPUT_READY_WAIT: Duration = Duration::from_millis(300);
    const CLIPBOARD_READ_WAIT: Duration = Duration::from_millis(700);

    pub(super) fn send_text_to_desktop_thread(
        thread_id: &str,
        text: &str,
    ) -> Result<(), DesktopControlError> {
        if !is_safe_thread_id(thread_id) {
            return Err(DesktopControlError("任务 ID 格式无效".to_string()));
        }
        if text.trim().is_empty() {
            return Err(DesktopControlError("消息不能为空".to_string()));
        }

        open_desktop_thread(thread_id)?;
        thread::sleep(DEEP_LINK_WAIT);
        let window = find_codex_window().ok_or_else(|| {
            DesktopControlError(
                "无法控制 Codex 桌面窗口；请确认 ChatGPT/Codex 未关闭且 Windows 未锁屏".to_string(),
            )
        })?;
        focus_composer(window)?;

        let mut clipboard = Clipboard::new()
            .map_err(|error| DesktopControlError(format!("无法访问系统剪贴板: {error}")))?;
        let previous_clipboard = clipboard.get_text().ok();
        clipboard
            .set_text(text.to_string())
            .map_err(|error| DesktopControlError(format!("无法写入系统剪贴板: {error}")))?;
        paste_and_submit();
        thread::sleep(CLIPBOARD_READ_WAIT);
        if let Some(previous) = previous_clipboard {
            let _ = clipboard.set_text(previous);
        }
        Ok(())
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

    fn focus_composer(window: HWND) -> Result<(), DesktopControlError> {
        unsafe {
            ShowWindow(window, SW_RESTORE);
            if SetForegroundWindow(window) == 0 {
                return Err(DesktopControlError("无法聚焦 Codex 桌面窗口".to_string()));
            }
            let mut rect = RECT::default();
            if GetWindowRect(window, &mut rect) == 0 {
                return Err(DesktopControlError("无法读取 Codex 窗口位置".to_string()));
            }
            let x = rect.left + (rect.right - rect.left) / 2;
            let y = rect.bottom - 92;
            if SetCursorPos(x, y) == 0 {
                return Err(DesktopControlError("无法定位 Codex 输入框".to_string()));
            }
            mouse_event(MOUSEEVENTF_LEFTDOWN, 0, 0, 0, 0);
            mouse_event(MOUSEEVENTF_LEFTUP, 0, 0, 0, 0);
        }
        thread::sleep(INPUT_READY_WAIT);
        Ok(())
    }

    fn paste_and_submit() {
        unsafe {
            keybd_event(VK_CONTROL as u8, 0, 0, 0);
            keybd_event(VK_V as u8, 0, 0, 0);
            keybd_event(VK_V as u8, 0, KEYEVENTF_KEYUP, 0);
            keybd_event(VK_CONTROL as u8, 0, KEYEVENTF_KEYUP, 0);
            keybd_event(VK_RETURN as u8, 0, 0, 0);
            keybd_event(VK_RETURN as u8, 0, KEYEVENTF_KEYUP, 0);
        }
    }

    fn find_codex_window() -> Option<HWND> {
        let mut window: HWND = std::ptr::null_mut();
        unsafe {
            EnumWindows(Some(find_window_callback), &mut window as *mut HWND as LPARAM);
        }
        (!window.is_null()).then_some(window)
    }

    unsafe extern "system" fn find_window_callback(window: HWND, data: LPARAM) -> i32 {
        if IsWindowVisible(window) == 0 {
            return 1;
        }
        let mut title = [0_u16; 512];
        let length: c_int = GetWindowTextW(window, title.as_mut_ptr(), title.len() as i32);
        if length <= 0 {
            return 1;
        }
        let title = String::from_utf16_lossy(&title[..length as usize]);
        if (title.contains("Codex") || title.contains("ChatGPT")) && !title.contains("CC Switch") {
            *(data as *mut HWND) = window;
            return 0;
        }
        1
    }

    fn is_safe_thread_id(thread_id: &str) -> bool {
        !thread_id.is_empty()
            && thread_id
                .chars()
                .all(|character| character.is_ascii_hexdigit() || character == '-')
    }
}
