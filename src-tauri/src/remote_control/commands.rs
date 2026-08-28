use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::agent::RemoteControlAgent;
use super::config::{load_remote_control_config, save_remote_control_config, RemoteControlConfig};

const ACCESS_KEY_MIN_LENGTH: usize = 32;
const ACCESS_KEY_MAX_LENGTH: usize = 256;

#[derive(Clone)]
pub(crate) struct RemoteControlState {
    runtime: Arc<Mutex<RemoteControlRuntime>>,
    operation: Arc<Mutex<()>>,
}

struct RemoteControlRuntime {
    config: RemoteControlConfig,
    agent: Option<RemoteControlAgent>,
    last_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RemoteControlSettingsInput {
    enabled: bool,
    relay_url: String,
    access_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RemoteControlSettingsView {
    enabled: bool,
    relay_url: String,
    has_access_key: bool,
    status: RemoteControlStatus,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
enum RemoteControlStatus {
    Disabled,
    Connecting,
    Online,
    Error,
}

impl RemoteControlState {
    /// 创建 Remote 状态容器，并读取当前电脑的独立配置文件。
    pub(crate) fn new() -> Self {
        let (config, last_error) = match load_remote_control_config() {
            Ok(config) => (config, None),
            Err(error) => (RemoteControlConfig::default(), Some(error.to_string())),
        };
        Self {
            runtime: Arc::new(Mutex::new(RemoteControlRuntime {
                config,
                agent: None,
                last_error,
            })),
            operation: Arc::new(Mutex::new(())),
        }
    }

    /// 按已保存配置恢复 Remote Agent；失败时保留配置供用户修正或重试。
    pub(crate) async fn start_saved(&self) {
        let _operation = self.operation.lock().await;
        let config = self.runtime.lock().await.config.clone();
        if !config.enabled {
            return;
        }
        self.start_agent(config).await;
    }

    async fn configure(
        &self,
        input: RemoteControlSettingsInput,
    ) -> Result<RemoteControlSettingsView, String> {
        let _operation = self.operation.lock().await;
        let current = self.runtime.lock().await.config.clone();
        let access_key = input
            .access_key
            .map(|key| key.trim().to_string())
            .filter(|key| !key.is_empty())
            .unwrap_or(current.access_key);
        let config = RemoteControlConfig {
            enabled: input.enabled,
            relay_url: input.relay_url.trim().to_string(),
            access_key,
        };
        validate_config(&config)?;
        save_remote_control_config(&config).map_err(|error| error.to_string())?;

        let previous_agent = {
            let mut runtime = self.runtime.lock().await;
            runtime.config = config.clone();
            runtime.last_error = None;
            runtime.agent.take()
        };
        if let Some(agent) = previous_agent {
            agent.shutdown().await;
        }
        if config.enabled {
            self.start_agent(config).await;
        }

        Ok(self.settings_view().await)
    }

    async fn start_agent(&self, config: RemoteControlConfig) {
        match RemoteControlAgent::start(&config.relay_url, config.access_key.clone()).await {
            Ok(agent) => {
                let mut runtime = self.runtime.lock().await;
                runtime.agent = Some(agent);
                runtime.last_error = None;
            }
            Err(error) => {
                let mut runtime = self.runtime.lock().await;
                runtime.agent = None;
                runtime.last_error = Some(error.to_string());
            }
        }
    }

    async fn settings_view(&self) -> RemoteControlSettingsView {
        let runtime = self.runtime.lock().await;
        let status = if !runtime.config.enabled {
            RemoteControlStatus::Disabled
        } else if let Some(agent) = &runtime.agent {
            if agent.is_finished() {
                RemoteControlStatus::Error
            } else if agent.is_connected() {
                RemoteControlStatus::Online
            } else {
                RemoteControlStatus::Connecting
            }
        } else if runtime.last_error.is_some() {
            RemoteControlStatus::Error
        } else {
            RemoteControlStatus::Connecting
        };
        RemoteControlSettingsView {
            enabled: runtime.config.enabled,
            relay_url: runtime.config.relay_url.clone(),
            has_access_key: !runtime.config.access_key.is_empty(),
            status,
            last_error: runtime.last_error.clone(),
        }
    }

    async fn access_key(&self) -> Result<String, String> {
        let access_key = self.runtime.lock().await.config.access_key.clone();
        if access_key.is_empty() {
            return Err("尚未配置 Access Key".to_string());
        }
        Ok(access_key)
    }
}

/// 查询不含 Access Key 明文的 Remote 设置和运行状态。
#[tauri::command]
pub(crate) async fn get_remote_control_settings(
    state: tauri::State<'_, RemoteControlState>,
) -> Result<RemoteControlSettingsView, String> {
    Ok(state.settings_view().await)
}

/// 保存 Remote 设置并立即启动或停止电脑端代理。
#[tauri::command]
pub(crate) async fn save_remote_control_settings(
    state: tauri::State<'_, RemoteControlState>,
    settings: RemoteControlSettingsInput,
) -> Result<RemoteControlSettingsView, String> {
    state.configure(settings).await
}

/// 生成新的高熵 Access Key；仅本次调用返回明文，由用户复制到手机。
#[tauri::command]
pub(crate) fn generate_remote_access_key() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

/// 返回当前电脑已保存的 Access Key，供本机用户重复复制。
#[tauri::command]
pub(crate) async fn get_remote_access_key(
    state: tauri::State<'_, RemoteControlState>,
) -> Result<String, String> {
    state.access_key().await
}

fn validate_config(config: &RemoteControlConfig) -> Result<(), String> {
    if !config.enabled {
        return Ok(());
    }
    if config.relay_url.is_empty() {
        return Err("启用 Remote 前必须填写 Relay URL".to_string());
    }
    if !(ACCESS_KEY_MIN_LENGTH..=ACCESS_KEY_MAX_LENGTH).contains(&config.access_key.len()) {
        return Err("Access Key 长度必须在 32 到 256 个字符之间".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_access_key_matches_relay_requirements() {
        let key = generate_remote_access_key();
        assert_eq!(key.len(), 64);
        assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[test]
    fn enabled_config_requires_url_and_key() {
        let mut config = RemoteControlConfig {
            enabled: true,
            relay_url: String::new(),
            access_key: String::new(),
        };
        assert!(validate_config(&config).is_err());
        config.relay_url = "wss://relay.example.com/ws/agent".to_string();
        assert!(validate_config(&config).is_err());
        config.access_key = "x".repeat(32);
        assert!(validate_config(&config).is_ok());
    }
}
