use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{atomic_write_private, get_app_config_dir};

const REMOTE_CONTROL_CONFIG_FILENAME: &str = "remote-control.json";

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RemoteControlConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) relay_url: String,
    #[serde(default)]
    pub(crate) access_key: String,
}

#[derive(Debug, Error)]
pub(crate) enum RemoteControlConfigError {
    #[error("无法读取远程控制配置 {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("无法解析远程控制配置 {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("无法写入远程控制配置 {path}: {message}")]
    Write { path: PathBuf, message: String },
}

/// 读取只保存在当前电脑上的 Remote 配置和 Access Key。
pub(crate) fn load_remote_control_config() -> Result<RemoteControlConfig, RemoteControlConfigError>
{
    let path = remote_control_config_path();
    if !path.exists() {
        return Ok(RemoteControlConfig::default());
    }
    let contents = fs::read_to_string(&path).map_err(|source| RemoteControlConfigError::Read {
        path: path.clone(),
        source,
    })?;
    serde_json::from_str(&contents)
        .map_err(|source| RemoteControlConfigError::Parse { path, source })
}

/// 原子保存 Remote 配置；该文件不参与 WebDAV 或 S3 配置同步。
pub(crate) fn save_remote_control_config(
    config: &RemoteControlConfig,
) -> Result<(), RemoteControlConfigError> {
    let path = remote_control_config_path();
    let bytes = serde_json::to_vec(config).map_err(|source| RemoteControlConfigError::Parse {
        path: path.clone(),
        source,
    })?;
    atomic_write_private(&path, &bytes).map_err(|error| RemoteControlConfigError::Write {
        path,
        message: error.to_string(),
    })
}

fn remote_control_config_path() -> PathBuf {
    get_app_config_dir().join(REMOTE_CONTROL_CONFIG_FILENAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_to_disabled() {
        let config: RemoteControlConfig = serde_json::from_str("{}").expect("parse defaults");
        assert!(!config.enabled);
        assert!(config.relay_url.is_empty());
        assert!(config.access_key.is_empty());
    }

    #[test]
    fn config_rejects_unknown_fields() {
        assert!(serde_json::from_str::<RemoteControlConfig>(
            r#"{"enabled":false,"shell":"whoami"}"#
        )
        .is_err());
    }
}
