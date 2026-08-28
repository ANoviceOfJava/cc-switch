use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

use crate::config::{atomic_write, get_home_dir};

const CODEX_GLOBAL_STATE_FILENAME: &str = ".codex-global-state.json";
const READ_ATTEMPTS: usize = 3;
const READ_RETRY_DELAY: Duration = Duration::from_millis(25);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexProjectState {
    pub(crate) projects: Vec<CodexProject>,
    pub(crate) thread_project_assignments: HashMap<String, ThreadProjectAssignment>,
    pub(crate) pinned_thread_ids: Vec<String>,
    pub(crate) projectless_thread_ids: Vec<String>,
    pub(crate) dangling_thread_assignment_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexProject {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) root_paths: Vec<String>,
    pub(crate) created_at: i64,
    pub(crate) updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ThreadProjectAssignment {
    pub(crate) project_kind: ProjectKind,
    pub(crate) project_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ProjectKind {
    Local,
}

#[derive(Debug, Error)]
pub(crate) enum ProjectStateError {
    #[error("无法读取 Codex 桌面项目状态文件 {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("无法解析 Codex 桌面项目状态文件 {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("无法写入 Codex 桌面项目状态文件 {path}: {message}")]
    Write { path: PathBuf, message: String },
    #[error("Codex 桌面项目状态结构不兼容: {0}")]
    Incompatible(String),
}

#[derive(Debug, Deserialize)]
struct RawProjectState {
    #[serde(rename = "local-projects")]
    local_projects: HashMap<String, CodexProject>,
    #[serde(rename = "project-order")]
    project_order: Vec<String>,
    #[serde(rename = "thread-project-assignments")]
    thread_project_assignments: HashMap<String, ThreadProjectAssignment>,
    #[serde(rename = "pinned-thread-ids")]
    pinned_thread_ids: Vec<String>,
    #[serde(rename = "projectless-thread-ids")]
    projectless_thread_ids: Vec<String>,
}

/// 读取 Codex 桌面端的项目、排序和线程归属状态。
///
/// 该文件是桌面端私有格式。字段缺失、关系不完整或出现未知项目类型时会明确失败，
/// 不会退化为按线程 `cwd` 猜测项目。
pub(crate) fn load_codex_project_state() -> Result<CodexProjectState, ProjectStateError> {
    let path = codex_project_state_path();
    load_codex_project_state_from_path(&path)
}

/// 将新创建的 Codex 任务绑定到桌面端现有项目。
///
/// 写入前会按当前私有格式完整校验，并保留状态文件中的未知字段。若格式不兼容，
/// 会明确失败而不会按工作目录猜测归属。
pub(crate) fn assign_thread_to_project(
    thread_id: &str,
    project_id: &str,
) -> Result<(), ProjectStateError> {
    let path = codex_project_state_path();
    assign_thread_to_project_at_path(&path, thread_id, project_id)
}

/// 将任务置顶状态写回 Codex 桌面端的全局项目状态。
pub(crate) fn set_thread_pinned(thread_id: &str, pinned: bool) -> Result<(), ProjectStateError> {
    let path = codex_project_state_path();
    set_thread_pinned_at_path(&path, thread_id, pinned)
}

fn codex_project_state_path() -> PathBuf {
    get_home_dir()
        .join(".codex")
        .join(CODEX_GLOBAL_STATE_FILENAME)
}

fn assign_thread_to_project_at_path(
    path: &Path,
    thread_id: &str,
    project_id: &str,
) -> Result<(), ProjectStateError> {
    let contents = fs::read_to_string(path).map_err(|source| ProjectStateError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let state = parse_codex_project_state(path, &contents)?;
    if !state
        .projects
        .iter()
        .any(|project| project.id == project_id)
    {
        return Err(ProjectStateError::Incompatible(format!(
            "无法把任务绑定到不存在的项目: {project_id}"
        )));
    }

    let mut document: Value =
        serde_json::from_str(&contents).map_err(|source| ProjectStateError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
    let root = document.as_object_mut().ok_or_else(|| {
        ProjectStateError::Incompatible("项目状态根节点不是 JSON 对象".to_string())
    })?;
    let assignments = root
        .get_mut("thread-project-assignments")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| {
            ProjectStateError::Incompatible("thread-project-assignments 不是 JSON 对象".to_string())
        })?;
    assignments.insert(
        thread_id.to_string(),
        json!({ "projectKind": "local", "projectId": project_id }),
    );

    let projectless = root
        .get_mut("projectless-thread-ids")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| {
            ProjectStateError::Incompatible("projectless-thread-ids 不是数组".to_string())
        })?;
    projectless.retain(|value| value.as_str() != Some(thread_id));

    let bytes = serde_json::to_vec(&document).map_err(|source| ProjectStateError::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    atomic_write(path, &bytes).map_err(|error| ProjectStateError::Write {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;

    // 重新读取发布后的文件，确认原子替换结果仍满足桌面端关系约束。
    let verified = load_codex_project_state_from_path(path)?;
    let assigned_project_id = verified
        .thread_project_assignments
        .get(thread_id)
        .map(|assignment| assignment.project_id.as_str());
    if assigned_project_id != Some(project_id) {
        return Err(ProjectStateError::Incompatible(
            "写入后无法确认新任务的项目归属".to_string(),
        ));
    }
    Ok(())
}

fn set_thread_pinned_at_path(
    path: &Path,
    thread_id: &str,
    pinned: bool,
) -> Result<(), ProjectStateError> {
    let contents = fs::read_to_string(path).map_err(|source| ProjectStateError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let state = parse_codex_project_state(path, &contents)?;
    let known_thread = state.thread_project_assignments.contains_key(thread_id)
        || state
            .projectless_thread_ids
            .iter()
            .any(|id| id == thread_id)
        || state.pinned_thread_ids.iter().any(|id| id == thread_id);
    if !known_thread {
        return Err(ProjectStateError::Incompatible(format!(
            "无法修改不存在的任务置顶状态: {thread_id}"
        )));
    }

    let mut document: Value =
        serde_json::from_str(&contents).map_err(|source| ProjectStateError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
    let pinned_ids = document
        .get_mut("pinned-thread-ids")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| ProjectStateError::Incompatible("pinned-thread-ids 不是数组".to_string()))?;
    pinned_ids.retain(|value| value.as_str() != Some(thread_id));
    if pinned {
        pinned_ids.insert(0, Value::String(thread_id.to_string()));
    }

    let bytes = serde_json::to_vec(&document).map_err(|source| ProjectStateError::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    atomic_write(path, &bytes).map_err(|error| ProjectStateError::Write {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;

    let verified = load_codex_project_state_from_path(path)?;
    if verified.pinned_thread_ids.iter().any(|id| id == thread_id) != pinned {
        return Err(ProjectStateError::Incompatible(
            "写入后无法确认任务置顶状态".to_string(),
        ));
    }
    Ok(())
}

fn load_codex_project_state_from_path(path: &Path) -> Result<CodexProjectState, ProjectStateError> {
    let mut last_error = None;
    for attempt in 0..READ_ATTEMPTS {
        match read_codex_project_state_once(path) {
            Ok(state) => return Ok(state),
            Err(error) => last_error = Some(error),
        }

        if attempt + 1 < READ_ATTEMPTS {
            thread::sleep(READ_RETRY_DELAY);
        }
    }

    Err(last_error.expect("READ_ATTEMPTS must be greater than zero"))
}

fn read_codex_project_state_once(path: &Path) -> Result<CodexProjectState, ProjectStateError> {
    let contents = fs::read_to_string(path).map_err(|source| ProjectStateError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    parse_codex_project_state(path, &contents)
}

fn parse_codex_project_state(
    path: &Path,
    contents: &str,
) -> Result<CodexProjectState, ProjectStateError> {
    let raw: RawProjectState =
        serde_json::from_str(contents).map_err(|source| ProjectStateError::Parse {
            path: path.to_path_buf(),
            source,
        })?;

    let ordered_ids: HashSet<&str> = raw.project_order.iter().map(String::as_str).collect();
    if ordered_ids.len() != raw.project_order.len() {
        return Err(ProjectStateError::Incompatible(
            "project-order 中存在重复项目 ID".to_string(),
        ));
    }
    if ordered_ids.len() != raw.local_projects.len()
        || !raw
            .local_projects
            .keys()
            .all(|project_id| ordered_ids.contains(project_id.as_str()))
    {
        return Err(ProjectStateError::Incompatible(
            "local-projects 与 project-order 不一致".to_string(),
        ));
    }

    for (map_id, project) in &raw.local_projects {
        if map_id != &project.id {
            return Err(ProjectStateError::Incompatible(format!(
                "local-projects 的键与项目 id 不一致: {map_id}"
            )));
        }
    }

    let mut projects = Vec::with_capacity(raw.project_order.len());
    for project_id in &raw.project_order {
        let project = raw.local_projects.get(project_id).ok_or_else(|| {
            ProjectStateError::Incompatible(format!(
                "project-order 引用了不存在的项目: {project_id}"
            ))
        })?;
        projects.push(project.clone());
    }

    let project_ids: HashSet<&str> = raw.local_projects.keys().map(String::as_str).collect();
    let mut dangling_thread_assignment_ids: Vec<String> = raw
        .thread_project_assignments
        .iter()
        .filter(|(_, assignment)| !project_ids.contains(assignment.project_id.as_str()))
        .map(|(thread_id, _)| thread_id.clone())
        .collect();
    dangling_thread_assignment_ids.sort_unstable();

    Ok(CodexProjectState {
        projects,
        thread_project_assignments: raw.thread_project_assignments,
        pinned_thread_ids: raw.pinned_thread_ids,
        projectless_thread_ids: raw.projectless_thread_ids,
        dangling_thread_assignment_ids,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_STATE: &str = r#"
    {
      "local-projects": {
        "project-b": {
          "id": "project-b",
          "name": "Second",
          "rootPaths": ["C:\\work\\second"],
          "createdAt": 20,
          "updatedAt": 21
        },
        "project-a": {
          "id": "project-a",
          "name": "First",
          "rootPaths": ["C:\\work\\first"],
          "createdAt": 10,
          "updatedAt": 11
        }
      },
      "project-order": ["project-a", "project-b"],
      "thread-project-assignments": {
        "thread-a": { "projectKind": "local", "projectId": "project-a" },
        "thread-stale": { "projectKind": "local", "projectId": "project-deleted" }
      },
      "pinned-thread-ids": ["thread-a"],
      "projectless-thread-ids": ["thread-free"],
      "future-field": true
    }
    "#;

    #[test]
    fn parses_desktop_order_and_preserves_dangling_assignment() {
        let state = parse_codex_project_state(Path::new("state.json"), VALID_STATE)
            .expect("parse valid state");

        assert_eq!(
            state
                .projects
                .iter()
                .map(|project| project.id.as_str())
                .collect::<Vec<_>>(),
            vec!["project-a", "project-b"]
        );
        assert_eq!(state.dangling_thread_assignment_ids, vec!["thread-stale"]);
        assert_eq!(state.thread_project_assignments.len(), 2);
    }

    #[test]
    fn rejects_project_order_that_does_not_match_projects() {
        let invalid = VALID_STATE.replace(
            "\"project-order\": [\"project-a\", \"project-b\"]",
            "\"project-order\": [\"project-a\"]",
        );

        let error = parse_codex_project_state(Path::new("state.json"), &invalid)
            .expect_err("reject incomplete project order");

        assert!(matches!(error, ProjectStateError::Incompatible(_)));
    }

    #[test]
    fn rejects_unknown_project_kind() {
        let invalid =
            VALID_STATE.replace("\"projectKind\": \"local\"", "\"projectKind\": \"cloud\"");

        let error = parse_codex_project_state(Path::new("state.json"), &invalid)
            .expect_err("reject unknown project kind");

        assert!(matches!(error, ProjectStateError::Parse { .. }));
    }

    #[test]
    fn retries_are_not_needed_for_valid_file() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let path = temp.path().join("state.json");
        fs::write(&path, VALID_STATE).expect("write state fixture");

        let state = load_codex_project_state_from_path(&path).expect("load state file");

        assert_eq!(state.projects.len(), 2);
    }

    #[test]
    fn assigns_new_thread_without_dropping_unknown_fields() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let path = temp.path().join("state.json");
        fs::write(&path, VALID_STATE).expect("write state fixture");

        assign_thread_to_project_at_path(&path, "thread-free", "project-b").expect("assign thread");

        let document: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read assigned state"))
                .expect("parse assigned state");
        assert_eq!(document.get("future-field"), Some(&Value::Bool(true)));
        assert_eq!(
            document
                .pointer("/thread-project-assignments/thread-free/projectId")
                .and_then(Value::as_str),
            Some("project-b")
        );
        assert!(!document["projectless-thread-ids"]
            .as_array()
            .expect("projectless array")
            .iter()
            .any(|value| value.as_str() == Some("thread-free")));
    }

    #[test]
    fn pins_and_unpins_thread_without_dropping_unknown_fields() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let path = temp.path().join("state.json");
        fs::write(&path, VALID_STATE).expect("write state fixture");

        set_thread_pinned_at_path(&path, "thread-free", true).expect("pin thread");
        let pinned = load_codex_project_state_from_path(&path).expect("load pinned state");
        assert_eq!(
            pinned.pinned_thread_ids.first().map(String::as_str),
            Some("thread-free")
        );

        set_thread_pinned_at_path(&path, "thread-free", false).expect("unpin thread");
        let document: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read unpinned state"))
                .expect("parse unpinned state");
        assert_eq!(document.get("future-field"), Some(&Value::Bool(true)));
        assert!(!document["pinned-thread-ids"]
            .as_array()
            .expect("pinned array")
            .iter()
            .any(|value| value.as_str() == Some("thread-free")));
    }
}
