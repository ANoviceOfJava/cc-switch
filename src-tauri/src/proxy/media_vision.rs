//! Vision bridge: text-only upstream models are fed image descriptions instead
//! of image pixels.

use crate::proxy::tool_media::{chat_media_part_from_tool_part, ToolMediaScope};
use crate::proxy::types::VisionBridgeConfig;
use serde_json::{json, Value};
use std::time::Duration;

const DEFAULT_RECOGNIZE_PROMPT: &str =
    "你是图片信息提取器。请完整保留图片里能读到的文字（标题、正文、按钮、\
     报错、堆栈、表格等），并简单说明图片类型和布局；看不清的写[无法识别]，不要编造内容。";
const MAX_VISION_TIMEOUT_SECONDS: u64 = 300;
const VISION_RETRY_COUNT: usize = 5;
const VISION_RETRY_DELAY_MS: u64 = 500;
const FAILED_MARKER: &str = "[Unsupported Image]";
const TOTAL_FAILURE_MARKER: &str = "[图片识别失败]";

#[derive(Debug)]
struct RecognitionResult {
    text: String,
    ok: bool,
}

#[derive(Debug)]
struct DirectImageRef {
    path: String,
    url: String,
    text_type: String,
}

pub(crate) async fn replace_direct_images_with_vision(
    body: &mut Value,
    config: &VisionBridgeConfig,
) -> usize {
    if !config.enabled || config.api_url.trim().is_empty() || config.api_key.trim().is_empty() {
        return 0;
    }

    let refs = collect_direct_image_refs(body);
    if refs.is_empty() {
        return 0;
    }

    let results = recognize_images(config, &refs).await;
    let successful = results.iter().filter(|result| result.ok).count();
    let replaced = replace_image_refs_with_results(body, &refs, &results, successful);
    if replaced > 0 {
        log::info!(
            "[MediaVision] Replaced {replaced} image block(s) with vision descriptions (model={})",
            config.model
        );
    }
    replaced
}

fn collect_direct_image_refs(body: &Value) -> Vec<DirectImageRef> {
    let mut refs = Vec::new();

    if let Some(messages) = body.get("messages").and_then(Value::as_array) {
        for (message_index, message) in messages.iter().enumerate() {
            if let Some(content) = message.get("content").and_then(Value::as_array) {
                for (block_index, block) in content.iter().enumerate() {
                    collect_content_image_ref(
                        &mut refs,
                        block,
                        &format!("/messages/{message_index}/content/{block_index}"),
                        "text",
                    );
                }
            }
        }
    }

    collect_responses_image_refs(body, &mut refs);
    collect_gemini_image_refs(body, &mut refs);
    refs
}

fn collect_responses_image_refs(body: &Value, refs: &mut Vec<DirectImageRef>) {
    let input = match body.get("input") {
        Some(Value::Array(items)) => items.iter().collect::<Vec<_>>(),
        Some(item @ Value::Object(_)) => vec![item],
        _ => return,
    };

    for (item_index, item) in input.iter().enumerate() {
        if item.get("type").and_then(Value::as_str) == Some("input_image") {
            collect_content_image_ref(
                refs,
                item,
                &format!("/input/{item_index}"),
                "input_text",
            );
        }

        if let Some(content) = item.get("content").and_then(Value::as_array) {
            for (block_index, block) in content.iter().enumerate() {
                collect_content_image_ref(
                    refs,
                    block,
                    &format!("/input/{item_index}/content/{block_index}"),
                    "input_text",
                );
            }
        }
    }
}

fn collect_content_image_ref(
    refs: &mut Vec<DirectImageRef>,
    block: &Value,
    path: &str,
    text_type: &str,
) {
    let Some(url) = image_url_from_block(block) else {
        return;
    };
    refs.push(DirectImageRef {
        path: path.to_string(),
        url,
        text_type: text_type.to_string(),
    });
}

fn image_url_from_block(block: &Value) -> Option<String> {
    let mapped = chat_media_part_from_tool_part(block, ToolMediaScope::AllSupported)?;
    mapped
        .pointer("/image_url/url")
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn collect_gemini_image_refs(body: &Value, refs: &mut Vec<DirectImageRef>) {
    let Some(contents) = body.get("contents").and_then(Value::as_array) else {
        return;
    };

    for (content_index, content) in contents.iter().enumerate() {
        let Some(parts) = content.get("parts").and_then(Value::as_array) else {
            continue;
        };
        for (part_index, part) in parts.iter().enumerate() {
            let Some(url) = gemini_image_url(part) else {
                continue;
            };
            refs.push(DirectImageRef {
                path: format!("/contents/{content_index}/parts/{part_index}"),
                url,
                text_type: "gemini".to_string(),
            });
        }
    }
}

fn gemini_image_url(part: &Value) -> Option<String> {
    for key in ["inlineData", "inline_data", "fileData", "file_data"] {
        let payload = part.get(key)?;
        let mime_type = payload
            .get("mimeType")
            .or_else(|| payload.get("mime_type"))
            .and_then(Value::as_str)?;
        if !mime_type.to_ascii_lowercase().starts_with("image/") {
            continue;
        }
        if let Some(data) = payload.get("data").and_then(Value::as_str) {
            if !data.is_empty() {
                return Some(format!("data:{mime_type};base64,{data}"));
            }
        }
        if let Some(file_uri) = payload.get("fileUri").or_else(|| payload.get("file_uri")) {
            if let Some(uri) = file_uri.as_str().filter(|uri| !uri.is_empty()) {
                return Some(uri.to_string());
            }
        }
    }
    None
}

fn replace_image_refs_with_results(
    body: &mut Value,
    refs: &[DirectImageRef],
    results: &[RecognitionResult],
    successful: usize,
) -> usize {
    let count = refs.len().min(results.len());
    let mut replaced = 0;
    for (image, result) in refs.iter().zip(results.iter()).take(count) {
        let Some(block) = body.pointer_mut(&image.path) else {
            continue;
        };
        let fallback = if result.ok {
            format!("[图片 {} 识别结果]\n{}", replaced + 1, result.text.trim())
        } else if successful > 0 {
            FAILED_MARKER.to_string()
        } else {
            TOTAL_FAILURE_MARKER.to_string()
        };
        replace_block_with_text(block, &fallback, &image.text_type);
        replaced += 1;
    }
    replaced
}

fn replace_block_with_text(block: &mut Value, text: &str, text_type: &str) {
    if text_type == "gemini" {
        *block = json!({"text": text});
        return;
    }

    let cache_control = block.get("cache_control").cloned();
    *block = json!({
        "type": text_type,
        "text": text
    });
    if let (Some(cache_control), Some(object)) = (cache_control, block.as_object_mut()) {
        object.insert("cache_control".to_string(), cache_control);
    }
}

async fn recognize_images(
    config: &VisionBridgeConfig,
    refs: &[DirectImageRef],
) -> Vec<RecognitionResult> {
    let timeout = Duration::from_secs(
        config
            .timeout_seconds
            .max(5)
            .min(MAX_VISION_TIMEOUT_SECONDS),
    );
    let client = match reqwest::Client::builder().timeout(timeout).build() {
        Ok(client) => client,
        Err(err) => {
            log::warn!("[MediaVision] failed to build HTTP client: {err}");
            return refs
                .iter()
                .map(|_| RecognitionResult {
                    text: TOTAL_FAILURE_MARKER.to_string(),
                    ok: false,
                })
                .collect();
        }
    };

    let prompt = if config.prompt.trim().is_empty() {
        DEFAULT_RECOGNIZE_PROMPT
    } else {
        config.prompt.trim()
    };
    let tasks = refs
        .iter()
        .enumerate()
        .map(|(index, image)| {
            let client = client.clone();
            recognize_one(
                config,
                client,
                prompt,
                index + 1,
                image.url.clone(),
            )
        })
        .collect::<Vec<_>>();
    futures::future::join_all(tasks).await
}

async fn recognize_one(
    config: &VisionBridgeConfig,
    client: reqwest::Client,
    prompt: &str,
    index: usize,
    image_url: String,
) -> RecognitionResult {
    if image_url.is_empty() {
        return RecognitionResult {
            text: FAILED_MARKER.to_string(),
            ok: false,
        };
    }

    let mut last_error = String::new();
    for attempt in 1..=VISION_RETRY_COUNT {
        let payload = json!({
            "model": config.model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": image_url.clone()}},
                        {"type": "text", "text": prompt}
                    ]
                }
            ]
        });

        match client
            .post(config.api_url.trim())
            .bearer_auth(config.api_key.trim())
            .json(&payload)
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    let status = response.status();
                    let detail = response.text().await.unwrap_or_default();
                    last_error = format!("HTTP {status}: {detail}");
                } else {
                    match response.json::<Value>().await {
                        Ok(data) => {
                            let content = data
                                .pointer("/choices/0/message/content")
                                .and_then(extract_content_text);
                            if let Some(content) = content.filter(|text| !text.trim().is_empty()) {
                                return RecognitionResult {
                                    text: content.trim().to_string(),
                                    ok: true,
                                };
                            }
                            last_error = "empty content".to_string();
                        }
                        Err(err) => last_error = format!("response parse failed: {err}"),
                    }
                }
            }
            Err(err) => last_error = err.to_string(),
        }

        if attempt < VISION_RETRY_COUNT {
            tokio::time::sleep(Duration::from_millis(VISION_RETRY_DELAY_MS)).await;
        }
    }
    log::warn!(
        "[MediaVision] image {index} recognition failed after {VISION_RETRY_COUNT} attempts: {last_error}"
    );
    RecognitionResult {
        text: FAILED_MARKER.to_string(),
        ok: false,
    }
}

fn extract_content_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Array(items) => {
            let mut text = String::new();
            for item in items {
                if let Some(part) = item.get("text").and_then(Value::as_str) {
                    text.push_str(part);
                }
            }
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn collects_and_replaces_chat_and_responses_images_in_order() {
        let mut body = json!({
            "model": "deepseek-v4-flash",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "see"},
                    {"type": "image_url", "image_url": {"url": "https://example.com/one.png"}},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "YWJj"}}
                ]
            }]
        });

        let refs = collect_direct_image_refs(&body);
        assert_eq!(refs.len(), 2);
        let results = vec![
            RecognitionResult {
                text: "第一个图片结果".to_string(),
                ok: true,
            },
            RecognitionResult {
                text: "第二个图片结果".to_string(),
                ok: true,
            },
        ];
        let replaced =
            replace_image_refs_with_results(&mut body, &refs, &results, results.len());

        assert_eq!(replaced, 2);
        assert_eq!(body["messages"][0]["content"][1]["type"], "text");
        assert_eq!(
            body["messages"][0]["content"][1]["text"],
            "[图片 1 识别结果]\n第一个图片结果"
        );
        assert_eq!(body["messages"][0]["content"][2]["type"], "text");
        assert_eq!(
            body["messages"][0]["content"][2]["text"],
            "[图片 2 识别结果]\n第二个图片结果"
        );
    }

    #[test]
    fn collects_responses_input_image_items() {
        let body = json!({
            "input": [
                {"type": "input_image", "image_url": "https://example.com/one.png"},
                {"role": "user", "content": [
                    {"type": "input_text", "text": "see"},
                    {"type": "input_image", "image_url": "data:image/png;base64,YWJj"}
                ]}
            ]
        });

        let refs = collect_direct_image_refs(&body);

        assert_eq!(refs.len(), 2);
        assert!(refs.iter().all(|item| item.text_type == "input_text"));
    }
}
