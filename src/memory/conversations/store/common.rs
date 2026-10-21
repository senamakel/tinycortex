//! Stable stream-name and label helpers.

use super::STREAM_PREFIX;

pub(super) fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub(super) fn decode_stream_id(stream: &str) -> Option<String> {
    let encoded = stream.strip_prefix(STREAM_PREFIX)?;
    if encoded.len() % 2 != 0 {
        return None;
    }
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16).ok())
        .collect::<Option<Vec<_>>>()?;
    String::from_utf8(bytes).ok()
}

pub(super) fn infer_labels(thread_id: &str) -> Vec<String> {
    if thread_id == "proactive:morning_briefing" {
        vec!["briefing".to_string()]
    } else if thread_id.starts_with("proactive:") {
        vec!["notification".to_string()]
    } else {
        vec!["general".to_string()]
    }
}

pub(super) fn normalize_labels(labels: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::with_capacity(labels.len());
    for label in labels {
        let label = match label.as_str() {
            "work" => "general".to_string(),
            "from_reflection" | "subconscious_tick" => "subconscious".to_string(),
            "agent-task" | "worker" => "tasks".to_string(),
            _ => label,
        };
        if !normalized.contains(&label) {
            normalized.push(label);
        }
    }
    normalized
}
