//! Source-level ingest gates, chunk lifecycle status, and the raw-archive file
//! coverage gate — all stored in `mem_tree_ingested_sources` + the
//! `lifecycle_status` column.

use anyhow::Result;
use chrono::Utc;

use super::collections::{CHUNK_LIFECYCLE, INGESTED_SOURCES};
use super::types::SourceKind;
use super::versioned::{
    insert_state_changes_if_absent, list_chunk_snapshot, list_state,
    read_chunk_lifecycle_status as read_implicit_lifecycle_status, read_state, write_state,
    StateChange,
};
use crate::memory::config::MemoryConfig;

// ── Lifecycle status ─────────────────────────────────────────────────────────

/// Set the lifecycle status column for `chunk_id`. See the
/// `super::store::CHUNK_STATUS_*` constants for valid values — this function
/// does not validate `status` against them, so passing an arbitrary string
/// stores it as-is.
///
/// # Errors
/// Returns `Err` if the update fails or `chunk_id` does not exist.
pub fn set_chunk_lifecycle_status(
    config: &MemoryConfig,
    chunk_id: &str,
    status: &str,
) -> Result<()> {
    if super::versioned::get_chunk(config, chunk_id)?.is_none() {
        anyhow::bail!("set_chunk_lifecycle_status: no chunk row for id={chunk_id}");
    }
    write_state(
        config,
        StateChange {
            collection: CHUNK_LIFECYCLE.into(),
            key: chunk_id.into(),
            value: Some(serde_json::json!({"status": status})),
        },
    )
}

/// Read the lifecycle status column for `chunk_id`, or `None` if the row is absent.
///
/// # Errors
/// Returns `Err` only if the underlying query fails.
pub fn get_chunk_lifecycle_status(config: &MemoryConfig, chunk_id: &str) -> Result<Option<String>> {
    read_implicit_lifecycle_status(config, chunk_id)
}

/// Count chunks currently sitting at a given lifecycle status. Matches
/// exactly (no prefix/wildcard semantics) — an unknown status string simply
/// counts as `0`.
///
/// # Errors
/// Returns `Err` only if the underlying query fails.
pub fn count_chunks_by_lifecycle_status(config: &MemoryConfig, status: &str) -> Result<u64> {
    let (_, states) = list_chunk_snapshot(config, &[CHUNK_LIFECYCLE])?;
    Ok(states
        .get(CHUNK_LIFECYCLE)
        .into_iter()
        .flatten()
        .filter(|(_, value)| value["status"] == status)
        .count() as u64)
}

// ── Source ingest gate ───────────────────────────────────────────────────────

/// Check whether a source has already been ingested. Call
/// [`claim_source_ingest`] when a conditional claim is required.
///
/// # Errors
/// Returns `Err` only if the underlying query fails.
pub fn is_source_ingested(
    config: &MemoryConfig,
    source_kind: SourceKind,
    source_id: &str,
) -> Result<bool> {
    Ok(read_state(
        config,
        INGESTED_SOURCES,
        &source_gate_key(source_kind, source_id),
    )?
    .is_some())
}

pub(crate) fn source_gate_key(source_kind: SourceKind, source_id: &str) -> String {
    format!("{}\0{source_id}", source_kind.as_str())
}

pub fn claim_source_ingest(
    config: &MemoryConfig,
    source_kind: SourceKind,
    source_id: &str,
    now_ms: i64,
) -> Result<bool> {
    Ok(insert_state_changes_if_absent(
        config,
        &[StateChange {
            collection: INGESTED_SOURCES.into(),
            key: source_gate_key(source_kind, source_id),
            value: Some(serde_json::json!({
                "source_kind": source_kind.as_str(),
                "source_id": source_id,
                "ingested_at_ms": now_ms,
            })),
        }],
    )? == 1)
}

/// Atomically claim `(source_kind, source_id)` for ingestion. Returns `true`
/// if the row was newly inserted; `false` if a previous ingest already
/// claimed it.
///
/// Idempotent: repeated calls keep returning `false` and never overwrite the
/// original `ingested_at_ms`.
///
/// Release a previously-claimed source gate row so a later retry can re-claim it.
///
/// Compensation for a document ingest that claimed the gate but then failed
/// before its chunks + jobs were durably persisted. Without this, a single
/// failed ingest would permanently mark the source ingested with zero chunks
/// and every retry would short-circuit as already-ingested. Idempotent: a
/// missing row is a no-op.
pub fn delete_source_ingest(
    config: &MemoryConfig,
    source_kind: SourceKind,
    source_id: &str,
) -> Result<()> {
    write_state(
        config,
        StateChange {
            collection: INGESTED_SOURCES.into(),
            key: source_gate_key(source_kind, source_id),
            value: None,
        },
    )
}

// ── Raw-archive file coverage gate ───────────────────────────────────────────

/// `source_kind` value used in `mem_tree_ingested_sources` to record that a raw
/// archive file (relative path under `<content_root>/`) has been covered by a
/// tree summary. Distinct from the chunk-store [`SourceKind`] values.
pub const RAW_FILE_GATE_KIND: &str = "raw_file";

/// Record that the given raw archive files are covered by a tree summary.
/// Idempotent (`INSERT OR IGNORE`); returns the number of newly-recorded
/// paths (already-recorded paths in `rel_paths` are silently skipped and not
/// counted).
///
/// # Errors
/// Returns `Err` if beginning/committing the transaction or preparing/
/// executing the insert for any path fails. Returns `Ok(0)` immediately (no
/// DB access) for an empty `rel_paths`.
pub fn mark_raw_paths_ingested(config: &MemoryConfig, rel_paths: &[String]) -> Result<u64> {
    if rel_paths.is_empty() {
        return Ok(0);
    }
    let now_ms = Utc::now().timestamp_millis();
    let changes = rel_paths
        .iter()
        .map(|path| StateChange {
            collection: INGESTED_SOURCES.into(),
            key: source_gate_key(
                SourceKind::Document,
                &format!("{RAW_FILE_GATE_KIND}:{path}"),
            ),
            value: Some(serde_json::json!({
                "source_kind": RAW_FILE_GATE_KIND,
                "source_id": path,
                "ingested_at_ms": now_ms,
            })),
        })
        .collect::<Vec<_>>();
    Ok(insert_state_changes_if_absent(config, &changes)? as u64)
}

/// Filter `rel_paths` down to the ones NOT yet recorded as ingested raw files.
/// Order of the surviving paths is preserved.
///
/// Issues one `COUNT(*)` query per path (not batched) — cost scales linearly
/// with `rel_paths.len()`, each a separate round-trip against the reused
/// prepared statement.
///
/// # Errors
/// Returns `Err` if preparing the statement or executing it for any path
/// fails. Returns `Ok(Vec::new())` immediately (no DB access) for an empty
/// `rel_paths`.
pub fn filter_raw_paths_not_ingested(
    config: &MemoryConfig,
    rel_paths: &[String],
) -> Result<Vec<String>> {
    if rel_paths.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for path in rel_paths {
        if read_state(
            config,
            INGESTED_SOURCES,
            &source_gate_key(
                SourceKind::Document,
                &format!("{RAW_FILE_GATE_KIND}:{path}"),
            ),
        )?
        .is_none()
        {
            out.push(path.clone());
        }
    }
    Ok(out)
}

/// Count raw-file gate rows whose path starts with `rel_prefix`. Rust-side
/// prefix filter (not SQL `LIKE`) so `_` / `%` in slugs are treated literally.
///
/// Scans every `raw_file`-kind gate row in `mem_tree_ingested_sources` — cost
/// scales with total gate-row count, not with the number of matching paths.
///
/// # Errors
/// Returns `Err` if preparing/executing the query or reading any row fails.
pub fn count_raw_paths_ingested_with_prefix(
    config: &MemoryConfig,
    rel_prefix: &str,
) -> Result<u64> {
    Ok(list_state(config, INGESTED_SOURCES)?
        .iter()
        .filter(|(_, value)| value["source_kind"] == RAW_FILE_GATE_KIND)
        .filter(|(_, value)| {
            value["source_id"]
                .as_str()
                .is_some_and(|path| path.starts_with(rel_prefix))
        })
        .count() as u64)
}
