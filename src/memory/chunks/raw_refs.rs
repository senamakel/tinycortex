//! Raw-archive pointers and content-pointer accessors for chunk/summary rows.
//!
//! [`RawRef`] lets ingest pipelines mirror full message bodies to on-disk
//! archives under `<content_root>/raw/` while storing only a ≤500-char preview
//! in the chunk record. Retrieval reads the archive directly instead of using
//! the inline preview.
//!
//! For email chunks, the raw archive referenced by the versioned raw-ref state
//! is the *only* copy of the message body — the chunk record holds just the
//! preview. This module only reads/writes the pointers; it does not
//! delete the archive files themselves. Chunk deletion
//! ([`super::store_delete`]) owns reachability-based cleanup of unreferenced
//! raw files and their ingest-gate rows.

use rusqlite::{params, OptionalExtension};

use super::collections::{CHUNK_CONTENT_POINTERS, RAW_REFS};
use super::connection::with_connection;
use super::versioned::{list_state, read_state, write_state, StateChange};
use crate::memory::config::MemoryConfig;
use anyhow::{Context, Result};

/// One pointer into the raw archive. A chunk's body is reconstructed by reading
/// each [`RawRef`] in order and joining with `"\n\n"`.
///
/// `start` / `end` are byte offsets into the raw `.md` file. `end = None` means
/// "read to end of file". Both default to "the whole file" (`start = 0`,
/// `end = None`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawRef {
    /// Forward-slash relative path under `<content_root>/`.
    pub path: String,
    /// Byte offset where this chunk's slice begins.
    #[serde(default)]
    pub start: usize,
    /// Byte offset where this chunk's slice ends (`None` = end of file).
    #[serde(default)]
    pub end: Option<usize>,
}

#[derive(serde::Deserialize)]
struct ChunkContentPointer {
    chunk_id: String,
    content_path: String,
    content_sha256: String,
}

/// Stash a list of [`RawRef`] entries on a chunk. Replaces any previous
/// value (last write wins; not additive).
///
/// # Errors
/// Returns `Err` if serialization or host storage fails. Silently affects no
/// state if `chunk_id` does not exist.
pub fn set_chunk_raw_refs(config: &MemoryConfig, chunk_id: &str, refs: &[RawRef]) -> Result<()> {
    if super::versioned::get_chunk(config, chunk_id)?.is_none() {
        return Ok(());
    }
    write_state(
        config,
        StateChange {
            collection: RAW_REFS.into(),
            key: chunk_id.into(),
            value: Some(serde_json::to_value(refs).context("serialize raw_refs")?),
        },
    )
}

/// Stash raw archive pointers on a chunk row inside a caller-owned
/// transaction. The caller commits/rolls back `tx`.
///
/// # Errors
/// See [`set_chunk_raw_refs`].
pub fn set_chunk_raw_refs_tx(
    _tx: &rusqlite::Transaction<'_>,
    _chunk_id: &str,
    _refs: &[RawRef],
) -> Result<()> {
    anyhow::bail!("raw refs must be committed with host storage, not a SQLite transaction")
}

/// Return the raw-archive pointers stored for `chunk_id`, or `None` if no raw
/// reference state was recorded.
///
/// # Errors
/// Returns `Err` if the host read fails or the stored value fails to deserialize
/// as `Vec<RawRef>` (a malformed/corrupt record — this is
/// surfaced as an error here, unlike [`list_chunk_raw_ref_paths_with_prefix`]
/// which tolerates the same failure per-row).
pub fn get_chunk_raw_refs(config: &MemoryConfig, chunk_id: &str) -> Result<Option<Vec<RawRef>>> {
    read_state(config, RAW_REFS, chunk_id)?
        .map(serde_json::from_value)
        .transpose()
        .context("deserialize raw refs")
}

/// Collect every raw-archive path referenced by any chunk raw-ref record,
/// restricted to paths under `rel_prefix`. Rust-side
/// prefix filter so `_` / `%` in slugs are treated literally (a SQL `LIKE`
/// prefix would otherwise treat those as wildcard metacharacters).
///
/// Scans the raw-ref state namespace, so cost scales with stored raw-ref
/// records rather than the number of matching paths.
///
/// # Errors
/// Returns `Err` only if a host read fails. A value that fails to deserialize
/// is silently skipped
/// rather than failing the whole scan, so a corrupt individual row cannot
/// block coverage computation — but its paths are also invisible to the
/// caller as a result.
pub fn list_chunk_raw_ref_paths_with_prefix(
    config: &MemoryConfig,
    rel_prefix: &str,
) -> Result<std::collections::HashSet<String>> {
    let mut out = std::collections::HashSet::new();
    for (_, value) in list_state(config, RAW_REFS)? {
        if let Ok(refs) = serde_json::from_value::<Vec<RawRef>>(value) {
            for raw_ref in refs {
                if raw_ref.path.starts_with(rel_prefix) {
                    out.insert(raw_ref.path);
                }
            }
        }
    }
    Ok(out)
}

/// Return both `content_path` and `content_sha256` stored in host state for
/// `chunk_id`.
///
/// Returns `None` when the row is missing, either column is SQL NULL, or the
/// path is empty. Email chunks intentionally use empty pointer strings because
/// their bodies live in the raw archive; those are not readable content-file
/// pointers.
///
/// # Errors
/// Returns `Err` only if the underlying query fails.
pub fn get_chunk_content_pointers(
    config: &MemoryConfig,
    chunk_id: &str,
) -> Result<Option<(String, String)>> {
    let Some(value) = read_state(config, CHUNK_CONTENT_POINTERS, chunk_id)? else {
        return Ok(None);
    };
    let pointer: ChunkContentPointer = serde_json::from_value(value)?;
    if pointer.chunk_id != chunk_id || pointer.content_path.is_empty() {
        return Ok(None);
    }
    Ok(Some((pointer.content_path, pointer.content_sha256)))
}

/// Return the `content_path` stored in host state for `chunk_id`, if any.
///
/// Empty strings are normalised to `None`, matching
/// [`get_chunk_content_pointers`].
///
/// # Errors
/// Returns `Err` only if the underlying query fails.
pub fn get_chunk_content_path(config: &MemoryConfig, chunk_id: &str) -> Result<Option<String>> {
    Ok(get_chunk_content_pointers(config, chunk_id)?.map(|(path, _)| path))
}

/// Return both `content_path` and `content_sha256` stored in SQLite for
/// `summary_id`. `None` means either column is SQL NULL; see
/// [`get_chunk_content_pointers`] for chunk-pointer semantics.
///
/// # Errors
/// Returns `Err` only if the underlying query fails.
pub fn get_summary_content_pointers(
    config: &MemoryConfig,
    summary_id: &str,
) -> Result<Option<(String, String)>> {
    with_connection(config, |conn| {
        let row = conn
            .query_row(
                "SELECT content_path, content_sha256 FROM mem_tree_summaries WHERE id = ?1",
                params![summary_id],
                |r| {
                    let path: Option<String> = r.get(0)?;
                    let sha: Option<String> = r.get(1)?;
                    Ok((path, sha))
                },
            )
            .optional()?;
        Ok(row.and_then(|(p, s)| p.zip(s)))
    })
}

/// List all non-deleted summary rows that have both a non-NULL
/// `content_path` and `content_sha256`.
///
/// # Errors
/// Returns `Err` if preparing/executing the query, or collecting any row,
/// fails.
pub fn list_summaries_with_content_path(
    config: &MemoryConfig,
) -> Result<Vec<(String, String, String)>> {
    with_connection(config, |conn| {
        let mut stmt = conn.prepare(
            "SELECT id, content_path, content_sha256
               FROM mem_tree_summaries
              WHERE content_path IS NOT NULL AND content_sha256 IS NOT NULL
                AND deleted = 0",
        )?;
        let rows = stmt
            .query_map([], |r| {
                let id: String = r.get(0)?;
                let path: String = r.get(1)?;
                let sha: String = r.get(2)?;
                Ok((id, path, sha))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("Failed to list summaries with content_path")?;
        Ok(rows)
    })
}
