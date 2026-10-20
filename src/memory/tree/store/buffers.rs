//! Persistence for unsealed buffers (`mem_tree_buffers`).

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction};

use super::common::ms_to_utc;
use super::types::Buffer;
use crate::memory::config::MemoryConfig;

/// Buffer replacement and preconditions computed without committing.
#[derive(Clone, Debug)]
pub(crate) struct StagedBufferMutation {
    pub(crate) expected: Vec<crate::memory::chunks::versioned::StateChange>,
    pub(crate) changes: Vec<crate::memory::chunks::versioned::StateChange>,
    pub(crate) buffer: Buffer,
}

/// Read the current buffer at `(tree_id, level)` or return an empty one.
pub fn get_buffer(config: &MemoryConfig, tree_id: &str, level: u32) -> Result<Buffer> {
    let value = crate::memory::chunks::versioned::read_state(
        config,
        crate::memory::chunks::collections::BUFFERS,
        &buffer_key(tree_id, level),
    )?;
    value
        .map(serde_json::from_value)
        .transpose()
        .map(|buffer| buffer.unwrap_or_else(|| Buffer::empty(tree_id, level)))
        .map_err(Into::into)
}

pub(crate) async fn get_buffer_async(
    config: &MemoryConfig,
    tree_id: &str,
    level: u32,
) -> Result<Buffer> {
    let value = crate::memory::chunks::versioned::read_state_async(
        config,
        crate::memory::chunks::collections::BUFFERS,
        &buffer_key(tree_id, level),
    )
    .await?;
    value
        .map(serde_json::from_value)
        .transpose()
        .map(|buffer| buffer.unwrap_or_else(|| Buffer::empty(tree_id, level)))
        .map_err(Into::into)
}

/// Compute an append under one captured owner snapshot without publishing it.
/// Queue handlers use this to include the buffer change in lease settlement.
pub(crate) async fn stage_buffer_item_async(
    config: &MemoryConfig,
    tree_id: &str,
    level: u32,
    item_id: &str,
    token_delta: i64,
    item_ts: DateTime<Utc>,
) -> Result<StagedBufferMutation> {
    let tree_key = format!("tree:{tree_id}");
    let key = buffer_key(tree_id, level);
    let keys = [
        crate::memory::chunks::versioned::StateKey {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: tree_key.clone(),
        },
        crate::memory::chunks::versioned::StateKey {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: key.clone(),
        },
    ];
    let values = crate::memory::chunks::versioned::read_state_keys_async(config, &keys).await?;
    let Some(tree_value) = values[0].as_ref() else {
        anyhow::bail!("tree '{tree_id}' does not exist");
    };
    let tree: super::types::Tree = serde_json::from_value(tree_value.clone())?;
    anyhow::ensure!(
        tree.status == super::types::TreeStatus::Active,
        "tree '{tree_id}' is archived"
    );
    let mut buffer = values[1]
        .clone()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_else(|| Buffer::empty(tree_id, level));
    let mut expected = vec![crate::memory::chunks::versioned::StateChange {
        collection: crate::memory::chunks::collections::TREES.to_owned(),
        key: tree_key,
        value: Some(tree_value.clone()),
    }];
    let mut changes = Vec::new();
    if !buffer.item_ids.iter().any(|existing| existing == item_id) {
        expected.push(crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: key.clone(),
            value: values[1].clone(),
        });
        buffer.item_ids.push(item_id.to_owned());
        buffer.token_sum = buffer.token_sum.saturating_add(token_delta);
        buffer.oldest_at = Some(
            buffer
                .oldest_at
                .map_or(item_ts, |oldest| oldest.min(item_ts)),
        );
        changes.push(crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key,
            value: Some(serde_json::to_value(&buffer)?),
        });
    }
    Ok(StagedBufferMutation {
        expected,
        changes,
        buffer,
    })
}

pub(crate) fn append_buffer_item(
    config: &MemoryConfig,
    tree_id: &str,
    level: u32,
    item_id: &str,
    token_delta: i64,
    item_ts: DateTime<Utc>,
) -> Result<()> {
    for _ in 0..8 {
        let tree_key = format!("tree:{tree_id}");
        let buffer_key = buffer_key(tree_id, level);
        let keys = [
            crate::memory::chunks::versioned::StateKey {
                collection: crate::memory::chunks::collections::TREES.to_owned(),
                key: tree_key.clone(),
            },
            crate::memory::chunks::versioned::StateKey {
                collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
                key: buffer_key.clone(),
            },
        ];
        let values = crate::memory::chunks::versioned::read_state_keys(config, &keys)?;
        let Some(tree_value) = values[0].as_ref() else {
            anyhow::bail!("tree '{tree_id}' does not exist");
        };
        let tree: super::types::Tree = serde_json::from_value(tree_value.clone())?;
        anyhow::ensure!(
            tree.status == super::types::TreeStatus::Active,
            "tree '{tree_id}' is archived"
        );
        let mut buffer = values[1]
            .clone()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_else(|| Buffer::empty(tree_id, level));
        if buffer.item_ids.iter().any(|existing| existing == item_id) {
            return Ok(());
        }
        buffer.item_ids.push(item_id.to_owned());
        buffer.token_sum = buffer.token_sum.saturating_add(token_delta);
        buffer.oldest_at = Some(
            buffer
                .oldest_at
                .map_or(item_ts, |oldest| oldest.min(item_ts)),
        );
        let expected = [
            crate::memory::chunks::versioned::StateChange {
                collection: crate::memory::chunks::collections::TREES.to_owned(),
                key: tree_key,
                value: Some(tree_value.clone()),
            },
            crate::memory::chunks::versioned::StateChange {
                collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
                key: buffer_key.clone(),
                value: values[1].clone(),
            },
        ];
        let changes = [crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: buffer_key,
            value: Some(serde_json::to_value(buffer)?),
        }];
        if crate::memory::chunks::versioned::commit_if_state_matches(
            config,
            &expected,
            &[],
            &changes,
        )? {
            return Ok(());
        }
    }
    anyhow::bail!("buffer append conflicted repeatedly for tree '{tree_id}'")
}

pub(crate) async fn append_buffer_item_async(
    config: &MemoryConfig,
    tree_id: &str,
    level: u32,
    item_id: &str,
    token_delta: i64,
    item_ts: DateTime<Utc>,
) -> Result<()> {
    for _ in 0..8 {
        let tree_key = format!("tree:{tree_id}");
        let buffer_key = buffer_key(tree_id, level);
        let keys = [
            crate::memory::chunks::versioned::StateKey {
                collection: crate::memory::chunks::collections::TREES.to_owned(),
                key: tree_key.clone(),
            },
            crate::memory::chunks::versioned::StateKey {
                collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
                key: buffer_key.clone(),
            },
        ];
        let values = crate::memory::chunks::versioned::read_state_keys_async(config, &keys).await?;
        let Some(tree_value) = values[0].as_ref() else {
            anyhow::bail!("tree '{tree_id}' does not exist");
        };
        let tree: super::types::Tree = serde_json::from_value(tree_value.clone())?;
        anyhow::ensure!(
            tree.status == super::types::TreeStatus::Active,
            "tree '{tree_id}' is archived"
        );
        let mut buffer = values[1]
            .clone()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_else(|| Buffer::empty(tree_id, level));
        if buffer.item_ids.iter().any(|existing| existing == item_id) {
            return Ok(());
        }
        buffer.item_ids.push(item_id.to_owned());
        buffer.token_sum = buffer.token_sum.saturating_add(token_delta);
        buffer.oldest_at = Some(
            buffer
                .oldest_at
                .map_or(item_ts, |oldest| oldest.min(item_ts)),
        );
        let expected = [
            crate::memory::chunks::versioned::StateChange {
                collection: crate::memory::chunks::collections::TREES.to_owned(),
                key: tree_key,
                value: Some(tree_value.clone()),
            },
            crate::memory::chunks::versioned::StateChange {
                collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
                key: buffer_key.clone(),
                value: values[1].clone(),
            },
        ];
        let changes = [crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: buffer_key,
            value: Some(serde_json::to_value(buffer)?),
        }];
        if crate::memory::chunks::versioned::commit_if_state_matches_async(
            config,
            &expected,
            &[],
            &changes,
        )
        .await?
        {
            return Ok(());
        }
    }
    anyhow::bail!("buffer append conflicted repeatedly for tree '{tree_id}'")
}

fn buffer_key(tree_id: &str, level: u32) -> String {
    format!("buffer:{tree_id}:{level}")
}

#[cfg(test)]
#[path = "buffers_tests.rs"]
mod tests;

pub fn get_buffer_conn(conn: &Connection, tree_id: &str, level: u32) -> Result<Buffer> {
    let mut stmt = conn.prepare(
        "SELECT tree_id, level, item_ids_json, token_sum, oldest_at_ms
           FROM mem_tree_buffers WHERE tree_id = ?1 AND level = ?2",
    )?;
    let row = stmt
        .query_row(params![tree_id, level], row_to_buffer)
        .optional()
        .context("Failed to query buffer")?;
    Ok(row.unwrap_or_else(|| Buffer::empty(tree_id, level)))
}

/// Upsert a buffer row.
pub fn upsert_buffer_tx(tx: &Transaction<'_>, buf: &Buffer) -> Result<()> {
    let now_ms = Utc::now().timestamp_millis();
    tx.execute(
        "INSERT INTO mem_tree_buffers (
            tree_id, level, item_ids_json, token_sum, oldest_at_ms, updated_at_ms
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(tree_id, level) DO UPDATE SET
            item_ids_json = excluded.item_ids_json,
            token_sum = excluded.token_sum,
            oldest_at_ms = excluded.oldest_at_ms,
            updated_at_ms = excluded.updated_at_ms",
        params![
            buf.tree_id,
            buf.level,
            serde_json::to_string(&buf.item_ids)?,
            buf.token_sum,
            buf.oldest_at.map(|t| t.timestamp_millis()),
            now_ms,
        ],
    )
    .with_context(|| {
        format!(
            "Failed to upsert buffer tree_id={} level={}",
            buf.tree_id, buf.level
        )
    })?;
    Ok(())
}

/// Reset a buffer at `(tree_id, level)` to empty.
///
/// Seal transactions should use the snapshot-aware internal consumer instead.
pub fn clear_buffer_tx(tx: &Transaction<'_>, tree_id: &str, level: u32) -> Result<()> {
    upsert_buffer_tx(tx, &Buffer::empty(tree_id, level))
}

/// Consume exactly a previously-read buffer snapshot during a seal.
///
/// The current row must still begin with the snapshot ids. Items appended
/// while the summariser was running remain in their original order. If a
/// competing seal already changed the prefix, the transaction fails instead
/// of inserting a duplicate summary or deleting unrelated items.
pub(crate) fn consume_snapshot_tx(tx: &Transaction<'_>, snapshot: &Buffer) -> Result<()> {
    let mut current = get_buffer_conn(tx, &snapshot.tree_id, snapshot.level)?;
    if !current.item_ids.starts_with(&snapshot.item_ids) {
        anyhow::bail!(
            "buffer changed while sealing tree_id={} level={}",
            snapshot.tree_id,
            snapshot.level
        );
    }

    current.item_ids.drain(..snapshot.item_ids.len());
    current.token_sum = current.token_sum.saturating_sub(snapshot.token_sum).max(0);
    if current.item_ids.is_empty() {
        current.oldest_at = None;
        current.token_sum = 0;
    }
    upsert_buffer_tx(tx, &current)
}

/// List stale **L0** buffers ordered by `oldest_at_ms` ASC. Only L0 (raw-leaf)
/// buffers are returned — force-sealing an under-fanout upper buffer would
/// produce a degenerate single-child summary and collapse the tree into a chain.
pub fn list_stale_buffers(config: &MemoryConfig, older_than: DateTime<Utc>) -> Result<Vec<Buffer>> {
    let rows = crate::memory::chunks::versioned::list_state(
        config,
        crate::memory::chunks::collections::BUFFERS,
    )?;
    let cutoff = older_than;
    let mut buffers = rows
        .into_iter()
        .filter(|(key, _)| key.starts_with("buffer:"))
        .map(|(_, value)| serde_json::from_value::<Buffer>(value))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    buffers.retain(|buffer| {
        buffer.level == 0
            && buffer
                .oldest_at
                .is_some_and(|oldest_at| oldest_at <= cutoff)
    });
    buffers.sort_by_key(|buffer| buffer.oldest_at);
    Ok(buffers)
}

/// Async counterpart to [`list_stale_buffers`].
pub async fn list_stale_buffers_async(
    config: &MemoryConfig,
    older_than: DateTime<Utc>,
) -> Result<Vec<Buffer>> {
    let rows = crate::memory::chunks::versioned::list_state_async(
        config,
        crate::memory::chunks::collections::BUFFERS,
    )
    .await?;
    let mut buffers = rows
        .into_iter()
        .filter(|(key, _)| key.starts_with("buffer:"))
        .map(|(_, value)| serde_json::from_value::<Buffer>(value))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    buffers.retain(|buffer| {
        buffer.level == 0
            && buffer
                .oldest_at
                .is_some_and(|oldest_at| oldest_at <= older_than)
    });
    buffers.sort_by_key(|buffer| buffer.oldest_at);
    Ok(buffers)
}

fn row_to_buffer(row: &rusqlite::Row<'_>) -> rusqlite::Result<Buffer> {
    let tree_id: String = row.get(0)?;
    let level: i64 = row.get(1)?;
    let item_ids_json: String = row.get(2)?;
    let token_sum: i64 = row.get(3)?;
    let oldest_ms: Option<i64> = row.get(4)?;

    let item_ids: Vec<String> = serde_json::from_str(&item_ids_json).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let oldest_at = oldest_ms.map(ms_to_utc).transpose()?;
    Ok(Buffer {
        tree_id,
        level: level.max(0) as u32,
        item_ids,
        token_sum,
        oldest_at,
    })
}
