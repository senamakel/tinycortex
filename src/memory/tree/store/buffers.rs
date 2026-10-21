//! Unsealed buffer records in the host-bound versioned document owner.

use anyhow::Result;
use chrono::{DateTime, Utc};

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
