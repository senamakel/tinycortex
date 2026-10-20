//! Persistence for tree rows in `mem_tree_trees`.

use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction};

use super::common::ms_to_utc;
use super::types::{Tree, TreeKind, TreeStatus};
use crate::memory::config::MemoryConfig;

/// Insert a new tree row. Fails if `(kind, scope)` already exists; callers that
/// want "get or create" semantics should go through the registry.
pub fn insert_tree(config: &MemoryConfig, tree: &Tree) -> Result<()> {
    let tree_key = tree_state_key(&tree.id);
    let scope_key = scope_state_key(tree.kind, &tree.scope);
    let expected = vec![
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: tree_key.clone(),
            value: None,
        },
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: scope_key.clone(),
            value: None,
        },
    ];
    let changes = vec![
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: tree_key,
            value: Some(serde_json::to_value(tree)?),
        },
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: scope_key,
            value: Some(serde_json::Value::String(tree.id.clone())),
        },
    ];
    if crate::memory::chunks::versioned::commit_if_state_matches(config, &expected, &[], &changes)? {
        Ok(())
    } else {
        anyhow::bail!("UNIQUE constraint failed: trees(kind, scope)")
    }
}

fn tree_state_key(id: &str) -> String {
    format!("tree:{id}")
}

fn scope_state_key(kind: TreeKind, scope: &str) -> String {
    format!("scope:{}:{scope}", kind.as_str())
}

pub fn insert_tree_conn(conn: &Connection, tree: &Tree) -> Result<()> {
    conn.execute(
        "INSERT INTO mem_tree_trees (
            id, kind, scope, root_id, max_level, status,
            created_at_ms, last_sealed_at_ms, ask
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            tree.id,
            tree.kind.as_str(),
            tree.scope,
            tree.root_id,
            tree.max_level,
            tree.status.as_str(),
            tree.created_at.timestamp_millis(),
            tree.last_sealed_at.map(|t| t.timestamp_millis()),
            tree.ask,
        ],
    )
    .with_context(|| format!("Failed to insert tree id={}", tree.id))?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeCascadeDeletion {
    pub removed_summaries: usize,
    pub content_paths: Vec<String>,
}

/// Delete a tree and all dependent rows inside a caller-owned transaction.
pub fn delete_tree_cascade_tx(tx: &Transaction<'_>, tree_id: &str) -> Result<TreeCascadeDeletion> {
    let content_paths = {
        let mut stmt = tx.prepare(
            "SELECT content_path FROM mem_tree_summaries
             WHERE tree_id = ?1 AND content_path IS NOT NULL AND content_path <> ''",
        )?;
        let paths = stmt
            .query_map(params![tree_id], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        paths
    };
    for sql in [
        "DELETE FROM mem_tree_summary_embeddings WHERE summary_id IN
         (SELECT id FROM mem_tree_summaries WHERE tree_id = ?1)",
        "DELETE FROM mem_tree_summary_reembed_skipped WHERE summary_id IN
         (SELECT id FROM mem_tree_summaries WHERE tree_id = ?1)",
        "DELETE FROM mem_tree_entity_index WHERE tree_id = ?1",
    ] {
        tx.execute(sql, params![tree_id])?;
    }
    let removed_summaries = tx.execute(
        "DELETE FROM mem_tree_summaries WHERE tree_id = ?1",
        params![tree_id],
    )?;
    tx.execute(
        "DELETE FROM mem_tree_buffers WHERE tree_id = ?1",
        params![tree_id],
    )?;
    tx.execute("DELETE FROM mem_tree_trees WHERE id = ?1", params![tree_id])?;
    Ok(TreeCascadeDeletion {
        removed_summaries,
        content_paths,
    })
}

/// Fetch a tree by `(kind, scope)`. Returns `None` if no such tree exists.
pub fn get_tree_by_scope(
    config: &MemoryConfig,
    kind: TreeKind,
    scope: &str,
) -> Result<Option<Tree>> {
    let scope_key = scope_state_key(kind, scope);
    let tree_key = crate::memory::chunks::versioned::read_state(
        config,
        crate::memory::chunks::collections::TREES,
        &scope_key,
    )?;
    let Some(tree_id) = tree_key.and_then(|value| value.as_str().map(str::to_owned)) else {
        return Ok(None);
    };
    get_tree(config, &tree_id)
}

pub fn get_tree_by_scope_conn(
    conn: &Connection,
    kind: TreeKind,
    scope: &str,
) -> Result<Option<Tree>> {
    let mut stmt = conn.prepare(
        "SELECT id, kind, scope, root_id, max_level, status,
                created_at_ms, last_sealed_at_ms, ask
           FROM mem_tree_trees WHERE kind = ?1 AND scope = ?2",
    )?;
    let row = stmt
        .query_row(params![kind.as_str(), scope], row_to_tree)
        .optional()
        .context("Failed to query tree by scope")?;
    Ok(row)
}

/// Fetch a tree by primary key id.
pub fn get_tree(config: &MemoryConfig, id: &str) -> Result<Option<Tree>> {
    let value = crate::memory::chunks::versioned::read_state(
        config,
        crate::memory::chunks::collections::TREES,
        &tree_state_key(id),
    )?;
    value.map(serde_json::from_value).transpose().map_err(Into::into)
}

/// Per-batch cap on `?` placeholders — well under SQLite's compile-time
/// `SQLITE_MAX_VARIABLE_NUMBER` (≥ 32766).
const TREES_MAX_FETCH_BATCH: usize = 500;

/// Fetch many trees by id in a single SQL round-trip per window. Missing ids
/// are silently absent from the map.
pub fn get_trees_batch(
    config: &MemoryConfig,
    tree_ids: &[String],
) -> Result<HashMap<String, Tree>> {
    if tree_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut out = HashMap::with_capacity(tree_ids.len());
    for window in tree_ids.chunks(TREES_MAX_FETCH_BATCH) {
        for id in window {
            if let Some(tree) = get_tree(config, id)? {
                out.insert(tree.id.clone(), tree);
            }
        }
    }
    Ok(out)
}

/// List every tree of a given kind, ordered by `created_at_ms` ASC.
pub fn list_trees_by_kind(config: &MemoryConfig, kind: TreeKind) -> Result<Vec<Tree>> {
    let rows = crate::memory::chunks::versioned::list_state(
        config,
        crate::memory::chunks::collections::TREES,
    )?;
    let mut trees = rows
        .into_iter()
        .filter(|(key, _)| key.starts_with("tree:"))
        .map(|(_, value)| serde_json::from_value::<Tree>(value))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    trees.retain(|tree| tree.kind == kind);
    trees.sort_by_key(|tree| tree.created_at);
    Ok(trees)
}

/// Update a tree's root / max_level / last_sealed after a seal climbed a level.
pub fn update_tree_after_seal_tx(
    tx: &Transaction<'_>,
    tree_id: &str,
    root_id: &str,
    max_level: u32,
    sealed_at: DateTime<Utc>,
) -> Result<()> {
    tx.execute(
        "UPDATE mem_tree_trees
            SET root_id = ?1,
                max_level = ?2,
                last_sealed_at_ms = ?3
          WHERE id = ?4",
        params![root_id, max_level, sealed_at.timestamp_millis(), tree_id],
    )
    .with_context(|| format!("Failed to update tree {tree_id} after seal"))?;
    Ok(())
}

/// Refresh `last_sealed_at` without changing the root (same-level seal).
pub fn refresh_last_sealed_tx(
    tx: &Transaction<'_>,
    tree_id: &str,
    sealed_at: DateTime<Utc>,
) -> Result<()> {
    tx.execute(
        "UPDATE mem_tree_trees SET last_sealed_at_ms = ?1 WHERE id = ?2",
        params![sealed_at.timestamp_millis(), tree_id],
    )
    .with_context(|| format!("Failed to refresh last_sealed_at for tree {tree_id}"))?;
    Ok(())
}

/// Flip a tree's status to `archived`. Idempotent.
pub fn archive_tree(config: &MemoryConfig, tree_id: &str) -> Result<()> {
    let key = tree_state_key(tree_id);
    for _ in 0..8 {
        let Some(current) = crate::memory::chunks::versioned::read_state(
            config,
            crate::memory::chunks::collections::TREES,
            &key,
        )? else {
            return Ok(());
        };
        let mut tree: Tree = serde_json::from_value(current.clone())?;
        tree.status = TreeStatus::Archived;
        let expected = [crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: key.clone(),
            value: Some(current),
        }];
        let changes = [crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: key.clone(),
            value: Some(serde_json::to_value(tree)?),
        }];
        if crate::memory::chunks::versioned::commit_if_state_matches(config, &expected, &[], &changes)? {
            return Ok(());
        }
    }
    anyhow::bail!("failed to archive tree {tree_id}: state changed repeatedly")
}

pub(crate) fn row_to_tree(row: &rusqlite::Row<'_>) -> rusqlite::Result<Tree> {
    let id: String = row.get(0)?;
    let kind_s: String = row.get(1)?;
    let scope: String = row.get(2)?;
    let root_id: Option<String> = row.get(3)?;
    let max_level: i64 = row.get(4)?;
    let status_s: String = row.get(5)?;
    let created_ms: i64 = row.get(6)?;
    let last_sealed_ms: Option<i64> = row.get(7)?;
    let ask: Option<String> = row.get(8)?;

    let kind = TreeKind::parse(&kind_s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, e.into())
    })?;
    let status = TreeStatus::parse(&status_s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, e.into())
    })?;
    Ok(Tree {
        id,
        kind,
        scope,
        root_id,
        max_level: max_level.max(0) as u32,
        status,
        created_at: ms_to_utc(created_ms)?,
        last_sealed_at: last_sealed_ms.map(ms_to_utc).transpose()?,
        ask,
    })
}
