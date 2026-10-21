//! Tree records and scope indexes in the host-bound versioned document owner.

use std::collections::HashMap;

use super::types::{Tree, TreeInsertConflict, TreeKind, TreeStatus};
use crate::memory::config::MemoryConfig;
use anyhow::Result;

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
    if crate::memory::chunks::versioned::commit_if_state_matches(config, &expected, &[], &changes)?
    {
        Ok(())
    } else {
        Err(TreeInsertConflict {
            kind: tree.kind,
            scope: tree.scope.clone(),
        }
        .into())
    }
}

/// Async counterpart to [`insert_tree`] for callers already running on Tokio.
pub async fn insert_tree_async(config: &MemoryConfig, tree: &Tree) -> Result<()> {
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
    if crate::memory::chunks::versioned::commit_if_state_matches_async(
        config,
        &expected,
        &[],
        &changes,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TreeInsertConflict {
            kind: tree.kind,
            scope: tree.scope.clone(),
        }
        .into())
    }
}

fn tree_state_key(id: &str) -> String {
    format!("tree:{id}")
}

fn scope_state_key(kind: TreeKind, scope: &str) -> String {
    format!("scope:{}:{scope}", kind.as_str())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeCascadeDeletion {
    pub removed_summaries: usize,
    pub content_paths: Vec<String>,
}

/// Captured tree cascade. The owner snapshot prevents phantom summaries or
/// buffers created after enumeration from surviving a successful deletion.
pub(crate) struct StagedTreeCascadeDeletion {
    pub(crate) snapshot: crate::memory::chunks::versioned::HeadSnapshot,
    pub(crate) expected: Vec<crate::memory::chunks::versioned::StateChange>,
    pub(crate) changes: Vec<crate::memory::chunks::versioned::StateChange>,
    pub(crate) result: TreeCascadeDeletion,
}

/// Delete a tree and every tree-owned record in one strict owner-head commit.
/// A concurrent owner write invalidates the captured snapshot and causes the
/// operation to enumerate again, so an append racing deletion cannot leave an
/// orphan buffer behind.
pub async fn delete_tree_cascade_async(
    config: &MemoryConfig,
    tree_id: &str,
) -> Result<TreeCascadeDeletion> {
    for _ in 0..8 {
        let Some(staged) = stage_tree_cascade_deletion_async(config, tree_id).await? else {
            return Ok(TreeCascadeDeletion {
                removed_summaries: 0,
                content_paths: Vec::new(),
            });
        };
        if crate::memory::chunks::versioned::commit_if_state_matches_at_snapshot_async(
            config,
            &staged.snapshot,
            &staged.expected,
            &[],
            &staged.changes,
        )
        .await?
        {
            return Ok(staged.result);
        }
    }
    anyhow::bail!("tree '{tree_id}' changed too frequently to delete safely")
}

pub(crate) async fn stage_tree_cascade_deletion_async(
    config: &MemoryConfig,
    tree_id: &str,
) -> Result<Option<StagedTreeCascadeDeletion>> {
    use crate::memory::chunks::collections as c;
    use crate::memory::chunks::versioned::StateChange;

    let snapshot = crate::memory::chunks::versioned::capture_state_snapshot_async(config).await?;
    let trees =
        crate::memory::chunks::versioned::list_state_at_snapshot_async(config, &snapshot, c::TREES)
            .await?;
    let tree_key = tree_state_key(tree_id);
    let Some((_, tree_value)) = trees.iter().find(|(key, _)| key == &tree_key) else {
        return Ok(None);
    };
    let tree: Tree = serde_json::from_value(tree_value.clone())?;
    let scope_key = scope_state_key(tree.kind, &tree.scope);

    let mut expected = vec![
        StateChange {
            collection: c::TREES.to_owned(),
            key: tree_key.clone(),
            value: Some(tree_value.clone()),
        },
        StateChange {
            collection: c::TREES.to_owned(),
            key: scope_key.clone(),
            value: Some(serde_json::Value::String(tree_id.to_owned())),
        },
    ];
    let mut changes = vec![
        StateChange {
            collection: c::TREES.to_owned(),
            key: tree_key,
            value: None,
        },
        StateChange {
            collection: c::TREES.to_owned(),
            key: scope_key,
            value: None,
        },
    ];

    let summary_rows = crate::memory::chunks::versioned::list_state_at_snapshot_async(
        config,
        &snapshot,
        c::SUMMARIES,
    )
    .await?;
    let mut summary_ids = Vec::new();
    for (key, value) in summary_rows {
        let node: super::types::SummaryNode = serde_json::from_value(value.clone())?;
        if node.tree_id != tree_id {
            continue;
        }
        summary_ids.push(node.id);
        expected.push(StateChange {
            collection: c::SUMMARIES.to_owned(),
            key: key.clone(),
            value: Some(value),
        });
        changes.push(StateChange {
            collection: c::SUMMARIES.to_owned(),
            key,
            value: None,
        });
    }
    let mut content_paths = Vec::new();
    for collection in [c::SUMMARY_CONTENT_POINTERS, c::SUMMARY_EMBEDDINGS] {
        for (key, value) in crate::memory::chunks::versioned::list_state_at_snapshot_async(
            config, &snapshot, collection,
        )
        .await?
        {
            if value
                .get("summary_id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|id| summary_ids.iter().any(|summary_id| summary_id == id))
            {
                if collection == c::SUMMARY_CONTENT_POINTERS {
                    if let Some(path) = value
                        .get("content_path")
                        .and_then(serde_json::Value::as_str)
                    {
                        content_paths.push(path.to_owned());
                    }
                }
                expected.push(StateChange {
                    collection: collection.to_owned(),
                    key: key.clone(),
                    value: Some(value),
                });
                changes.push(StateChange {
                    collection: collection.to_owned(),
                    key,
                    value: None,
                });
            }
        }
    }
    for (key, value) in crate::memory::chunks::versioned::list_state_at_snapshot_async(
        config,
        &snapshot,
        c::BUFFERS,
    )
    .await?
    {
        if value.get("tree_id").and_then(serde_json::Value::as_str) == Some(tree_id) {
            expected.push(StateChange {
                collection: c::BUFFERS.to_owned(),
                key: key.clone(),
                value: Some(value),
            });
            changes.push(StateChange {
                collection: c::BUFFERS.to_owned(),
                key,
                value: None,
            });
        }
    }
    let entities =
        crate::memory::score::store::stage_delete_tree_entities_async(config, tree_id).await?;
    expected.extend(entities.expected);
    changes.extend(entities.changes);

    Ok(Some(StagedTreeCascadeDeletion {
        snapshot,
        expected,
        changes,
        result: TreeCascadeDeletion {
            removed_summaries: summary_ids.len(),
            content_paths,
        },
    }))
}

/// Fetch a tree by `(kind, scope)`. Returns `None` if no such tree exists.
pub fn get_tree_by_scope(
    config: &MemoryConfig,
    kind: TreeKind,
    scope: &str,
) -> Result<Option<Tree>> {
    let scope_key = scope_state_key(kind, scope);
    let rows = crate::memory::chunks::versioned::list_state(
        config,
        crate::memory::chunks::collections::TREES,
    )?;
    let Some(tree_id) = rows
        .iter()
        .find(|(key, _)| key == &scope_key)
        .and_then(|(_, value)| value.as_str())
    else {
        return Ok(None);
    };
    let Some(value) = rows
        .iter()
        .find(|(key, _)| key == &tree_state_key(tree_id))
        .map(|(_, value)| value.clone())
    else {
        anyhow::bail!("tree scope index points to missing tree id={tree_id}");
    };
    let tree: Tree = serde_json::from_value(value)?;
    anyhow::ensure!(
        tree.kind == kind && tree.scope == scope,
        "tree scope index is inconsistent for id={tree_id}"
    );
    Ok(Some(tree))
}

/// Async counterpart to [`get_tree_by_scope`], reading both the scope pointer
/// and tree row from one captured owner snapshot.
pub async fn get_tree_by_scope_async(
    config: &MemoryConfig,
    kind: TreeKind,
    scope: &str,
) -> Result<Option<Tree>> {
    let rows = crate::memory::chunks::versioned::list_state_async(
        config,
        crate::memory::chunks::collections::TREES,
    )
    .await?;
    let scope_key = scope_state_key(kind, scope);
    let Some(tree_id) = rows
        .iter()
        .find(|(key, _)| key == &scope_key)
        .and_then(|(_, value)| value.as_str())
    else {
        return Ok(None);
    };
    let Some(value) = rows
        .iter()
        .find(|(key, _)| key == &tree_state_key(tree_id))
        .map(|(_, value)| value.clone())
    else {
        anyhow::bail!("tree scope index points to missing tree id={tree_id}");
    };
    let tree: Tree = serde_json::from_value(value)?;
    anyhow::ensure!(
        tree.kind == kind && tree.scope == scope,
        "tree scope index is inconsistent for id={tree_id}"
    );
    Ok(Some(tree))
}

/// Fetch a tree by primary key id.
pub fn get_tree(config: &MemoryConfig, id: &str) -> Result<Option<Tree>> {
    let value = crate::memory::chunks::versioned::read_state(
        config,
        crate::memory::chunks::collections::TREES,
        &tree_state_key(id),
    )?;
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

/// Async counterpart to [`get_tree`].
pub async fn get_tree_async(config: &MemoryConfig, id: &str) -> Result<Option<Tree>> {
    crate::memory::chunks::versioned::read_state_async(
        config,
        crate::memory::chunks::collections::TREES,
        &tree_state_key(id),
    )
    .await?
    .map(serde_json::from_value)
    .transpose()
    .map_err(Into::into)
}

/// Fetch many trees by id from one committed owner snapshot. Missing ids are
/// silently absent from the map.
pub fn get_trees_batch(
    config: &MemoryConfig,
    tree_ids: &[String],
) -> Result<HashMap<String, Tree>> {
    if tree_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let wanted: std::collections::HashSet<_> = tree_ids.iter().map(String::as_str).collect();
    let rows = crate::memory::chunks::versioned::list_state(
        config,
        crate::memory::chunks::collections::TREES,
    )?;
    let mut out = HashMap::with_capacity(tree_ids.len());
    for (key, value) in rows {
        if let Some(id) = key.strip_prefix("tree:") {
            if wanted.contains(id) {
                let tree: Tree = serde_json::from_value(value)?;
                out.insert(tree.id.clone(), tree);
            }
        }
    }
    Ok(out)
}

/// Async counterpart to [`get_trees_batch`].
pub async fn get_trees_batch_async(
    config: &MemoryConfig,
    tree_ids: &[String],
) -> Result<HashMap<String, Tree>> {
    if tree_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let wanted: std::collections::HashSet<_> = tree_ids.iter().map(String::as_str).collect();
    let rows = crate::memory::chunks::versioned::list_state_async(
        config,
        crate::memory::chunks::collections::TREES,
    )
    .await?;
    let mut out = HashMap::with_capacity(tree_ids.len());
    for (key, value) in rows {
        if let Some(id) = key.strip_prefix("tree:") {
            if wanted.contains(id) {
                let tree: Tree = serde_json::from_value(value)?;
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

/// Async counterpart to [`list_trees_by_kind`].
pub async fn list_trees_by_kind_async(config: &MemoryConfig, kind: TreeKind) -> Result<Vec<Tree>> {
    let rows = crate::memory::chunks::versioned::list_state_async(
        config,
        crate::memory::chunks::collections::TREES,
    )
    .await?;
    let mut trees = rows
        .into_iter()
        .filter(|(key, _)| key.starts_with("tree:"))
        .map(|(_, value)| serde_json::from_value::<Tree>(value))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    trees.retain(|tree| tree.kind == kind);
    trees.sort_by_key(|tree| tree.created_at);
    Ok(trees)
}

/// Flip a tree's status to `archived`. Idempotent.
pub fn archive_tree(config: &MemoryConfig, tree_id: &str) -> Result<()> {
    let key = tree_state_key(tree_id);
    for _ in 0..8 {
        let Some(current) = crate::memory::chunks::versioned::read_state(
            config,
            crate::memory::chunks::collections::TREES,
            &key,
        )?
        else {
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
        if crate::memory::chunks::versioned::commit_if_state_matches(
            config,
            &expected,
            &[],
            &changes,
        )? {
            return Ok(());
        }
    }
    anyhow::bail!("failed to archive tree {tree_id}: state changed repeatedly")
}

/// Async counterpart to [`archive_tree`].
pub async fn archive_tree_async(config: &MemoryConfig, tree_id: &str) -> Result<()> {
    let key = tree_state_key(tree_id);
    for _ in 0..8 {
        let Some(current) = crate::memory::chunks::versioned::read_state_async(
            config,
            crate::memory::chunks::collections::TREES,
            &key,
        )
        .await?
        else {
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
    anyhow::bail!("failed to archive tree {tree_id}: state changed repeatedly")
}

#[cfg(test)]
#[path = "trees_tests.rs"]
mod tests;
