//! Generic tree registry — get-or-create for any tree kind.
//!
//! All tree flavors share `UNIQUE(kind, scope)` and the same race-recovery
//! dance, so there is one implementation parameterised by [`TreeKind`].

use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

use crate::memory::config::MemoryConfig;
use crate::memory::tree::store::{self, Tree, TreeInsertConflict, TreeKind, TreeStatus};

/// Generic get-or-create. Returns the existing tree for `(kind, scope)` or
/// inserts a fresh one, recovering from a `UNIQUE` race by re-querying.
pub fn get_or_create_tree(config: &MemoryConfig, kind: TreeKind, scope: &str) -> Result<Tree> {
    get_or_create_tree_with_ask(config, kind, scope, None)
}

/// Async get-or-create for callers already running on Tokio.
pub async fn get_or_create_tree_async(
    config: &MemoryConfig,
    kind: TreeKind,
    scope: &str,
) -> Result<Tree> {
    get_or_create_tree_with_ask_async(config, kind, scope, None).await
}

/// Get-or-create variant that stamps a natural-language `ask` on the row when a
/// fresh tree is inserted. Used by [`crate::memory::tree::TreeFactory::flavoured`].
///
/// The `ask` is only applied at insert time; an existing `(kind, scope)` row is
/// returned unchanged (its stored ask wins), so re-instantiating a flavoured
/// factory never rewrites the ask of a live tree.
pub fn get_or_create_tree_with_ask(
    config: &MemoryConfig,
    kind: TreeKind,
    scope: &str,
    ask: Option<&str>,
) -> Result<Tree> {
    if let Some(existing) = store::get_tree_by_scope(config, kind, scope)? {
        return Ok(existing);
    }

    let tree = Tree {
        id: new_tree_id(kind),
        kind,
        scope: scope.to_string(),
        root_id: None,
        max_level: 0,
        status: TreeStatus::Active,
        created_at: Utc::now(),
        last_sealed_at: None,
        ask: ask.map(str::to_string),
    };
    match store::insert_tree(config, &tree) {
        Ok(()) => Ok(tree),
        Err(err) if is_tree_insert_conflict(&err) => {
            // Another caller created the same (kind, scope) between our lookup
            // and insert; re-query and return the winner.
            store::get_tree_by_scope(config, kind, scope)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "UNIQUE violation on insert but no row found on re-query for kind={} scope={}",
                    kind.as_str(),
                    scope
                )
            })
        }
        Err(err) => Err(err),
    }
}

/// Async get-or-create variant that stamps `ask` only on a newly inserted row.
pub async fn get_or_create_tree_with_ask_async(
    config: &MemoryConfig,
    kind: TreeKind,
    scope: &str,
    ask: Option<&str>,
) -> Result<Tree> {
    if let Some(existing) = store::get_tree_by_scope_async(config, kind, scope).await? {
        return Ok(existing);
    }
    let tree = Tree {
        id: new_tree_id(kind),
        kind,
        scope: scope.to_owned(),
        root_id: None,
        max_level: 0,
        status: TreeStatus::Active,
        created_at: Utc::now(),
        last_sealed_at: None,
        ask: ask.map(str::to_owned),
    };
    match store::insert_tree_async(config, &tree).await {
        Ok(()) => Ok(tree),
        Err(err) if is_tree_insert_conflict(&err) => {
            store::get_tree_by_scope_async(config, kind, scope)
                .await?
                .ok_or(err)
        }
        Err(err) => Err(err),
    }
}

/// Return true if `err` represents an owner-state tree insert conflict.
pub fn is_tree_insert_conflict(err: &anyhow::Error) -> bool {
    err.downcast_ref::<TreeInsertConflict>().is_some()
}

/// Generate a stable id for a new tree row, prefixed with the kind discriminator.
pub fn new_tree_id(kind: TreeKind) -> String {
    format!("{}:{}", kind.as_str(), Uuid::new_v4())
}

/// Id generator for summary nodes. The zero-padded Unix-ms timestamp is the
/// leading sort key so `ORDER BY id` is globally chronological across levels;
/// the level is suffixed for filter-by-level queries.
pub fn new_summary_id(level: u32) -> String {
    let ms = Utc::now().timestamp_millis() as u64;
    let rand_tail: u32 = rand::random();
    format!("summary:{ms:013}:L{level}-{rand_tail:08x}")
}

#[cfg(test)]
#[path = "registry_tests.rs"]
mod tests;
