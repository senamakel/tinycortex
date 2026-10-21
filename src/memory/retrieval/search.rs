//! Case-insensitive substring search over the host-owned entity occurrence index.

use std::collections::HashMap;

use anyhow::{Context, Result};

use crate::memory::chunks::collections::ENTITY_INDEX;
use crate::memory::chunks::versioned;
use crate::memory::config::MemoryConfig;
use crate::memory::score::extract::EntityKind;
use crate::memory::store::entity_index::transaction::EntityOccurrence;

use super::types::EntityMatch;

/// Search entity ids and surfaces, aggregating matching occurrences by id.
///
/// Matching is a literal, case-insensitive substring search. A zero limit uses
/// the configured default; explicit limits are bounded by `max_limit`.
pub fn search_entities(
    config: &MemoryConfig,
    query: &str,
    kinds: Option<&[EntityKind]>,
    limit: usize,
) -> Result<Vec<EntityMatch>> {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return Ok(Vec::new());
    }
    let allowed: Option<std::collections::HashSet<_>> = kinds
        .filter(|values| !values.is_empty())
        .map(|values| values.iter().copied().collect());
    let mut grouped = HashMap::<String, EntityMatch>::new();
    for (key, value) in versioned::list_state(config, ENTITY_INDEX)? {
        if !key.starts_with("occ:") {
            continue;
        }
        let row: EntityOccurrence =
            serde_json::from_value(value).context("decode entity occurrence during search")?;
        if !row.entity_id.to_lowercase().contains(&query)
            && !row.surface.to_lowercase().contains(&query)
        {
            continue;
        }
        let kind = EntityKind::parse(&row.entity_kind)
            .map_err(anyhow::Error::msg)
            .context("decode entity kind during search")?;
        if allowed.as_ref().is_some_and(|set| !set.contains(&kind)) {
            continue;
        }
        grouped
            .entry(row.entity_id.clone())
            .and_modify(|hit| {
                hit.mention_count = hit.mention_count.saturating_add(1);
                hit.last_seen_ms = hit.last_seen_ms.max(row.timestamp_ms);
                if row.surface > hit.surface {
                    hit.surface.clone_from(&row.surface);
                }
            })
            .or_insert(EntityMatch {
                canonical_id: row.entity_id,
                kind,
                surface: row.surface,
                mention_count: 1,
                last_seen_ms: row.timestamp_ms,
            });
    }
    let mut matches: Vec<_> = grouped.into_values().collect();
    matches.sort_by(|left, right| {
        right
            .mention_count
            .cmp(&left.mention_count)
            .then_with(|| right.last_seen_ms.cmp(&left.last_seen_ms))
            .then_with(|| left.canonical_id.cmp(&right.canonical_id))
    });
    matches.truncate(normalise_limit(config, limit));
    Ok(matches)
}

fn normalise_limit(config: &MemoryConfig, limit: usize) -> usize {
    if limit == 0 {
        config.retrieval.limits.search_default_limit
    } else {
        limit.min(config.retrieval.limits.max_limit)
    }
}

#[cfg(test)]
#[path = "search_tests.rs"]
mod tests;
