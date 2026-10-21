//! Direct ingestion of pre-built L1 summaries.

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::memory::chunks::tree_active_signature;
use crate::memory::config::MemoryConfig;
use crate::memory::tree::bucket_seal::cascade_all_from;
use crate::memory::tree::registry::new_summary_id;
use crate::memory::tree::store::{self, SummaryNode, Tree};
use crate::memory::tree::summarise::Summariser;
use crate::memory::tree::TreeFactory;

#[derive(Clone, Debug)]
pub struct SummaryIngestInput {
    pub content: String,
    pub token_count: u32,
    pub entities: Vec<String>,
    pub topics: Vec<String>,
    pub time_range_start: DateTime<Utc>,
    pub time_range_end: DateTime<Utc>,
    pub score: f32,
    pub child_labels: Vec<String>,
    pub child_basenames: Vec<Option<String>>,
}

#[derive(Clone, Debug)]
pub struct SummaryIngestOutcome {
    pub summary_id: String,
    /// Optional host-side projection path. The durable summary body lives in
    /// owner storage and does not imply that a filesystem export exists.
    pub content_path: Option<String>,
    pub sealed_ids: Vec<String>,
}

pub async fn ingest_summary(
    config: &MemoryConfig,
    tree: &Tree,
    input: SummaryIngestInput,
    summariser: &dyn Summariser,
) -> Result<SummaryIngestOutcome> {
    const TARGET_LEVEL: u32 = 1;
    let summary_id = new_summary_id(TARGET_LEVEL);
    let sealed_at = Utc::now();
    let node = SummaryNode {
        id: summary_id.clone(),
        tree_id: tree.id.clone(),
        tree_kind: tree.kind,
        level: TARGET_LEVEL,
        parent_id: None,
        child_ids: input.child_labels,
        content: input.content,
        token_count: input.token_count,
        entities: input.entities,
        topics: input.topics,
        time_range_start: input.time_range_start,
        time_range_end: input.time_range_end,
        score: input.score,
        sealed_at,
        deleted: false,
        embedding: None,
        doc_id: None,
        version_ms: None,
    };
    let signature = tree_active_signature(config);
    let tree_key = format!("tree:{}", tree.id);
    let buffer_key = format!("buffer:{}:{TARGET_LEVEL}", tree.id);
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
        anyhow::bail!("tree '{}' does not exist", tree.id);
    };
    let mut current_tree: Tree = serde_json::from_value(tree_value.clone())?;
    anyhow::ensure!(
        current_tree.status == crate::memory::tree::TreeStatus::Active,
        "tree '{}' is archived",
        tree.id
    );
    let mut buffer = values[1]
        .clone()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_else(|| store::Buffer::empty(&tree.id, TARGET_LEVEL));
    if !buffer.item_ids.contains(&summary_id) {
        buffer.item_ids.push(summary_id.clone());
        buffer.token_sum = buffer.token_sum.saturating_add(node.token_count as i64);
        buffer.oldest_at = Some(buffer.oldest_at.map_or(node.time_range_start, |existing| {
            existing.min(node.time_range_start)
        }));
    }
    if TARGET_LEVEL > current_tree.max_level {
        current_tree.root_id = Some(summary_id.clone());
        current_tree.max_level = TARGET_LEVEL;
        current_tree.last_sealed_at = Some(sealed_at);
    }
    let summary_changes = store::stage_summary_changes(&node, None, &signature)?;
    let entity_changes = crate::memory::score::store::stage_summary_entity_ids_async(
        config,
        &node.entities,
        &node.id,
        node.score,
        sealed_at.timestamp_millis(),
        Some(&tree.id),
    )
    .await?;
    let mut expected = vec![
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: tree_key.clone(),
            value: Some(tree_value.clone()),
        },
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: buffer_key.clone(),
            value: values[1].clone(),
        },
    ];
    expected.extend(summary_changes.expected);
    expected.extend(entity_changes.expected);
    let mut changes = vec![
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: tree_key,
            value: Some(serde_json::to_value(current_tree)?),
        },
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: buffer_key,
            value: Some(serde_json::to_value(buffer)?),
        },
    ];
    changes.extend(summary_changes.changes);
    changes.extend(entity_changes.changes);
    anyhow::ensure!(
        crate::memory::chunks::versioned::commit_if_state_matches_async(
            config,
            &expected,
            &[],
            &changes,
        )
        .await?,
        "tree state changed while directly ingesting summary {}",
        node.id
    );

    let strategy = TreeFactory::from_tree(tree).label_strategy();
    let sealed_ids =
        cascade_all_from(config, tree, TARGET_LEVEL, false, summariser, &strategy).await?;
    #[cfg(feature = "sync")]
    tracing::info!(summary_id, tree_id = %tree.id, cascaded = sealed_ids.len(), "[memory_tree:direct_ingest] summary ingested");
    Ok(SummaryIngestOutcome {
        summary_id,
        content_path: None,
        sealed_ids,
    })
}

#[cfg(test)]
#[path = "direct_ingest_tests.rs"]
mod tests;
