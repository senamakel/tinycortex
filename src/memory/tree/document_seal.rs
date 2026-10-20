//! Per-document immutable subtree sealing and merge-tier folding.

use anyhow::{Context, Result};
use chrono::Utc;
use futures::stream::{StreamExt, TryStreamExt};

use super::bucket_seal::{append_to_buffer_async, cascade_all_from_with_services};
use super::hydrate::hydrate_inputs;
use super::label_resolver::resolve_labels;
use super::registry::new_summary_id;
use super::store::{self, SummaryNode, Tree};
use super::summarise::{fallback_summary, SummaryContext};
use super::types::{LabelStrategy, SealServices};
use crate::memory::config::MemoryConfig;

/// Level offset reserved for cross-document merge nodes.
pub const MERGE_LEVEL_BASE: u32 = 1_000;
const DOC_SUBTREE_MAX_FANIN: usize = 32;
const DOC_SUBTREE_SEAL_CONCURRENCY: usize = 8;

/// Build one immutable document-version subtree and feed its root into the
/// shared cross-document merge tier.
pub async fn seal_document_subtree_with_services(
    config: &MemoryConfig,
    tree: &Tree,
    doc_id: &str,
    version_ms: Option<i64>,
    chunk_ids: &[String],
    services: &SealServices<'_>,
    strategy: &LabelStrategy,
) -> Result<String> {
    if chunk_ids.is_empty() {
        anyhow::bail!("seal_document_subtree: empty chunk set");
    }
    log::debug!(
        "[memory_tree:seal_document] enter chunks={} has_version={}",
        chunk_ids.len(),
        version_ms.is_some()
    );
    let mut level = 0;
    let mut current_ids = chunk_ids.to_vec();
    let doc_root = loop {
        let batches = if level == 0 {
            batch_leaves_by_token_budget(config, &current_ids).await?
        } else {
            batch_by_count(&current_ids, DOC_SUBTREE_MAX_FANIN)
        };
        let batch_futures: Vec<_> = batches
            .iter()
            .map(|batch| {
                seal_explicit_children(
                    config, tree, level, batch, doc_id, version_ms, services, strategy,
                )
            })
            .collect();
        let nodes: Vec<SummaryNode> = futures::stream::iter(batch_futures)
            .buffered(DOC_SUBTREE_SEAL_CONCURRENCY)
            .try_collect()
            .await?;
        current_ids = nodes.iter().map(|node| node.id.clone()).collect();
        level += 1;
        if current_ids.len() <= 1 {
            break nodes
                .into_iter()
                .next()
                .context("document seal produced no root")?;
        }
    };

    append_to_buffer_async(
        config,
        &tree.id,
        MERGE_LEVEL_BASE,
        &doc_root.id,
        doc_root.token_count as i64,
        doc_root.time_range_start,
    )
    .await?;
    let root_id = doc_root.id.clone();
    let root_level = doc_root.level;
    let tree_key = format!("tree:{}", tree.id);
    let buffer_key = format!("buffer:{}:{MERGE_LEVEL_BASE}", tree.id);
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
        current_tree.status == super::TreeStatus::Active,
        "tree '{}' is archived",
        tree.id
    );
    let mut merge_buffer = values[1]
        .clone()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_else(|| store::Buffer::empty(&tree.id, MERGE_LEVEL_BASE));
    if !merge_buffer.item_ids.contains(&root_id) {
        merge_buffer.item_ids.push(root_id.clone());
        merge_buffer.token_sum = merge_buffer
            .token_sum
            .saturating_add(doc_root.token_count as i64);
        merge_buffer.oldest_at = Some(
            merge_buffer
                .oldest_at
                .map_or(doc_root.time_range_start, |oldest| {
                    oldest.min(doc_root.time_range_start)
                }),
        );
    }
    if current_tree.root_id.as_deref() != Some(root_id.as_str())
        || root_level > current_tree.max_level
    {
        current_tree.root_id = Some(root_id.clone());
        current_tree.max_level = root_level;
        current_tree.last_sealed_at = Some(Utc::now());
    }
    let expected = [
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
    let changes = [
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: tree_key,
            value: Some(serde_json::to_value(current_tree)?),
        },
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: buffer_key,
            value: Some(serde_json::to_value(merge_buffer)?),
        },
    ];
    anyhow::ensure!(
        crate::memory::chunks::versioned::commit_if_state_matches_async(
            config,
            &expected,
            &[],
            &changes,
        )
        .await?,
        "tree changed while publishing document root {}",
        root_id
    );
    cascade_all_from_with_services(config, tree, MERGE_LEVEL_BASE, false, services, strategy)
        .await?;
    log::debug!("[memory_tree:seal_document] complete levels={level}");
    Ok(doc_root.id)
}

async fn batch_leaves_by_token_budget(
    config: &MemoryConfig,
    chunk_ids: &[String],
) -> Result<Vec<Vec<String>>> {
    let chunks = crate::memory::chunks::get_chunks_batch_async(config, chunk_ids).await?;
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut tokens = 0_i64;
    for id in chunk_ids {
        let Some(chunk) = chunks.get(id) else {
            continue;
        };
        let next = chunk.token_count as i64;
        if !current.is_empty()
            && (tokens + next > config.tree.input_token_budget as i64
                || current.len() >= DOC_SUBTREE_MAX_FANIN)
        {
            batches.push(std::mem::take(&mut current));
            tokens = 0;
        }
        current.push(id.clone());
        tokens += next;
    }
    if !current.is_empty() {
        batches.push(current);
    }
    if batches.is_empty() {
        anyhow::bail!("seal_document_subtree: no resolvable chunks");
    }
    Ok(batches)
}

fn batch_by_count(ids: &[String], max: usize) -> Vec<Vec<String>> {
    ids.chunks(max.max(1)).map(<[String]>::to_vec).collect()
}

#[allow(clippy::too_many_arguments)]
async fn seal_explicit_children(
    config: &MemoryConfig,
    tree: &Tree,
    level: u32,
    child_ids: &[String],
    doc_id: &str,
    version_ms: Option<i64>,
    services: &SealServices<'_>,
    strategy: &LabelStrategy,
) -> Result<SummaryNode> {
    let target_level = level + 1;
    let inputs = hydrate_inputs(config, level, child_ids).await?;
    if inputs.is_empty() {
        anyhow::bail!("document seal has no hydrated inputs at level {level}");
    }
    let time_range_start = inputs.iter().map(|i| i.time_range_start).min().unwrap();
    let time_range_end = inputs.iter().map(|i| i.time_range_end).max().unwrap();
    let score = inputs
        .iter()
        .map(|i| i.score)
        .fold(f32::NEG_INFINITY, f32::max)
        .max(0.0);
    let output = if inputs.len() == 1 && inputs[0].token_count <= config.tree.output_token_budget {
        crate::memory::tree::SummaryOutput {
            content: inputs[0].content.clone(),
            token_count: inputs[0].token_count,
            ..Default::default()
        }
    } else {
        let context = SummaryContext {
            tree_id: &tree.id,
            tree_kind: tree.kind,
            target_level,
            token_budget: config.tree.output_token_budget,
            input_token_budget: config.tree.input_token_budget,
            overhead_reserve_tokens: config.tree.summary_overhead_reserve_tokens,
            ask: tree.ask.as_deref(),
        };
        match services.summariser.summarise(&inputs, &context).await {
            Ok(output) if !output.content.trim().is_empty() => output,
            _ => fallback_summary(&inputs, context.token_budget),
        }
    };
    let (entities, topics) = resolve_labels(strategy, &inputs, &output.content).await?;
    let embedding = match services.embedder {
        Some(embedder) if !output.content.trim().is_empty() => {
            let input: String = output.content.chars().take(4_000).collect();
            Some(
                embedder
                    .embed(&input)
                    .await
                    .context("embed document summary")?,
            )
        }
        _ => None,
    };
    let now = Utc::now();
    let node = SummaryNode {
        id: new_summary_id(target_level),
        tree_id: tree.id.clone(),
        tree_kind: tree.kind,
        level: target_level,
        parent_id: None,
        child_ids: child_ids.to_vec(),
        content: output.content,
        token_count: output.token_count,
        entities,
        topics,
        time_range_start,
        time_range_end,
        score,
        sealed_at: now,
        deleted: false,
        embedding,
        doc_id: Some(doc_id.to_string()),
        version_ms,
    };
    let signature = crate::memory::chunks::tree_active_signature(config);
    let tree_key = format!("tree:{}", node.tree_id);
    let mut keys = vec![crate::memory::chunks::versioned::StateKey {
        collection: crate::memory::chunks::collections::TREES.to_owned(),
        key: tree_key.clone(),
    }];
    if level > 0 {
        keys.extend(
            node.child_ids
                .iter()
                .map(|id| crate::memory::chunks::versioned::StateKey {
                    collection: crate::memory::chunks::collections::SUMMARIES.to_owned(),
                    key: format!("summary:{id}"),
                }),
        );
    }
    let values = crate::memory::chunks::versioned::read_state_keys_async(config, &keys).await?;
    let Some(tree_value) = values[0].as_ref() else {
        anyhow::bail!("tree '{}' does not exist", node.tree_id);
    };
    let tree_state: Tree = serde_json::from_value(tree_value.clone())?;
    anyhow::ensure!(
        tree_state.status == super::TreeStatus::Active,
        "tree '{}' is archived",
        node.tree_id
    );
    let summary_changes = store::stage_summary_changes(&node, None, &signature)?;
    let entity_changes = crate::memory::score::store::stage_summary_entity_ids_async(
        config,
        &node.entities,
        &node.id,
        node.score,
        now.timestamp_millis(),
        Some(&node.tree_id),
    )
    .await?;
    let mut expected = vec![crate::memory::chunks::versioned::StateChange {
        collection: crate::memory::chunks::collections::TREES.to_owned(),
        key: tree_key,
        value: Some(tree_value.clone()),
    }];
    let mut changes = Vec::new();
    if level > 0 {
        for (child_id, value) in node.child_ids.iter().zip(values.into_iter().skip(1)) {
            if let Some(value) = value {
                let mut child: SummaryNode = serde_json::from_value(value.clone())?;
                if child.parent_id.is_none() {
                    child.parent_id = Some(node.id.clone());
                    expected.push(crate::memory::chunks::versioned::StateChange {
                        collection: crate::memory::chunks::collections::SUMMARIES.to_owned(),
                        key: format!("summary:{child_id}"),
                        value: Some(value),
                    });
                    changes.push(crate::memory::chunks::versioned::StateChange {
                        collection: crate::memory::chunks::collections::SUMMARIES.to_owned(),
                        key: format!("summary:{child_id}"),
                        value: Some(serde_json::to_value(child)?),
                    });
                }
            }
        }
    }
    expected.extend(summary_changes.expected);
    expected.extend(entity_changes.expected);
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
        "tree changed while sealing document summary {}",
        node.id
    );
    services
        .observer
        .summary_committed(tree, &node, None, "document_subtree_seal")?;
    Ok(node)
}
