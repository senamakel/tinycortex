//! Per-document immutable subtree sealing and merge-tier folding.

use anyhow::{Context, Result};
use chrono::Utc;
use futures::stream::{StreamExt, TryStreamExt};
use std::collections::{BTreeMap, HashMap};

use super::bucket_seal::{append_to_buffer_async, cascade_all_from_with_services};
use super::hydrate::hydrate_inputs;
use super::label_resolver::resolve_labels;
use super::registry::new_summary_id;
use super::store::{self, SummaryNode, Tree};
use super::summarise::{fallback_summary, SummaryContext};
use super::types::{LabelStrategy, SealServices};
use crate::memory::config::MemoryConfig;

/// The durable output of planning one document-subtree job. Summary
/// projections are returned to the host so it can publish them only after the
/// queue worker commits the job lease and these mutations together.
#[derive(Clone, Debug)]
pub struct DocumentSealPlanResult {
    /// Tree that will own the projected summaries.
    pub tree_id: String,
    /// Root summary for this document version.
    pub root_id: String,
    /// Newly-created summaries, in child-before-parent order.
    pub summaries: Vec<SummaryNode>,
    /// Follow-up merge-tier seals to enqueue with this job's settlement.
    pub follow_ups: Vec<crate::memory::queue::NewJob>,
}

/// Build a document subtree as an in-memory overlay for queue settlement.
///
/// No summary, entity, tree, or buffer state is published while planning. The
/// returned mutations must be committed together with the queue lease and
/// `result.follow_ups`; a stale lease therefore leaves all domain state
/// untouched. Summarisation, label extraction, and embedding happen once
/// before settlement and are not repeated by a CAS retry.
pub async fn plan_document_subtree_for_queue(
    config: &MemoryConfig,
    tree: &Tree,
    payload: &crate::memory::queue::SealDocumentPayload,
    services: &SealServices<'_>,
    strategy: &LabelStrategy,
) -> Result<crate::memory::queue::DelegateOutput<DocumentSealPlanResult>> {
    if payload.chunk_ids.is_empty() {
        anyhow::bail!("seal_document_subtree: empty chunk set");
    }

    let tree_key = format!("tree:{}", tree.id);
    let tree_value = crate::memory::chunks::versioned::read_state_async(
        config,
        crate::memory::chunks::collections::TREES,
        &tree_key,
    )
    .await?
    .context("document subtree tree does not exist")?;
    let mut current_tree: Tree = serde_json::from_value(tree_value.clone())?;
    anyhow::ensure!(
        current_tree.status == super::TreeStatus::Active,
        "tree '{}' is archived",
        tree.id
    );

    let mut accumulated = PlannedDocumentMutations::default();
    accumulated.add_noop(
        crate::memory::chunks::collections::TREES,
        &tree_key,
        Some(tree_value),
    )?;

    let mut level = 0;
    let mut current_ids = payload.chunk_ids.clone();
    let mut generated = HashMap::<String, SummaryNode>::new();
    let doc_root = loop {
        let batches = if level == 0 {
            batch_leaves_by_token_budget(config, &current_ids).await?
        } else {
            batch_by_count(&current_ids, DOC_SUBTREE_MAX_FANIN)
        };
        let mut nodes = Vec::with_capacity(batches.len());
        for batch in batches {
            let node = plan_explicit_children(
                config,
                tree,
                level,
                &batch,
                &payload.doc_id,
                payload.version_ms,
                services,
                strategy,
                &generated,
                &mut accumulated,
            )
            .await?;
            for child_id in &node.child_ids {
                if let Some(child) = generated.get_mut(child_id) {
                    if child.parent_id.is_none() {
                        child.parent_id = Some(node.id.clone());
                    }
                }
            }
            current_ids.push(node.id.clone());
            generated.insert(node.id.clone(), node.clone());
            nodes.push(node);
        }
        current_ids = nodes.iter().map(|node| node.id.clone()).collect();
        level += 1;
        if current_ids.len() <= 1 {
            break nodes
                .into_iter()
                .next()
                .context("document seal produced no root")?;
        }
    };

    // Stage immutable summary records and their entity/embedding sidecars only
    // after every generated child has its final parent link in the overlay.
    let signature = crate::memory::chunks::tree_active_signature(config);
    let mut summaries: Vec<_> = generated.into_values().collect();
    summaries.sort_by(|left, right| {
        left.level
            .cmp(&right.level)
            .then_with(|| left.id.cmp(&right.id))
    });
    for node in &summaries {
        let summary = store::stage_summary_changes(node, None, &signature)?;
        accumulated.add_many(summary.expected, summary.changes)?;
        if node.embedding.is_some() {
            let skip_key = crate::memory::chunks::embeddings::summary_reembed_skip_key(
                &node.id,
                &signature,
            );
            let skip = crate::memory::chunks::versioned::read_state_async(
                config,
                crate::memory::chunks::collections::SUMMARY_REEMBED_SKIPS,
                &skip_key,
            )
            .await?;
            accumulated.add(
                crate::memory::chunks::collections::SUMMARY_REEMBED_SKIPS,
                skip_key,
                skip,
                None,
            )?;
        }
        let entities = crate::memory::score::store::stage_summary_entity_ids_async(
            config,
            &node.entities,
            &node.id,
            node.score,
            node.sealed_at.timestamp_millis(),
            Some(&node.tree_id),
        )
        .await?;
        accumulated.add_many(entities.expected, entities.changes)?;
    }

    // Add the document root to the connection tree's merge-tier buffer in the
    // same overlay. The regular Seal worker takes over if that buffer crosses
    // its gate, so no nested summary commits escape this job's lease.
    let buffer_key = format!("buffer:{}:{MERGE_LEVEL_BASE}", tree.id);
    let buffer_value = crate::memory::chunks::versioned::read_state_async(
        config,
        crate::memory::chunks::collections::BUFFERS,
        &buffer_key,
    )
    .await?;
    let mut merge_buffer = buffer_value
        .clone()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_else(|| store::Buffer::empty(&tree.id, MERGE_LEVEL_BASE));
    if !merge_buffer.item_ids.contains(&doc_root.id) {
        merge_buffer.item_ids.push(doc_root.id.clone());
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
    accumulated.add(
        crate::memory::chunks::collections::BUFFERS,
        buffer_key,
        buffer_value,
        Some(serde_json::to_value(&merge_buffer)?),
    )?;

    if current_tree.root_id.as_deref() != Some(doc_root.id.as_str())
        || doc_root.level > current_tree.max_level
    {
        current_tree.root_id = Some(doc_root.id.clone());
        current_tree.max_level = doc_root.level;
        current_tree.last_sealed_at = Some(Utc::now());
    }
    accumulated.replace(
        crate::memory::chunks::collections::TREES,
        tree_key,
        Some(serde_json::to_value(current_tree)?),
    )?;

    let follow_ups = if crate::memory::tree::should_seal(config, &merge_buffer) {
        vec![crate::memory::queue::NewJob::seal(&crate::memory::queue::SealPayload {
            tree_id: tree.id.clone(),
            level: MERGE_LEVEL_BASE,
            force_now_ms: None,
        })?]
    } else {
        Vec::new()
    };
    let root_id = doc_root.id.clone();
    let result = DocumentSealPlanResult {
        tree_id: tree.id.clone(),
        root_id,
        summaries,
        follow_ups,
    };
    Ok(crate::memory::queue::DelegateOutput {
        result,
        mutations: accumulated.into_job_mutations()?,
    })
}

#[derive(Default)]
struct PlannedDocumentMutations {
    expected: BTreeMap<(String, String), Option<serde_json::Value>>,
    changes: BTreeMap<(String, String), Option<serde_json::Value>>,
    edge_increments: HashMap<(String, String), usize>,
}

impl PlannedDocumentMutations {
    fn add_noop(
        &mut self,
        collection: &str,
        key: &str,
        value: Option<serde_json::Value>,
    ) -> Result<()> {
        self.add(collection, key.to_owned(), value.clone(), value)
    }

    fn add(
        &mut self,
        collection: &str,
        key: String,
        expected: Option<serde_json::Value>,
        value: Option<serde_json::Value>,
    ) -> Result<()> {
        let map_key = (collection.to_owned(), key.clone());
        if let Some(previous) = self.expected.get(&map_key) {
            anyhow::ensure!(
                previous == &expected,
                "document seal observed conflicting values for {collection}:{key}"
            );
        } else {
            self.expected.insert(map_key.clone(), expected.clone());
        }
        if self.changes.contains_key(&map_key) {
            match (collection, key.as_str()) {
                (crate::memory::chunks::collections::ENTITY_EDGES, _) => {
                    let count = self.edge_increments.entry(map_key.clone()).or_insert(1);
                    *count += 1;
                    let Some(mut replacement) = value else {
                        anyhow::bail!("entity edge mutation unexpectedly deletes {key}");
                    };
                    let old_weight = expected
                        .as_ref()
                        .and_then(|row| row.get("weight"))
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or(0);
                    replacement["weight"] = serde_json::json!(old_weight.saturating_add(*count as i64));
                    self.changes.insert(map_key, Some(replacement));
                }
                (crate::memory::chunks::collections::ENTITY_INDEX, key)
                    if key.starts_with("tree:") =>
                {
                    let Some(previous) = self.changes.get(&map_key).and_then(Clone::clone) else {
                        anyhow::bail!("tree entity manifest mutation unexpectedly deletes {key}");
                    };
                    let Some(next) = value else {
                        anyhow::bail!("tree entity manifest mutation unexpectedly deletes {key}");
                    };
                    let mut merged: serde_json::Value = previous;
                    let mut ids: std::collections::BTreeSet<String> = merged
                        .get("node_ids")
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .collect();
                    ids.extend(
                        next.get("node_ids")
                            .and_then(serde_json::Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_owned),
                    );
                    merged["node_ids"] = serde_json::to_value(ids)?;
                    self.changes.insert(map_key, Some(merged));
                }
                _ => {
                    let previous = self.changes.get(&map_key);
                    anyhow::ensure!(
                        previous == Some(&value),
                        "document seal produced conflicting replacements for {collection}:{key}"
                    );
                }
            }
        } else {
            if collection == crate::memory::chunks::collections::ENTITY_EDGES {
                self.edge_increments.insert(map_key.clone(), 1);
            }
            self.changes.insert(map_key, value);
        }
        Ok(())
    }

    fn add_many(
        &mut self,
        expected: Vec<crate::memory::chunks::versioned::StateChange>,
        changes: Vec<crate::memory::chunks::versioned::StateChange>,
    ) -> Result<()> {
        let mut by_key = BTreeMap::new();
        for change in changes {
            by_key.insert((change.collection, change.key), change.value);
        }
        for condition in expected {
            let value = by_key
                .remove(&(condition.collection.clone(), condition.key.clone()))
                .unwrap_or_else(|| condition.value.clone());
            self.add(
                &condition.collection,
                condition.key,
                condition.value,
                value,
            )?;
        }
        anyhow::ensure!(by_key.is_empty(), "document seal mutation has no precondition");
        Ok(())
    }

    fn replace(
        &mut self,
        collection: &str,
        key: String,
        value: Option<serde_json::Value>,
    ) -> Result<()> {
        let map_key = (collection.to_owned(), key.clone());
        let expected = self
            .expected
            .get(&map_key)
            .cloned()
            .context("document seal replacement has no precondition")?;
        self.add(collection, key, expected, value)
    }

    fn into_job_mutations(self) -> Result<Vec<crate::memory::queue::JobMutation>> {
        self.expected
            .into_iter()
            .map(|(key, expected)| {
                let value = self
                    .changes
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| expected.clone());
                let collection = match key.0.as_str() {
                    crate::memory::chunks::collections::TREES => {
                        crate::memory::queue::JobStateCollection::Trees
                    }
                    crate::memory::chunks::collections::BUFFERS => {
                        crate::memory::queue::JobStateCollection::TreeBuffers
                    }
                    crate::memory::chunks::collections::SUMMARIES => {
                        crate::memory::queue::JobStateCollection::Summaries
                    }
                    crate::memory::chunks::collections::SUMMARY_EMBEDDINGS => {
                        crate::memory::queue::JobStateCollection::SummaryEmbeddings
                    }
                    crate::memory::chunks::collections::ENTITY_INDEX => {
                        crate::memory::queue::JobStateCollection::EntityIndex
                    }
                    crate::memory::chunks::collections::ENTITY_EDGES => {
                        crate::memory::queue::JobStateCollection::EntityEdges
                    }
                    other => anyhow::bail!("unsupported document seal collection {other}"),
                };
                Ok(crate::memory::queue::JobMutation {
                    collection,
                    key: key.1,
                    expected,
                    value,
                })
            })
            .collect()
    }
}

#[allow(clippy::too_many_arguments)]
async fn plan_explicit_children(
    config: &MemoryConfig,
    tree: &Tree,
    level: u32,
    child_ids: &[String],
    doc_id: &str,
    version_ms: Option<i64>,
    services: &SealServices<'_>,
    strategy: &LabelStrategy,
    generated: &HashMap<String, SummaryNode>,
    mutations: &mut PlannedDocumentMutations,
) -> Result<SummaryNode> {
    let target_level = level + 1;
    let inputs = if level == 0 {
        for id in child_ids {
            let value = crate::memory::chunks::versioned::read_state_async(
                config,
                crate::memory::chunks::collections::CHUNK_LIFECYCLE,
                id,
            )
            .await?;
            mutations.add_noop(
                crate::memory::chunks::collections::CHUNK_LIFECYCLE,
                id,
                value,
            )?;
        }
        hydrate_inputs(config, level, child_ids).await?
    } else {
        child_ids
            .iter()
            .map(|id| {
                generated
                    .get(id)
                    .with_context(|| format!("document seal overlay is missing child summary {id}"))
                    .map(summary_input_from_node)
            })
            .collect::<Result<Vec<_>>>()?
    };
    anyhow::ensure!(
        !inputs.is_empty(),
        "document seal has no hydrated inputs at level {level}"
    );
    let time_range_start = inputs.iter().map(|input| input.time_range_start).min().unwrap();
    let time_range_end = inputs.iter().map(|input| input.time_range_end).max().unwrap();
    let score = inputs
        .iter()
        .map(|input| input.score)
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
            let content: String = output.content.chars().take(4_000).collect();
            Some(embedder.embed(&content).await.context("embed document summary")?)
        }
        _ => None,
    };
    Ok(SummaryNode {
        id: new_summary_id(target_level),
        tree_id: tree.id.clone(),
        tree_kind: tree.kind,
        level: target_level,
        parent_id: None,
        child_ids: inputs.iter().map(|input| input.id.clone()).collect(),
        content: output.content,
        token_count: output.token_count,
        entities,
        topics,
        time_range_start,
        time_range_end,
        score,
        sealed_at: Utc::now(),
        deleted: false,
        embedding,
        doc_id: Some(doc_id.to_owned()),
        version_ms,
    })
}

fn summary_input_from_node(node: &SummaryNode) -> crate::memory::tree::SummaryInput {
    crate::memory::tree::SummaryInput {
        id: node.id.clone(),
        content: node.content.clone(),
        token_count: node.token_count,
        entities: node.entities.clone(),
        topics: node.topics.clone(),
        time_range_start: node.time_range_start,
        time_range_end: node.time_range_end,
        score: node.score,
    }
}

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
