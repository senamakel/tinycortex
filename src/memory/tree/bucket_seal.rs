//! Append + cascade-seal for summary trees — ported from OpenHuman's
//! `memory_tree/tree/bucket_seal.rs`, simplified to the reduced foundation.
//!
//! `append_leaf` pushes a persisted chunk into the L0 buffer of a tree. Seal
//! gates differ by level:
//!
//! - **L0 (leaves → L1)**: seal when `token_sum >= input_token_budget`.
//! - **L≥1 (summaries → next level)**: seal when `item_ids.len() >=
//!   summary_fanout`. Counting siblings keeps the tree's fan-in stable
//!   regardless of summariser quality.
//!
//! When a buffer seals, its items move into the new summary's `child_ids`, the
//! buffer clears, and the new summary id is queued at the next level. The
//! cascade continues upward until a buffer fails its gate.
//!
//! ## Differences from OpenHuman
//!
//! The LLM is abstracted behind [`Summariser`]; on a summariser error or blank
//! output the seal falls back to the deterministic [`fallback_summary`]. The
//! content-staging / git-mirror, async-queue follow-up enqueue, event-bus
//! progress events, seal-time embedding, and per-document subtree paths are not
//! ported (deferred). Summary bodies are stored inline in `mem_tree_summaries`.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::memory::config::MemoryConfig;
use crate::memory::tree::hydrate::hydrate_inputs;
use crate::memory::tree::label_resolver::resolve_labels;
use crate::memory::tree::registry::new_summary_id;
use crate::memory::tree::store::{self, Buffer, SummaryNode, Tree};
use crate::memory::tree::summarise::{fallback_summary, Summariser, SummaryContext};
pub(crate) use crate::memory::tree::types::NoopSealObserver;
pub use crate::memory::tree::types::{LabelStrategy, LeafRef, SealObserver, SealServices};

/// Hard cap on cascade depth — guards against runaway loops if token accounting
/// ever slips.
const MAX_CASCADE_DEPTH: u32 = 32;

/// Result of persisting one summary level. Queue workers can include the
/// optional parent seal in the same lease-fenced owner commit as this seal's
/// state mutations; the tree layer never writes queue state itself.
#[derive(Clone, Debug)]
pub struct SealLevelResult {
    /// Stable id of the summary written for this level.
    pub summary_id: String,
    /// Parent level that crossed its gate and should be sealed next.
    pub follow_up: Option<crate::memory::queue::SealPayload>,
}

/// Compute one seal for queue settlement without publishing domain state.
///
/// The returned mutations include the tree, source/parent buffers, summary,
/// entity occurrences, and edge records. A queue worker must commit them with
/// the job lease and terminal receipt in one owner-head CAS. This function
/// performs summarisation and embedding before that CAS, so those computations
/// may be repeated after a crash; no tree state is visible until settlement.
pub async fn plan_seal_one_level_for_queue(
    config: &MemoryConfig,
    tree: &Tree,
    buffer: &Buffer,
    services: &SealServices<'_>,
    strategy: &LabelStrategy,
) -> Result<crate::memory::queue::DelegateOutput<Option<crate::memory::queue::SealPayload>>> {
    let staged =
        stage_seal_one_level_with_services(config, tree, buffer, services, strategy).await?;
    let mutations = merge_staged_seal_changes(staged.expected, staged.changes)?;
    Ok(crate::memory::queue::DelegateOutput {
        result: staged.result.follow_up,
        mutations,
    })
}

/// Plan one queue-owned append without publishing any tree state.
///
/// The returned buffer and tree mutations can be combined with the append
/// job's lease receipt in the queue's owner-head CAS. This keeps a stale
/// worker from appending after its lease has expired.
pub(crate) async fn plan_append_to_buffer_for_queue(
    config: &MemoryConfig,
    tree_id: &str,
    level: u32,
    item_id: &str,
    token_delta: i64,
    item_ts: DateTime<Utc>,
) -> Result<crate::memory::queue::DelegateOutput<crate::memory::queue::AppendDecision>> {
    let staged =
        store::stage_buffer_item_async(config, tree_id, level, item_id, token_delta, item_ts)
            .await?;
    let mutations = merge_staged_seal_changes(staged.expected, staged.changes)?;
    Ok(crate::memory::queue::DelegateOutput {
        result: crate::memory::queue::AppendDecision {
            tree_id: tree_id.to_owned(),
            should_seal: should_seal(config, &staged.buffer),
        },
        mutations,
    })
}

/// Plan a queue append from its typed node and destination references.
///
/// Cortex resolves the node's stored token/time metadata, rejects missing or
/// dropped leaves and deleted summaries, and resolves or stages a source tree
/// itself. The returned mutations include all existence predicates and remain
/// invisible until queue lease settlement commits them.
pub async fn plan_append_node_for_queue(
    config: &MemoryConfig,
    node: &crate::memory::queue::NodeRef,
    target: &crate::memory::queue::AppendTarget,
) -> Result<Option<crate::memory::queue::DelegateOutput<crate::memory::queue::AppendDecision>>> {
    use crate::memory::chunks::collections as c;
    use crate::memory::chunks::versioned::{read_state_keys_async, StateKey};
    use crate::memory::queue::{JobMutation, JobStateCollection as C};
    use crate::memory::tree::store::TreeStatus;

    let (item_id, token_delta, timestamp, node_mutations) = match node {
        crate::memory::queue::NodeRef::Leaf { chunk_id } => {
            let keys = [
                StateKey {
                    collection: c::CHUNKS.to_owned(),
                    key: chunk_id.clone(),
                },
                StateKey {
                    collection: c::CHUNK_LIFECYCLE.to_owned(),
                    key: chunk_id.clone(),
                },
            ];
            let values = read_state_keys_async(config, &keys).await?;
            let Some(chunk_value) = values[0].clone() else {
                return Ok(None);
            };
            if values[1]
                .as_ref()
                .and_then(|v| v.get("status"))
                .and_then(serde_json::Value::as_str)
                == Some(crate::memory::chunks::CHUNK_STATUS_DROPPED)
            {
                return Ok(None);
            }
            let chunk: crate::memory::chunks::Chunk = serde_json::from_value(chunk_value.clone())?;
            (
                chunk_id.clone(),
                i64::from(chunk.token_count),
                chunk.created_at,
                vec![
                    JobMutation {
                        collection: C::Chunks,
                        key: chunk_id.clone(),
                        expected: Some(chunk_value.clone()),
                        value: Some(chunk_value),
                    },
                    JobMutation {
                        collection: C::ChunkLifecycle,
                        key: chunk_id.clone(),
                        expected: values[1].clone(),
                        value: values[1].clone(),
                    },
                ],
            )
        }
        crate::memory::queue::NodeRef::Summary { summary_id } => {
            let Some(value) = crate::memory::chunks::versioned::read_state_async(
                config,
                c::SUMMARIES,
                &format!("summary:{summary_id}"),
            )
            .await?
            else {
                return Ok(None);
            };
            let summary: SummaryNode = serde_json::from_value(value.clone())?;
            if summary.deleted {
                return Ok(None);
            }
            (
                summary_id.clone(),
                summary.token_count as i64,
                summary.time_range_start,
                vec![JobMutation {
                    collection: C::Summaries,
                    key: format!("summary:{summary_id}"),
                    expected: Some(value.clone()),
                    value: Some(value),
                }],
            )
        }
    };

    let (tree_id, tree_mutations, new_tree) = match target {
        crate::memory::queue::AppendTarget::Topic { tree_id } => {
            let Some(value) = crate::memory::chunks::versioned::read_state_async(
                config,
                c::TREES,
                &format!("tree:{tree_id}"),
            )
            .await?
            else {
                return Ok(None);
            };
            let tree: Tree = serde_json::from_value(value.clone())?;
            anyhow::ensure!(
                tree.status == TreeStatus::Active,
                "tree '{tree_id}' is archived"
            );
            (
                tree_id.clone(),
                vec![JobMutation {
                    collection: C::Trees,
                    key: format!("tree:{tree_id}"),
                    expected: Some(value.clone()),
                    value: Some(value),
                }],
                None,
            )
        }
        crate::memory::queue::AppendTarget::Source { source_id } => {
            let rows = crate::memory::chunks::versioned::list_state_async(config, c::TREES).await?;
            let scope_key = format!("scope:source:{source_id}");
            if let Some(tree_id) = rows
                .iter()
                .find(|(key, _)| key == &scope_key)
                .and_then(|(_, value)| value.as_str())
            {
                let value = rows
                    .iter()
                    .find(|(key, _)| key == &format!("tree:{tree_id}"))
                    .map(|(_, value)| value.clone())
                    .context("source tree scope points to a missing tree")?;
                let tree: Tree = serde_json::from_value(value.clone())?;
                anyhow::ensure!(
                    tree.status == TreeStatus::Active,
                    "tree '{tree_id}' is archived"
                );
                (
                    tree_id.to_owned(),
                    vec![
                        JobMutation {
                            collection: C::Trees,
                            key: scope_key.clone(),
                            expected: Some(serde_json::Value::String(tree_id.to_owned())),
                            value: Some(serde_json::Value::String(tree_id.to_owned())),
                        },
                        JobMutation {
                            collection: C::Trees,
                            key: format!("tree:{tree_id}"),
                            expected: Some(value.clone()),
                            value: Some(value),
                        },
                    ],
                    None,
                )
            } else {
                let tree = Tree {
                    id: crate::memory::tree::new_tree_id(crate::memory::tree::TreeKind::Source),
                    kind: crate::memory::tree::TreeKind::Source,
                    scope: source_id.clone(),
                    root_id: None,
                    max_level: 0,
                    status: TreeStatus::Active,
                    created_at: Utc::now(),
                    last_sealed_at: None,
                    ask: None,
                };
                let tree_key = format!("tree:{}", tree.id);
                let scope_value = serde_json::Value::String(tree.id.clone());
                (
                    tree.id.clone(),
                    vec![
                        JobMutation {
                            collection: C::Trees,
                            key: scope_key,
                            expected: None,
                            value: Some(scope_value),
                        },
                        JobMutation {
                            collection: C::Trees,
                            key: tree_key,
                            expected: None,
                            value: Some(serde_json::to_value(tree.clone())?),
                        },
                    ],
                    Some(tree),
                )
            }
        }
    };

    let mut mutations = node_mutations;
    mutations.extend(tree_mutations);
    let (buffer, buffer_mutations) = if let Some(tree) = new_tree {
        let mut buffer = Buffer::empty(&tree.id, 0);
        buffer.item_ids.push(item_id);
        buffer.token_sum = token_delta;
        buffer.oldest_at = Some(timestamp);
        let mutation = crate::memory::queue::JobMutation {
            collection: crate::memory::queue::JobStateCollection::TreeBuffers,
            key: format!("buffer:{}:0", tree.id),
            expected: None,
            value: Some(serde_json::to_value(&buffer)?),
        };
        (buffer, vec![mutation])
    } else {
        let staged =
            store::stage_buffer_item_async(config, &tree_id, 0, &item_id, token_delta, timestamp)
                .await?;
        let buffer = staged.buffer;
        let mutations = merge_staged_seal_changes(staged.expected, staged.changes)?;
        (buffer, mutations)
    };
    mutations.extend(buffer_mutations);
    Ok(Some(crate::memory::queue::DelegateOutput {
        result: crate::memory::queue::AppendDecision {
            tree_id,
            should_seal: should_seal(config, &buffer.buffer),
        },
        mutations,
    }))
}

fn merge_staged_seal_changes(
    expected: Vec<crate::memory::chunks::versioned::StateChange>,
    changes: Vec<crate::memory::chunks::versioned::StateChange>,
) -> Result<Vec<crate::memory::queue::JobMutation>> {
    use std::collections::BTreeMap;

    let mut preconditions = BTreeMap::new();
    for condition in expected {
        let key = (condition.collection.clone(), condition.key.clone());
        if let Some(previous) = preconditions.insert(key, condition.value.clone()) {
            anyhow::ensure!(
                previous == condition.value,
                "conflicting staged expectations for {}:{}",
                condition.collection,
                condition.key
            );
        }
    }

    let mut replacements = BTreeMap::new();
    for change in changes {
        let key = (change.collection.clone(), change.key.clone());
        anyhow::ensure!(
            preconditions.contains_key(&key),
            "staged seal change has no expectation for {}:{}",
            change.collection,
            change.key
        );
        if let Some(previous) = replacements.insert(key, change.value.clone()) {
            anyhow::ensure!(
                previous == change.value,
                "conflicting staged replacements for {}:{}",
                change.collection,
                change.key
            );
        }
    }

    // A staged expectation without a change still needs to participate in the
    // worker's owner-head CAS. Its unchanged value is an explicit no-op write.
    preconditions
        .into_iter()
        .map(|((collection, key), expected)| {
            let value = replacements
                .remove(&(collection.clone(), key.clone()))
                .unwrap_or_else(|| expected.clone());
            Ok(crate::memory::queue::JobMutation {
                collection: queue_collection(&collection)?,
                key,
                expected,
                value,
            })
        })
        .collect()
}

fn queue_collection(collection: &str) -> Result<crate::memory::queue::JobStateCollection> {
    use crate::memory::chunks::collections as c;
    use crate::memory::queue::JobStateCollection as J;
    Ok(match collection {
        c::TREES => J::Trees,
        c::BUFFERS => J::TreeBuffers,
        c::SUMMARIES => J::Summaries,
        c::SUMMARY_CONTENT_POINTERS => J::SummaryContentPointers,
        c::SCORES => J::ChunkScores,
        c::CHUNK_LIFECYCLE => J::ChunkLifecycle,
        c::CHUNK_EMBEDDINGS => J::ChunkEmbeddings,
        c::CHUNK_REEMBED_SKIPS => J::ChunkReembedSkips,
        c::ENTITY_INDEX => J::EntityIndex,
        c::ENTITY_EDGES => J::EntityEdges,
        c::ENTITY_HOTNESS => J::EntityHotness,
        other => anyhow::bail!("tree seal staged unsupported owner collection '{other}'"),
    })
}

/// Prepared seal work for queue settlement. The worker combines these owner
/// mutations with its lease receipt and follow-up jobs in one CAS; no domain
/// records are visible before that commit succeeds.
#[derive(Clone, Debug)]
pub(crate) struct StagedSealLevel {
    pub(crate) result: SealLevelResult,
    pub(crate) expected: Vec<crate::memory::chunks::versioned::StateChange>,
    pub(crate) changes: Vec<crate::memory::chunks::versioned::StateChange>,
    pub(crate) node: SummaryNode,
}

/// Append a leaf to `tree`, sealing buffers as they fill. Returns the ids of
/// any summaries that sealed during this call.
pub async fn append_leaf(
    config: &MemoryConfig,
    tree: &Tree,
    leaf: &LeafRef,
    summariser: &dyn Summariser,
    strategy: &LabelStrategy,
) -> Result<Vec<String>> {
    append_to_buffer_async(
        config,
        &tree.id,
        0,
        &leaf.chunk_id,
        leaf.token_count as i64,
        leaf.timestamp,
    )
    .await?;
    cascade_all_from(config, tree, 0, false, summariser, strategy).await
}

/// Queue-oriented variant of [`append_leaf`]: only stage the leaf in the L0
/// buffer and report whether the caller should enqueue a follow-up seal job.
pub fn append_leaf_deferred(config: &MemoryConfig, tree: &Tree, leaf: &LeafRef) -> Result<bool> {
    append_to_buffer(
        config,
        &tree.id,
        0,
        &leaf.chunk_id,
        leaf.token_count as i64,
        leaf.timestamp,
    )?;
    let buf = store::get_buffer(config, &tree.id, 0)?;
    Ok(should_seal(config, &buf))
}

/// Transactionally append a single item to `(tree_id, level)`'s buffer.
/// Idempotent on `(tree_id, level, item_id)`.
pub fn append_to_buffer(
    config: &MemoryConfig,
    tree_id: &str,
    level: u32,
    item_id: &str,
    token_delta: i64,
    item_ts: DateTime<Utc>,
) -> Result<()> {
    store::append_buffer_item(config, tree_id, level, item_id, token_delta, item_ts)
}

pub(crate) async fn append_to_buffer_async(
    config: &MemoryConfig,
    tree_id: &str,
    level: u32,
    item_id: &str,
    token_delta: i64,
    item_ts: DateTime<Utc>,
) -> Result<()> {
    store::append_buffer_item_async(config, tree_id, level, item_id, token_delta, item_ts).await
}

/// Seal buffers starting at `start_level` and cascade upward. When `force` is
/// `true`, the buffer at `start_level` is sealed regardless of its token/fan-in
/// gate (used by time-based flush and the disconnect force-flush). Upper levels
/// are sealed only when they cross their gate.
pub async fn cascade_all_from(
    config: &MemoryConfig,
    tree: &Tree,
    start_level: u32,
    force: bool,
    summariser: &dyn Summariser,
    strategy: &LabelStrategy,
) -> Result<Vec<String>> {
    cascade_all_from_with_services(
        config,
        tree,
        start_level,
        force,
        &SealServices {
            summariser,
            embedder: None,
            observer: &NoopSealObserver,
        },
        strategy,
    )
    .await
}

pub async fn cascade_all_from_with_services(
    config: &MemoryConfig,
    tree: &Tree,
    start_level: u32,
    force: bool,
    services: &SealServices<'_>,
    strategy: &LabelStrategy,
) -> Result<Vec<String>> {
    let mut sealed_ids: Vec<String> = Vec::new();
    let mut first_iteration = true;

    for level in (start_level..).take(MAX_CASCADE_DEPTH as usize) {
        let buf = store::get_buffer_async(config, &tree.id, level).await?;
        let forced = first_iteration && force;
        first_iteration = false;

        if !forced && !should_seal(config, &buf) {
            break;
        }
        if buf.is_empty() {
            break;
        }

        let summary_id =
            seal_one_level_with_services(config, tree, &buf, services, strategy).await?;
        sealed_ids.push(summary_id.summary_id);
    }

    // Flavoured trees carry a first-class compiled root artifact; refresh it
    // whenever a seal in this cascade may have moved the root. `tree` here is a
    // pre-seal snapshot, so `compile_flavoured_root` re-reads the live row to
    // pick up the new `root_id`. Best-effort: a stale artifact must never fail
    // an otherwise-successful seal.
    if !sealed_ids.is_empty() && tree.kind == crate::memory::tree::TreeKind::Flavoured {
        if let Err(err) =
            crate::memory::tree::flavoured::compile_flavoured_root_async(config, &tree.id).await
        {
            log::warn!(
                "[memory_tree:flavoured] compile root failed tree_id={}: {err:#}",
                tree.id
            );
        }
    }

    Ok(sealed_ids)
}

/// Level-aware seal gate. L0 gates on `token_sum`; L≥1 gates on sibling count.
/// Budgets are read from [`MemoryConfig::tree`], not hardcoded.
pub fn should_seal(config: &MemoryConfig, buf: &Buffer) -> bool {
    if buf.is_empty() {
        return false;
    }
    if buf.level == 0 {
        buf.token_sum >= config.tree.input_token_budget as i64
    } else {
        (buf.item_ids.len() as u32) >= config.tree.summary_fanout
    }
}

/// Seal `buf` at `level` into one summary at `level + 1`. Returns the new
/// summary id.
///
/// # Errors
/// Returns `Err` if `buf.item_ids` all fail to hydrate (e.g. their chunk/summary
/// rows were deleted) — the function refuses to persist a summary with zero
/// inputs. Also propagates any SQL failure from the seal transaction.
///
/// The potentially slow summariser runs without a database lock. The commit
/// transaction therefore re-reads and consumes only the snapshotted prefix;
/// concurrent appends remain buffered, while a competing seal causes this
/// transaction to abort cleanly.
pub async fn seal_one_level_with_services(
    config: &MemoryConfig,
    tree: &Tree,
    buf: &Buffer,
    services: &SealServices<'_>,
    strategy: &LabelStrategy,
) -> Result<SealLevelResult> {
    let plan = stage_seal_one_level_with_services(config, tree, buf, services, strategy).await?;
    anyhow::ensure!(
        crate::memory::chunks::versioned::commit_if_state_matches_async(
            config,
            &plan.expected,
            &[],
            &plan.changes,
        )
        .await?,
        "tree or summary state changed while sealing tree_id={} level={}",
        tree.id,
        buf.level
    );
    services
        .observer
        .summary_committed(tree, &plan.node, None, "bucket_seal")?;
    Ok(plan.result)
}

/// Compute and stage one seal without publishing domain state. Queue workers
/// use this so the summary, tree, buffer, entity-index and job receipt become
/// visible under the same lease-fenced owner-head CAS.
pub(crate) async fn stage_seal_one_level_with_services(
    config: &MemoryConfig,
    tree: &Tree,
    buf: &Buffer,
    services: &SealServices<'_>,
    strategy: &LabelStrategy,
) -> Result<StagedSealLevel> {
    let level = buf.level;
    let target_level = level + 1;

    let inputs = hydrate_inputs(config, level, &buf.item_ids).await?;
    if inputs.is_empty() {
        anyhow::bail!(
            "refused to seal empty buffer tree_id={} level={}",
            tree.id,
            level
        );
    }

    let time_range_start = inputs
        .iter()
        .map(|i| i.time_range_start)
        .min()
        .unwrap_or_else(Utc::now);
    let time_range_end = inputs
        .iter()
        .map(|i| i.time_range_end)
        .max()
        .unwrap_or_else(Utc::now);
    let score = inputs
        .iter()
        .map(|i| i.score)
        .fold(f32::NEG_INFINITY, f32::max)
        .max(0.0);

    let budget = config.tree.output_token_budget;
    let ctx = SummaryContext {
        tree_id: &tree.id,
        tree_kind: tree.kind,
        target_level,
        token_budget: budget,
        input_token_budget: config.tree.input_token_budget,
        overhead_reserve_tokens: config.tree.summary_overhead_reserve_tokens,
        ask: tree.ask.as_deref(),
    };
    // Treat a blank summary the same as a hard error — fall back to the
    // deterministic concat so we never persist `content = ""`.
    services
        .observer
        .progress(tree, "summarising", level, Some(inputs.len() as u32));
    let output = match services.summariser.summarise(&inputs, &ctx).await {
        Ok(o) if !o.content.trim().is_empty() => o,
        _ => fallback_summary(&inputs, budget),
    };

    let (node_entities, node_topics) = resolve_labels(strategy, &inputs, &output.content).await?;

    services.observer.progress(tree, "embedding", level, None);
    let embedding = match services.embedder {
        Some(embedder) if !output.content.trim().is_empty() => {
            let input: String = output.content.chars().take(4_000).collect();
            Some(
                embedder
                    .embed(&input)
                    .await
                    .context("embed sealed summary")?,
            )
        }
        _ => None,
    };

    let now = Utc::now();
    let summary_id = new_summary_id(target_level);
    let node = SummaryNode {
        id: summary_id.clone(),
        tree_id: tree.id.clone(),
        tree_kind: tree.kind,
        level: target_level,
        parent_id: None,
        // Hydration can skip deleted inputs. Never claim missing children in
        // the persisted summary merely because their ids remained buffered.
        child_ids: inputs.iter().map(|input| input.id.clone()).collect(),
        content: output.content,
        token_count: output.token_count,
        entities: node_entities,
        topics: node_topics,
        time_range_start,
        time_range_end,
        score,
        sealed_at: now,
        deleted: false,
        embedding,
        doc_id: None,
        version_ms: None,
    };

    services
        .observer
        .progress(tree, "persisting", target_level, None);
    let signature = crate::memory::chunks::tree_active_signature(config);
    let tree_id = tree.id.clone();
    let tree_key = format!("tree:{tree_id}");
    let source_buffer_key = format!("buffer:{tree_id}:{level}");
    let parent_buffer_key = format!("buffer:{tree_id}:{target_level}");
    let keys = [
        crate::memory::chunks::versioned::StateKey {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: tree_key.clone(),
        },
        crate::memory::chunks::versioned::StateKey {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: source_buffer_key.clone(),
        },
        crate::memory::chunks::versioned::StateKey {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: parent_buffer_key.clone(),
        },
    ];
    let values = crate::memory::chunks::versioned::read_state_keys_async(config, &keys).await?;
    let Some(current_tree_value) = values[0].as_ref() else {
        anyhow::bail!("tree '{tree_id}' does not exist");
    };
    let mut current_tree: Tree = serde_json::from_value(current_tree_value.clone())?;
    anyhow::ensure!(
        current_tree.status == crate::memory::tree::TreeStatus::Active,
        "tree '{tree_id}' is archived"
    );
    let Some(current_source_value) = values[1].as_ref() else {
        anyhow::bail!("buffer changed while sealing tree_id={tree_id} level={level}");
    };
    let mut remaining: Buffer = serde_json::from_value(current_source_value.clone())?;
    if !remaining.item_ids.starts_with(&buf.item_ids) {
        anyhow::bail!("buffer changed while sealing tree_id={tree_id} level={level}");
    }
    remaining.item_ids.drain(..buf.item_ids.len());
    remaining.token_sum = remaining.token_sum.saturating_sub(buf.token_sum).max(0);
    if remaining.item_ids.is_empty() {
        remaining.oldest_at = None;
        remaining.token_sum = 0;
    }
    let mut parent = values[2]
        .clone()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_else(|| Buffer::empty(&tree_id, target_level));
    if !parent.item_ids.contains(&summary_id) {
        parent.item_ids.push(summary_id.clone());
        parent.token_sum = parent.token_sum.saturating_add(node.token_count as i64);
        parent.oldest_at = Some(
            parent
                .oldest_at
                .map_or(time_range_start, |oldest| oldest.min(time_range_start)),
        );
    }
    let follow_up = should_seal(config, &parent).then(|| crate::memory::queue::SealPayload {
        tree_id: tree_id.clone(),
        level: target_level,
        force_now_ms: None,
    });

    if target_level > current_tree.max_level {
        current_tree.root_id = Some(summary_id.clone());
        current_tree.max_level = target_level;
    }
    current_tree.last_sealed_at = Some(now);

    let summary_changes = store::stage_summary_changes(&node, None, &signature)?;
    let entity_changes = crate::memory::score::store::stage_summary_entity_ids_async(
        config,
        &node.entities,
        &node.id,
        node.score,
        now.timestamp_millis(),
        Some(&tree_id),
    )
    .await?;
    let mut expected = vec![
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::TREES.to_owned(),
            key: tree_key.clone(),
            value: Some(current_tree_value.clone()),
        },
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: source_buffer_key.clone(),
            value: Some(current_source_value.clone()),
        },
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: parent_buffer_key.clone(),
            value: values[2].clone(),
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
            key: source_buffer_key,
            value: Some(serde_json::to_value(remaining)?),
        },
        crate::memory::chunks::versioned::StateChange {
            collection: crate::memory::chunks::collections::BUFFERS.to_owned(),
            key: parent_buffer_key,
            value: Some(serde_json::to_value(parent)?),
        },
    ];

    if level > 0 {
        let child_keys: Vec<_> = node
            .child_ids
            .iter()
            .map(|id| crate::memory::chunks::versioned::StateKey {
                collection: crate::memory::chunks::collections::SUMMARIES.to_owned(),
                key: format!("summary:{id}"),
            })
            .collect();
        let child_values =
            crate::memory::chunks::versioned::read_state_keys_async(config, &child_keys).await?;
        for (id, value) in node.child_ids.iter().zip(child_values) {
            if let Some(value) = value {
                let mut child: SummaryNode = serde_json::from_value(value.clone())?;
                if child.parent_id.is_none() {
                    child.parent_id = Some(summary_id.clone());
                    expected.push(crate::memory::chunks::versioned::StateChange {
                        collection: crate::memory::chunks::collections::SUMMARIES.to_owned(),
                        key: format!("summary:{id}"),
                        value: Some(value),
                    });
                    changes.push(crate::memory::chunks::versioned::StateChange {
                        collection: crate::memory::chunks::collections::SUMMARIES.to_owned(),
                        key: format!("summary:{id}"),
                        value: Some(serde_json::to_value(child)?),
                    });
                }
            }
        }
    }
    changes.extend(summary_changes.changes);
    changes.extend(entity_changes.changes);
    Ok(StagedSealLevel {
        result: SealLevelResult {
            summary_id,
            follow_up,
        },
        expected,
        changes,
        node,
    })
}
pub use super::document_seal::{seal_document_subtree_with_services, MERGE_LEVEL_BASE};

#[cfg(test)]
#[path = "bucket_seal_label_tests.rs"]
mod label_tests;

#[cfg(test)]
#[path = "bucket_seal_queue_tests.rs"]
mod queue_tests;
#[cfg(test)]
#[path = "bucket_seal_tests.rs"]
mod tests;
