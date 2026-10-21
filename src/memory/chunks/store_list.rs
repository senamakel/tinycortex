//! Filtered chunk listing and the reads that must agree with it: the count of
//! what it matched, the detail view of the same page, and the per-source
//! rollup over the same scope.

use anyhow::{Context, Result};

use super::store::CHUNK_STATUS_DROPPED;
use super::types::{Chunk, SourceKind};
use crate::memory::config::MemoryConfig;

const DEFAULT_LIST_LIMIT: usize = 100;
const MAX_LIST_LIMIT: usize = 10_000;

/// Optional filters and pagination for [`list_chunks`].
///
/// Every field is a filter and they compose with `AND`; the default matches
/// everything the store holds, bounded by the store's own row cap.
///
/// # An empty list means "unfiltered", not "match nothing"
///
/// The `Vec` fields are absent-by-default, exactly like the `Option` ones: an
/// empty `ids` does not restrict the id, it declines to. That is forced by
/// [`Default`] — a query that left every list empty would otherwise match no
/// rows at all — and it is the reading a caller deserialising a partial query
/// off the wire gets for free.
///
/// The cost is real and worth naming: a caller that *computed* a candidate set
/// and got nothing must not hand the empty set to this struct expecting an
/// empty page. It has to short-circuit and skip the query, because the query
/// will happily return everything.
#[derive(Debug, Default, Clone)]
pub struct ListChunksQuery {
    /// Exact source-kind filter.
    pub source_kind: Option<SourceKind>,
    /// Exact logical source-id filter.
    pub source_id: Option<String>,
    /// Exact owner filter.
    pub owner: Option<String>,
    /// Inclusive lower source-time bound in epoch milliseconds.
    pub since_ms: Option<i64>,
    /// Inclusive upper source-time bound in epoch milliseconds.
    pub until_ms: Option<i64>,
    /// Maximum rows, clamped to the store safety cap.
    pub limit: Option<usize>,
    /// Ordered rows to skip for internal pagination.
    pub offset: Option<usize>,
    /// Allowed memory-source identifiers.
    pub source_scope: Option<std::collections::HashSet<String>>,
    /// Exclude lifecycle-dropped chunks.
    pub exclude_dropped: bool,
    /// Exact chunk ids to keep.
    pub ids: Vec<String>,
    /// Allowed source kinds. Composes with — does not replace — `source_kind`;
    /// setting both keeps only rows satisfying each, which is empty whenever
    /// the scalar is not one of the listed kinds.
    pub source_kinds: Vec<SourceKind>,
    /// Allowed logical source ids. Composes with `source_id` on the same terms
    /// as `source_kinds` does with `source_kind`.
    pub source_ids: Vec<String>,
    /// Keep only chunks the entity index links to one of these canonical
    /// entity ids.
    pub entity_ids: Vec<String>,
    /// Keep only chunks the entity index links to an entity of one of these
    /// kinds.
    ///
    /// Independent of `entity_ids`: setting both asks for a chunk carrying
    /// *some* listed entity and *some* entity of a listed kind, not for one
    /// index row satisfying both. Each predicate therefore means exactly one
    /// thing whether or not the other is set, which is worth more than the
    /// narrower joint reading — a caller that wants the joint one already
    /// knows the kind of the ids it is asking about.
    pub entity_kinds: Vec<String>,
    /// Substring the stored `content` column must contain, matched literally
    /// — `%` and `_` in the value are text, not wildcards — and
    /// case-insensitively for ASCII to preserve the previous SQLite behavior.
    ///
    /// This searches the stored preview, which for a staged chunk holds only
    /// the first 500 characters of a body that lives in the content vault. A
    /// match is therefore proof the text is in the chunk; a miss is not proof
    /// it is absent.
    pub content_contains: Option<String>,
}

/// List chunks matching all supplied filters in deterministic newest-first
/// order. Results come from one captured host-storage commit snapshot, with
/// source allowlisting applied before the page limit.
///
/// # Errors
/// Returns an error when the host read fails or the entity index contains a
/// malformed occurrence record.
pub fn list_chunks(config: &MemoryConfig, query: &ListChunksQuery) -> Result<Vec<Chunk>> {
    let (mut chunks, _) = matching_chunks(config, query, false)?;
    let offset = query.offset.unwrap_or(0).min(chunks.len());
    let limit = normalized_limit(query.limit) as usize;
    Ok(chunks.drain(offset..).take(limit).collect())
}

fn matching_chunks(
    config: &MemoryConfig,
    query: &ListChunksQuery,
    include_details: bool,
) -> Result<(
    Vec<Chunk>,
    std::collections::HashMap<String, Vec<(String, serde_json::Value)>>,
)> {
    let mut collections = Vec::new();
    if query.exclude_dropped || include_details {
        collections.push(super::collections::CHUNK_LIFECYCLE);
    }
    if !query.entity_ids.is_empty() || !query.entity_kinds.is_empty() {
        collections.push(super::collections::ENTITY_INDEX);
    }
    if include_details {
        collections.push(super::collections::CHUNK_EMBEDDINGS);
        collections.push(super::collections::CHUNK_CONTENT_POINTERS);
    }
    let (mut chunks, states) = super::versioned::list_chunk_snapshot(config, &collections)?;
    let lifecycle = states
        .get(super::collections::CHUNK_LIFECYCLE)
        .into_iter()
        .flatten()
        .cloned()
        .collect::<std::collections::HashMap<_, _>>();
    let mut indexed_entities = std::collections::HashMap::<
        String,
        Vec<crate::memory::store::entity_index::transaction::EntityOccurrence>,
    >::new();
    if let Some(rows) = states.get(super::collections::ENTITY_INDEX) {
        for (key, value) in rows.iter().filter(|(key, _)| key.starts_with("occ:")) {
            let occurrence: crate::memory::store::entity_index::transaction::EntityOccurrence =
                serde_json::from_value(value.clone())
                    .context("decode entity-index occurrence for chunk filter")?;
            anyhow::ensure!(
                crate::memory::store::entity_index::transaction::occurrence_key(
                    &occurrence.entity_id,
                    &occurrence.node_id,
                ) == *key,
                "entity-index occurrence key does not match its record"
            );
            indexed_entities
                .entry(occurrence.node_id.clone())
                .or_default()
                .push(occurrence);
        }
    }
    let requested_entity_ids = query.entity_ids.iter().collect::<std::collections::HashSet<_>>();
    let requested_entity_kinds = query
        .entity_kinds
        .iter()
        .collect::<std::collections::HashSet<_>>();
    chunks.retain(|chunk| {
        if query
            .source_kind
            .is_some_and(|kind| kind != chunk.metadata.source_kind)
            || query
                .source_id
                .as_ref()
                .is_some_and(|id| id != &chunk.metadata.source_id)
            || query
                .owner
                .as_ref()
                .is_some_and(|owner| owner != &chunk.metadata.owner)
            || query
                .since_ms
                .is_some_and(|since| chunk.metadata.timestamp.timestamp_millis() < since)
            || query
                .until_ms
                .is_some_and(|until| chunk.metadata.timestamp.timestamp_millis() > until)
            || (!query.ids.is_empty() && !query.ids.contains(&chunk.id))
            || (!query.source_kinds.is_empty()
                && !query.source_kinds.contains(&chunk.metadata.source_kind))
            || (!query.source_ids.is_empty()
                && !query.source_ids.contains(&chunk.metadata.source_id))
        {
            return false;
        }
        let occurrences = indexed_entities.get(&chunk.id).map(Vec::as_slice).unwrap_or(&[]);
        if (!query.entity_ids.is_empty()
            && !occurrences
                .iter()
                .any(|occurrence| requested_entity_ids.contains(&occurrence.entity_id)))
            || (!query.entity_kinds.is_empty()
                && !occurrences
                    .iter()
                    .any(|occurrence| requested_entity_kinds.contains(&occurrence.entity_kind)))
        {
            return false;
        }
        if let Some(scope) = &query.source_scope {
            let is_memory_source = chunk
                .metadata
                .tags
                .iter()
                .any(|tag| tag == "memory_sources");
            let scoped = scope.iter().any(|source| {
                chunk.metadata.source_id == *source
                    || chunk
                        .metadata
                        .source_id
                        .starts_with(&format!("mem_src:{source}:"))
            });
            if is_memory_source && !scoped {
                return false;
            }
        }
        if query.exclude_dropped
            && lifecycle.get(&chunk.id).and_then(|value| value.as_str())
                == Some(CHUNK_STATUS_DROPPED)
        {
            return false;
        }
        if let Some(needle) = &query.content_contains {
            if !chunk
                .content
                .to_ascii_lowercase()
                .contains(&needle.to_ascii_lowercase())
            {
                return false;
            }
        }
        true
    });
    chunks.sort_by(|left, right| {
        right
            .metadata
            .timestamp
            .cmp(&left.metadata.timestamp)
            .then_with(|| left.seq_in_source.cmp(&right.seq_in_source))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok((chunks, states))
}

/// One chunk plus the per-chunk facts stored beside it in the same row.
///
/// # No body
///
/// The vault file named by `content_path` is deliberately not read. A list
/// view renders a page of these, and reading each body would turn one query
/// into one query plus a filesystem round trip per row, for text most callers
/// never show — [`Chunk::content`] already carries the stored preview. A
/// caller that needs a full body asks for that one chunk's body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkDetailRow {
    /// The chunk row, decoded exactly as [`list_chunks`] decodes it.
    pub chunk: Chunk,
    /// Vault path of the full body, when this chunk has one.
    ///
    /// An empty string reads back as `None`, matching
    /// [`get_chunk_content_path`](super::get_chunk_content_path): the column is
    /// NULL for an inline chunk and a path for a staged one, and an empty
    /// string is neither.
    pub content_path: Option<String>,
    /// Lifecycle state — `admitted`, `sealed`, `dropped`, …
    ///
    /// The value is optional because the lifecycle status is a sparse owner
    /// sidecar; an absent record means no explicit lifecycle status.
    ///
    /// This matters for `exclude_dropped`: chunks with no lifecycle sidecar
    /// remain visible, while an explicit `dropped` status excludes them.
    pub lifecycle_status: Option<String>,
    /// Whether an embedding vector exists for this chunk in **any** signature.
    ///
    /// Not scoped to a model on purpose — this answers "has this been embedded
    /// at all", which is the question an inspection view asks. Whether a
    /// *particular* space holds it is
    /// [`get_chunk_embedding_for_signature`](super::get_chunk_embedding_for_signature).
    pub has_embedding: bool,
}

/// The same page [`list_chunks`] returns, with the three per-chunk facts an
/// inspection view shows beside each row.
///
/// One `SELECT`, not four reads per row: `content_path`, `lifecycle_status`
/// and the embedding existence test are all answerable from the chunk row and
/// one correlated `EXISTS`, and a caller rendering a page of them out of
/// process would otherwise pay a round trip per fact per row.
///
/// Filters, order and page bounds come from the same builders [`list_chunks`]
/// uses, so switching a caller between the two views changes what it learns
/// about each row and nothing about which rows it gets.
///
/// # Errors
/// Returns an error when the host read fails or an entity occurrence is malformed.
pub fn list_chunk_details(
    config: &MemoryConfig,
    query: &ListChunksQuery,
) -> Result<Vec<ChunkDetailRow>> {
    let (chunks, states) = matching_chunks(config, query, true)?;
    let lifecycle = states
        .get(super::collections::CHUNK_LIFECYCLE)
        .into_iter()
        .flatten()
        .cloned()
        .collect::<std::collections::HashMap<_, _>>();
    let embeddings = states
        .get(super::collections::CHUNK_EMBEDDINGS)
        .into_iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    let embedded_ids = embeddings
        .into_iter()
        .filter_map(|(_, value)| value.get("chunk_id")?.as_str().map(str::to_owned))
        .collect::<std::collections::HashSet<_>>();
    let content_paths = states
        .get(super::collections::CHUNK_CONTENT_POINTERS)
        .into_iter()
        .flatten()
        .filter_map(|(key, value)| {
            let id = value.get("chunk_id")?.as_str()?;
            if id != key {
                return None;
            }
            let path = value.get("content_path")?.as_str()?;
            (!path.is_empty()).then_some((id.to_owned(), path.to_owned()))
        })
        .collect::<std::collections::HashMap<_, _>>();
    Ok(chunks
        .into_iter()
        .map(|chunk| ChunkDetailRow {
            lifecycle_status: lifecycle
                .get(&chunk.id)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            has_embedding: embedded_ids.contains(&chunk.id),
            content_path: content_paths.get(&chunk.id).cloned(),
            chunk,
        })
        .collect())
}

/// How many rows [`list_chunks`] would match for `query`, ignoring its `limit`
/// and `offset`.
///
/// The predicate is built by the same private filter builder the listing uses,
/// so the two cannot drift apart. That matters more than the saving: a total that
/// disagrees with the page beside it sends a caller paging towards rows that
/// are not there, which is worse than having no total at all.
///
/// [`count_chunks`](super::count_chunks) answers a different question — every
/// row in the table, no filters — and is not a substitute for this.
///
/// # Errors
/// Returns an error when the host read fails or an entity occurrence is malformed.
pub fn count_chunks_matching(config: &MemoryConfig, query: &ListChunksQuery) -> Result<u64> {
    Ok(matching_chunks(config, query, false)?.0.len() as u64)
}

/// What the store holds under one `(source_kind, source_id)` pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTotal {
    /// The source kind the chunks were ingested under.
    pub source_kind: SourceKind,
    /// The logical source id, verbatim as stored.
    pub source_id: String,
    /// Chunks currently held under this pair, at any lifecycle status.
    pub chunk_count: u64,
    /// The newest source time among them, in epoch milliseconds.
    pub last_timestamp_ms: i64,
}

/// Every source the store holds chunks for, most-recently-active first.
///
/// The rollup a "what is in my memory" view needs, answered by one grouped
/// query instead of a listing the caller counts itself: the counts here are
/// over *all* matching rows, where a listing would only ever see one page of
/// them.
///
/// `scope` is the same memory-source allowlist [`ListChunksQuery::source_scope`]
/// carries and is applied by the same builder, so a scoped caller's totals
/// cover exactly the chunks its scoped listing can reach. `limit` shares the
/// listing's clamp — it bounds returned *groups*, not chunks.
///
/// Ordering breaks ties down to the group key so the result is stable across
/// calls; without that a caller polling this would watch equally-recent
/// sources swap places for no reason.
///
/// # Errors
/// Returns an error when the host read fails, an entity occurrence is
/// malformed, or a stored `source_kind` is not one this build knows.
pub fn source_totals(
    config: &MemoryConfig,
    limit: Option<usize>,
    scope: Option<&std::collections::HashSet<String>>,
) -> Result<Vec<SourceTotal>> {
    let chunks = matching_chunks(
        config,
        &ListChunksQuery {
            source_scope: scope.cloned(),
            ..Default::default()
        },
        false,
    )?
    .0;
    let mut totals = std::collections::HashMap::<(SourceKind, String), SourceTotal>::new();
    for chunk in chunks {
        let key = (chunk.metadata.source_kind, chunk.metadata.source_id.clone());
        let timestamp_ms = chunk.metadata.timestamp.timestamp_millis();
        let total = totals.entry(key.clone()).or_insert_with(|| SourceTotal {
            source_kind: key.0,
            source_id: key.1,
            chunk_count: 0,
            last_timestamp_ms: timestamp_ms,
        });
        total.chunk_count += 1;
        total.last_timestamp_ms = total.last_timestamp_ms.max(timestamp_ms);
    }
    let mut totals = totals.into_values().collect::<Vec<_>>();
    totals.sort_by(|left, right| {
        right
            .last_timestamp_ms
            .cmp(&left.last_timestamp_ms)
            .then_with(|| right.chunk_count.cmp(&left.chunk_count))
            .then_with(|| left.source_kind.as_str().cmp(right.source_kind.as_str()))
            .then_with(|| left.source_id.cmp(&right.source_id))
    });
    totals.truncate(normalized_limit(limit) as usize);
    Ok(totals)
}

fn normalized_limit(requested: Option<usize>) -> i64 {
    let limit = requested
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    i64::try_from(limit).unwrap_or(MAX_LIST_LIMIT as i64)
}
