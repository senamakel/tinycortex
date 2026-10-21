//! Unit tests for the filtered listing surface (`super::store_list`): the
//! predicates every read shares, the detail view over a page, and the
//! per-source rollup.

use super::types::{chunk_id, Chunk, Metadata, SourceKind, SourceRef, StagedChunk};
use super::{
    count_chunks_matching, list_chunk_details, list_chunks, set_chunk_embedding_for_signature,
    set_chunk_lifecycle_status, source_totals, upsert_chunks, upsert_staged_chunks,
    ListChunksQuery,
};
use crate::memory::chunks::collections::CHUNK_LIFECYCLE;
use crate::memory::chunks::versioned::{self, StateChange};
use crate::memory::config::MemoryConfig;
use crate::memory::store::entity_index::{CanonicalEntity, EntityIndex, EntityKind};
use chrono::{TimeZone, Utc};
use std::collections::HashSet;
use tempfile::TempDir;

fn test_config() -> (TempDir, MemoryConfig) {
    let tmp = TempDir::new().expect("tempdir");
    let cfg = super::test_support::host_config(tmp.path());
    (tmp, cfg)
}

fn chunk_with(
    source_kind: SourceKind,
    source_id: &str,
    seq: u32,
    ts_ms: i64,
    content: &str,
) -> Chunk {
    let ts = Utc.timestamp_millis_opt(ts_ms).unwrap();
    Chunk {
        id: chunk_id(source_kind, source_id, seq, content),
        content: content.to_string(),
        metadata: Metadata {
            source_kind,
            source_id: source_id.to_string(),
            owner: "alice@example.com".to_string(),
            timestamp: ts,
            time_range: (ts, ts),
            tags: vec!["eng".into()],
            source_ref: Some(SourceRef::new(format!("slack://{source_id}/{seq}"))),
            path_scope: None,
        },
        token_count: 12,
        seq_in_source: seq,
        created_at: ts,
        partial_message: false,
    }
}

/// Seed entity predicates through the same host-owned index used by listing.
fn index_entity(cfg: &MemoryConfig, node_id: &str, entity_id: &str, entity_kind: EntityKind) {
    let entity_index = EntityIndex::for_memory_config(cfg).unwrap();
    entity_index
        .index_entity(
            &CanonicalEntity {
                canonical_id: entity_id.to_owned(),
                kind: entity_kind,
                surface: entity_id.to_owned(),
                span_start: 0,
                span_end: entity_id.len() as u32,
                score: 0.9,
            },
            node_id,
            "leaf",
            1_700_000_000_000,
            None,
        )
        .unwrap();
}

fn ids_of(chunks: &[Chunk]) -> Vec<String> {
    chunks.iter().map(|chunk| chunk.id.clone()).collect()
}

// ── The six list predicates ─────────────────────────────────────────────────

/// Each of the six list predicates is built from one near-miss chunk that
/// differs from the wanted chunk in exactly that dimension and matches it in
/// every other. The full query must return the one chunk; dropping any single
/// predicate must let exactly its own near-miss back in — which is what proves
/// the predicate does work, rather than being a clause that matches nothing.
#[test]
fn every_list_predicate_composes_with_and() {
    let (_tmp, cfg) = test_config();

    let wanted = chunk_with(SourceKind::Chat, "slack:#eng", 0, 6_000, "quarterly plan");
    let other_kind = chunk_with(SourceKind::Email, "slack:#eng", 1, 5_000, "quarterly plan");
    let other_source = chunk_with(SourceKind::Chat, "slack:#ops", 2, 4_000, "quarterly plan");
    let other_entity = chunk_with(SourceKind::Chat, "slack:#eng", 3, 3_000, "quarterly plan");
    let other_entity_kind = chunk_with(SourceKind::Chat, "slack:#eng", 4, 2_000, "quarterly plan");
    let other_content = chunk_with(SourceKind::Chat, "slack:#eng", 5, 1_000, "annual plan");
    let unlisted_id = chunk_with(SourceKind::Chat, "slack:#eng", 6, 7_000, "quarterly plan");

    let all = [
        wanted.clone(),
        other_kind.clone(),
        other_source.clone(),
        other_entity.clone(),
        other_entity_kind.clone(),
        other_content.clone(),
        unlisted_id.clone(),
    ];
    upsert_chunks(&cfg, &all).unwrap();

    for chunk in [
        &wanted,
        &other_kind,
        &other_source,
        &other_content,
        &unlisted_id,
    ] {
        index_entity(&cfg, &chunk.id, "entity:acme", EntityKind::Organization);
    }
    // The wanted entity, of the wrong kind: `entity_ids` accepts this chunk and
    // `entity_kinds` is the only predicate that may reject it.
    index_entity(
        &cfg,
        &other_entity_kind.id,
        "entity:acme",
        EntityKind::Person,
    );
    // The wanted kind, under the wrong entity: the mirror image.
    index_entity(
        &cfg,
        &other_entity.id,
        "entity:other",
        EntityKind::Organization,
    );

    let full = ListChunksQuery {
        ids: ids_of(&[
            wanted.clone(),
            other_kind.clone(),
            other_source.clone(),
            other_entity.clone(),
            other_entity_kind.clone(),
            other_content.clone(),
        ]),
        source_kinds: vec![SourceKind::Chat],
        source_ids: vec!["slack:#eng".to_string()],
        entity_ids: vec!["entity:acme".to_string()],
        entity_kinds: vec![EntityKind::Organization.as_str().to_string()],
        content_contains: Some("quarterly".to_string()),
        ..Default::default()
    };

    let rows = list_chunks(&cfg, &full).unwrap();
    assert_eq!(ids_of(&rows), vec![wanted.id.clone()]);
    assert_eq!(count_chunks_matching(&cfg, &full).unwrap(), 1);
    let details = list_chunk_details(&cfg, &full).unwrap();
    assert_eq!(details.len(), 1);
    assert_eq!(details[0].chunk.id, wanted.id);

    let readmits = |dropped: &str, query: ListChunksQuery, readmitted: &Chunk| {
        let mut got = ids_of(&list_chunks(&cfg, &query).unwrap());
        got.sort();
        let mut expected = vec![wanted.id.clone(), readmitted.id.clone()];
        expected.sort();
        assert_eq!(
            got, expected,
            "dropping {dropped} must readmit exactly its own near-miss"
        );
        assert_eq!(
            count_chunks_matching(&cfg, &query).unwrap(),
            2,
            "the total must follow the page when {dropped} is dropped"
        );
    };

    readmits(
        "ids",
        ListChunksQuery {
            ids: Vec::new(),
            ..full.clone()
        },
        &unlisted_id,
    );
    readmits(
        "source_kinds",
        ListChunksQuery {
            source_kinds: Vec::new(),
            ..full.clone()
        },
        &other_kind,
    );
    readmits(
        "source_ids",
        ListChunksQuery {
            source_ids: Vec::new(),
            ..full.clone()
        },
        &other_source,
    );
    readmits(
        "entity_ids",
        ListChunksQuery {
            entity_ids: Vec::new(),
            ..full.clone()
        },
        &other_entity,
    );
    readmits(
        "entity_kinds",
        ListChunksQuery {
            entity_kinds: Vec::new(),
            ..full.clone()
        },
        &other_entity_kind,
    );
    readmits(
        "content_contains",
        ListChunksQuery {
            content_contains: None,
            ..full.clone()
        },
        &other_content,
    );
}

/// A chunk carrying several matching entities is still one chunk. A join would
/// return it once per index row and inflate the total with it; `EXISTS` cannot.
#[test]
fn entity_predicates_never_multiply_a_chunk() {
    let (_tmp, cfg) = test_config();
    let chunk = chunk_with(SourceKind::Chat, "slack:#eng", 0, 1_000, "three entities");
    let single = chunk_with(SourceKind::Chat, "slack:#eng", 1, 2_000, "one entity");
    upsert_chunks(&cfg, &[chunk.clone(), single.clone()]).unwrap();
    for entity in ["entity:a", "entity:b", "entity:c"] {
        index_entity(&cfg, &chunk.id, entity, EntityKind::Organization);
    }
    index_entity(&cfg, &single.id, "entity:a", EntityKind::Organization);

    let query = ListChunksQuery {
        entity_ids: vec!["entity:a".into(), "entity:b".into(), "entity:c".into()],
        entity_kinds: vec![EntityKind::Organization.as_str().into()],
        ..Default::default()
    };
    let rows = list_chunks(&cfg, &query).unwrap();
    assert_eq!(rows.len(), 2, "two chunks, not four index rows");
    assert_eq!(rows.iter().filter(|row| row.id == chunk.id).count(), 1);
    assert_eq!(count_chunks_matching(&cfg, &query).unwrap(), 2);
    assert_eq!(list_chunk_details(&cfg, &query).unwrap().len(), 2);
}

/// `_` and `%` inside `content_contains` are text the chunk must contain, not
/// wildcards. Without the `ESCAPE` clause each of these would also match its
/// look-alike.
#[test]
fn content_contains_treats_like_metacharacters_literally() {
    let (_tmp, cfg) = test_config();
    let underscore = chunk_with(SourceKind::Chat, "s", 0, 4_000, "release a_b notes");
    let lookalike = chunk_with(SourceKind::Chat, "s", 1, 3_000, "release axb notes");
    let percent = chunk_with(SourceKind::Chat, "s", 2, 2_000, "growth 100% target");
    let no_percent = chunk_with(SourceKind::Chat, "s", 3, 1_000, "growth 1000 target");
    upsert_chunks(
        &cfg,
        &[
            underscore.clone(),
            lookalike.clone(),
            percent.clone(),
            no_percent.clone(),
        ],
    )
    .unwrap();

    for (needle, expected) in [("a_b", &underscore), ("100%", &percent)] {
        let query = ListChunksQuery {
            content_contains: Some(needle.to_string()),
            ..Default::default()
        };
        assert_eq!(
            ids_of(&list_chunks(&cfg, &query).unwrap()),
            vec![expected.id.clone()],
            "{needle} must match literally"
        );
        assert_eq!(count_chunks_matching(&cfg, &query).unwrap(), 1);
    }

    // The escape character itself is escaped too, so a body containing it is
    // matched rather than being read as the start of an escape sequence.
    let backslash = chunk_with(SourceKind::Chat, "s", 4, 5_000, r"path\to\file");
    upsert_chunks(&cfg, std::slice::from_ref(&backslash)).unwrap();
    let query = ListChunksQuery {
        content_contains: Some(r"\to".to_string()),
        ..Default::default()
    };
    assert_eq!(
        ids_of(&list_chunks(&cfg, &query).unwrap()),
        vec![backslash.id.clone()]
    );
}

// ── The detail view and the source rollup ───────────────────────────────────

#[test]
fn chunk_details_report_stored_facts_without_reading_the_vault() {
    let (_tmp, cfg) = test_config();
    let staged = chunk_with(SourceKind::Chat, "slack:#eng", 0, 2_000, "staged body");
    let inline = chunk_with(SourceKind::Chat, "slack:#eng", 1, 1_000, "inline body");
    upsert_staged_chunks(
        &cfg,
        &[StagedChunk {
            chunk: staged.clone(),
            // Deliberately do not create this vault file. Listing details must
            // report the durable pointer without opening the file.
            content_path: "chat/never-written.md".into(),
            content_sha256: "test-sha".into(),
        }],
    )
    .unwrap();
    upsert_chunks(&cfg, std::slice::from_ref(&inline)).unwrap();
    set_chunk_lifecycle_status(&cfg, &staged.id, "sealed").unwrap();
    set_chunk_embedding_for_signature(&cfg, &staged.id, "test/model@3", &[1.0, 2.0, 3.0]).unwrap();

    let details = list_chunk_details(&cfg, &ListChunksQuery::default()).unwrap();
    assert_eq!(details.len(), 2);

    let staged_row = &details[0];
    assert_eq!(staged_row.chunk.id, staged.id);
    assert_eq!(
        staged_row.content_path.as_deref(),
        Some("chat/never-written.md")
    );
    assert_eq!(staged_row.lifecycle_status.as_deref(), Some("sealed"));
    assert!(staged_row.has_embedding);

    let inline_row = &details[1];
    assert_eq!(inline_row.chunk.id, inline.id);
    assert_eq!(inline_row.content_path, None);
    assert_eq!(inline_row.lifecycle_status, None);
    assert!(!inline_row.has_embedding);
}

#[test]
fn source_totals_group_by_source_and_honour_the_scope() {
    let (_tmp, cfg) = test_config();
    let mut chunks = vec![
        chunk_with(SourceKind::Chat, "slack:#eng", 0, 1_000, "a"),
        chunk_with(SourceKind::Chat, "slack:#eng", 1, 9_000, "b"),
        chunk_with(SourceKind::Email, "gmail:t-1", 0, 5_000, "c"),
    ];
    for chunk in &mut chunks {
        chunk.metadata.tags = vec!["memory_sources".to_string()];
    }
    upsert_chunks(&cfg, &chunks).unwrap();

    let totals = source_totals(&cfg, None, None).unwrap();
    assert_eq!(totals.len(), 2);
    assert_eq!(totals[0].source_kind, SourceKind::Chat);
    assert_eq!(totals[0].source_id, "slack:#eng");
    assert_eq!(totals[0].chunk_count, 2);
    assert_eq!(totals[0].last_timestamp_ms, 9_000);
    assert_eq!(totals[1].source_kind, SourceKind::Email);
    assert_eq!(totals[1].chunk_count, 1);
    assert_eq!(totals[1].last_timestamp_ms, 5_000);

    let scoped =
        source_totals(&cfg, None, Some(&HashSet::from(["gmail:t-1".to_string()]))).unwrap();
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].source_id, "gmail:t-1");

    // `limit` bounds groups, not chunks.
    assert_eq!(source_totals(&cfg, Some(1), None).unwrap().len(), 1);
}

/// A legacy/imported chunk without a lifecycle record is not implicitly
/// dropped. `exclude_dropped` filters only an explicit dropped marker.
#[test]
fn exclude_dropped_keeps_a_row_whose_lifecycle_was_never_written() {
    let (_tmp, cfg) = test_config();
    let seed = chunk_with(SourceKind::Chat, "slack:#eng", 0, 1_000, "seed");
    upsert_chunks(&cfg, std::slice::from_ref(&seed)).expect("seed");
    let bypassed = chunk_with(SourceKind::Chat, "slack:#eng", 1, 2_000, "bypassed");
    upsert_chunks(&cfg, std::slice::from_ref(&bypassed)).expect("persist chunk");
    versioned::write_state_changes(
        &cfg,
        &[StateChange {
            collection: CHUNK_LIFECYCLE.into(),
            key: bypassed.id.clone(),
            value: None,
        }],
    )
    .unwrap();
    assert!(versioned::read_state(&cfg, CHUNK_LIFECYCLE, &bypassed.id)
        .unwrap()
        .is_none());

    let query = ListChunksQuery {
        exclude_dropped: true,
        ..Default::default()
    };
    let ids: HashSet<String> = list_chunks(&cfg, &query)
        .expect("list")
        .into_iter()
        .map(|chunk| chunk.id)
        .collect();
    assert!(
        ids.contains(&bypassed.id),
        "missing lifecycle is not dropped"
    );
    assert_eq!(
        count_chunks_matching(&cfg, &query).expect("count"),
        ids.len() as u64,
        "the count must agree with the page it labels"
    );
}
