use super::*;

#[test]
fn delete_chunks_by_source_removes_safe_content_files_but_rejects_escape_paths() {
    let (_tmp, cfg) = test_config();
    let safe = sample_chunk("slack:c-1", 0, 1_700_000_000_000);
    let unsafe_chunk = sample_chunk("slack:c-1", 1, 1_700_000_001_000);
    upsert_staged_chunks(
        &cfg,
        &[
            StagedChunk {
                chunk: safe.clone(),
                content_path: "chunks/safe.md".into(),
                content_sha256: "safe-hash".into(),
            },
            StagedChunk {
                chunk: unsafe_chunk.clone(),
                content_path: "../outside.md".into(),
                content_sha256: "unsafe-hash".into(),
            },
        ],
    )
    .unwrap();

    let root = content_root(&cfg);
    let safe_rel = "chunks/safe.md";
    let safe_path = root.join(safe_rel);
    std::fs::create_dir_all(safe_path.parent().unwrap()).unwrap();
    std::fs::write(&safe_path, "safe").unwrap();

    let outside_path = root.parent().unwrap().join("outside.md");
    std::fs::write(&outside_path, "outside").unwrap();

    let deleted = delete_chunks_by_source(&cfg, SourceKind::Chat, "slack:c-1").unwrap();

    assert_eq!(deleted, 2);
    assert!(!safe_path.exists());
    assert!(outside_path.exists());
}

#[test]
fn deleting_one_chunk_keeps_content_file_referenced_by_another_chunk() {
    let (_tmp, cfg) = test_config();
    let first = sample_chunk("notion:shared-content", 0, 1_700_000_000_000);
    let second = sample_chunk("notion:shared-content", 1, 1_700_000_001_000);
    let shared_path = "chunks/notion/shared.md";
    let staged = [first.clone(), second.clone()]
        .into_iter()
        .map(|chunk| StagedChunk {
            chunk,
            content_path: shared_path.to_owned(),
            content_sha256: "shared-hash".to_owned(),
        })
        .collect::<Vec<_>>();
    upsert_staged_chunks(&cfg, &staged).unwrap();

    let absolute = content_root(&cfg).join(shared_path);
    std::fs::create_dir_all(absolute.parent().unwrap()).unwrap();
    std::fs::write(&absolute, "shared body").unwrap();

    assert_eq!(delete_chunk_by_id(&cfg, &first.id).unwrap(), 1);
    assert!(
        absolute.exists(),
        "the surviving chunk still owns this file"
    );
    assert_eq!(
        get_chunk_content_path(&cfg, &second.id).unwrap().as_deref(),
        Some(shared_path)
    );
}

#[test]
fn delete_chunks_by_source_removes_only_unreferenced_raw_files_and_gates() {
    let (_tmp, cfg) = test_config();
    let first = sample_chunk("gmail:thread-1", 0, 1_700_000_000_000);
    let second = sample_chunk("gmail:thread-2", 0, 1_700_000_001_000);
    upsert_chunks(&cfg, &[first.clone(), second.clone()]).unwrap();

    let shared = "raw/gmail/shared.md".to_string();
    let unique = "raw/gmail/unique.md".to_string();
    set_chunk_raw_refs(
        &cfg,
        &first.id,
        &[
            RawRef {
                path: shared.clone(),
                start: 0,
                end: None,
            },
            RawRef {
                path: unique.clone(),
                start: 0,
                end: None,
            },
        ],
    )
    .unwrap();
    set_chunk_raw_refs(
        &cfg,
        &second.id,
        &[RawRef {
            path: shared.clone(),
            start: 0,
            end: None,
        }],
    )
    .unwrap();
    let root = content_root(&cfg);
    for path in [&shared, &unique] {
        let absolute = root.join(path);
        std::fs::create_dir_all(absolute.parent().unwrap()).unwrap();
        std::fs::write(absolute, path).unwrap();
    }
    mark_raw_paths_ingested(&cfg, &[shared.clone(), unique.clone()]).unwrap();

    assert_eq!(
        delete_chunks_by_source(&cfg, SourceKind::Chat, "gmail:thread-1").unwrap(),
        1
    );
    assert!(root.join(&shared).exists());
    assert!(!root.join(&unique).exists());
    assert_eq!(
        filter_raw_paths_not_ingested(&cfg, &[shared.clone(), unique.clone()]).unwrap(),
        vec![unique.clone()]
    );

    assert_eq!(
        delete_chunks_by_source(&cfg, SourceKind::Chat, "gmail:thread-2").unwrap(),
        1
    );
    assert!(!root.join(&shared).exists());
    assert_eq!(
        filter_raw_paths_not_ingested(&cfg, std::slice::from_ref(&shared)).unwrap(),
        vec![shared]
    );
}

#[test]
fn corrupt_surviving_raw_refs_make_deletion_cleanup_fail_closed() {
    let (_tmp, cfg) = test_config();
    let removed = sample_chunk("gmail:removed", 0, 1_700_000_000_000);
    let corrupt = sample_chunk("gmail:corrupt", 0, 1_700_000_001_000);
    upsert_chunks(&cfg, &[removed.clone(), corrupt.clone()]).unwrap();
    let raw_path = "raw/gmail/maybe-shared.md".to_string();
    set_chunk_raw_refs(
        &cfg,
        &removed.id,
        &[RawRef {
            path: raw_path.clone(),
            start: 0,
            end: None,
        }],
    )
    .unwrap();
    super::versioned::write_state_changes(
        &cfg,
        &[super::versioned::StateChange {
            collection: super::collections::RAW_REFS.into(),
            key: corrupt.id.clone(),
            value: Some(serde_json::Value::String("corrupt".into())),
        }],
    )
    .unwrap();
    let absolute = content_root(&cfg).join(&raw_path);
    std::fs::create_dir_all(absolute.parent().unwrap()).unwrap();
    std::fs::write(&absolute, "preserve").unwrap();
    mark_raw_paths_ingested(&cfg, std::slice::from_ref(&raw_path)).unwrap();

    delete_chunks_by_source(&cfg, SourceKind::Chat, "gmail:removed").unwrap();

    assert!(absolute.exists());
    assert!(filter_raw_paths_not_ingested(&cfg, &[raw_path])
        .unwrap()
        .is_empty());
}

#[test]
fn raw_refs_round_trip_and_prefix_listing_tolerates_corrupt_rows() {
    let (_tmp, cfg) = test_config();
    let c1 = sample_chunk("gmail:thread-1", 0, 1_700_000_000_000);
    let c2 = sample_chunk("gmail:thread-2", 0, 1_700_000_001_000);
    let corrupt = sample_chunk("gmail:thread-3", 0, 1_700_000_002_000);
    upsert_chunks(&cfg, &[c1.clone(), c2.clone(), corrupt.clone()]).unwrap();

    let refs = vec![
        RawRef {
            path: "raw/gmail/thread_1/message_1.md".into(),
            start: 10,
            end: Some(42),
        },
        RawRef {
            path: "raw/gmail/thread_1/message_2.md".into(),
            start: 0,
            end: None,
        },
    ];
    set_chunk_raw_refs(&cfg, &c1.id, &refs).unwrap();
    set_chunk_raw_refs(
        &cfg,
        &c2.id,
        &[RawRef {
            path: "raw/slack/channel/message.md".into(),
            start: 0,
            end: None,
        }],
    )
    .unwrap();
    super::versioned::write_state_changes(
        &cfg,
        &[super::versioned::StateChange {
            collection: super::collections::RAW_REFS.into(),
            key: corrupt.id.clone(),
            value: Some(serde_json::Value::String("not valid json".into())),
        }],
    )
    .unwrap();

    let got = get_chunk_raw_refs(&cfg, &c1.id).unwrap().unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].path, "raw/gmail/thread_1/message_1.md");
    assert_eq!(got[0].start, 10);
    assert_eq!(got[0].end, Some(42));
    assert!(get_chunk_raw_refs(&cfg, "missing").unwrap().is_none());

    let paths = list_chunk_raw_ref_paths_with_prefix(&cfg, "raw/gmail/").unwrap();
    assert_eq!(paths.len(), 2);
    assert!(paths.contains("raw/gmail/thread_1/message_1.md"));
    assert!(paths.contains("raw/gmail/thread_1/message_2.md"));
}

#[test]
fn content_pointer_accessors_return_only_complete_non_deleted_rows() {
    let (_tmp, cfg) = test_config();
    let chunk = sample_chunk("notion:page-1", 0, 1_700_000_000_000);
    upsert_staged_chunks(
        &cfg,
        &[StagedChunk {
            chunk: chunk.clone(),
            content_path: "chunks/notion/page-1.md".into(),
            content_sha256: "abc123".into(),
        }],
    )
    .unwrap();
    super::versioned::write_state_changes(
        &cfg,
        &[
            super::versioned::StateChange {
                collection: super::collections::SUMMARY_CONTENT_POINTERS.into(),
                key: "summary:summary-live".into(),
                value: Some(serde_json::json!({
                    "summary_id": "summary-live",
                    "content_path": "summaries/live.md",
                    "content_sha256": "sum123"
                })),
            },
            super::versioned::StateChange {
                collection: super::collections::SUMMARY_CONTENT_POINTERS.into(),
                key: "summary:summary-deleted".into(),
                value: Some(serde_json::json!({
                    "summary_id": "summary-deleted",
                    "content_path": "summaries/deleted.md",
                    "content_sha256": "sum456"
                })),
            },
            super::versioned::StateChange {
                collection: super::collections::SUMMARY_CONTENT_POINTERS.into(),
                key: "summary:summary-incomplete".into(),
                value: Some(serde_json::json!({
                    "summary_id": "summary-incomplete",
                    "content_path": "summaries/incomplete.md",
                    "content_sha256": ""
                })),
            },
            super::versioned::StateChange {
                collection: super::collections::SUMMARIES.into(),
                key: "summary:summary-live".into(),
                value: Some(serde_json::json!({"deleted": false})),
            },
            super::versioned::StateChange {
                collection: super::collections::SUMMARIES.into(),
                key: "summary:summary-deleted".into(),
                value: Some(serde_json::json!({"deleted": true})),
            },
            super::versioned::StateChange {
                collection: super::collections::SUMMARIES.into(),
                key: "summary:summary-incomplete".into(),
                value: Some(serde_json::json!({"deleted": false})),
            },
        ],
    )
    .unwrap();

    assert_eq!(
        get_chunk_content_path(&cfg, &chunk.id).unwrap().as_deref(),
        Some("chunks/notion/page-1.md")
    );
    assert_eq!(
        get_chunk_content_pointers(&cfg, &chunk.id).unwrap(),
        Some(("chunks/notion/page-1.md".into(), "abc123".into()))
    );
    assert!(get_chunk_content_pointers(&cfg, "missing")
        .unwrap()
        .is_none());
    super::versioned::write_state_changes(
        &cfg,
        &[super::versioned::StateChange {
            collection: super::collections::CHUNK_CONTENT_POINTERS.into(),
            key: chunk.id.clone(),
            value: Some(serde_json::json!({
                "chunk_id": chunk.id,
                "content_path": "",
                "content_sha256": ""
            })),
        }],
    )
    .unwrap();
    assert!(get_chunk_content_path(&cfg, &chunk.id).unwrap().is_none());
    assert!(get_chunk_content_pointers(&cfg, &chunk.id)
        .unwrap()
        .is_none());
    assert_eq!(
        get_summary_content_pointers(&cfg, "summary-live").unwrap(),
        Some(("summaries/live.md".into(), "sum123".into()))
    );

    let summaries = list_summaries_with_content_path(&cfg).unwrap();
    assert_eq!(
        summaries,
        vec![(
            "summary-live".into(),
            "summaries/live.md".into(),
            "sum123".into()
        )]
    );
}

#[test]
fn raw_file_ingest_gate_is_order_preserving_and_prefix_scoped() {
    let (_tmp, cfg) = test_config();
    let paths = vec![
        "raw/gmail/a.md".to_string(),
        "raw/gmail/b.md".to_string(),
        "raw/slack/c.md".to_string(),
    ];

    assert_eq!(filter_raw_paths_not_ingested(&cfg, &paths).unwrap(), paths);
    assert_eq!(mark_raw_paths_ingested(&cfg, &paths[..2]).unwrap(), 2);
    assert_eq!(mark_raw_paths_ingested(&cfg, &paths[..2]).unwrap(), 0);
    assert_eq!(
        filter_raw_paths_not_ingested(&cfg, &paths).unwrap(),
        vec!["raw/slack/c.md".to_string()]
    );
    assert_eq!(
        count_raw_paths_ingested_with_prefix(&cfg, "raw/gmail/").unwrap(),
        2
    );
    assert_eq!(
        count_raw_paths_ingested_with_prefix(&cfg, "raw/slack/").unwrap(),
        0
    );
}

#[test]
fn staged_chunk_upsert_persists_preview_and_content_pointers() {
    let (_tmp, cfg) = test_config();
    let mut chunk = sample_chunk("notion:staged", 0, 1_700_000_000_000);
    chunk.metadata.source_kind = SourceKind::Document;
    chunk.metadata.path_scope = Some("notion:workspace".into());
    chunk.content = "x".repeat(620);
    chunk.token_count = 155;
    let staged = StagedChunk {
        chunk: chunk.clone(),
        content_path: "chunks/notion/workspace/page.md".into(),
        content_sha256: "sha-staged".into(),
    };

    let inserted = upsert_staged_chunks(&cfg, std::slice::from_ref(&staged)).unwrap();
    assert_eq!(inserted, 1);

    let got = get_chunk(&cfg, &chunk.id).unwrap().unwrap();
    assert_eq!(got.content.len(), 500);
    assert!(got.content.chars().all(|ch| ch == 'x'));
    assert_eq!(got.metadata.path_scope.as_deref(), Some("notion:workspace"));
    assert_eq!(got.token_count, 155);
    assert_eq!(
        get_chunk_content_pointers(&cfg, &chunk.id).unwrap(),
        Some((
            "chunks/notion/workspace/page.md".into(),
            "sha-staged".into()
        ))
    );
}

#[test]
fn missing_chunk_returns_none() {
    let (_tmp, cfg) = test_config();
    assert!(get_chunk(&cfg, "nonexistent").unwrap().is_none());
}

#[test]
fn empty_batch_is_noop() {
    let (_tmp, cfg) = test_config();
    assert_eq!(upsert_chunks(&cfg, &[]).unwrap(), 0);
    assert_eq!(count_chunks(&cfg).unwrap(), 0);
}

#[test]
fn schema_has_content_path_and_content_sha256_columns() {
    let (_tmp, cfg) = test_config();
    with_connection(&cfg, |conn| {
        let mut stmt = conn.prepare("PRAGMA table_info(mem_tree_chunks)")?;
        let names: Vec<String> = stmt
            .query_map(params![], |row| row.get::<_, String>(1))?
            .filter_map(|r| r.ok())
            .collect();
        assert!(
            names.iter().any(|n| n == "path_scope"),
            "missing path_scope; found: {names:?}"
        );
        assert!(
            names.iter().any(|n| n == "content_path"),
            "missing content_path; found: {names:?}"
        );
        assert!(
            names.iter().any(|n| n == "content_sha256"),
            "missing content_sha256; found: {names:?}"
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn plain_upsert_clears_staged_pointer_and_readmits_dropped_row() {
    let (_tmp, cfg) = test_config();
    let chunk = sample_chunk("notion:plain-replace", 0, 1_700_000_000_000);
    upsert_chunks(&cfg, std::slice::from_ref(&chunk)).unwrap();
    let seeded = [
        super::super::versioned::StateChange {
            collection: super::super::collections::CHUNK_CONTENT_POINTERS.into(),
            key: chunk.id.clone(),
            value: Some(serde_json::json!({
                "chunk_id": chunk.id,
                "content_path": "stale.md",
                "content_sha256": "stale"
            })),
        },
        super::super::versioned::StateChange {
            collection: super::super::collections::CHUNK_LIFECYCLE.into(),
            key: chunk.id.clone(),
            value: Some(serde_json::json!({"status":"dropped"})),
        },
    ];
    assert!(super::super::versioned::commit_if_state_matches(&cfg, &[], &[], &seeded).unwrap());

    upsert_chunks(&cfg, std::slice::from_ref(&chunk)).unwrap();
    assert!(get_chunk_content_pointers(&cfg, &chunk.id)
        .unwrap()
        .is_none());
    assert_eq!(
        super::super::get_chunk_lifecycle_status(&cfg, &chunk.id)
            .unwrap()
            .as_deref(),
        Some("admitted")
    );
}
