use super::*;
use chrono::TimeZone;

fn entry(id: &str, timestamp: DateTime<Utc>) -> SyncAuditEntry {
    SyncAuditEntry {
        timestamp,
        source_id: id.into(),
        source_kind: "test".into(),
        scope: "all".into(),
        items_fetched: 1,
        batches: 1,
        input_tokens: 10,
        output_tokens: 2,
        estimated_cost_usd: 0.1,
        composio_actions_called: 1,
        composio_cost_usd: 0.02,
        actual_charged_usd: None,
        duration_ms: 5,
        success: true,
        error: None,
    }
}

#[tokio::test]
async fn audit_round_trip_is_newest_first_and_skips_malformed_lines() {
    let temp = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(temp.path());
    let first = entry("first", Utc::now());
    let second = entry("second", Utc::now());
    append_audit_entry_async(&config, &first).await.unwrap();
    config
        .storage()
        .unwrap()
        .streams()
        .append(
            "tinycortex:sync-audit:v1",
            serde_json::json!({"broken": true}),
        )
        .await
        .unwrap();
    append_audit_entry_async(&config, &second).await.unwrap();
    let entries = read_audit_log_async(&config).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].source_id, "second");
    assert_eq!(entries[1].source_id, "first");
    assert!(!temp.path().join("memory_tree/sync_audit.jsonl").exists());
}

#[tokio::test]
async fn legacy_audit_import_preserves_append_order_and_is_retry_safe() {
    let temp = tempfile::tempdir().unwrap();
    let config = crate::memory::chunks::test_support::host_config(temp.path());
    let old_file_order = vec![
        entry(
            "legacy-first",
            Utc.timestamp_millis_opt(1_750_000_000_000).unwrap(),
        ),
        entry(
            "legacy-second",
            Utc.timestamp_millis_opt(1_600_000_000_000).unwrap(),
        ),
    ];

    import_legacy_audit_entries_async(&config, "sha256:source-file", &old_file_order)
        .await
        .unwrap();
    let streams = config.storage().unwrap().streams();
    let after_first_import = streams
        .len("tinycortex:sync-audit:legacy-import:v1")
        .await
        .unwrap();
    // Retrying after the host lost the successful response does not append.
    import_legacy_audit_entries_async(&config, "sha256:source-file", &old_file_order)
        .await
        .unwrap();
    assert_eq!(
        streams
            .len("tinycortex:sync-audit:legacy-import:v1")
            .await
            .unwrap(),
        after_first_import
    );

    append_audit_entry_async(
        &config,
        &entry("live", Utc.timestamp_millis_opt(1).unwrap()),
    )
    .await
    .unwrap();
    let visible = read_audit_log_async(&config).await.unwrap();
    assert_eq!(
        visible
            .iter()
            .map(|record| record.source_id.as_str())
            .collect::<Vec<_>>(),
        ["live", "legacy-second", "legacy-first"]
    );

    let changed = vec![entry(
        "different-content",
        Utc.timestamp_millis_opt(1_750_000_000_000).unwrap(),
    )];
    assert!(
        import_legacy_audit_entries_async(&config, "sha256:source-file", &changed)
            .await
            .is_err()
    );
    assert_eq!(read_audit_log_async(&config).await.unwrap(), visible);
}

#[test]
fn accumulator_uses_real_values_only_when_every_batch_reports_them() {
    let mut complete = RealCostAccumulator::new();
    complete.add_batch(100, 10, 80, 8, Some(0.01));
    complete.add_batch(100, 10, 90, 9, Some(0.02));
    assert_eq!(complete.audit_input_tokens(), 170);
    assert_eq!(complete.actual_charged_usd(), Some(0.03));
    assert!(complete.cost_is_actual());

    let mut partial = RealCostAccumulator::new();
    partial.add_batch(100, 10, 80, 8, Some(0.01));
    partial.add_batch(200, 20, 0, 0, None);
    assert_eq!(partial.audit_input_tokens(), 300);
    assert_eq!(partial.actual_charged_usd(), None);
    assert!(!partial.usage_is_real());
}
