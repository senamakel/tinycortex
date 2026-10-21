use super::*;

fn person(id: PersonId, name: &str) -> Person {
    let created_at = Utc.timestamp_opt(1_700_000_000, 0).single().unwrap();
    Person {
        id,
        display_name: Some(name.to_owned()),
        primary_email: None,
        primary_phone: None,
        handles: vec![],
        created_at,
        updated_at: created_at,
    }
}

#[tokio::test]
async fn legacy_import_preserves_alias_and_interaction_history_and_retries_idempotently() {
    let (_workspace, config) = crate::memory::people::test_support::config();
    let store = crate::memory::people::store::PeopleStore::open(&config).unwrap();
    let person_id = PersonId::new();
    let interaction_time = Utc.timestamp_opt(1_600_000_000, 0).single().unwrap();
    let batch = LegacyPeopleBatch {
        people: vec![LegacyPersonRecord {
            person: person(person_id, "Imported contact"),
            aliases: vec![LegacyHandleAlias {
                handle: Handle::Email("Import@Example.com".into()),
                created_at: 1_550_000_000,
            }],
            interactions: vec![LegacyInteraction {
                source_id: "sqlite-row:42".into(),
                interaction: Interaction {
                    person_id,
                    ts: interaction_time,
                    is_outbound: true,
                    length: 73,
                },
            }],
        }],
    };

    assert_eq!(
        import_legacy_people(&config, &batch).await.unwrap(),
        LegacyPeopleImportCounts {
            people: 1,
            aliases: 1,
            interactions: 1,
        }
    );
    import_legacy_people(&config, &batch).await.unwrap();

    assert_eq!(
        store
            .lookup(&Handle::Email("import@example.com".into()))
            .await
            .unwrap(),
        Some(person_id)
    );
    let aliases =
        crate::memory::chunks::versioned::list_state_async(&config, store::PEOPLE_HANDLES)
            .await
            .unwrap();
    assert_eq!(aliases[0].1["created_at"].as_i64(), Some(1_550_000_000));
    let interactions = store.interactions_for(person_id).await.unwrap();
    assert_eq!(interactions.len(), 1);
    assert_eq!(interactions[0].ts.timestamp(), 1_600_000_000);
    let interaction_rows =
        crate::memory::chunks::versioned::list_state_async(&config, store::PEOPLE_INTERACTIONS)
            .await
            .unwrap();
    assert_eq!(interaction_rows[0].1["id"], "legacy:sqlite-row:42");
}

#[tokio::test]
async fn legacy_import_rejects_duplicate_handle_owners_before_writing_anything() {
    let (_workspace, config) = crate::memory::people::test_support::config();
    let first_id = PersonId::new();
    let second_id = PersonId::new();
    let alias = || LegacyHandleAlias {
        handle: Handle::Email("shared@example.com".into()),
        created_at: 100,
    };
    let batch = LegacyPeopleBatch {
        people: vec![
            LegacyPersonRecord {
                person: person(first_id, "First"),
                aliases: vec![alias()],
                interactions: vec![],
            },
            LegacyPersonRecord {
                person: person(second_id, "Second"),
                aliases: vec![alias()],
                interactions: vec![],
            },
        ],
    };

    assert!(import_legacy_people(&config, &batch).await.is_err());
    assert!(
        crate::memory::chunks::versioned::list_state_async(&config, store::PEOPLE)
            .await
            .unwrap()
            .is_empty()
    );
}
