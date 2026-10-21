use super::*;

#[tokio::test]
async fn insert_list_and_lookup_round_trip() {
    let s = crate::memory::people::test_support::store();
    let now = Utc::now();
    let p = Person {
        id: PersonId::new(),
        display_name: Some("Sarah Lee".into()),
        primary_email: Some("sarah@example.com".into()),
        primary_phone: None,
        handles: vec![],
        created_at: now,
        updated_at: now,
    };
    s.insert_person(
        &p,
        &[
            Handle::Email("Sarah@Example.com".into()),
            Handle::DisplayName("Sarah Lee".into()),
        ],
    )
    .await
    .unwrap();

    let got = s
        .lookup(&Handle::Email("sarah@example.com".into()))
        .await
        .unwrap();
    assert_eq!(got, Some(p.id));

    let list = s.list().await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].handles.len(), 2);
}

#[tokio::test]
async fn aliases_store_their_creation_time_in_the_document_body() {
    let (_workspace, config) = crate::memory::people::test_support::config();
    let s = PeopleStore::open(&config).unwrap();
    let before_insert = Utc::now().timestamp();
    let now = Utc::now();
    let person = Person {
        id: PersonId::new(),
        display_name: Some("Timestamped alias".into()),
        primary_email: None,
        primary_phone: None,
        handles: vec![],
        created_at: now,
        updated_at: now,
    };
    s.insert_person(&person, &[Handle::Email("time@example.com".into())])
        .await
        .unwrap();

    let value = crate::memory::chunks::versioned::read_state_async(
        &config,
        PEOPLE_HANDLES,
        "email:time@example.com",
    )
    .await
    .unwrap()
    .unwrap();
    let created_at = value["created_at"].as_i64().unwrap();
    assert!(created_at >= before_insert && created_at <= Utc::now().timestamp());
}

#[tokio::test]
async fn interactions_round_trip() {
    let s = crate::memory::people::test_support::store();
    let now = Utc::now();
    let pid = PersonId::new();
    let p = Person {
        id: pid,
        display_name: Some("X".into()),
        primary_email: None,
        primary_phone: None,
        handles: vec![],
        created_at: now,
        updated_at: now,
    };
    s.insert_person(&p, &[]).await.unwrap();
    s.record_interaction(Interaction {
        person_id: pid,
        ts: now,
        is_outbound: true,
        length: 100,
    })
    .await
    .unwrap();
    s.record_interaction(Interaction {
        person_id: pid,
        ts: now,
        is_outbound: false,
        length: 50,
    })
    .await
    .unwrap();
    let ints = s.interactions_for(pid).await.unwrap();
    assert_eq!(ints.len(), 2);
}

#[tokio::test]
async fn stable_interaction_id_import_is_idempotent_and_rejects_reuse() {
    let s = crate::memory::people::test_support::store();
    let now = Utc::now();
    let person_id = PersonId::new();
    let person = Person {
        id: person_id,
        display_name: None,
        primary_email: None,
        primary_phone: None,
        handles: vec![],
        created_at: now,
        updated_at: now,
    };
    s.insert_person(&person, &[]).await.unwrap();
    let imported = Interaction {
        person_id,
        ts: now,
        is_outbound: true,
        length: 12,
    };

    s.record_interaction_with_id(imported.clone(), "sqlite-row:23".into())
        .await
        .unwrap();
    s.record_interaction_with_id(imported.clone(), "sqlite-row:23".into())
        .await
        .unwrap();
    assert_eq!(s.interactions_for(person_id).await.unwrap().len(), 1);

    let conflict = Interaction {
        length: 99,
        ..imported
    };
    assert!(s
        .record_interaction_with_id(conflict, "sqlite-row:23".into())
        .await
        .is_err());
    assert_eq!(s.interactions_for(person_id).await.unwrap().len(), 1);
}

#[test]
fn opening_people_requires_prepared_host_storage() {
    let config = crate::memory::config::MemoryConfig::new("workspace");

    let error = PeopleStore::open(&config).err().unwrap();

    assert!(error.to_string().contains("prepared host storage"));
}

#[tokio::test]
async fn failed_owner_commit_exposes_neither_person_nor_initial_aliases() {
    use std::sync::Arc;
    use tinystoragedrivers_api::Storage;
    use tinystoragedrivers_testkit::Failing;
    use tinystoragedrivers_types::{ErrorKind, Scope};

    let workspace = tempfile::tempdir().unwrap();
    let scope = Scope::new("test:people-head-failure").unwrap();
    let raw = tinystoragedrivers_testkit::memory_storage(&scope);
    let failing = Failing::new(raw.documents().clone());
    let faults = failing.faults.clone();
    let storage = Storage::new(
        Arc::new(failing),
        raw.streams().clone(),
        raw.kv().clone(),
        raw.blobs().clone(),
        raw.queues().clone(),
        raw.secrets().clone(),
    );
    let config = crate::memory::config::MemoryConfig::open_with_storage(workspace.path(), storage)
        .await
        .unwrap();
    let store = PeopleStore::open(&config).unwrap();
    let now = Utc::now();
    let person = Person {
        id: PersonId::new(),
        display_name: Some("Atomic person".into()),
        primary_email: None,
        primary_phone: None,
        handles: vec![],
        created_at: now,
        updated_at: now,
    };
    faults.fail_operation_nth("documents.put_with", 1, ErrorKind::Unavailable);

    assert!(store
        .insert_person(&person, &[Handle::Email("atomic@example.com".into())])
        .await
        .is_err());

    faults.clear();
    assert!(store.get(person.id).await.unwrap().is_none());
    assert_eq!(
        store
            .lookup(&Handle::Email("atomic@example.com".into()))
            .await
            .unwrap(),
        None
    );
}
