use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use rusqlite::Connection;
use tinycortex::memory::config::MemoryConfig;
use tinycortex::memory::people::legacy_import::{
    import_legacy_people, LegacyHandleAlias, LegacyInteraction, LegacyPeopleBatch,
    LegacyPersonRecord,
};
use tinycortex::memory::people::store::PeopleStore;
use tinycortex::memory::people::types::{Handle, Interaction, Person, PersonId};
use tinystoragedrivers_api::Storage;
use tinystoragedrivers_types::Scope;

#[tokio::test]
async fn v013_public_writer_people_records_import_and_read_back_with_history() -> Result<()> {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/legacy-v0.1.3-people-public-writer");
    let source_path = fixture.join("memory_tree/people/people.db");
    let batch = read_legacy_people(&source_path)?;
    assert_eq!(batch.people.len(), 2);

    let workspace = tempfile::tempdir()?;
    let scope = Scope::new("test:people-legacy-import")?;
    let raw = tinystoragedrivers_testkit::memory_storage(&scope);
    let storage = Storage::new(
        raw.documents().clone(),
        raw.streams().clone(),
        raw.kv().clone(),
        raw.blobs().clone(),
        raw.queues().clone(),
        raw.secrets().clone(),
    );
    let config = MemoryConfig::open_with_storage(workspace.path(), storage).await?;

    let counts = import_legacy_people(&config, &batch).await?;
    assert_eq!(counts.people, 2);
    assert_eq!(counts.aliases, 5);
    assert_eq!(counts.interactions, 3);

    let store = PeopleStore::open(&config)?;
    let people = store.list().await?;
    assert_eq!(people.len(), 2);
    let ada = people
        .iter()
        .find(|person| person.display_name.as_deref() == Some("Ada Lovelace"))
        .context("legacy Ada record missing")?;
    assert_eq!(ada.handles.len(), 3);
    assert_eq!(ada.created_at.timestamp(), 1_720_000_000);
    assert_eq!(ada.updated_at.timestamp(), 1_750_000_000);
    assert_eq!(
        store
            .lookup(&Handle::Email("ADA@example.com".into()))
            .await?,
        Some(ada.id)
    );
    assert_eq!(
        store
            .lookup(&Handle::IMessage("+15550000001".into()))
            .await?,
        Some(ada.id)
    );
    assert_eq!(
        store
            .lookup(&Handle::DisplayName("Ada Lovelace".into()))
            .await?,
        Some(ada.id)
    );
    let interactions = store.interactions_for(ada.id).await?;
    assert_eq!(interactions.len(), 2);
    assert_eq!(interactions[0].ts.timestamp(), 1_745_000_000);
    assert_eq!(interactions[0].length, 73);
    assert!(!interactions[0].is_outbound);

    let lin = people
        .iter()
        .find(|person| person.display_name.as_deref() == Some("Lin Chen"))
        .context("legacy Lin record missing")?;
    assert_eq!(lin.handles.len(), 2);
    assert_eq!(store.interactions_for(lin.id).await?.len(), 1);

    import_legacy_people(&config, &batch).await?;
    assert_eq!(store.list().await?.len(), 2);
    assert_eq!(store.interactions_for(ada.id).await?.len(), 2);
    Ok(())
}

fn read_legacy_people(path: &Path) -> Result<LegacyPeopleBatch> {
    let connection = Connection::open(path)
        .with_context(|| format!("open legacy people fixture {}", path.display()))?;
    let mut people = HashMap::<PersonId, LegacyPersonRecord>::new();
    {
        let mut statement = connection.prepare(
            "SELECT id, display_name, primary_email, primary_phone, created_at, updated_at \
             FROM people ORDER BY id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?;
        for row in rows {
            let (id, display_name, primary_email, primary_phone, created_at, updated_at) = row?;
            let id = PersonId(id.parse()?);
            people.insert(
                id,
                LegacyPersonRecord {
                    person: Person {
                        id,
                        display_name,
                        primary_email,
                        primary_phone,
                        handles: Vec::new(),
                        created_at: timestamp(created_at),
                        updated_at: timestamp(updated_at),
                    },
                    aliases: Vec::new(),
                    interactions: Vec::new(),
                },
            );
        }
    }
    {
        let mut statement = connection.prepare(
            "SELECT kind, value, person_id, created_at FROM handle_aliases ORDER BY kind, value",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        for row in rows {
            let (kind, value, person_id, created_at) = row?;
            let person_id = PersonId(person_id.parse()?);
            let record = people
                .get_mut(&person_id)
                .with_context(|| format!("alias references missing person {person_id}"))?;
            record.aliases.push(LegacyHandleAlias {
                handle: match kind.as_str() {
                    "imessage" => Handle::IMessage(value),
                    "email" => Handle::Email(value),
                    "display_name" => Handle::DisplayName(value),
                    other => anyhow::bail!("unknown legacy handle kind {other:?}"),
                },
                created_at,
            });
        }
    }
    {
        let mut statement = connection.prepare(
            "SELECT rowid, person_id, ts, is_outbound, length FROM interactions ORDER BY rowid",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        for row in rows {
            let (row_id, person_id, ts, is_outbound, length) = row?;
            let person_id = PersonId(person_id.parse()?);
            let record = people
                .get_mut(&person_id)
                .with_context(|| format!("interaction references missing person {person_id}"))?;
            record.interactions.push(LegacyInteraction {
                source_id: row_id.to_string(),
                interaction: Interaction {
                    person_id,
                    ts: timestamp(ts),
                    is_outbound: is_outbound != 0,
                    length: u32::try_from(length)?,
                },
            });
        }
    }
    Ok(LegacyPeopleBatch {
        people: people.into_values().collect(),
    })
}

fn timestamp(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().unwrap()
}
