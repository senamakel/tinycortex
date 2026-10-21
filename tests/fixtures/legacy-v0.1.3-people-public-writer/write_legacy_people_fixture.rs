use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use tinycortex::memory::people::store::PeopleStore;
use tinycortex::memory::people::types::{Handle, Interaction, Person, PersonId};

#[tokio::main]
async fn main() -> Result<()> {
    let root = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: write_legacy_people_fixture <output-workspace>")?;
    anyhow::ensure!(
        !root.exists() || std::fs::read_dir(&root)?.next().is_none(),
        "output workspace must be empty: {}",
        root.display()
    );
    let database = root.join("people").join("people.db");
    let store = PeopleStore::open_at(&database)?;

    let ada_id = PersonId::new();
    let ada = Person {
        id: ada_id,
        display_name: Some("Ada Lovelace".into()),
        primary_email: Some("ada@example.com".into()),
        primary_phone: Some("+15550000001".into()),
        handles: vec![],
        created_at: timestamp(1_720_000_000),
        updated_at: timestamp(1_750_000_000),
    };
    store
        .insert_person(
            &ada,
            &[
                Handle::Email("Ada@Example.com".into()),
                Handle::IMessage("+15550000001".into()),
            ],
        )
        .await?;
    store
        .add_alias(ada_id, Handle::DisplayName("Ada Lovelace".into()))
        .await?;

    let lin_id = PersonId::new();
    let lin = Person {
        id: lin_id,
        display_name: Some("Lin Chen".into()),
        primary_email: Some("lin@example.com".into()),
        primary_phone: None,
        handles: vec![],
        created_at: timestamp(1_730_000_000),
        updated_at: timestamp(1_750_000_001),
    };
    let (resolved_id, inserted) = store
        .resolve_or_insert_person(&lin, &Handle::Email("LIN@EXAMPLE.COM".into()))
        .await?;
    anyhow::ensure!(inserted && resolved_id == lin_id);
    store
        .add_alias(lin_id, Handle::IMessage("+15550000002".into()))
        .await?;

    for (person_id, seconds, outbound, length) in [
        (ada_id, 1_740_000_000, true, 128),
        (ada_id, 1_745_000_000, false, 73),
        (lin_id, 1_746_000_000, true, 51),
    ] {
        store
            .record_interaction(Interaction {
                person_id,
                ts: timestamp(seconds),
                is_outbound: outbound,
                length,
            })
            .await?;
    }

    println!("wrote two people, five aliases, and three interactions to {}", database.display());
    Ok(())
}

fn timestamp(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().unwrap()
}
