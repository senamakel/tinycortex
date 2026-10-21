//! Typed destination for host-owned legacy people import.
//!
//! This module never reads SQLite or the filesystem. A host reads and
//! quiesces its legacy source, converts it to these domain records, then asks
//! this module to publish the batch atomically to prepared host storage.

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{bail, Context, Result};
use chrono::{TimeZone, Utc};
use serde_json::Value;

use crate::memory::chunks::versioned::{self, StateChange};
use crate::memory::config::MemoryConfig;
use crate::memory::people::store::{
    HandleRecord, InteractionRecord, PEOPLE, PEOPLE_HANDLES, PEOPLE_INTERACTIONS,
};
use crate::memory::people::types::{Handle, Interaction, Person};

const IMPORT_RETRIES: usize = 8;

/// One legacy alias, including its source insertion time in Unix seconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyHandleAlias {
    /// The domain handle. Import canonicalizes it using the people resolver's
    /// established rules before constructing the unique lookup key.
    pub handle: Handle,
    /// Original `handle_aliases.created_at`, in Unix seconds.
    pub created_at: i64,
}

/// One legacy interaction with its stable source database row identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyInteraction {
    /// Stable source row identifier (normally the SQLite rowid rendered as a
    /// string). It is namespaced by person in the destination.
    pub source_id: String,
    /// Historical interaction values. The old writer stores `ts` at second
    /// precision; that timestamp is preserved as domain data.
    pub interaction: Interaction,
}

/// One person and all of its aliases and interactions from the old store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyPersonRecord {
    pub person: Person,
    pub aliases: Vec<LegacyHandleAlias>,
    pub interactions: Vec<LegacyInteraction>,
}

/// A complete typed people-store snapshot supplied by a host adapter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LegacyPeopleBatch {
    pub people: Vec<LegacyPersonRecord>,
}

/// Number of records found identical or inserted by an import call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LegacyPeopleImportCounts {
    pub people: usize,
    pub aliases: usize,
    pub interactions: usize,
}

/// Atomically import typed people records into prepared host storage.
///
/// The operation is safe to repeat after a lost response. Identical existing
/// records are accepted; conflicting values for the same person, canonical
/// handle, or stable interaction ID fail before publication. A captured owner
/// head prevents concurrent person or alias changes from producing dangling
/// aliases or partially imported interactions.
pub async fn import_legacy_people(
    config: &MemoryConfig,
    batch: &LegacyPeopleBatch,
) -> Result<LegacyPeopleImportCounts> {
    let normalized = normalize_and_validate(batch)?;

    for _ in 0..IMPORT_RETRIES {
        let collections = [PEOPLE, PEOPLE_HANDLES, PEOPLE_INTERACTIONS];
        let (snapshot, _, states) =
            versioned::list_chunk_snapshot_with_head_async(config, &collections).await?;
        let current = states
            .into_iter()
            .map(|(collection, rows)| (collection, rows.into_iter().collect::<HashMap<_, _>>()))
            .collect::<HashMap<_, _>>();
        let mut expected = BTreeMap::<(String, String), Option<Value>>::new();
        let mut changes = BTreeMap::<(String, String), Option<Value>>::new();
        let mut present_people = current
            .get(PEOPLE)
            .into_iter()
            .flat_map(|rows| rows.keys().cloned())
            .collect::<HashSet<_>>();
        let mut counts = LegacyPeopleImportCounts::default();

        for record in &normalized {
            let (key, value) = &record.person;
            stage_record(
                current.get(PEOPLE),
                &mut expected,
                &mut changes,
                PEOPLE,
                key,
                value.clone(),
                "person",
            )?;
            present_people.insert(key.clone());
            counts.people += 1;
        }

        for record in &normalized {
            for (key, value) in &record.aliases {
                let owner_key = value["person_id"]
                    .as_str()
                    .context("normalized alias has no person ID")?;
                anyhow::ensure!(
                    present_people.contains(owner_key),
                    "legacy alias {key} refers to missing person {owner_key}"
                );
                stage_record(
                    current.get(PEOPLE_HANDLES),
                    &mut expected,
                    &mut changes,
                    PEOPLE_HANDLES,
                    key,
                    value.clone(),
                    "handle alias",
                )?;
                counts.aliases += 1;
            }
            for (key, value) in &record.interactions {
                stage_record(
                    current.get(PEOPLE_INTERACTIONS),
                    &mut expected,
                    &mut changes,
                    PEOPLE_INTERACTIONS,
                    key,
                    value.clone(),
                    "interaction",
                )?;
                counts.interactions += 1;
            }
        }

        let expected = expected
            .into_iter()
            .map(|((collection, key), value)| StateChange {
                collection,
                key,
                value,
            })
            .collect::<Vec<_>>();
        let changes = changes
            .into_iter()
            .map(|((collection, key), value)| StateChange {
                collection,
                key,
                value,
            })
            .collect::<Vec<_>>();
        if versioned::commit_if_state_matches_at_snapshot_async(
            config,
            &snapshot,
            &expected,
            &[],
            &changes,
        )
        .await?
        {
            return Ok(counts);
        }
    }
    bail!("legacy people import conflicted with concurrent writes")
}

struct NormalizedPerson {
    person: (String, Value),
    aliases: Vec<(String, Value)>,
    interactions: Vec<(String, Value)>,
}

fn normalize_and_validate(batch: &LegacyPeopleBatch) -> Result<Vec<NormalizedPerson>> {
    let mut people = BTreeMap::<String, Value>::new();
    let mut aliases = BTreeMap::<String, Value>::new();
    let mut interactions = BTreeMap::<String, Value>::new();
    let mut alias_owners = HashMap::<String, String>::new();
    let mut owners = HashSet::new();

    for source in &batch.people {
        let mut person = source.person.clone();
        person.handles.clear();
        person.created_at = seconds_precision(person.created_at);
        person.updated_at = seconds_precision(person.updated_at);
        let person_key = person.id.to_string();
        let person_value = serde_json::to_value(person)?;
        if let Some(previous) = people.insert(person_key.clone(), person_value.clone()) {
            anyhow::ensure!(
                previous == person_value,
                "duplicate legacy person ID {person_key}"
            );
        }
        owners.insert(person_key.clone());

        for alias in &source.aliases {
            let canonical = alias.handle.canonicalize();
            let (kind, value) = canonical.as_key();
            let key = format!("{kind}:{value}");
            let record = HandleRecord {
                kind: kind.to_owned(),
                value: value.to_owned(),
                person_id: source.person.id,
                created_at: alias.created_at,
            };
            let value = serde_json::to_value(record)?;
            if let Some(owner) = alias_owners.insert(key.clone(), person_key.clone()) {
                anyhow::ensure!(
                    owner == person_key,
                    "legacy handle alias {key} is assigned to multiple people"
                );
            }
            if let Some(previous) = aliases.insert(key.clone(), value.clone()) {
                anyhow::ensure!(
                    previous == value,
                    "conflicting duplicate legacy alias {key}"
                );
            }
        }

        for interaction in &source.interactions {
            anyhow::ensure!(
                !interaction.source_id.is_empty(),
                "legacy interaction source ID cannot be empty"
            );
            anyhow::ensure!(
                interaction.interaction.person_id == source.person.id,
                "legacy interaction belongs to a different person"
            );
            let id = format!("legacy:{}", interaction.source_id);
            let key = format!("{}:{id}", source.person.id);
            let record = InteractionRecord {
                id,
                interaction: Interaction {
                    ts: seconds_precision(interaction.interaction.ts),
                    ..interaction.interaction.clone()
                },
            };
            let value = serde_json::to_value(record)?;
            if let Some(previous) = interactions.insert(key.clone(), value.clone()) {
                anyhow::ensure!(
                    previous == value,
                    "legacy interaction ID {key} has conflicting values"
                );
            }
        }
    }

    let mut records = Vec::with_capacity(people.len());
    for (person_key, person_value) in people {
        let aliases = aliases
            .iter()
            .filter(|(_, value)| value["person_id"].as_str() == Some(person_key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let interactions = interactions
            .iter()
            .filter(|(key, _)| key.starts_with(&format!("{person_key}:")))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        records.push(NormalizedPerson {
            person: (person_key, person_value),
            aliases,
            interactions,
        });
    }
    anyhow::ensure!(
        aliases.values().all(|value| {
            value["person_id"]
                .as_str()
                .is_some_and(|id| owners.contains(id))
        }),
        "legacy aliases must belong to a person in the import batch"
    );
    Ok(records)
}

fn stage_record(
    existing: Option<&HashMap<String, Value>>,
    expected: &mut BTreeMap<(String, String), Option<Value>>,
    changes: &mut BTreeMap<(String, String), Option<Value>>,
    collection: &str,
    key: &str,
    value: Value,
    label: &str,
) -> Result<()> {
    let composite = (collection.to_owned(), key.to_owned());
    match existing.and_then(|rows| rows.get(key)) {
        Some(current) => anyhow::ensure!(
            *current == value,
            "existing {label} {key} differs from the legacy source"
        ),
        None => {
            if let Some(previous) = changes.get(&composite) {
                anyhow::ensure!(
                    previous.as_ref() == Some(&value),
                    "duplicate staged {label} {key} has conflicting values"
                );
            } else {
                expected.insert(composite.clone(), None);
                changes.insert(composite, Some(value));
            }
        }
    }
    Ok(())
}

fn seconds_precision(value: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(value.timestamp(), 0)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().expect("epoch is valid"))
}

#[cfg(test)]
#[path = "legacy_import_tests.rs"]
mod tests;
