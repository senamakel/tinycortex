//! Host-backed, versioned store for people, handle aliases, and interactions.
//!
//! All logical records are committed through the memory owner's captured-head
//! compare-and-swap. A person and its initial aliases therefore become visible
//! together, even though TSD's document port exposes conditional single-record
//! writes rather than a cross-record transaction.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, RwLock};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::memory::chunks::versioned::{self, StateChange};
use crate::memory::config::MemoryConfig;
use crate::memory::people::types::{Handle, Interaction, Person, PersonId};

pub(crate) const PEOPLE: &str = "memory_people";
pub(crate) const PEOPLE_HANDLES: &str = "memory_people_handles";
pub(crate) const PEOPLE_INTERACTIONS: &str = "memory_people_interactions";

const WRITE_RETRIES: usize = 12;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HandleRecord {
    pub(crate) kind: String,
    pub(crate) value: String,
    pub(crate) person_id: PersonId,
    /// Historical alias insertion time in Unix seconds.
    pub(crate) created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct InteractionRecord {
    pub(crate) id: String,
    pub(crate) interaction: Interaction,
}

/// Process-global handle to the active workspace's configured people store.
/// Hosts bind it with [`init`] only after preparing their memory storage.
#[derive(Clone)]
struct GlobalPeopleStore {
    store: Arc<PeopleStore>,
}

static GLOBAL: OnceLock<RwLock<Option<GlobalPeopleStore>>> = OnceLock::new();

fn global_slot() -> &'static RwLock<Option<GlobalPeopleStore>> {
    GLOBAL.get_or_init(Default::default)
}

/// Create a people store bound to the supplied prepared host configuration.
///
/// The configuration must have been opened with [`MemoryConfig::open_with_storage`]
/// or otherwise have completed `prepare_storage`. This function never creates
/// a local database or selects a storage engine.
pub fn for_config(config: &MemoryConfig) -> Result<Arc<PeopleStore>> {
    Ok(Arc::new(PeopleStore::open(config)?))
}

/// Bind or rebind the process-global people store to the prepared host storage.
///
/// Every call adopts the supplied configuration. Workspace paths do not
/// identify a storage backend, so they cannot safely be used to reuse a handle.
pub fn init(config: &MemoryConfig) -> Result<Arc<PeopleStore>> {
    let mut slot = global_slot()
        .write()
        .map_err(|_| anyhow!("[people:store] global write lock poisoned"))?;
    let store = for_config(config)?;
    *slot = Some(GlobalPeopleStore {
        store: Arc::clone(&store),
    });
    Ok(store)
}

/// Return the active process-global people store after the host has bound it.
pub fn get() -> Result<Arc<PeopleStore>> {
    global_slot()
        .read()
        .map_err(|_| anyhow!("[people:store] global read lock poisoned"))?
        .as_ref()
        .map(|entry| Arc::clone(&entry.store))
        .ok_or_else(|| anyhow!("people store not initialised — host startup hasn't completed"))
}

/// A people store bound to one prepared, host-supplied memory configuration.
#[derive(Clone)]
pub struct PeopleStore {
    config: MemoryConfig,
}

impl PeopleStore {
    /// Bind to an already prepared host storage bundle.
    pub fn open(config: &MemoryConfig) -> Result<Self> {
        config
            .storage()
            .context("people store requires prepared host storage")?;
        Ok(Self {
            config: config.clone(),
        })
    }

    /// Insert a person and all currently unclaimed aliases atomically.
    ///
    /// Aliases are canonicalized before uniqueness checks. As in the previous
    /// SQLite store, a handle already owned by another person is ignored while
    /// the person row itself is still inserted.
    pub async fn insert_person(&self, person: &Person, handles: &[Handle]) -> Result<()> {
        for _ in 0..WRITE_RETRIES {
            let (expected, changes) = stage_person_insert(&self.config, person, handles).await?;
            if versioned::commit_if_state_matches_async(&self.config, &expected, &[], &changes)
                .await?
            {
                return Ok(());
            }
        }
        bail!("person insert did not commit within the retry budget")
    }

    /// Resolve an existing canonical handle or insert a person and its alias
    /// atomically. Concurrent resolvers for one handle return the winning ID.
    pub async fn resolve_or_insert_person(
        &self,
        person: &Person,
        handle: &Handle,
    ) -> Result<(PersonId, bool)> {
        let handle = handle.canonicalize();
        let (kind, value) = handle.as_key();
        let alias_key = handle_key(kind, value);
        for _ in 0..WRITE_RETRIES {
            if let Some(existing) =
                versioned::read_state_async(&self.config, PEOPLE_HANDLES, &alias_key).await?
            {
                return Ok((decode_handle(&alias_key, existing)?.person_id, false));
            }
            let person_key = person.id.to_string();
            if versioned::read_state_async(&self.config, PEOPLE, &person_key)
                .await?
                .is_some()
            {
                bail!("person {} already exists", person.id);
            }
            let mut stored_person = person.clone();
            stored_person.handles.clear();
            stored_person.created_at = seconds_precision(stored_person.created_at);
            stored_person.updated_at = seconds_precision(stored_person.updated_at);
            let expected = [
                StateChange {
                    collection: PEOPLE_HANDLES.to_owned(),
                    key: alias_key.clone(),
                    value: None,
                },
                StateChange {
                    collection: PEOPLE.to_owned(),
                    key: person_key.clone(),
                    value: None,
                },
            ];
            let changes = [
                StateChange {
                    collection: PEOPLE.to_owned(),
                    key: person_key,
                    value: Some(serde_json::to_value(stored_person)?),
                },
                StateChange {
                    collection: PEOPLE_HANDLES.to_owned(),
                    key: alias_key.clone(),
                    value: Some(serde_json::to_value(HandleRecord {
                        kind: kind.to_owned(),
                        value: value.to_owned(),
                        person_id: person.id,
                        created_at: Utc::now().timestamp(),
                    })?),
                },
            ];
            if versioned::commit_if_state_matches_async(&self.config, &expected, &[], &changes)
                .await?
            {
                return Ok((person.id, true));
            }
        }
        bail!("handle resolution conflicted with concurrent writes")
    }

    /// Attach a canonical handle alias to an existing person. An alias already
    /// claimed by any person remains an idempotent no-op, matching SQLite's
    /// `INSERT OR IGNORE` behavior.
    pub async fn add_alias(&self, person_id: PersonId, handle: Handle) -> Result<()> {
        let handle = handle.canonicalize();
        let (kind, value) = handle.as_key();
        let key = handle_key(kind, value);
        for _ in 0..WRITE_RETRIES {
            if let Some(existing) =
                versioned::read_state_async(&self.config, PEOPLE_HANDLES, &key).await?
            {
                decode_handle(&key, existing)?;
                return Ok(());
            }
            let person_key = person_id.to_string();
            let Some(person) =
                versioned::read_state_async(&self.config, PEOPLE, &person_key).await?
            else {
                bail!("person {person_id} does not exist")
            };
            let expected = [
                StateChange {
                    collection: PEOPLE.to_owned(),
                    key: person_key,
                    value: Some(person),
                },
                StateChange {
                    collection: PEOPLE_HANDLES.to_owned(),
                    key: key.clone(),
                    value: None,
                },
            ];
            let changes = [StateChange {
                collection: PEOPLE_HANDLES.to_owned(),
                key: key.clone(),
                value: Some(serde_json::to_value(HandleRecord {
                    kind: kind.to_owned(),
                    value: value.to_owned(),
                    person_id,
                    created_at: Utc::now().timestamp(),
                })?),
            }];
            if versioned::commit_if_state_matches_async(&self.config, &expected, &[], &changes)
                .await?
            {
                return Ok(());
            }
        }
        bail!("handle alias write conflicted with concurrent changes")
    }

    /// Resolve a canonicalized handle to its person ID, or return `None`.
    pub async fn lookup(&self, handle: &Handle) -> Result<Option<PersonId>> {
        let handle = handle.canonicalize();
        let (kind, value) = handle.as_key();
        let key = handle_key(kind, value);
        versioned::read_state_async(&self.config, PEOPLE_HANDLES, &key)
            .await?
            .map(|value| decode_handle(&key, value).map(|record| record.person_id))
            .transpose()
    }

    /// Load a person and all its aliases in canonical `(kind, value)` order.
    pub async fn get(&self, person_id: PersonId) -> Result<Option<Person>> {
        let key = person_id.to_string();
        let Some(value) = versioned::read_state_async(&self.config, PEOPLE, &key).await? else {
            return Ok(None);
        };
        let mut person: Person = serde_json::from_value(value)
            .with_context(|| format!("invalid people record for {person_id}"))?;
        if person.id != person_id {
            bail!("people record key does not match person ID {person_id}")
        }
        person.handles = self.handles_for(person_id).await?;
        Ok(Some(person))
    }

    /// List people ordered by optional display name, with aliases attached.
    pub async fn list(&self) -> Result<Vec<Person>> {
        let records = versioned::list_state_async(&self.config, PEOPLE).await?;
        let mut people = Vec::with_capacity(records.len());
        for (key, value) in records {
            let mut person: Person = serde_json::from_value(value)
                .with_context(|| format!("invalid people record at key {key}"))?;
            if person.id.to_string() != key {
                bail!("people record key does not match person ID {}", person.id)
            }
            person.handles = self.handles_for(person.id).await?;
            people.push(person);
        }
        people.sort_by(|left, right| left.display_name.cmp(&right.display_name));
        Ok(people)
    }

    /// Record one interaction for an existing person.
    pub async fn record_interaction(&self, interaction: Interaction) -> Result<()> {
        self.record_interaction_with_id(interaction, uuid::Uuid::new_v4().to_string())
            .await
    }

    /// Record one imported interaction under a stable source row identifier.
    /// Repeating the same import is idempotent; a reused ID with different
    /// content fails closed.
    pub async fn record_interaction_with_id(
        &self,
        interaction: Interaction,
        source_id: String,
    ) -> Result<()> {
        let owner_key = interaction.person_id.to_string();
        let interaction = Interaction {
            ts: seconds_precision(interaction.ts),
            ..interaction
        };
        let id = format!("legacy:{source_id}");
        let key = format!("{}:{id}", interaction.person_id);
        for _ in 0..WRITE_RETRIES {
            let Some(person) =
                versioned::read_state_async(&self.config, PEOPLE, &owner_key).await?
            else {
                bail!("person {} does not exist", interaction.person_id)
            };
            if let Some(existing) =
                versioned::read_state_async(&self.config, PEOPLE_INTERACTIONS, &key).await?
            {
                let existing: InteractionRecord = serde_json::from_value(existing)
                    .context("decode existing imported interaction")?;
                anyhow::ensure!(
                    existing.id == id && existing.interaction == interaction,
                    "interaction source ID was reused with different content"
                );
                return Ok(());
            }
            let expected = [StateChange {
                collection: PEOPLE.to_owned(),
                key: owner_key.clone(),
                value: Some(person),
            }];
            let changes = [StateChange {
                collection: PEOPLE_INTERACTIONS.to_owned(),
                key: key.clone(),
                value: Some(serde_json::to_value(InteractionRecord {
                    id: id.clone(),
                    interaction: interaction.clone(),
                })?),
            }];
            if versioned::commit_if_state_matches_async(&self.config, &expected, &[], &changes)
                .await?
            {
                return Ok(());
            }
        }
        bail!("interaction insert conflicted with concurrent changes")
    }

    /// Fetch interactions for a person, newest first.
    pub async fn interactions_for(&self, person_id: PersonId) -> Result<Vec<Interaction>> {
        let records = versioned::list_state_async(&self.config, PEOPLE_INTERACTIONS).await?;
        let mut interactions = Vec::new();
        for (key, value) in records {
            let record: InteractionRecord = serde_json::from_value(value)
                .with_context(|| format!("invalid interaction record at key {key}"))?;
            validate_interaction_key(&key, &record)?;
            if record.interaction.person_id != person_id {
                continue;
            }
            interactions.push(record.interaction);
        }
        interactions.sort_by(|left, right| right.ts.cmp(&left.ts));
        Ok(interactions)
    }

    /// Fetch interactions for several people, newest first per person.
    pub async fn batch_interactions_for(
        &self,
        person_ids: &[PersonId],
    ) -> Result<HashMap<PersonId, Vec<Interaction>>> {
        if person_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let requested: HashSet<_> = person_ids.iter().copied().collect();
        let records = versioned::list_state_async(&self.config, PEOPLE_INTERACTIONS).await?;
        let mut out: HashMap<PersonId, Vec<Interaction>> = HashMap::new();
        for (key, value) in records {
            let record: InteractionRecord = serde_json::from_value(value)
                .with_context(|| format!("invalid interaction record at key {key}"))?;
            validate_interaction_key(&key, &record)?;
            if requested.contains(&record.interaction.person_id) {
                out.entry(record.interaction.person_id)
                    .or_default()
                    .push(record.interaction);
            }
        }
        for interactions in out.values_mut() {
            interactions.sort_by(|left, right| right.ts.cmp(&left.ts));
        }
        Ok(out)
    }

    async fn handles_for(&self, person_id: PersonId) -> Result<Vec<Handle>> {
        let records = versioned::list_state_async(&self.config, PEOPLE_HANDLES).await?;
        let mut handles = Vec::new();
        for (key, value) in records {
            let record = decode_handle(&key, value)?;
            if record.person_id != person_id {
                continue;
            }
            let handle = match record.kind.as_str() {
                "imessage" => Handle::IMessage(record.value),
                "email" => Handle::Email(record.value),
                "display_name" => Handle::DisplayName(record.value),
                other => bail!("unknown people handle kind {other:?}"),
            };
            handles.push(handle);
        }
        handles.sort_by(|left, right| left.as_key().cmp(&right.as_key()));
        Ok(handles)
    }
}

async fn stage_person_insert(
    config: &MemoryConfig,
    person: &Person,
    handles: &[Handle],
) -> Result<(Vec<StateChange>, Vec<StateChange>)> {
    let person_key = person.id.to_string();
    if versioned::read_state_async(config, PEOPLE, &person_key)
        .await?
        .is_some()
    {
        bail!("person {} already exists", person.id)
    }
    let mut stored_person = person.clone();
    stored_person.handles.clear();
    stored_person.created_at = seconds_precision(stored_person.created_at);
    stored_person.updated_at = seconds_precision(stored_person.updated_at);
    let mut expected = vec![StateChange {
        collection: PEOPLE.to_owned(),
        key: person_key.clone(),
        value: None,
    }];
    let mut changes = vec![StateChange {
        collection: PEOPLE.to_owned(),
        key: person_key,
        value: Some(serde_json::to_value(stored_person)?),
    }];
    let mut seen = HashSet::new();
    for handle in handles.iter().map(Handle::canonicalize) {
        let (kind, value) = handle.as_key();
        let key = handle_key(kind, value);
        if !seen.insert(key.clone()) {
            continue;
        }
        if let Some(current) = versioned::read_state_async(config, PEOPLE_HANDLES, &key).await? {
            decode_handle(&key, current)?;
            continue;
        }
        expected.push(StateChange {
            collection: PEOPLE_HANDLES.to_owned(),
            key: key.clone(),
            value: None,
        });
        changes.push(StateChange {
            collection: PEOPLE_HANDLES.to_owned(),
            key,
            value: Some(serde_json::to_value(HandleRecord {
                kind: kind.to_owned(),
                value: value.to_owned(),
                person_id: person.id,
                created_at: Utc::now().timestamp(),
            })?),
        });
    }
    Ok((expected, changes))
}

fn handle_key(kind: &str, value: &str) -> String {
    format!("{kind}:{value}")
}

pub(crate) fn interaction_key(record: &InteractionRecord) -> String {
    format!("{}:{}", record.interaction.person_id, record.id)
}

fn validate_interaction_key(key: &str, record: &InteractionRecord) -> Result<()> {
    anyhow::ensure!(
        interaction_key(record) == key,
        "interaction record key does not match its body at {key}"
    );
    Ok(())
}

fn decode_handle(key: &str, value: Value) -> Result<HandleRecord> {
    let record: HandleRecord = serde_json::from_value(value)
        .with_context(|| format!("invalid handle alias record at key {key}"))?;
    let expected_key = handle_key(&record.kind, &record.value);
    if expected_key != key {
        bail!("handle alias record key does not match its body at {key}")
    }
    match record.kind.as_str() {
        "imessage" | "email" | "display_name" => Ok(record),
        other => bail!("unknown people handle kind {other:?}"),
    }
}

fn seconds_precision(value: DateTime<Utc>) -> DateTime<Utc> {
    Utc.timestamp_opt(value.timestamp(), 0)
        .single()
        .unwrap_or_else(|| {
            Utc.timestamp_opt(0, 0)
                .single()
                .expect("Unix epoch is valid")
        })
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
