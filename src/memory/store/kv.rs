//! Global and namespace-scoped JSON key-value storage over the host KV port.
//!
//! Logical keys are encoded as SHA-256 identities under a TinyCortex prefix;
//! the stored envelope repeats the full logical identity so a digest collision
//! or corrupt/misrouted record fails closed instead of returning another
//! caller's value. Timestamps are application data, not driver metadata.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::sync::Arc;
use tinystoragedrivers_api::KvStore as KvPort;
use tinystoragedrivers_types::{Cursor, ErrorKind, Precondition, Versioned};

use crate::memory::config::MemoryConfig;
use crate::memory::store::safety;
use crate::memory::storage::Blocking;
use crate::memory::types::MemoryKvRecord;

const KEY_PREFIX: &str = "tinycortex:kv:v1:";
const PAGE_SIZE: usize = 256;
const WRITE_RETRIES: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    kind: EntryKind,
    namespace: Option<String>,
    key: String,
    value: Value,
    updated_at: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum EntryKind {
    Global,
    Namespace,
}

/// A host-backed global and namespace JSON key-value store.
///
/// The host selects and binds the driver. Construct this store from a prepared
/// [`MemoryConfig`]; TinyCortex never opens a local database or fallback store.
#[derive(Clone)]
pub struct KvStore {
    port: Arc<dyn KvPort>,
    blocking: Blocking,
}

impl std::fmt::Debug for KvStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvStore").finish_non_exhaustive()
    }
}

impl KvStore {
    /// Attach to the prepared, host-owned KV port in `config`.
    pub fn from_config(config: &MemoryConfig) -> anyhow::Result<Self> {
        Ok(Self {
            port: Arc::clone(config.storage()?.kv()),
            blocking: config.storage_blocking()?.clone(),
        })
    }

    fn now_ts() -> f64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs_f64())
            .unwrap_or(0.0)
    }

    fn sanitize_namespace(namespace: &str) -> String {
        let trimmed = namespace.trim();
        if trimmed.is_empty() {
            return crate::memory::types::GLOBAL_NAMESPACE.to_string();
        }
        trimmed
            .chars()
            .map(|character| match character {
                c if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '/') => c,
                _ => '_',
            })
            .collect()
    }

    fn normalized_global_key(key: &str) -> String {
        if safety::has_likely_pii(key) {
            let sanitized = safety::pii::redact_pii(key);
            log::info!(
                "[kv:safety] set_global auto-sanitized PII from key original_len_chars={}",
                key.chars().count()
            );
            sanitized.value
        } else {
            key.to_string()
        }
    }

    fn normalize_namespace_write(namespace: &str) -> String {
        let namespace = if safety::has_likely_pii(namespace) {
            let sanitized = safety::pii::redact_pii(namespace);
            log::info!(
                "[kv:safety] set_namespace auto-sanitized PII from namespace original_len_chars={}",
                namespace.chars().count()
            );
            sanitized.value
        } else {
            namespace.to_string()
        };
        Self::sanitize_namespace(&namespace)
    }

    fn normalized_namespace_key(key: &str) -> String {
        if safety::has_likely_pii(key) {
            let sanitized = safety::pii::redact_pii(key);
            log::info!(
                "[kv:safety] set_namespace auto-sanitized PII from key original_len_chars={}",
                key.chars().count()
            );
            sanitized.value
        } else {
            key.to_string()
        }
    }

    fn storage_key(kind: EntryKind, namespace: Option<&str>, key: &str) -> String {
        let identity = serde_json::to_vec(&(kind, namespace, key))
            .expect("serializing a tuple of strings cannot fail");
        let digest = Sha256::digest(identity);
        let mut encoded = String::with_capacity(KEY_PREFIX.len() + digest.len() * 2);
        encoded.push_str(KEY_PREFIX);
        for byte in digest {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
        }
        encoded
    }

    fn checked_entry(
        record: Versioned<Value>,
        kind: EntryKind,
        namespace: Option<&str>,
        key: &str,
    ) -> Result<Entry, String> {
        let entry: Entry = serde_json::from_value(record.doc)
            .map_err(|error| format!("decode KV entry for key '{key}': {error}"))?;
        if entry.kind != kind || entry.namespace.as_deref() != namespace || entry.key != key {
            return Err(format!("KV identity mismatch for key '{key}'"));
        }
        Ok(entry)
    }

    async fn get_entry(
        &self,
        kind: EntryKind,
        namespace: Option<&str>,
        key: &str,
    ) -> Result<Option<Entry>, String> {
        let storage_key = Self::storage_key(kind, namespace, key);
        self.port
            .get(&storage_key)
            .await
            .map_err(|error| format!("get KV key '{key}': {error}"))?
            .map(|record| Self::checked_entry(record, kind, namespace, key))
            .transpose()
    }

    async fn put_entry(
        &self,
        kind: EntryKind,
        namespace: Option<String>,
        key: String,
        value: Value,
    ) -> Result<(), String> {
        let storage_key = Self::storage_key(kind, namespace.as_deref(), &key);
        let entry = Entry {
            kind,
            namespace,
            key: key.clone(),
            value: safety::sanitize_json(&value).value,
            updated_at: Self::now_ts(),
        };
        let encoded = serde_json::to_value(entry)
            .map_err(|error| format!("encode KV key '{key}': {error}"))?;
        for _ in 0..WRITE_RETRIES {
            let current = self
                .port
                .get(&storage_key)
                .await
                .map_err(|error| format!("read before writing KV key '{key}': {error}"))?;
            if let Some(current) = current {
                Self::checked_entry(current.clone(), kind, namespace.as_deref(), &key)?;
                match self
                    .port
                    .cas(&storage_key, encoded.clone(), Precondition::Version(current.version))
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(error) if error.kind() == ErrorKind::Conflict => continue,
                    Err(error) => return Err(format!("write KV key '{key}': {error}")),
                }
            } else {
                match self
                    .port
                    .cas(&storage_key, encoded.clone(), Precondition::Absent)
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(error) if error.kind() == ErrorKind::Conflict => continue,
                    Err(error) => return Err(format!("write KV key '{key}': {error}")),
                }
            }
        }
        Err(format!("write KV key '{key}': contention retry limit reached"))
    }

    async fn get_value(
        &self,
        kind: EntryKind,
        namespace: Option<&str>,
        key: &str,
    ) -> Result<Option<Value>, String> {
        self.get_entry(kind, namespace, key)
            .await
            .map(|entry| entry.map(|entry| entry.value))
    }

    async fn delete_key(
        &self,
        kind: EntryKind,
        namespace: Option<&str>,
        key: &str,
    ) -> Result<bool, String> {
        let storage_key = Self::storage_key(kind, namespace, key);
        for _ in 0..WRITE_RETRIES {
            let Some(current) = self
                .port
                .get(&storage_key)
                .await
                .map_err(|error| format!("read before deleting KV key '{key}': {error}"))?
            else {
                return Ok(false);
            };
            Self::checked_entry(current.clone(), kind, namespace, key)?;
            match self
                .port
                .delete(&storage_key, Precondition::Version(current.version))
                .await
            {
                Ok(deleted) => return Ok(deleted),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(format!("delete KV key '{key}': {error}")),
            }
        }
        Err(format!("delete KV key '{key}': contention retry limit reached"))
    }

    async fn list_entries(&self, namespace: Option<&str>) -> Result<Vec<Entry>, String> {
        // The actual digest keys share KEY_PREFIX; list that bounded family and
        // select by the identity envelope. Drivers return stable key order.
        let mut cursor = None;
        let mut entries = Vec::new();
        loop {
            let page = self
                .port
                .list(KEY_PREFIX, cursor, PAGE_SIZE)
                .await
                .map_err(|error| format!("list KV entries: {error}"))?;
            for record in page.items {
                let entry: Entry = serde_json::from_value(record.doc)
                    .map_err(|error| format!("decode listed KV entry: {error}"))?;
                let belongs = match (entry.kind, namespace) {
                    (EntryKind::Global, None) => true,
                    (EntryKind::Namespace, Some(expected)) => {
                        entry.namespace.as_deref() == Some(expected)
                    }
                    _ => false,
                };
                if belongs {
                    let expected_key =
                        Self::storage_key(entry.kind, entry.namespace.as_deref(), &entry.key);
                    if record.id != expected_key {
                        return Err(format!("KV identity mismatch while listing key '{}'", entry.key));
                    }
                    entries.push(entry);
                }
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        entries.sort_by(|left, right| {
            right
                .updated_at
                .partial_cmp(&left.updated_at)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.key.cmp(&right.key))
        });
        Ok(entries)
    }

    /// Insert or update a global key. PII-like keys are redacted and values
    /// are sanitized before persistence.
    pub fn set_global(&self, key: &str, value: &Value) -> Result<(), String> {
        let key = Self::normalized_global_key(key);
        if safety::has_likely_secret(&key) {
            return Err("kv key cannot contain secrets".to_string());
        }
        let store = self.clone();
        self.blocking
            .run(async move { store.put_entry(EntryKind::Global, None, key, value.clone()).await })
            .map_err(|error| error.to_string())?
    }

    /// Async form of [`Self::set_global`], safe on a current-thread runtime.
    pub async fn set_global_async(&self, key: &str, value: &Value) -> Result<(), String> {
        let key = Self::normalized_global_key(key);
        if safety::has_likely_secret(&key) {
            return Err("kv key cannot contain secrets".to_string());
        }
        self.put_entry(EntryKind::Global, None, key, value.clone()).await
    }

    /// Read a global key; corrupt stored data returns an error.
    pub fn get_global(&self, key: &str) -> Result<Option<Value>, String> {
        let key = Self::normalized_global_key(key);
        let store = self.clone();
        self.blocking
            .run(async move { store.get_value(EntryKind::Global, None, &key).await })
            .map_err(|error| error.to_string())?
    }

    /// Async form of [`Self::get_global`].
    pub async fn get_global_async(&self, key: &str) -> Result<Option<Value>, String> {
        let key = Self::normalized_global_key(key);
        self.get_value(EntryKind::Global, None, &key).await
    }

    /// Insert or update a namespace-scoped key.
    pub fn set_namespace(&self, namespace: &str, key: &str, value: &Value) -> Result<(), String> {
        let (namespace, key) = normalize_write_identity(namespace, key)?;
        let store = self.clone();
        self.blocking
            .run(async move {
                store
                    .put_entry(EntryKind::Namespace, Some(namespace), key, value.clone())
                    .await
            })
            .map_err(|error| error.to_string())?
    }

    /// Async form of [`Self::set_namespace`], safe on a current-thread runtime.
    pub async fn set_namespace_async(
        &self,
        namespace: &str,
        key: &str,
        value: &Value,
    ) -> Result<(), String> {
        let (namespace, key) = normalize_write_identity(namespace, key)?;
        self.put_entry(EntryKind::Namespace, Some(namespace), key, value.clone())
            .await
    }

    /// Read a namespace-scoped key; corrupt stored data returns an error.
    pub fn get_namespace(&self, namespace: &str, key: &str) -> Result<Option<Value>, String> {
        let namespace = Self::sanitize_namespace(namespace);
        let store = self.clone();
        self.blocking
            .run(async move {
                store
                    .get_value(EntryKind::Namespace, Some(&namespace), key)
                    .await
            })
            .map_err(|error| error.to_string())?
    }

    /// Async form of [`Self::get_namespace`].
    pub async fn get_namespace_async(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<Value>, String> {
        let namespace = Self::sanitize_namespace(namespace);
        self.get_value(EntryKind::Namespace, Some(&namespace), key).await
    }

    /// Delete a global key; returns whether a record existed.
    pub fn delete_global(&self, key: &str) -> Result<bool, String> {
        let key = Self::normalized_global_key(key);
        let store = self.clone();
        self.blocking
            .run(async move { store.delete_key(EntryKind::Global, None, &key).await })
            .map_err(|error| error.to_string())?
    }

    /// Async form of [`Self::delete_global`].
    pub async fn delete_global_async(&self, key: &str) -> Result<bool, String> {
        let key = Self::normalized_global_key(key);
        self.delete_key(EntryKind::Global, None, &key).await
    }

    /// Delete a namespace key; returns whether a record existed.
    pub fn delete_namespace(&self, namespace: &str, key: &str) -> Result<bool, String> {
        let namespace = Self::sanitize_namespace(namespace);
        let store = self.clone();
        self.blocking
            .run(async move {
                store
                    .delete_key(EntryKind::Namespace, Some(&namespace), key)
                    .await
            })
            .map_err(|error| error.to_string())?
    }

    /// Async form of [`Self::delete_namespace`].
    pub async fn delete_namespace_async(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<bool, String> {
        let namespace = Self::sanitize_namespace(namespace);
        self.delete_key(EntryKind::Namespace, Some(&namespace), key)
            .await
    }

    /// List namespace keys newest-first as `{key, value, updatedAt}` objects.
    pub fn list_namespace(&self, namespace: &str) -> Result<Vec<Value>, String> {
        let namespace = Self::sanitize_namespace(namespace);
        let store = self.clone();
        self.blocking
            .run(async move { store.list_entries(Some(&namespace)).await })
            .map(|entries| entries.map(|entries| entries.into_iter().map(list_value).collect()))
            .map_err(|error| error.to_string())?
    }

    /// Async form of [`Self::list_namespace`].
    pub async fn list_namespace_async(&self, namespace: &str) -> Result<Vec<Value>, String> {
        let namespace = Self::sanitize_namespace(namespace);
        self.list_entries(Some(&namespace))
            .await
            .map(|entries| entries.into_iter().map(list_value).collect())
    }

    /// Return namespace and global records newest-first. Equal timestamps
    /// retain namespace-before-global precedence, matching the former merge.
    pub fn records_for_scope(&self, namespace: &str) -> Result<Vec<MemoryKvRecord>, String> {
        let namespace = Self::sanitize_namespace(namespace);
        let store = self.clone();
        self.blocking
            .run(async move { store.records_for_scope_async(&namespace).await })
            .map_err(|error| error.to_string())?
    }

    /// Async form of [`Self::records_for_scope`].
    pub async fn records_for_scope_async(
        &self,
        namespace: &str,
    ) -> Result<Vec<MemoryKvRecord>, String> {
        let namespace = Self::sanitize_namespace(namespace);
        let mut records = self.records_namespace_async(&namespace).await?;
        records.extend(self.records_global_async().await?);
        records.sort_by(|left, right| {
            right
                .updated_at
                .partial_cmp(&left.updated_at)
                .unwrap_or(Ordering::Equal)
        });
        Ok(records)
    }

    /// Return all namespace records newest-first.
    pub fn records_namespace(&self, namespace: &str) -> Result<Vec<MemoryKvRecord>, String> {
        let namespace = Self::sanitize_namespace(namespace);
        let store = self.clone();
        self.blocking
            .run(async move { store.records_namespace_async(&namespace).await })
            .map_err(|error| error.to_string())?
    }

    /// Async form of [`Self::records_namespace`].
    pub async fn records_namespace_async(
        &self,
        namespace: &str,
    ) -> Result<Vec<MemoryKvRecord>, String> {
        let namespace = Self::sanitize_namespace(namespace);
        self.list_entries(Some(&namespace))
            .await
            .map(|entries| entries.into_iter().map(|entry| record(entry, Some(&namespace))).collect())
    }

    /// Return all global records newest-first.
    pub fn records_global(&self) -> Result<Vec<MemoryKvRecord>, String> {
        let store = self.clone();
        self.blocking
            .run(async move { store.records_global_async().await })
            .map_err(|error| error.to_string())?
    }

    /// Async form of [`Self::records_global`].
    pub async fn records_global_async(&self) -> Result<Vec<MemoryKvRecord>, String> {
        self.list_entries(None)
            .await
            .map(|entries| entries.into_iter().map(|entry| record(entry, None)).collect())
    }
}

fn normalize_write_identity(namespace: &str, key: &str) -> Result<(String, String), String> {
    if safety::has_likely_secret(namespace) || safety::has_likely_secret(key) {
        return Err("kv namespace/key cannot contain secrets".to_string());
    }
    Ok((
        KvStore::normalize_namespace_write(namespace),
        KvStore::normalized_namespace_key(key),
    ))
}

fn list_value(entry: Entry) -> Value {
    json!({"key": entry.key, "value": entry.value, "updatedAt": entry.updated_at})
}

fn record(entry: Entry, namespace: Option<&str>) -> MemoryKvRecord {
    MemoryKvRecord {
        namespace: namespace.map(ToOwned::to_owned),
        key: entry.key,
        value: entry.value,
        updated_at: entry.updated_at,
    }
}

#[cfg(test)]
#[path = "kv_tests.rs"]
mod tests;
