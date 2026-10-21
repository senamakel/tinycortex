//! Raw host-KV storage for opaque engine state.
//!
//! Unlike user-facing memory KV, this adapter never applies PII or secret
//! rewriting. Cursor paths and integration identifiers are application keys,
//! and their JSON values must round-trip exactly.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tinystoragedrivers_api::KvStore as KvPort;
use tinystoragedrivers_types::{ErrorKind, Precondition};

use crate::memory::config::MemoryConfig;

const PREFIX: &str = "tinycortex:opaque-state:v1:";
const WRITE_RETRIES: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    owner: String,
    key: String,
    value: Value,
}

#[derive(Clone)]
pub(crate) struct StateKvStore {
    port: Arc<dyn KvPort>,
}

impl StateKvStore {
    pub(crate) fn from_config(config: &MemoryConfig) -> anyhow::Result<Self> {
        Ok(Self {
            port: Arc::clone(config.storage()?.kv()),
        })
    }

    fn storage_key(owner: &str, key: &str) -> String {
        format!(
            "{PREFIX}{}:{}",
            digest(owner.as_bytes()),
            digest(key.as_bytes())
        )
    }

    fn decode(doc: Value, owner: &str, key: &str) -> anyhow::Result<Value> {
        let record: Record = serde_json::from_value(doc)?;
        anyhow::ensure!(
            record.owner == owner && record.key == key,
            "opaque state identity mismatch"
        );
        Ok(record.value)
    }

    pub(crate) async fn get(&self, owner: &str, key: &str) -> anyhow::Result<Option<Value>> {
        self.port
            .get(&Self::storage_key(owner, key))
            .await?
            .map(|record| Self::decode(record.doc, owner, key))
            .transpose()
    }

    pub(crate) async fn set(&self, owner: &str, key: &str, value: Value) -> anyhow::Result<()> {
        let address = Self::storage_key(owner, key);
        let encoded = serde_json::to_value(Record {
            owner: owner.to_owned(),
            key: key.to_owned(),
            value,
        })?;
        for _ in 0..WRITE_RETRIES {
            let current = self.port.get(&address).await?;
            let expected = match current {
                Some(record) => {
                    Self::decode(record.doc.clone(), owner, key)?;
                    Precondition::Version(record.version)
                }
                None => Precondition::Absent,
            };
            match self.port.cas(&address, encoded.clone(), expected).await {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(error.into()),
            }
        }
        anyhow::bail!("opaque state write contention retry limit reached")
    }

    pub(crate) async fn set_if_absent(
        &self,
        owner: &str,
        key: &str,
        value: Value,
    ) -> anyhow::Result<bool> {
        let address = Self::storage_key(owner, key);
        let encoded = serde_json::to_value(Record {
            owner: owner.to_owned(),
            key: key.to_owned(),
            value,
        })?;
        match self.port.cas(&address, encoded, Precondition::Absent).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == ErrorKind::Conflict => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Import an immutable legacy snapshot without replacing progressed owner
    /// state. Exact retries are no-ops; conflicting destination values fail.
    pub(crate) async fn import_entries(
        &self,
        owner: &str,
        entries: &[(String, Value)],
    ) -> anyhow::Result<()> {
        let mut unique = HashMap::with_capacity(entries.len());
        for (key, value) in entries {
            if let Some(previous) = unique.insert(key, value) {
                anyhow::ensure!(previous == value, "conflicting duplicate legacy state key");
            }
        }

        // Preflight every existing value before publishing any missing row.
        for (key, value) in entries {
            if let Some(existing) = self.get(owner, key).await? {
                anyhow::ensure!(
                    existing == *value,
                    "legacy state conflicts with existing owner value for key {key:?}"
                );
            }
        }

        for (key, value) in entries {
            if self.get(owner, key).await?.is_some() {
                continue;
            }
            if !self.set_if_absent(owner, key, value.clone()).await? {
                let current = self.get(owner, key).await?;
                anyhow::ensure!(
                    current.as_ref() == Some(value),
                    "legacy state changed during import for key {key:?}"
                );
            }
        }
        Ok(())
    }
}

fn digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}
