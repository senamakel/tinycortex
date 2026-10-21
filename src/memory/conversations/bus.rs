//! Event-bus subscriber that mirrors inbound channel messages into the
//! host-backed conversation store, so non-web channels (Slack, Telegram,
//! etc.) persist alongside UI-driven threads.
//!
//! ## What was abstracted from OpenHuman
//!
//! OpenHuman's version implemented `crate::core::event_bus::EventHandler` and
//! reacted to `DomainEvent::ChannelMessage*` variants, and derived the thread
//! id via `crate::openhuman::channels::context::conversation_history_key`.
//! Those types live outside the memory engine, so this port replaces the
//! hard dependency with two small local contracts:
//!
//! - [`ChannelEvent`] — a self-contained description of an inbound/processed
//!   channel turn (the only fields the persistence path actually reads).
//! - [`ConversationEventBus`] — the trait a host implements to wire the
//!   [`ConversationPersistenceSubscriber`] into whatever real event bus it
//!   runs. The subscriber and its persistence logic stay here, fully testable
//!   without any bus implementation.
//!
//! The conversation-history-key derivation is reproduced inline so persisted
//! thread ids stay byte-identical to OpenHuman's.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock, RwLock};

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;

use super::{ConversationMessage, CreateConversationThread};

static CONVERSATION_PERSISTENCE_STORE: OnceLock<Arc<RwLock<super::ConversationStore>>> =
    OnceLock::new();
static CONVERSATION_PERSISTENCE_REGISTERED: OnceLock<()> = OnceLock::new();

/// A channel turn the persistence subscriber knows how to mirror into the
/// conversation store. Decoupled from any concrete event-bus event type so the
/// memory engine carries no dependency on the host's channel layer.
#[derive(Debug, Clone)]
pub enum ChannelEvent {
    /// An inbound message received from a channel (persisted as role `user`).
    Received {
        /// Channel wire id (e.g. `slack`, `telegram`); `telegram` is special-cased
        /// in thread-id derivation.
        channel: String,
        /// Host-side id of the source message; reused to build the persisted,
        /// dedup-keyed message id.
        message_id: String,
        /// Channel-scoped sender id; part of the conversation-history key.
        sender: String,
        /// Channel-scoped reply destination (channel/chat/DM id); part of the key.
        reply_target: String,
        /// Message body, persisted verbatim as the user turn content.
        content: String,
        /// Optional thread/timestamp anchor; a non-blank value splits the thread
        /// for non-`telegram` channels.
        thread_ts: Option<String>,
        /// Workspace this turn targets; turns for a different bound workspace are
        /// dropped (workspace-switch race).
        workspace_dir: PathBuf,
    },
    /// A processed/answered turn (the assistant response, persisted as role
    /// `assistant`).
    Processed {
        /// Channel wire id (e.g. `slack`, `telegram`); `telegram` is special-cased
        /// in thread-id derivation.
        channel: String,
        /// Host-side id of the originating message; reused to build the persisted,
        /// dedup-keyed message id.
        message_id: String,
        /// Channel-scoped sender id; part of the conversation-history key.
        sender: String,
        /// Channel-scoped reply destination (channel/chat/DM id); part of the key.
        reply_target: String,
        /// Optional thread/timestamp anchor; a non-blank value splits the thread
        /// for non-`telegram` channels.
        thread_ts: Option<String>,
        /// Assistant response body, persisted verbatim as the assistant turn content.
        response: String,
        /// Wall-clock processing latency in milliseconds, recorded in turn metadata.
        elapsed_ms: u64,
        /// Whether processing succeeded, recorded in turn metadata.
        success: bool,
        /// Workspace this turn targets; turns for a different bound workspace are
        /// dropped (workspace-switch race).
        workspace_dir: PathBuf,
    },
}

/// Handler contract for objects that react to [`ChannelEvent`]s. Mirrors the
/// shape OpenHuman's `EventHandler` exposed, minus the bus-specific routing
/// metadata.
#[async_trait]
pub trait ChannelEventHandler: Send + Sync {
    /// Human-readable handler name (diagnostics / dedup).
    fn name(&self) -> &str;
    /// React to a single channel event.
    async fn handle(&self, event: &ChannelEvent);
}

/// The host's event bus, abstracted so the memory engine does not depend on a
/// concrete bus implementation. A host wires the persistence subscriber by
/// implementing this trait over its real bus and forwarding channel events as
/// [`ChannelEvent`]s.
pub trait ConversationEventBus {
    /// Register `handler` to receive channel events. Returns `true` if the
    /// subscription was installed.
    fn subscribe_conversation_persistence(&self, handler: Arc<dyn ChannelEventHandler>) -> bool;
}

/// Register the long-lived channel conversation persistence subscriber on the
/// supplied bus.
///
/// This bridges typed channel events onto the configured host-backed
/// conversation store so non-web channels persist alongside UI threads.
/// Calling this again rebinds the single subscriber to the supplied prepared
/// config without double-subscribing.
pub fn register_conversation_persistence_subscriber(
    bus: &dyn ConversationEventBus,
    config: &crate::memory::config::MemoryConfig,
) -> anyhow::Result<()> {
    let store = super::ConversationStore::open(config)?;
    let shared =
        CONVERSATION_PERSISTENCE_STORE.get_or_init(|| Arc::new(RwLock::new(store.clone())));
    if let Ok(mut guard) = shared.write() {
        *guard = store;
    }

    if CONVERSATION_PERSISTENCE_REGISTERED.get().is_some() {
        return Ok(());
    }

    let subscriber: Arc<dyn ChannelEventHandler> = Arc::new(
        ConversationPersistenceSubscriber::new_shared(Arc::clone(shared)),
    );
    if bus.subscribe_conversation_persistence(subscriber) {
        let _ = CONVERSATION_PERSISTENCE_REGISTERED.set(());
    }
    Ok(())
}

/// Subscriber that persists channel turns into a rebindable prepared
/// conversation store.
pub struct ConversationPersistenceSubscriber {
    store: Arc<RwLock<super::ConversationStore>>,
}

impl ConversationPersistenceSubscriber {
    /// Construct a subscriber bound to an opened host configuration.
    pub fn new(config: &crate::memory::config::MemoryConfig) -> anyhow::Result<Self> {
        Ok(Self {
            store: Arc::new(RwLock::new(super::ConversationStore::open(config)?)),
        })
    }

    fn new_shared(store: Arc<RwLock<super::ConversationStore>>) -> Self {
        Self { store }
    }

    fn store_snapshot(&self) -> Result<super::ConversationStore, String> {
        self.store
            .read()
            .map(|guard| guard.clone())
            .map_err(|error| format!("conversation store binding poisoned: {error}"))
    }
}

#[async_trait]
impl ChannelEventHandler for ConversationPersistenceSubscriber {
    fn name(&self) -> &str {
        "memory::conversations::persistence"
    }

    async fn handle(&self, event: &ChannelEvent) {
        let store = match self.store_snapshot() {
            Ok(store) => store,
            Err(_) => return,
        };
        let workspace = store.workspace().to_path_buf();
        let descriptor = match event {
            ChannelEvent::Received {
                channel,
                message_id,
                sender,
                reply_target,
                content,
                thread_ts,
                workspace_dir,
            } => {
                // Drop events targeting a different workspace than the one this
                // subscriber is currently bound to (workspace-switch race).
                if *workspace_dir != workspace {
                    return;
                }
                ChannelTurnDescriptor {
                    channel,
                    message_id,
                    sender,
                    reply_target,
                    thread_ts: thread_ts.as_deref(),
                    content,
                    role: "user",
                    success: None,
                    elapsed_ms: None,
                    source: "channel_received",
                }
            }
            ChannelEvent::Processed {
                channel,
                message_id,
                sender,
                reply_target,
                thread_ts,
                response,
                elapsed_ms,
                success,
                workspace_dir,
            } => {
                if *workspace_dir != workspace {
                    return;
                }
                ChannelTurnDescriptor {
                    channel,
                    message_id,
                    sender,
                    reply_target,
                    thread_ts: thread_ts.as_deref(),
                    content: response,
                    role: "assistant",
                    success: Some(*success),
                    elapsed_ms: Some(*elapsed_ms),
                    source: "channel_processed",
                }
            }
        };
        // Persistence failures are non-fatal: a dropped channel turn must not
        // crash the bus handler. (OpenHuman logged here; this crate has no
        // logging facade, so the error is intentionally swallowed.)
        let _ = persist_channel_turn(&store, descriptor).await;
    }
}

/// Normalized view of a [`ChannelEvent::Received`] or [`ChannelEvent::Processed`]
/// carrying only the fields `persist_channel_turn` needs, so that function
/// stays event-variant-agnostic.
struct ChannelTurnDescriptor<'a> {
    /// Channel wire id (drives thread-id derivation and turn metadata).
    channel: &'a str,
    /// Host-side id of the source/originating message.
    message_id: &'a str,
    /// Channel-scoped sender id.
    sender: &'a str,
    /// Channel-scoped reply destination.
    reply_target: &'a str,
    /// Optional thread/timestamp anchor.
    thread_ts: Option<&'a str>,
    /// Turn body: the inbound message for `Received`, the response for
    /// `Processed`.
    content: &'a str,
    /// Persisted sender role: `"user"` for `Received`, `"assistant"` for
    /// `Processed`.
    role: &'a str,
    /// Whether processing succeeded; `None` for `Received` turns.
    success: Option<bool>,
    /// Processing latency in milliseconds; `None` for `Received` turns.
    elapsed_ms: Option<u64>,
    /// Diagnostic tag recorded in `extraMetadata.sourceEvent`.
    source: &'a str,
}

/// Mirror one channel turn into the workspace conversation store:
/// create-or-touch the channel thread, then append the message if it hasn't
/// already been persisted.
///
/// Idempotent per `(role, message_id)`: the owner stream CAS checks the
/// thread's current message IDs and appends only if this event has not already
/// been committed, so concurrent redelivery also becomes a no-op.
///
async fn persist_channel_turn(
    store: &super::ConversationStore,
    descriptor: ChannelTurnDescriptor<'_>,
) -> Result<(), String> {
    let collision_safe_id = persisted_channel_thread_id(
        descriptor.channel,
        descriptor.sender,
        descriptor.reply_target,
        descriptor.thread_ts,
    );
    let legacy_id = legacy_channel_thread_id(
        descriptor.channel,
        descriptor.sender,
        descriptor.reply_target,
        descriptor.thread_ts,
    );
    let thread_id = if legacy_id != collision_safe_id
        && store
            .list_threads_async()
            .await?
            .iter()
            .any(|thread| thread.id == legacy_id)
    {
        legacy_id
    } else {
        collision_safe_id
    };
    let title = channel_thread_title(
        descriptor.channel,
        descriptor.sender,
        descriptor.reply_target,
        descriptor.thread_ts,
    );
    let created_at = Utc::now().to_rfc3339();

    store
        .ensure_thread_async(CreateConversationThread {
            id: thread_id.clone(),
            title,
            created_at: created_at.clone(),
            parent_thread_id: None,
            // The store infers `general` when creating a channel thread. On
            // later touches, `None` preserves any labels the user assigned.
            labels: None,
            personality_id: None,
        })
        .await?;

    let persisted_message_id = format!("{}:{}", descriptor.role, descriptor.message_id);
    store
        .append_message_idempotent_async(
            &thread_id,
            ConversationMessage {
                id: persisted_message_id,
                content: descriptor.content.to_string(),
                message_type: "text".to_string(),
                extra_metadata: json!({
                    "scope": "channel",
                    "channel": descriptor.channel,
                    "channelSender": descriptor.sender,
                    "replyTarget": descriptor.reply_target,
                    "threadTs": descriptor.thread_ts,
                    "sourceEvent": descriptor.source,
                    "success": descriptor.success,
                    "elapsedMs": descriptor.elapsed_ms,
                    "sourceMessageId": descriptor.message_id,
                }),
                sender: descriptor.role.to_string(),
                created_at,
            },
        )
        .await?;
    Ok(())
}

/// Derive the persisted thread id for a channel turn. Mirrors OpenHuman's
/// `conversation_history_key` (Telegram does not split per `thread_ts`; other
/// channels append a `_thread:<ts>` suffix when a non-blank `thread_ts` is
/// present) and prefixes it with `channel:`.
///
fn persisted_channel_thread_id(
    channel: &str,
    sender: &str,
    reply_target: &str,
    thread_ts: Option<&str>,
) -> String {
    let mut base_key = format!("{channel}_{sender}_{reply_target}");
    if [channel, sender, reply_target]
        .iter()
        .any(|component| component.contains('_'))
    {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for component in [channel, sender, reply_target] {
            hasher.update(component.len().to_be_bytes());
            hasher.update(component.as_bytes());
        }
        let suffix = hasher.finalize()[..6]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        base_key.push_str("__");
        base_key.push_str(&suffix);
    }
    let key = if channel == "telegram" {
        base_key
    } else {
        match thread_ts.and_then(non_empty_trimmed) {
            Some(thread_ts) => format!("{base_key}_thread:{thread_ts}"),
            None => base_key,
        }
    };
    format!("channel:{key}")
}

/// Derive the pre-collision-fix id so existing underscore-containing channel
/// threads can continue receiving turns. New threads use
/// [`persisted_channel_thread_id`].
fn legacy_channel_thread_id(
    channel: &str,
    sender: &str,
    reply_target: &str,
    thread_ts: Option<&str>,
) -> String {
    let base_key = format!("{channel}_{sender}_{reply_target}");
    let key = if channel == "telegram" {
        base_key
    } else {
        match thread_ts.and_then(non_empty_trimmed) {
            Some(thread_ts) => format!("{base_key}_thread:{thread_ts}"),
            None => base_key,
        }
    };
    format!("channel:{key}")
}

/// Build the human-readable thread title shown for a channel thread.
/// Cosmetic only — never parsed back — so it does not need the collision
/// safety `persisted_channel_thread_id` requires.
fn channel_thread_title(
    channel: &str,
    sender: &str,
    reply_target: &str,
    thread_ts: Option<&str>,
) -> String {
    match thread_ts.and_then(non_empty_trimmed) {
        Some(thread_ts) if channel != "telegram" => {
            format!("{channel} · {sender} · {reply_target} · thread {thread_ts}")
        }
        _ => format!("{channel} · {sender} · {reply_target}"),
    }
}

/// Trim `value` and return `None` if the result is empty, so blank/whitespace
/// `thread_ts` values are treated the same as "absent" by thread-id and
/// title derivation.
fn non_empty_trimmed(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

#[cfg(test)]
#[path = "bus_tests.rs"]
mod tests;
