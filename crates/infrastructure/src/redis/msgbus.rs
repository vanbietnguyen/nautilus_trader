// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Redis-backed message bus backing for the system.
//!
//! # Architecture
//!
//! Runs background tasks on `get_runtime()` for publishing, stream reading,
//! and heartbeats. Messages are admitted to a pending queue shared with the
//! publish task, which writes them to Redis streams. Each background task
//! owns its own Redis connection created on the Nautilus runtime.
//!
//! Admission never waits on Redis or on the publish task's housekeeping, which holds the pending
//! messages for a bounded step at a time, so a Redis failure never holds up a publisher. What is
//! pending is bounded by count at admission and by age from admission, or for an entry the message
//! bus numbered from the reservation of its number, whether or not the publish task runs, and
//! beyond either bound the oldest messages not yet sent are dropped; their numbers show the hole.
//! The task retries a failed connect or write within a second, waits as long after a batch Redis
//! refuses, on a fresh connection when the server cannot serve now, never ends on a write error,
//! logs transport and availability trouble at warning level, with one record at each change of the
//! stream between connected and degraded (a write marks it connected only when no message was
//! dropped at admission since the last write), and logs a broken invariant at error level once per
//! change (see `publish_messages`).
//!
//! The stream task reads its streams the same way: it connects again within a second after a
//! failed connect or read, never ends on one, logs transport and availability trouble at warning
//! level with one record at each change between connected and degraded, logs a broken invariant at
//! error level once per change and reads again once a second, and goes on after the last entry it
//! sent, so no entry is delivered twice (see [`stream_messages_from`]). The backing starts its
//! stream task again within a second if it ever ends without the stop, after the same last
//! entries and on the same channel, and starts its reads at the time the task first started.
//! [`MessageBusBacking::stop_read`] stops the stream task alone: publications are still written.
//!
//! Handles are stored as `Option<JoinHandle>` for idempotent shutdown via
//! `close_async()`, which waits for the tasks together for at most 5 seconds from its call, the
//! publish task writing what is pending meanwhile, aborts any task still running, and logs once
//! how many entries the backing wrote, discarded and left uncertain. The synchronous `close()` uses
//! `block_in_place` to bridge into the async shutdown path and must be called from outside any
//! `current_thread` Tokio runtime.
//!
//! # Stream entries
//!
//! Each message is one stream entry, written with `XADD` on the key `<stream key>` or, with
//! `stream_per_topic`, `<stream key>:<topic>`; [`stream_fields`](super::stream_fields) lists its
//! fields. Every entry the message bus publishes through the backing carries its source:
//! `publisher_id` (the trader ID), `instance_id` (the instance ID the backing was made with, one
//! per program start) and `sequence`. The number counts from 1 per actual key, so two topics
//! written on one key share one count and two keys do not, and a new backing starts again at 1
//! under its own instance ID. It is reserved through [`MessageBusBacking::reserve_source`] before
//! the message bus encodes the message, so a publication that is not encoded leaves a gap in the
//! numbers. An entry keeps its number on every retry of its batch. Numbers that run out are
//! refused, never wrapped. The message bus passes the numbers of one key to the backing in the
//! order it reserved them: it refuses, before numbering it, a publication made while it encodes
//! another for this backing. A heartbeat entry is numbered on its own key,
//! `<stream key>:health:heartbeat`, before its payload is made: a backing with heartbeats and one
//! key for all topics is refused at construction, because the heartbeat task numbers on its own
//! thread, and a heartbeat numbered while the message bus encodes an entry of that key could reach
//! the publish task before it.
//!
//! The read side fills [`BusMessage::source`] from these fields, with the key the entry was read
//! from as its stream. An entry without them reads as having no source.

use std::{
    borrow::Cow,
    cell::Cell,
    collections::{HashMap, VecDeque},
    fmt::{Debug, Display},
    str::FromStr,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use bytes::Bytes;
use futures::{future::BoxFuture, stream::Stream};
use jiff::{Timestamp, tz::Offset};
use nautilus_common::{
    enums::SerializationEncoding,
    live::get_runtime,
    logging::{log_task_error, log_task_started, log_task_stopped},
    msgbus::{
        BusMessage, BusMessageSource, BusPayloadType, MessageBusBacking, MessageBusBackingFactory,
        MessageBusConfig, switchboard::CLOSE_TOPIC,
    },
};
use nautilus_core::{
    UUID4, UnixNanos,
    time::{duration_since_unix_epoch, get_atomic_clock_realtime},
};
use nautilus_cryptography::providers::install_cryptographic_provider;
use nautilus_model::identifiers::TraderId;
use redis::{AsyncCommands, RetryMethod, aio::ConnectionManager, streams};
use serde::{Deserialize, Serialize};
use streams::StreamReadOptions;
use ustr::Ustr;

use super::{
    REDIS_MINID, REDIS_XTRIM, await_handle,
    stream_fields::{
        INSTANCE_ID_FIELD, PAYLOAD_KIND_FIELD, PAYLOAD_KIND_TYPED, PUBLISHER_ID_FIELD,
        SEQUENCE_FIELD, bus_message_fields,
    },
};
use crate::redis::{RedisConnectionConfig, create_redis_connection, get_stream_key};

const MSGBUS_PUBLISH: &str = "msgbus-publish";
const MSGBUS_STREAM: &str = "msgbus-stream";
const MSGBUS_HEARTBEAT: &str = "msgbus-heartbeat";
const HEARTBEAT_TOPIC: &str = "health:heartbeat";
const TRIM_BUFFER_SECS: u64 = 60;
const PUBLISH_BATCH_SIZE: usize = 1_000;

/// Most entries one stream read takes from each stream (the `COUNT` of each `XREAD`).
///
/// A backlog read from [`StreamStart::Beginning`] or an old [`StreamStart::After`] ID then arrives
/// over several replies, so the bounded stream channel holds back the reads instead of one reply
/// carrying the whole retained backlog. The value follows [`PUBLISH_BATCH_SIZE`] in this file, the
/// most messages one publish write sends.
const STREAM_READ_BATCH_SIZE: usize = 1_000;

/// Longest one stream read asks Redis to wait for new entries (the `BLOCK` of each `XREAD`).
const STREAM_READ_BLOCK: Duration = Duration::from_millis(100);

/// Longest one stream read waits for its reply: its blocking time plus [`MAX_PUBLISH_ATTEMPT`], so a
/// healthy read, answered when its blocking time ends, is never cut short.
const MAX_STREAM_READ: Duration = STREAM_READ_BLOCK.saturating_add(MAX_PUBLISH_ATTEMPT);

type RedisStreamBulk = Vec<HashMap<String, Vec<HashMap<String, redis::Value>>>>;

/// Configuration for a Redis-backed message bus backing.
///
/// Redis 6.2 or higher is required for correct operation.
#[cfg_attr(
    feature = "python",
    expect(
        clippy::unsafe_derive_deserialize,
        reason = "config deserializes plain fields; unsafe methods come from generated PyO3 integration"
    )
)]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.infrastructure", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.infrastructure")
)]
pub struct RedisMessageBusConfig {
    /// The Redis host address. If `None`, `127.0.0.1` is used.
    pub host: Option<String>,
    /// The Redis port. If `None`, `6379` is used.
    pub port: Option<u16>,
    /// The Redis account username.
    pub username: Option<String>,
    /// The Redis account password.
    pub password: Option<String>,
    /// If Redis should use an SSL-enabled connection.
    pub ssl: bool,
    /// The timeout (in seconds) to wait for a new connection.
    pub connection_timeout: u16,
    /// The timeout (in seconds) to wait for a response.
    pub response_timeout: u16,
    /// The number of retry attempts with exponential backoff for connection attempts.
    pub number_of_retries: usize,
    /// The base value for exponential backoff calculation.
    pub exponent_base: u64,
    /// The maximum delay between retry attempts (in seconds).
    pub max_delay: u64,
    /// The multiplication factor for retry delay calculation.
    pub factor: u64,
}

impl Debug for RedisMessageBusConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted = self.password.as_ref().map(|_| "***");
        f.debug_struct(stringify!(RedisMessageBusConfig))
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &redacted)
            .field("ssl", &self.ssl)
            .field("connection_timeout", &self.connection_timeout)
            .field("response_timeout", &self.response_timeout)
            .field("number_of_retries", &self.number_of_retries)
            .field("exponent_base", &self.exponent_base)
            .field("max_delay", &self.max_delay)
            .field("factor", &self.factor)
            .finish()
    }
}

impl Default for RedisMessageBusConfig {
    fn default() -> Self {
        Self {
            host: None,
            port: None,
            username: None,
            password: None,
            ssl: false,
            connection_timeout: 20,
            response_timeout: 20,
            number_of_retries: 100,
            exponent_base: 2,
            max_delay: 1000,
            factor: 2,
        }
    }
}

impl RedisConnectionConfig for RedisMessageBusConfig {
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    fn port(&self) -> Option<u16> {
        self.port
    }

    fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    fn ssl(&self) -> bool {
        self.ssl
    }

    fn connection_timeout(&self) -> u16 {
        self.connection_timeout
    }

    fn response_timeout(&self) -> u16 {
        self.response_timeout
    }

    fn number_of_retries(&self) -> usize {
        self.number_of_retries
    }

    fn exponent_base(&self) -> u64 {
        self.exponent_base
    }

    fn max_delay(&self) -> u64 {
        self.max_delay
    }

    fn factor(&self) -> u64 {
        self.factor
    }
}

impl MessageBusBackingFactory for RedisMessageBusConfig {
    fn create(
        &self,
        trader_id: TraderId,
        instance_id: UUID4,
        config: MessageBusConfig,
    ) -> anyhow::Result<Box<dyn MessageBusBacking>> {
        Ok(Box::new(RedisMessageBusBacking::new(
            trader_id,
            instance_id,
            config,
            self.clone(),
        )?))
    }
}

/// Factory for constructing Redis message bus backings.
#[derive(Debug, Clone)]
pub struct RedisMessageBusFactory {
    config: RedisMessageBusConfig,
}

impl RedisMessageBusFactory {
    /// Creates a new [`RedisMessageBusFactory`] from the given Redis configuration.
    #[must_use]
    pub const fn new(config: RedisMessageBusConfig) -> Self {
        Self { config }
    }
}

impl MessageBusBackingFactory for RedisMessageBusFactory {
    fn create(
        &self,
        trader_id: TraderId,
        instance_id: UUID4,
        config: MessageBusConfig,
    ) -> anyhow::Result<Box<dyn MessageBusBacking>> {
        self.config.create(trader_id, instance_id, config)
    }
}

pub struct RedisMessageBusBacking {
    /// The trader ID for this message bus backing.
    pub trader_id: TraderId,
    /// The instance ID for this message bus backing.
    pub instance_id: UUID4,
    pub_tx: PublishSender,
    pub_handle: Option<tokio::task::JoinHandle<()>>,
    stream_rx: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
    stream_handle: Option<tokio::task::JoinHandle<()>>,
    /// Ends (`recv` returns `None`) once the stream task and every read task it started have
    /// ended.
    read_tasks: Option<tokio::sync::mpsc::Receiver<()>>,
    stream_signal: Arc<AtomicBool>,
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
    heartbeat_signal: Arc<AtomicBool>,
    sources: StreamSources,
}

impl Debug for RedisMessageBusBacking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RedisMessageBusBacking))
            .field("trader_id", &self.trader_id)
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

impl RedisMessageBusBacking {
    /// Creates a new [`RedisMessageBusBacking`] instance for the given `trader_id`, `instance_id`, and `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The heartbeat interval is configured as zero seconds.
    /// - Heartbeats are configured with one stream for all topics (`stream_per_topic` unset).
    pub fn new(
        trader_id: TraderId,
        instance_id: UUID4,
        config: MessageBusConfig,
        backing: RedisMessageBusConfig,
    ) -> anyhow::Result<Self> {
        install_cryptographic_provider();

        if config.heartbeat_interval_secs == Some(0) {
            anyhow::bail!("heartbeat_interval_secs must be greater than 0");
        }

        // A heartbeat numbered while the message bus encodes an entry of a shared key could reach
        // the stream before that entry
        if config.heartbeat_interval_secs.is_some() && !config.stream_per_topic {
            anyhow::bail!(
                "heartbeat_interval_secs needs stream_per_topic: on one stream for all topics a heartbeat can be written before an entry numbered ahead of it; set stream_per_topic, or unset heartbeat_interval_secs"
            );
        }

        let external_streams = config.external_streams.clone().unwrap_or_default();
        let heartbeat_interval_secs = config.heartbeat_interval_secs;
        let publish = backing.clone();
        let sources = StreamSources::new(trader_id, instance_id, &config);

        let (pub_tx, pub_rx) = publish_channel();

        // Create publish task (start the runtime here for now)
        let pub_handle = Some(get_runtime().spawn(async move {
            if let Err(e) = publish_messages(pub_rx, trader_id, instance_id, config, publish).await
            {
                log_task_error(MSGBUS_PUBLISH, &e);
            }
        }));

        // Conditionally create stream task and channel if external streams configured
        let stream_signal = Arc::new(AtomicBool::new(false));
        let (stream_rx, stream_handle, read_tasks) = if external_streams.is_empty() {
            (None, None, None)
        } else {
            let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<BusMessage>(100_000);
            let (stream_task, read_tasks) = stream_task(
                stream_tx,
                backing,
                external_streams,
                Arc::clone(&stream_signal),
                read_streams,
            );
            (
                Some(stream_rx),
                Some(get_runtime().spawn(stream_task)),
                Some(read_tasks),
            )
        };

        // Create heartbeat task
        let heartbeat_signal = Arc::new(AtomicBool::new(false));
        let heartbeat_handle = if let Some(heartbeat_interval_secs) = heartbeat_interval_secs {
            let signal = Arc::clone(&heartbeat_signal);
            let pub_tx_clone = pub_tx.clone();
            let heartbeat_sources = sources.clone();

            Some(get_runtime().spawn(async move {
                run_heartbeat(
                    heartbeat_interval_secs,
                    signal,
                    pub_tx_clone,
                    heartbeat_sources,
                )
                .await;
            }))
        } else {
            None
        };

        Ok(Self {
            trader_id,
            instance_id,
            pub_tx,
            pub_handle,
            stream_rx,
            stream_handle,
            read_tasks,
            stream_signal,
            heartbeat_handle,
            heartbeat_signal,
            sources,
        })
    }
}

impl MessageBusBacking for RedisMessageBusBacking {
    /// Returns whether the message bus backing publishing channel is closed.
    fn is_closed(&self) -> bool {
        self.pub_tx.is_closed()
    }

    /// Reserves the source of the next entry written for `topic`: this backing's trader ID and
    /// instance ID, the key the entry is written on, and the next number of that key.
    ///
    /// The entry's age runs from this call.
    fn reserve_source(&self, topic: Ustr) -> anyhow::Result<Option<BusMessageSource>> {
        let source = self.sources.reserve(topic)?;
        RESERVED_AT.set(Some((source, tokio::time::Instant::now())));
        Ok(Some(source))
    }

    /// Queues a serialized bus message for external publication.
    fn publish(&self, message: BusMessage) {
        if let Err(e) = self.pub_tx.send(message) {
            log::error!("Failed to send message: {e}");
        }
    }

    fn take_receiver(&mut self) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
        self.get_stream_receiver()
    }

    /// Stops the stream read: sets its stop, so the stream task and the task that starts it
    /// again end within one read's bound ([`MAX_STREAM_READ`]), and returns without waiting for
    /// them. The publish and heartbeat tasks go on.
    fn stop_read(&mut self) {
        if !self.stream_signal.swap(true, Ordering::Relaxed) {
            log::debug!("Stopping the stream read");
        }
    }

    /// Closes the message bus backing through [`Self::close_async`], blocking until it returns.
    fn close(&mut self) {
        log::debug!("Closing");

        // Keep close sync for now to avoid async trait method
        tokio::task::block_in_place(|| {
            get_runtime().block_on(async {
                self.close_async().await;
            });
        });

        log::debug!("Closed");
    }
}

impl RedisMessageBusBacking {
    /// Retrieves the Redis stream receiver for this message bus instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream receiver has already been taken.
    pub fn get_stream_receiver(
        &mut self,
    ) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
        self.stream_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("Stream receiver already taken"))
    }

    /// Streams messages arriving on the stream receiver channel.
    pub fn stream(
        mut stream_rx: tokio::sync::mpsc::Receiver<BusMessage>,
    ) -> impl Stream<Item = BusMessage> + 'static {
        async_stream::stream! {
            while let Some(msg) = stream_rx.recv().await {
                yield msg;
            }
        }
    }

    /// Closes the message bus backing: stops the stream read and the heartbeat, has the publish
    /// task write what is pending, and waits for these tasks together until [`CLOSE_DEADLINE`]
    /// from this call, then aborts each task still running and waits for its abort, so no task of
    /// the backing runs when this returns. Then logs once how many entries the backing wrote,
    /// discarded and left uncertain in its life, at warning level when any was discarded or is
    /// uncertain. A second call does nothing.
    pub async fn close_async(&mut self) {
        let deadline = tokio::time::Instant::now() + CLOSE_DEADLINE;
        let Some(pub_handle) = self.pub_handle.take() else {
            return;
        };

        self.stream_signal.store(true, Ordering::Relaxed);
        self.heartbeat_signal.store(true, Ordering::Relaxed);

        if !self.pub_tx.is_closed()
            && let Err(e) = self.pub_tx.send(BusMessage::new_close())
        {
            log::warn!("Failed to send close message: {e:?}");
        }

        let (stream_handle, read_tasks) = (self.stream_handle.take(), self.read_tasks.take());
        let stream = async move {
            await_handle(stream_handle, MSGBUS_STREAM, deadline).await;

            // Its read tasks end with it, aborted or not
            if let Some(mut read_tasks) = read_tasks {
                read_tasks.recv().await;
            }
        };
        let (publish_aborted, (), _) = tokio::join!(
            await_handle(Some(pub_handle), MSGBUS_PUBLISH, deadline),
            stream,
            await_handle(self.heartbeat_handle.take(), MSGBUS_HEARTBEAT, deadline),
        );

        let counts = self.pub_tx.closed_counts();
        let line = if publish_aborted {
            format!("Closed the stream at the close's deadline of {CLOSE_DEADLINE:?}: {counts}")
        } else {
            format!("Closed the stream: {counts}")
        };

        if counts.discarded == 0 && counts.uncertain == 0 {
            log::info!("{line}");
        } else {
            log::warn!("{line}");
        }
    }
}

/// Longest the close of a backing waits, from its call, for its tasks together: the publish task
/// writing what is pending, the stream read and the heartbeat. A task still running then is
/// aborted. Provisional until measured.
const CLOSE_DEADLINE: Duration = Duration::from_secs(5);

/// Gives the source of each entry one backing writes.
///
/// A source names the backing's trader ID and instance ID, the key the entry is written on, and
/// its number: the last number of that key plus one, from 1. The last number of each key is kept
/// for the backing's life, and a key whose numbers ran out refuses every later entry. Clones share
/// the last numbers, so the heartbeat task numbers its entries from the backing's count.
#[derive(Clone, Debug)]
struct StreamSources {
    trader_id: TraderId,
    instance_id: UUID4,
    stream_key: String,
    stream_per_topic: bool,
    last_sequences: Arc<Mutex<AHashMap<Ustr, u64>>>,
}

impl StreamSources {
    fn new(trader_id: TraderId, instance_id: UUID4, config: &MessageBusConfig) -> Self {
        Self {
            trader_id,
            instance_id,
            stream_key: get_stream_key(trader_id, instance_id, config),
            stream_per_topic: config.stream_per_topic,
            last_sequences: Arc::new(Mutex::new(AHashMap::new())),
        }
    }

    fn reserve(&self, topic: Ustr) -> anyhow::Result<BusMessageSource> {
        let stream = Ustr::from(&stream_entry_key(
            &self.stream_key,
            self.stream_per_topic,
            &topic,
        ));
        let mut last_sequences = self
            .last_sequences
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let last = last_sequences.entry(stream).or_insert(0);
        let sequence = last
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Stream '{stream}' has no sequence number left"))?;
        *last = sequence;

        Ok(BusMessageSource {
            publisher_id: self.trader_id,
            instance_id: self.instance_id,
            stream,
            sequence,
        })
    }
}

thread_local! {
    /// The source this thread last reserved through [`RedisMessageBusBacking::reserve_source`], and
    /// when, from which its entry is aged when it arrives. A thread has at most one numbered entry
    /// between its reservation and its admission, since the message bus refuses a publication made
    /// while it encodes one, so a reservation whose entry never arrives is overwritten by the next.
    static RESERVED_AT: Cell<Option<(BusMessageSource, tokio::time::Instant)>> =
        const { Cell::new(None) };
}

/// Returns the key an entry of `topic` is written on: `stream_key`, or with one stream per topic,
/// `<stream_key>:<topic>`.
fn stream_entry_key<'a>(stream_key: &'a str, stream_per_topic: bool, topic: &str) -> Cow<'a, str> {
    if stream_per_topic {
        Cow::Owned(format!("{stream_key}:{topic}"))
    } else {
        Cow::Borrowed(stream_key)
    }
}

/// Most messages pending at once: those admitted and not yet written, dropped or abandoned,
/// waiting for the publish task, waiting for a write, or in the batch being written.
///
/// Admission holds it whether or not the publish task runs: beyond it the oldest messages not yet
/// sent are dropped and counted as discarded. Provisional until the stream's entry rates and sizes
/// are measured: about 115 s of entries at 865 per second, and no bound on memory.
const MAX_PENDING_ENTRIES: usize = 100_000;

/// Longest a message stays pending, from its admission (the publish call), or for an entry the
/// message bus numbered from the reservation of its number, on the monotonic clock.
///
/// An older message not yet sent is dropped and counted as discarded, at admission when it arrives
/// older, and is never written. Entries can be admitted out of the order of their ages, so one can
/// stay pending past it behind younger ones, and is dropped when the publish task takes it into a
/// batch. A batch being written is abandoned whole at the first of its messages to reach it,
/// without another attempt, and counted as uncertain. Provisional until measured.
const MAX_PENDING_AGE: Duration = Duration::from_secs(180);

/// Longest wait of the publish task between two connect or write attempts, and of the stream task
/// between two connect or read attempts.
///
/// The stream's rule, not a configuration default: it caps the delay whatever `max_delay` the
/// [`RedisMessageBusConfig`] gives, so the stream is written and read again within about a second
/// of Redis coming back.
const MAX_PUBLISH_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Longest one connect or write attempt of the publish task, or connect attempt of the stream task,
/// takes before it counts as failed.
/// Provisional until measured.
const MAX_PUBLISH_ATTEMPT: Duration = Duration::from_secs(5);

/// The messages admitted for the publish task and not yet taken into a batch, shared by the
/// admitting [`PublishSender`] and the task, so the pending bounds hold whether or not the task
/// runs.
#[derive(Default)]
struct PendingEntries {
    buffer: VecDeque<PendingMessage>,
    /// The size of the batch being written, whose count capacity stays held.
    batch_len: usize,
    /// Messages dropped at admission and not yet counted in the task's state.
    discarded: usize,
    /// Set by the close message, after which nothing more is admitted.
    closing: bool,
    /// What the publish task has counted in its state, kept here so the close reads it whether
    /// the task ended or was aborted.
    counts: StreamCounts,
}

/// How many entries the publish task wrote in its life, discarded (certainly not written) and
/// left uncertain (may have been written), as its state counts them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct StreamCounts {
    written: usize,
    discarded: usize,
    uncertain: usize,
}

impl Display for StreamCounts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} written, {} discarded, {} uncertain",
            self.written, self.discarded, self.uncertain
        )
    }
}

impl PendingEntries {
    /// Admits `message` at `now`, aged from `accepted`, taking out the oldest messages not yet
    /// taken into a batch beyond [`MAX_PENDING_ENTRIES`] with the batch being written, and returns
    /// them for the caller to destroy once it no longer holds these entries. A message past
    /// [`MAX_PENDING_AGE`] from `accepted` is taken out instead.
    ///
    /// Each admission adds one message, so at most one is taken out.
    fn admit(
        &mut self,
        message: BusMessage,
        accepted: tokio::time::Instant,
        now: tokio::time::Instant,
    ) -> Vec<PendingMessage> {
        if accepted + MAX_PENDING_AGE <= now {
            self.discarded += 1;
            return vec![PendingMessage { accepted, message }];
        }
        self.buffer.push_back(PendingMessage { accepted, message });

        let excess = (self.buffer.len() + self.batch_len)
            .saturating_sub(MAX_PENDING_ENTRIES)
            .min(self.buffer.len());
        self.discarded += excess;
        self.buffer.drain(..excess).collect()
    }

    /// Takes out the oldest messages not yet taken into a batch that are past [`MAX_PENDING_AGE`]
    /// at `now`, at most [`PUBLISH_BATCH_SIZE`] of them, for the caller to destroy once it no
    /// longer holds these entries.
    fn take_expired(&mut self, now: tokio::time::Instant) -> Vec<PendingMessage> {
        let expired = self
            .buffer
            .iter()
            .take(PUBLISH_BATCH_SIZE)
            .take_while(|&pending| pending.accepted + MAX_PENDING_AGE <= now)
            .count();
        self.buffer.drain(..expired).collect()
    }
}

/// The admitting end of the publish task's pending messages, held by the backing and the
/// heartbeat task.
#[derive(Clone)]
struct PublishSender {
    pending: Arc<Mutex<PendingEntries>>,
    wake: tokio::sync::mpsc::Sender<()>,
}

/// The publish task's end: the pending messages, and the wake-up each admission sends.
struct PublishReceiver {
    pending: Arc<Mutex<PendingEntries>>,
    wake: tokio::sync::mpsc::Receiver<()>,
}

/// Returns the two ends of the publish task's pending messages.
fn publish_channel() -> (PublishSender, PublishReceiver) {
    let pending = Arc::new(Mutex::new(PendingEntries::default()));
    // One wake-up waiting covers every admission before the task takes it
    let (wake_tx, wake_rx) = tokio::sync::mpsc::channel(1);
    (
        PublishSender {
            pending: Arc::clone(&pending),
            wake: wake_tx,
        },
        PublishReceiver {
            pending,
            wake: wake_rx,
        },
    )
}

impl PublishSender {
    /// Admits `message`, its age running from this call, or from its reservation when it is the
    /// entry this thread last numbered through [`RedisMessageBusBacking::reserve_source`]; one past
    /// [`MAX_PENDING_AGE`] is dropped and counted as discarded, and the close message stops
    /// admission instead.
    ///
    /// # Errors
    ///
    /// Returns an error, and admits nothing, if the publish task has ended.
    fn send(&self, message: BusMessage) -> Result<(), tokio::sync::mpsc::error::SendError<()>> {
        // Its age runs from the call, not from when the lock is free
        let now = tokio::time::Instant::now();
        let accepted = RESERVED_AT
            .get()
            .filter(|&(source, _)| message.source == Some(source))
            .map_or(now, |(_, reserved_at)| reserved_at);

        if self.wake.is_closed() {
            return Err(tokio::sync::mpsc::error::SendError(()));
        }

        let dropped = {
            let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);

            if message.topic == CLOSE_TOPIC {
                pending.closing = true;
                Vec::new()
            } else if pending.closing {
                Vec::new()
            } else {
                pending.admit(message, accepted, now)
            }
        };
        // Destroyed with the lock released
        drop(dropped);

        // A full channel already holds a wake-up the task has not taken
        self.wake.try_send(()).ok();
        Ok(())
    }

    /// Returns whether the publish task has ended.
    fn is_closed(&self) -> bool {
        self.wake.is_closed()
    }

    /// Returns the publish task's counts once it has ended, with what it left pending: the
    /// messages it never took into a batch, or dropped at admission and did not count, as
    /// discarded, and those of the batch it was writing as uncertain.
    fn closed_counts(&self) -> StreamCounts {
        let pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        StreamCounts {
            written: pending.counts.written,
            discarded: pending.counts.discarded + pending.discarded + pending.buffer.len(),
            uncertain: pending.counts.uncertain + pending.batch_len,
        }
    }
}

/// Publishes the messages admitted through `rx` to Redis streams for the given `trader_id` and
/// `instance_id`, using `config`.
///
/// At most [`MAX_PENDING_ENTRIES`] messages are pending at once, each for at most
/// [`MAX_PENDING_AGE`] from its admission, and beyond either bound the oldest not yet sent are
/// dropped. The task takes what is admitted while a connect or write is pending. Every
/// connection, the first included, is opened by one bounded attempt at a time, at most
/// [`MAX_PUBLISH_RETRY_DELAY`] after a failed one.
///
/// Pending messages are written in batches of at most 1,000 per `MULTI`/`EXEC`. A batch is not
/// changed until its `EXEC` is acknowledged, and a retry writes the same messages again, so a batch
/// whose reply was lost can be written twice. A batch Redis refuses with a non-retryable error is
/// dropped, counted as discarded when Redis aborted its transaction before executing it and
/// otherwise as uncertain, and the next write waits the retry delay: on a fresh connection when
/// the refusal is availability trouble (such as `READONLY` from a demoted primary, or `OOM`), and on
/// the same connection for a broken invariant. A batch past the age bound is abandoned. A write
/// error never ends the task: transport and availability trouble is logged at warning level, with
/// one record at each change of the stream between connected and degraded, and a broken invariant
/// at error level once per change. An acknowledged write marks the stream connected only when no
/// message was dropped at admission since the last one.
///
/// # Errors
///
/// Returns an error if:
/// - The configured `autotrim_maxlen` does not fit in `usize`.
/// - The pending messages cannot be written with one more attempt when the task closes.
async fn publish_messages(
    rx: PublishReceiver,
    trader_id: TraderId,
    instance_id: UUID4,
    config: MessageBusConfig,
    backing: RedisMessageBusConfig,
) -> anyhow::Result<()> {
    log_task_started(MSGBUS_PUBLISH);

    let PublishReceiver { pending, mut wake } = rx;
    let stream_key = get_stream_key(trader_id, instance_id, &config);
    let mut writer = StreamWriter::new(stream_key, &config, backing, pending)?;

    // Buffering
    let buffer_interval = Duration::from_millis(u64::from(config.buffer_interval_ms.unwrap_or(0)));

    // A sleep used to trigger periodic flushing of the buffer.
    // When `buffer_interval` is zero we skip using the timer and write
    // as soon as messages are pending.
    let flush_timer = tokio::time::sleep(buffer_interval);
    tokio::pin!(flush_timer);
    let mut flush_due = false;

    // A sleep used to wait out the delay after a failed attempt,
    // only polled while a retry is pending.
    let retry_timer = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(retry_timer);
    let mut retry_pending = false;

    // A sleep until the oldest pending message reaches the age bound,
    // only polled while it is armed.
    let expiry_timer = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(expiry_timer);
    let mut expiry = None;

    // The connect or write attempt in progress, polled beside the channel
    let mut attempt: Option<BoxFuture<'static, PublishAttempt>> = None;

    loop {
        if !writer.has_pending() {
            flush_due = false;
        }

        if attempt.is_none() && !retry_pending {
            attempt = writer.next_attempt(flush_due || buffer_interval.is_zero());
        }

        let next_expiry = writer.next_expiry();
        if next_expiry != expiry {
            if let Some(deadline) = next_expiry {
                expiry_timer.as_mut().reset(deadline);
            }
            expiry = next_expiry;
        }

        tokio::select! {
            woken = wake.recv() => {
                if woken.is_none() {
                    log::debug!("Channel hung up");
                    break;
                }

                if writer.take_admitted() {
                    log::debug!("Received close message");
                    break;
                }
            }
            ended = async {
                match attempt.as_mut() {
                    Some(attempt) => attempt.await,
                    None => std::future::pending().await,
                }
            }, if attempt.is_some() => {
                attempt = None;

                match writer.finish(ended) {
                    AttemptEnd::Next => {}
                    AttemptEnd::Refused(retry_delay) | AttemptEnd::Failed(retry_delay) => {
                        retry_pending = true;
                        retry_timer
                            .as_mut()
                            .reset(tokio::time::Instant::now() + retry_delay);
                    }
                }
            }
            () = &mut retry_timer, if retry_pending => retry_pending = false,
            // Only poll the timer when the interval is non-zero. This avoids
            // unnecessarily waking the task when immediate flushing is enabled.
            () = &mut flush_timer, if !buffer_interval.is_zero() => {
                flush_due = true;

                // Schedule the next tick
                flush_timer.as_mut().reset(tokio::time::Instant::now() + buffer_interval);
            }
            () = &mut expiry_timer, if expiry.is_some() => {
                // An abandoned batch's attempt is dropped unfinished
                if writer.expire(tokio::time::Instant::now()) {
                    attempt = None;
                }

                // Armed again for the next oldest message
                expiry = None;
            }
        }
    }

    writer.close(attempt).await?;

    log_task_stopped(MSGBUS_PUBLISH);
    Ok(())
}

/// Holds the publish task's batch, its connection and the stream's state.
///
/// Messages wait in the shared [`PendingEntries`] until a write takes up to [`PUBLISH_BATCH_SIZE`]
/// of them from its front as `batch`, which stays unchanged until it is acknowledged, refused or
/// abandoned, so each retry writes the same messages with their numbers. A failed attempt drops the
/// connection, so the next attempt opens a fresh one, as the stream reader does; so does a batch
/// refused by a server that cannot serve now, while a batch refused for a broken invariant keeps
/// it, since Redis answered.
struct StreamWriter {
    con: Option<ConnectionManager>,
    reconnect_config: RedisMessageBusConfig,
    target: StreamTarget,
    last_trim_index: HashMap<String, usize>,
    pending: Arc<Mutex<PendingEntries>>,
    batch: Vec<PendingMessage>,
    write_error_count: usize,
    state: StreamState,
    /// The broken invariant last reported at error level for a write, until a change.
    write_fault: Option<String>,
    /// The broken invariant last reported at error level for a trim, until a change.
    trim_fault: Option<String>,
    /// Whether a message was dropped at admission since the last acknowledged write, which then
    /// does not mark the stream connected.
    dropped_at_admission: bool,
}

/// What follows an attempt the publish task finished.
enum AttemptEnd {
    /// The next attempt may start at once.
    Next,
    /// Redis refused the batch, which was dropped; the next write waits this delay, on a fresh
    /// connection when the server could not serve.
    Refused(Duration),
    /// The attempt failed and is tried again after this delay.
    Failed(Duration),
}

/// A message admitted for the publish task, and when it was admitted.
struct PendingMessage {
    accepted: tokio::time::Instant,
    message: BusMessage,
}

/// The stream keys and trimming the publish task writes with.
#[derive(Clone)]
struct StreamTarget {
    stream_key: String,
    stream_per_topic: bool,
    autotrim_duration: Option<Duration>,
    autotrim_maxlen: Option<usize>,
}

/// How one connect or write attempt of the publish task ended.
enum PublishAttempt {
    Connect(anyhow::Result<ConnectionManager>),
    /// A write's connection and trim index, the key and error of a trim that failed without
    /// failing the write, and its result, or `None` when it timed out.
    Write {
        con: ConnectionManager,
        last_trim_index: HashMap<String, usize>,
        trim_error: Option<(String, redis::RedisError)>,
        result: Option<redis::RedisResult<()>>,
    },
}

impl StreamWriter {
    fn new(
        stream_key: String,
        config: &MessageBusConfig,
        backing: RedisMessageBusConfig,
        pending: Arc<Mutex<PendingEntries>>,
    ) -> anyhow::Result<Self> {
        // Auto-trimming
        let autotrim_duration = config
            .autotrim_mins
            .filter(|&mins| mins > 0)
            .map(|mins| Duration::from_secs(u64::from(mins) * 60));
        let autotrim_maxlen = config
            .autotrim_maxlen
            .filter(|&maxlen| maxlen > 0)
            .map(usize::try_from)
            .transpose()?;

        Ok(Self {
            con: None,
            // Every connection, the first included, fails fast, so this task's backoff governs
            // rather than the connection manager's retry schedule, and a close is not held up
            // behind it.
            reconnect_config: RedisMessageBusConfig {
                number_of_retries: 0,
                ..backing
            },
            target: StreamTarget {
                stream_key,
                stream_per_topic: config.stream_per_topic,
                autotrim_duration,
                autotrim_maxlen,
            },
            last_trim_index: HashMap::new(),
            pending,
            batch: Vec::new(),
            write_error_count: 0,
            state: StreamState::new("write"),
            write_fault: None,
            trim_fault: None,
            dropped_at_admission: false,
        })
    }

    /// Waits for `attempt` to end, then drains the remaining messages with a single attempt per
    /// batch, and stops at the first batch Redis refuses: what is behind it is left pending, for
    /// the close to count as discarded.
    async fn close(
        &mut self,
        attempt: Option<BoxFuture<'static, PublishAttempt>>,
    ) -> anyhow::Result<()> {
        self.take_admitted();

        if let Some(attempt) = attempt
            && let AttemptEnd::Refused(_) = self.finish(attempt.await)
        {
            return Ok(());
        }

        while self.has_pending() {
            let Some(attempt) = self.next_attempt(true) else {
                break;
            };

            match self.finish(attempt.await) {
                AttemptEnd::Next => {}
                AttemptEnd::Refused(_) => break,
                AttemptEnd::Failed(_) => anyhow::bail!(
                    "Failed to publish buffered messages before closing ({} unpublished)",
                    self.pending()
                ),
            }
        }

        Ok(())
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, PendingEntries> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Counts the messages dropped at admission since the last call, and returns whether the
    /// close message arrived.
    fn take_admitted(&mut self) -> bool {
        let (discarded, closing) = {
            let mut entries = self.entries();
            (std::mem::take(&mut entries.discarded), entries.closing)
        };

        if discarded > 0 {
            self.dropped_at_admission = true;
            self.discard(discarded);
        }
        closing
    }

    fn has_pending(&self) -> bool {
        !self.batch.is_empty() || !self.entries().buffer.is_empty()
    }

    fn pending(&self) -> usize {
        self.batch.len() + self.entries().buffer.len()
    }

    /// Ends the batch being written, releasing its count capacity, and returns its size.
    fn clear_batch(&mut self) -> usize {
        let cleared = self.batch.len();
        self.batch.clear();
        self.entries().batch_len = 0;
        cleared
    }

    /// Returns when the oldest message of the batch being written, or else the first pending
    /// message, reaches [`MAX_PENDING_AGE`].
    fn next_expiry(&self) -> Option<tokio::time::Instant> {
        self.batch
            .iter()
            .map(|pending| pending.accepted)
            .min()
            .or_else(|| {
                self.entries()
                    .buffer
                    .front()
                    .map(|pending| pending.accepted)
            })
            .map(|accepted| accepted + MAX_PENDING_AGE)
    }

    /// Drops what is pending past [`MAX_PENDING_AGE`] at `now`: the messages not yet sent, counted
    /// as discarded, and a batch being written with any message past it, whole, counted as
    /// uncertain.
    ///
    /// The messages not yet sent are taken out a batch's worth per hold of the shared entries and
    /// destroyed after it, so admission never waits on more than one such step.
    ///
    /// Returns whether the batch was abandoned.
    fn expire(&mut self, now: tokio::time::Instant) -> bool {
        let mut expired = 0;

        loop {
            let taken = self.entries().take_expired(now);
            let count = taken.len();
            drop(taken);
            expired += count;

            if count < PUBLISH_BATCH_SIZE {
                break;
            }
        }

        if expired > 0 {
            self.discard(expired);
        }

        let is_expired = |pending: &PendingMessage| pending.accepted + MAX_PENDING_AGE <= now;

        if !self.batch.iter().any(is_expired) {
            return false;
        }

        let abandoned = self.clear_batch();
        self.count_uncertain(abandoned);

        log::warn!(
            "Abandoned a batch of {abandoned} stream entries pending for {MAX_PENDING_AGE:?}, which may have been written ({})",
            self.state
        );
        true
    }

    fn discard(&mut self, count: usize) {
        self.state.discarded += count;
        self.entries().counts.discarded += count;
        self.state.change(StreamLink::Degraded);
    }

    fn count_uncertain(&mut self, count: usize) {
        self.state.uncertain += count;
        self.entries().counts.uncertain += count;
        self.state.change(StreamLink::Degraded);
    }

    /// Returns the next attempt: a connection when there is none, then the batch being written,
    /// or when `write_due` a new batch from the front of the buffer.
    fn next_attempt(&mut self, write_due: bool) -> Option<BoxFuture<'static, PublishAttempt>> {
        let Some(con) = self.con.take() else {
            return Some(Box::pin(connect_attempt(self.reconnect_config.clone())));
        };

        if self.batch.is_empty() && write_due {
            // Nothing past the age bound is taken, whenever its timer fires, and wherever it waits
            let now = tokio::time::Instant::now();
            self.expire(now);

            let (expired, batch) = {
                let mut entries = self.entries();
                let count = entries.buffer.len().min(PUBLISH_BATCH_SIZE);
                let (expired, batch): (Vec<PendingMessage>, Vec<PendingMessage>) = entries
                    .buffer
                    .drain(..count)
                    .partition(|pending| pending.accepted + MAX_PENDING_AGE <= now);
                entries.batch_len = batch.len();
                (expired, batch)
            };

            if !expired.is_empty() {
                self.discard(expired.len());
            }
            self.batch = batch;
        }

        if self.batch.is_empty() {
            self.con = Some(con);
            return None;
        }

        let messages = self.batch.iter().map(|p| p.message.clone()).collect();
        let last_trim_index = std::mem::take(&mut self.last_trim_index);
        Some(Box::pin(write_attempt(
            con,
            self.target.clone(),
            last_trim_index,
            messages,
        )))
    }

    /// Takes how an attempt ended, and returns what follows it.
    fn finish(&mut self, attempt: PublishAttempt) -> AttemptEnd {
        let (con, last_trim_index, trim_error, result) = match attempt {
            PublishAttempt::Connect(Ok(con)) => {
                self.con = Some(con);
                return AttemptEnd::Next;
            }
            PublishAttempt::Connect(Err(e)) => {
                let retry_delay = self.fail();
                log::warn!(
                    "Error connecting to publish, retrying in {retry_delay:?} ({}): {e}",
                    self.state
                );
                return AttemptEnd::Failed(retry_delay);
            }
            PublishAttempt::Write {
                con,
                last_trim_index,
                trim_error,
                result,
            } => (con, last_trim_index, trim_error, result),
        };
        self.last_trim_index = last_trim_index;

        match trim_error {
            Some((stream_key, e)) => {
                let line = format!("Error trimming stream '{stream_key}' ({}): {e}", self.state);
                report_stream_fault(&e, &mut self.trim_fault, &line);
            }
            None => self.trim_fault = None,
        }

        match result {
            Some(Ok(())) => {
                self.con = Some(con);
                let written = self.clear_batch();
                self.entries().counts.written += written;
                self.write_error_count = 0;
                self.write_fault = None;
                self.state.last_success = Some(get_atomic_clock_realtime().get_time_ns());

                // A stream still losing messages at admission stays degraded
                self.take_admitted();
                if !std::mem::take(&mut self.dropped_at_admission) {
                    self.state.change(StreamLink::Connected);
                }
                AttemptEnd::Next
            }
            // Redis will not take this batch, so it is dropped and its numbers show the hole, and
            // the next write waits as after a failure. A server that cannot serve now, such as a
            // primary demoted to a replica that keeps the socket open, costs the connection, so
            // that write connects again; for a broken invariant it answered, so the connection is
            // kept.
            Some(Err(e)) if !is_retryable_write_error(&e) => {
                if stream_fault(&e) == StreamFault::Broken {
                    self.con = Some(con);
                }
                let dropped = self.clear_batch();

                // Only a transaction aborted before execution certainly wrote none of the batch
                let line = if e.kind()
                    == redis::ErrorKind::Server(redis::ServerErrorKind::ExecAbort)
                {
                    self.discard(dropped);
                    format!(
                        "Dropped a batch of {dropped} stream entries Redis refused ({}): {e}",
                        self.state
                    )
                } else {
                    self.count_uncertain(dropped);
                    format!(
                        "Dropped a batch of {dropped} stream entries whose transaction failed, which may have been written in part ({}): {e}",
                        self.state
                    )
                };
                report_stream_fault(&e, &mut self.write_fault, &line);
                AttemptEnd::Refused(self.fail())
            }
            Some(Err(e)) => {
                let retry_delay = self.fail();
                let line = format!(
                    "Error publishing to stream, retrying in {retry_delay:?} ({}): {e}",
                    self.state
                );
                report_stream_fault(&e, &mut self.write_fault, &line);
                AttemptEnd::Failed(retry_delay)
            }
            None => {
                let retry_delay = self.fail();
                log::warn!(
                    "No reply publishing to stream within {MAX_PUBLISH_ATTEMPT:?}, retrying in {retry_delay:?} ({})",
                    self.state
                );
                AttemptEnd::Failed(retry_delay)
            }
        }
    }

    /// Marks the stream degraded after a failed attempt and returns the delay before the next.
    fn fail(&mut self) -> Duration {
        self.state.change(StreamLink::Degraded);
        let retry_delay = stream_retry_delay(&self.reconnect_config, self.write_error_count)
            .min(MAX_PUBLISH_RETRY_DELAY);
        self.write_error_count = self.write_error_count.saturating_add(1);
        retry_delay
    }
}

/// Whether the publish task's stream is being written, or the stream task's streams read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamLink {
    Connected,
    Degraded,
}

/// A task's account of its stream: connected or degraded and since when, its last success (an
/// acknowledged write of the publish task, a read of the stream task), and how many entries the
/// publish task dropped as discarded or abandoned as uncertain.
#[derive(Debug)]
struct StreamState {
    /// What the task does on the stream, `write` or `read`, as its record names its last success.
    activity: &'static str,
    link: Option<StreamLink>,
    since: UnixNanos,
    last_success: Option<UnixNanos>,
    discarded: usize,
    uncertain: usize,
}

impl StreamState {
    fn new(activity: &'static str) -> Self {
        Self {
            activity,
            link: None,
            since: UnixNanos::default(),
            last_success: None,
            discarded: 0,
            uncertain: 0,
        }
    }

    /// Moves to `link`, logging one record when it changes.
    fn change(&mut self, link: StreamLink) {
        if self.link == Some(link) {
            return;
        }
        self.link = Some(link);
        self.since = get_atomic_clock_realtime().get_time_ns();

        log::warn!("Stream {self}");
    }
}

impl Display for StreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let link = match self.link {
            Some(StreamLink::Connected) => "connected",
            Some(StreamLink::Degraded) => "degraded",
            None => "starting",
        };
        let last_success = self
            .last_success
            .map_or_else(|| "none".to_string(), |at| at.to_rfc3339());
        write!(
            f,
            "{link} since {}, last {} {last_success}, {} discarded, {} uncertain",
            self.since.to_rfc3339(),
            self.activity,
            self.discarded,
            self.uncertain
        )
    }
}

/// Opens a connection for the publish task, failing after [`MAX_PUBLISH_ATTEMPT`].
async fn connect_attempt(config: RedisMessageBusConfig) -> PublishAttempt {
    let connect = create_redis_connection(MSGBUS_PUBLISH, &config);
    let result = tokio::time::timeout(MAX_PUBLISH_ATTEMPT, connect)
        .await
        .unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "No connection within {MAX_PUBLISH_ATTEMPT:?}"
            ))
        });
    PublishAttempt::Connect(result)
}

/// Writes `messages` as one batch on `con`, failing after [`MAX_PUBLISH_ATTEMPT`].
async fn write_attempt(
    mut con: ConnectionManager,
    target: StreamTarget,
    mut last_trim_index: HashMap<String, usize>,
    messages: Vec<BusMessage>,
) -> PublishAttempt {
    let mut trim_error = None;
    let write = write_buffer(
        &mut con,
        &target.stream_key,
        target.stream_per_topic,
        target.autotrim_duration,
        target.autotrim_maxlen,
        &mut last_trim_index,
        &mut trim_error,
        messages.iter(),
    );
    let result = tokio::time::timeout(MAX_PUBLISH_ATTEMPT, write).await.ok();
    PublishAttempt::Write {
        con,
        last_trim_index,
        trim_error,
        result,
    }
}

// A trim error that does not fail the write is kept in `trim_error` with its key, for the caller
// to report
#[expect(clippy::too_many_arguments)]
async fn write_buffer(
    conn: &mut redis::aio::ConnectionManager,
    stream_key: &str,
    stream_per_topic: bool,
    autotrim_duration: Option<Duration>,
    autotrim_maxlen: Option<usize>,
    last_trim_index: &mut HashMap<String, usize>,
    trim_error: &mut Option<(String, redis::RedisError)>,
    messages: impl Iterator<Item = &BusMessage>,
) -> redis::RedisResult<()> {
    let mut pipe = redis::pipe();
    pipe.atomic();

    for msg in messages {
        let stream_key = stream_entry_key(stream_key, stream_per_topic, &msg.topic).into_owned();
        add_stream_entry(&mut pipe, &stream_key, autotrim_maxlen, msg);

        if autotrim_duration.is_none() {
            continue; // Nothing else to do
        }

        // Autotrim stream
        let last_trim_ms = last_trim_index.entry(stream_key.clone()).or_insert(0); // Remove clone
        let unix_duration_now = duration_since_unix_epoch();
        let trim_buffer = Duration::from_secs(TRIM_BUFFER_SECS);

        // Improve efficiency of this by batching
        if *last_trim_ms < unix_duration_now.saturating_sub(trim_buffer).as_millis() as usize {
            let min_timestamp_ms = unix_duration_now
                .saturating_sub(autotrim_duration.unwrap())
                .as_millis() as usize;
            let result: Result<(), redis::RedisError> = redis::cmd(REDIS_XTRIM)
                .arg(stream_key.clone())
                .arg(REDIS_MINID)
                .arg(min_timestamp_ms)
                .query_async(conn)
                .await;

            if let Err(e) = result {
                // A dropped connection fails the batch here, before the connection manager's
                // own reconnect can hold up the pipeline below.
                if is_retryable_write_error(&e) {
                    return Err(e);
                }
                // A failed trim loses no entry, so the batch is still written
                *trim_error = Some((stream_key.clone(), e));
            } else {
                last_trim_index.insert(stream_key.clone(), unix_duration_now.as_millis() as usize);
            }
        }
    }

    pipe.query_async(conn).await
}

/// Writes `messages` as entries of the one stream `stream_key`, in one atomic `MULTI`/`EXEC`.
///
/// Each entry carries the fields of [`BusMessageFields`](super::stream_fields::BusMessageFields),
/// the source as the message carries it, so the entries are those the message bus backing writes
/// and a message written again keeps its number. With `maxlen`, each `XADD` trims the stream to
/// about that many entries (`MAXLEN ~`).
///
/// # Errors
///
/// Returns an error if Redis does not acknowledge the transaction.
pub async fn write_stream_entries<'a, C>(
    con: &mut C,
    stream_key: &str,
    maxlen: Option<usize>,
    messages: impl IntoIterator<Item = &'a BusMessage>,
) -> redis::RedisResult<()>
where
    C: redis::aio::ConnectionLike,
{
    let mut pipe = redis::pipe();
    pipe.atomic();

    for msg in messages {
        add_stream_entry(&mut pipe, stream_key, maxlen, msg);
    }

    pipe.query_async(con).await
}

fn add_stream_entry(
    pipe: &mut redis::Pipeline,
    stream_key: &str,
    maxlen: Option<usize>,
    msg: &BusMessage,
) {
    let encoding = msg.encoding.to_string();
    let fields = bus_message_fields(msg, &encoding);

    if let Some(maxlen) = maxlen {
        pipe.xadd_maxlen_map(
            stream_key,
            streams::StreamMaxlen::Approx(maxlen),
            "*",
            fields,
        );
    } else {
        pipe.xadd_map(stream_key, "*", fields);
    }
}

/// Where a Redis stream read starts: each stream delivers the entries after this position.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum StreamStart {
    /// Entries written from the current time on.
    #[default]
    Now,
    /// Every entry still held in the stream, then new entries.
    Beginning,
    /// Entries after the given stream entry ID (for example `1700000000000-0`).
    After(String),
}

/// Returns the stream entry ID a read starting at `start` begins after.
fn initial_stream_id(start: &StreamStart) -> String {
    match start {
        StreamStart::Now => get_atomic_clock_realtime().get_time_ms().to_string(),
        StreamStart::Beginning => "0".to_string(),
        StreamStart::After(id) => id.clone(),
    }
}

/// The ID each stream is read after, kept outside a stream task so that a task started again goes
/// on after the last entry the one before it sent.
type StreamPositions = Arc<Mutex<HashMap<String, String>>>;

/// Returns the positions of `stream_keys` for a read starting at `start`, each after the one ID
/// `start` gives now.
fn stream_positions(stream_keys: &[String], start: &StreamStart) -> StreamPositions {
    let initial_id = initial_stream_id(start);
    Arc::new(Mutex::new(
        stream_keys
            .iter()
            .map(|key| (key.clone(), initial_id.clone()))
            .collect(),
    ))
}

/// Streams messages from Redis streams and sends them over the provided `tx` channel.
///
/// Reads start from the current time; see [`stream_messages_from`] to read entries already
/// held in the streams.
///
/// # Errors
///
/// Never returns an error; see [`stream_messages_from`].
pub async fn stream_messages(
    tx: tokio::sync::mpsc::Sender<BusMessage>,
    config: RedisMessageBusConfig,
    stream_keys: Vec<String>,
    stream_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    stream_messages_from(tx, config, stream_keys, StreamStart::Now, stream_signal).await
}

/// Streams messages from Redis streams, starting after `start`, and sends them over the
/// provided `tx` channel.
///
/// One read loop delivers the entries already held after `start` and then tails new entries,
/// so the hand-over from history to live loses and repeats nothing. Each stream's position is
/// taken from `start` when the task starts and kept across connections: with [`StreamStart::Now`]
/// every entry written after the task started is sent, however late its first connection opens,
/// and after a reconnect the read goes on after the last entry it sent: no entry is sent twice,
/// and none Redis still holds is skipped.
///
/// Every connection, the first included, is opened by one attempt of at most
/// [`MAX_PUBLISH_ATTEMPT`] at a time. A read that gets no reply within its blocking time
/// ([`STREAM_READ_BLOCK`]) plus [`MAX_PUBLISH_ATTEMPT`] has failed. A failed connect or read never
/// ends the task. Transport or availability trouble (see `stream_fault`) is logged at warning
/// level, and the next attempt opens a fresh connection at most [`MAX_PUBLISH_RETRY_DELAY`] later.
/// A read error that is a broken invariant, such as a key of the wrong type, is logged at error
/// level once per change, and the read is tried again on its connection
/// [`MAX_PUBLISH_RETRY_DELAY`] later. Each failed attempt is part of one record at each change of
/// the read between connected and degraded. An entry that cannot be decoded is logged at error
/// level with its key and ID, and skipped. The task ends when `stream_signal` is set or the
/// receiver of `tx` is dropped.
///
/// # Errors
///
/// Never returns an error.
pub async fn stream_messages_from(
    tx: tokio::sync::mpsc::Sender<BusMessage>,
    config: RedisMessageBusConfig,
    stream_keys: Vec<String>,
    start: StreamStart,
    stream_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let positions = stream_positions(&stream_keys, &start);
    read_streams(tx, config, stream_keys, positions, stream_signal).await;
    Ok(())
}

/// Returns the backing's stream task, [`read_streams_until_stopped`] with `read`, and a receiver
/// that ends (`recv` returns `None`) once that task and every read task it started have ended, so
/// a close that aborts the stream task can wait for its read task's abort too.
fn stream_task<F, Fut>(
    tx: tokio::sync::mpsc::Sender<BusMessage>,
    config: RedisMessageBusConfig,
    stream_keys: Vec<String>,
    stream_signal: Arc<AtomicBool>,
    mut read: F,
) -> (
    impl Future<Output = ()> + Send + 'static,
    tokio::sync::mpsc::Receiver<()>,
)
where
    F: FnMut(
            tokio::sync::mpsc::Sender<BusMessage>,
            RedisMessageBusConfig,
            Vec<String>,
            StreamPositions,
            Arc<AtomicBool>,
        ) -> Fut
        + Send
        + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    // Nothing is sent: each read task holds a sender until it ends, aborted or not
    let (running, read_tasks) = tokio::sync::mpsc::channel(1);
    let task = read_streams_until_stopped(
        tx,
        config,
        stream_keys,
        stream_signal,
        move |tx, config, stream_keys, positions, stream_signal| {
            let running: tokio::sync::mpsc::Sender<()> = running.clone();
            let read = read(tx, config, stream_keys, positions, stream_signal);
            async move {
                let _running = running;
                read.await;
            }
        },
    );
    (task, read_tasks)
}

/// A read task [`read_streams_until_stopped`] started, aborted when it is dropped, so it ends with
/// the task that started it.
struct ReadTask(tokio::task::JoinHandle<()>);

impl Drop for ReadTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Reads `stream_keys` from the current time on, as the backing's stream task: starts a task with
/// `read` ([`read_streams`]) and starts it again, on the same channel and after the last entries
/// sent, whenever it ends without `stream_signal` set while the receiver of `tx` is held.
///
/// The current time is taken once, when this starts. Each such end is logged once at error level
/// with the task's name, and the next task starts [`MAX_PUBLISH_RETRY_DELAY`] later. `tx` is held
/// here throughout, so the receiver sees no end before the stop. The task started is aborted
/// with this one.
async fn read_streams_until_stopped<F, Fut>(
    tx: tokio::sync::mpsc::Sender<BusMessage>,
    config: RedisMessageBusConfig,
    stream_keys: Vec<String>,
    stream_signal: Arc<AtomicBool>,
    mut read: F,
) where
    F: FnMut(
        tokio::sync::mpsc::Sender<BusMessage>,
        RedisMessageBusConfig,
        Vec<String>,
        StreamPositions,
        Arc<AtomicBool>,
    ) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let positions = stream_positions(&stream_keys, &StreamStart::Now);

    loop {
        let task = read(
            tx.clone(),
            config.clone(),
            stream_keys.clone(),
            Arc::clone(&positions),
            Arc::clone(&stream_signal),
        );
        let mut read_task = ReadTask(tokio::spawn(task));
        let ended = (&mut read_task.0).await;

        if stream_signal.load(Ordering::Relaxed) || tx.is_closed() {
            break;
        }

        let reason = ended.map_or_else(|e| e.to_string(), |()| "it returned".to_string());
        log::error!(
            "Task '{MSGBUS_STREAM}' ended without the stop ({reason}), starting it again in {MAX_PUBLISH_RETRY_DELAY:?}"
        );

        if !wait_for_retry_delay(MAX_PUBLISH_RETRY_DELAY, &stream_signal).await {
            break;
        }
    }
}

/// The read loop of [`stream_messages_from`], reading each stream after its entry in `positions`
/// and moving it on with each entry read.
async fn read_streams(
    tx: tokio::sync::mpsc::Sender<BusMessage>,
    config: RedisMessageBusConfig,
    stream_keys: Vec<String>,
    positions: StreamPositions,
    stream_signal: Arc<AtomicBool>,
) {
    log_task_started(MSGBUS_STREAM);

    // Every connection, the first included, fails fast, so this task's backoff governs rather
    // than the connection manager's retry schedule
    let config = RedisMessageBusConfig {
        number_of_retries: 0,
        ..config
    };
    let mut con: Option<ConnectionManager> = None;
    let mut read_error_count = 0;
    let mut state = StreamState::new("read");

    let stream_keys = &stream_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<&str>>();

    log::debug!("Listening to streams: [{}]", stream_keys.join(", "));

    // A broken invariant last reported at error level, until a read succeeds
    let mut read_fault: Option<String> = None;

    let opts = stream_read_options();

    'outer: loop {
        if stream_signal.load(Ordering::Relaxed) {
            log::debug!("Received streaming terminate signal");
            break;
        }

        let Some(active) = con.as_mut() else {
            match connect_stream_connection(&config, &stream_signal).await {
                Ok(Some(connected)) => con = Some(connected),
                Ok(None) => break,
                Err(e) => {
                    let retry_delay = fail_stream_read(&mut state, &config, &mut read_error_count);
                    log::warn!(
                        "Error connecting to stream, retrying in {retry_delay:?} ({state}): {e}"
                    );

                    if !wait_for_retry_delay(retry_delay, &stream_signal).await {
                        break;
                    }
                }
            }
            continue;
        };

        match read_after_positions(active, stream_keys, &positions, &opts).await {
            Ok(stream_bulk) => {
                read_error_count = 0;
                read_fault = None;
                state.last_success = Some(get_atomic_clock_realtime().get_time_ns());
                state.change(StreamLink::Connected);

                if stream_bulk.is_empty() {
                    // Timeout occurred: no messages received
                    continue;
                }

                for entry in &stream_bulk {
                    for (stream_key, stream_msgs) in entry {
                        for stream_msg in stream_msgs {
                            for (id, array) in stream_msg {
                                positions
                                    .lock()
                                    .unwrap_or_else(PoisonError::into_inner)
                                    .insert(stream_key.clone(), id.clone());

                                match decode_stream_entry(stream_key, array) {
                                    Ok(msg) => {
                                        if let Err(e) = tx.send(msg).await {
                                            log::debug!("Channel closed: {e:?}");
                                            break 'outer; // End streaming
                                        }
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "Error decoding stream entry {id} of {stream_key}: {e:?}"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let retry_delay = fail_stream_read(&mut state, &config, &mut read_error_count);
                let retry_delay = match stream_fault(&e) {
                    // The next read opens a fresh connection, as the publish task's next write does
                    StreamFault::Unavailable => {
                        con = None;
                        retry_delay
                    }
                    // The connection serves; the read is tried on it once a second until repaired
                    StreamFault::Broken => MAX_PUBLISH_RETRY_DELAY,
                };
                report_stream_fault(
                    &e,
                    &mut read_fault,
                    &format!(
                        "Error reading from stream, retrying in {retry_delay:?} ({state}): {e}"
                    ),
                );

                if !wait_for_retry_delay(retry_delay, &stream_signal).await {
                    break;
                }
            }
        }
    }

    log_task_stopped(MSGBUS_STREAM);
}

/// Reads each of `stream_keys` after its entry in `positions`, failing with a timed out I/O error
/// when no reply comes within [`MAX_STREAM_READ`].
async fn read_after_positions(
    con: &mut ConnectionManager,
    stream_keys: &[&str],
    positions: &StreamPositions,
    opts: &StreamReadOptions,
) -> Result<RedisStreamBulk, redis::RedisError> {
    let ids: Vec<String> = {
        let positions = positions.lock().unwrap_or_else(PoisonError::into_inner);
        stream_keys
            .iter()
            .map(|&key| positions[key].clone())
            .collect()
    };
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();

    tokio::time::timeout(
        MAX_STREAM_READ,
        con.xread_options(&[stream_keys], &[&id_refs], opts),
    )
    .await
    .unwrap_or_else(|_| {
        Err(redis::RedisError::from(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("No reply within {MAX_STREAM_READ:?}"),
        )))
    })
}

/// Marks the stream read degraded after a failed connect or read and returns the delay before the
/// next attempt, at most [`MAX_PUBLISH_RETRY_DELAY`].
fn fail_stream_read(
    state: &mut StreamState,
    config: &RedisMessageBusConfig,
    read_error_count: &mut usize,
) -> Duration {
    state.change(StreamLink::Degraded);
    let retry_delay = stream_retry_delay(config, *read_error_count).min(MAX_PUBLISH_RETRY_DELAY);
    *read_error_count = read_error_count.saturating_add(1);
    retry_delay
}

/// Returns the options of each stream read: wait at most [`STREAM_READ_BLOCK`] for new entries and
/// take at most [`STREAM_READ_BATCH_SIZE`] entries from each stream.
fn stream_read_options() -> StreamReadOptions {
    StreamReadOptions::default()
        .block(usize::try_from(STREAM_READ_BLOCK.as_millis()).unwrap_or(usize::MAX))
        .count(STREAM_READ_BATCH_SIZE)
}

/// Opens a connection for the stream task, failing after [`MAX_PUBLISH_ATTEMPT`], or returns `None`
/// once `stream_signal` is set.
async fn connect_stream_connection(
    config: &RedisMessageBusConfig,
    stream_signal: &Arc<AtomicBool>,
) -> anyhow::Result<Option<ConnectionManager>> {
    let connect = tokio::time::timeout(
        MAX_PUBLISH_ATTEMPT,
        create_redis_connection(MSGBUS_STREAM, config),
    );
    let terminate = wait_for_stream_signal(stream_signal);

    tokio::pin!(connect);
    tokio::pin!(terminate);

    tokio::select! {
        result = &mut connect => result
            .unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "No connection within {MAX_PUBLISH_ATTEMPT:?}"
                ))
            })
            .map(Some),
        () = &mut terminate => Ok(None),
    }
}

fn stream_retry_delay(config: &RedisMessageBusConfig, attempt: usize) -> Duration {
    let exponent = u32::try_from(attempt.min(32)).unwrap_or(32);
    let delay_ms = config
        .factor
        .saturating_mul(config.exponent_base.saturating_pow(exponent));
    let max_delay = Duration::from_secs(config.max_delay);

    Duration::from_millis(delay_ms)
        .min(max_delay)
        .max(Duration::from_millis(1))
}

fn is_retryable_stream_error(error: &redis::RedisError) -> bool {
    matches!(
        error.retry_method(),
        RetryMethod::Reconnect
            | RetryMethod::ReconnectFromInitialConnections
            | RetryMethod::RetryImmediately
            | RetryMethod::WaitAndRetry
    )
}

// A transaction aborted with `EXECABORT` carries the errors its commands were queued with,
// so it is retryable when all of them are, such as `LOADING` while Redis restarts.
fn is_retryable_write_error(error: &redis::RedisError) -> bool {
    if is_retryable_stream_error(error) {
        return true;
    }

    error.clone().into_server_errors().is_some_and(|errors| {
        !errors.is_empty()
            && errors.iter().all(|(_, server_error)| {
                is_retryable_stream_error(&redis::RedisError::from(server_error.clone()))
            })
    })
}

/// The reply codes of a server that cannot serve now: loading its data, busy with a script, without
/// its master or a cluster, demoted to a replica, unable to save, out of memory, or short of
/// replicas. Whatever kind the redis crate gives them, the stream reports them as availability
/// trouble.
const UNAVAILABLE_REPLY_CODES: [&str; 9] = [
    "LOADING",
    "BUSY",
    "MASTERDOWN",
    "CLUSTERDOWN",
    "TRYAGAIN",
    "READONLY",
    "MISCONF",
    "OOM",
    "NOREPLICAS",
];

/// What a Redis error of the stream's writes, trims or reads is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamFault {
    /// Transport or availability trouble: a lost, refused or timed out connection, or the reply of
    /// a server that cannot serve now. Reported at warning level.
    Unavailable,
    /// A broken invariant, such as a key of the wrong type or a reply of the wrong shape. Reported
    /// at error level once per change.
    Broken,
}

/// Returns what `error` is: transport or availability trouble, or a broken invariant.
///
/// An I/O error and a connection the redis crate cannot find are transport trouble. A server reply,
/// or each reply of a transaction aborted before execution or executed in part, is availability
/// trouble when its code is one of [`UNAVAILABLE_REPLY_CODES`]. Every other error is a broken
/// invariant.
fn stream_fault(error: &redis::RedisError) -> StreamFault {
    if matches!(
        error.kind(),
        redis::ErrorKind::Io
            | redis::ErrorKind::ClusterConnectionNotFound
            | redis::ErrorKind::MasterNameNotFoundBySentinel
            | redis::ErrorKind::NoValidReplicasFoundBySentinel
    ) {
        return StreamFault::Unavailable;
    }

    let unavailable = error.clone().into_server_errors().is_some_and(|errors| {
        !errors.is_empty()
            && errors
                .iter()
                .all(|(_, server_error)| UNAVAILABLE_REPLY_CODES.contains(&server_error.code()))
    });

    if unavailable {
        StreamFault::Unavailable
    } else {
        StreamFault::Broken
    }
}

/// Logs `line`, about `error`, as [`stream_fault`] classifies it: at warning level for transport
/// or availability trouble, and for a broken invariant at error level when it differs from `last`,
/// the one last reported, and at debug level when it repeats it.
fn report_stream_fault(error: &redis::RedisError, last: &mut Option<String>, line: &str) {
    if stream_fault(error) == StreamFault::Unavailable {
        *last = None;
        log::warn!("{line}");
        return;
    }

    // The codes of the replies name the invariant; an error without a reply is named by its kind
    let fault = error.clone().into_server_errors().map_or_else(
        || format!("{:?}", error.kind()),
        |errors| {
            let mut codes: Vec<&str> = errors.iter().map(|(_, e)| e.code()).collect();
            codes.sort_unstable();
            codes.dedup();
            codes.join(",")
        },
    );

    if last.as_deref() == Some(fault.as_str()) {
        log::debug!("{line}");
    } else {
        log::error!("{line}");
        *last = Some(fault);
    }
}

async fn wait_for_retry_delay(retry_delay: Duration, stream_signal: &Arc<AtomicBool>) -> bool {
    let retry_timer = tokio::time::sleep(retry_delay);
    let terminate = wait_for_stream_signal(stream_signal);

    tokio::pin!(retry_timer);
    tokio::pin!(terminate);

    tokio::select! {
        () = &mut retry_timer => true,
        () = &mut terminate => false,
    }
}

async fn wait_for_stream_signal(stream_signal: &Arc<AtomicBool>) {
    let check_timer = tokio::time::interval(Duration::from_millis(100));

    tokio::pin!(check_timer);

    while !stream_signal.load(Ordering::Relaxed) {
        check_timer.tick().await;
    }
}

// The entry's message with its source, whose stream is the key the entry was read from
fn decode_stream_entry(stream_key: &str, stream_msg: &redis::Value) -> anyhow::Result<BusMessage> {
    let mut message = decode_bus_message(stream_msg)?;
    message.source = decode_bus_message_source(stream_key, stream_msg)?;
    Ok(message)
}

// An entry without source fields was written by an older writer and has no source
fn decode_bus_message_source(
    stream_key: &str,
    stream_msg: &redis::Value,
) -> anyhow::Result<Option<BusMessageSource>> {
    let redis::Value::Array(fields) = stream_msg else {
        anyhow::bail!("Invalid stream message format: {stream_msg:?}");
    };

    let mut publisher_id: Option<&str> = None;
    let mut instance_id: Option<&str> = None;
    let mut sequence: Option<&str> = None;

    for pair in fields.as_chunks::<2>().0 {
        let redis::Value::BulkString(key) = &pair[0] else {
            anyhow::bail!("Invalid stream field key: {stream_msg:?}");
        };
        let slot = match key.as_slice() {
            key if key == PUBLISHER_ID_FIELD.as_bytes() => &mut publisher_id,
            key if key == INSTANCE_ID_FIELD.as_bytes() => &mut instance_id,
            key if key == SEQUENCE_FIELD.as_bytes() => &mut sequence,
            _ => continue,
        };
        let redis::Value::BulkString(bytes) = &pair[1] else {
            anyhow::bail!("Invalid source field format: {stream_msg:?}");
        };
        *slot = Some(
            std::str::from_utf8(bytes)
                .map_err(|e| anyhow::anyhow!("Error parsing source field: {e}"))?,
        );
    }

    match (publisher_id, instance_id, sequence) {
        (None, None, None) => Ok(None),
        (Some(publisher_id), Some(instance_id), Some(sequence)) => Ok(Some(BusMessageSource {
            publisher_id: TraderId::new_checked(publisher_id)
                .map_err(|e| anyhow::anyhow!("Error parsing publisher ID: {e}"))?,
            instance_id: UUID4::from_str(instance_id)
                .map_err(|e| anyhow::anyhow!("Error parsing instance ID: {e}"))?,
            stream: Ustr::from(stream_key),
            sequence: sequence
                .parse()
                .map_err(|e| anyhow::anyhow!("Error parsing sequence: {e}"))?,
        })),
        _ => anyhow::bail!("Stream message has an incomplete source: {stream_msg:?}"),
    }
}

// Redis fields are unordered, and older streams may omit type or encoding headers
fn decode_bus_message(stream_msg: &redis::Value) -> anyhow::Result<BusMessage> {
    let redis::Value::Array(fields) = stream_msg else {
        anyhow::bail!("Invalid stream message format: {stream_msg:?}");
    };

    if fields.len() < 4 || fields.len() % 2 != 0 {
        anyhow::bail!("Invalid stream message format: {stream_msg:?}");
    }

    let mut topic: Option<String> = None;
    let mut type_name: Option<String> = None;
    let mut typed_payload = false;
    let mut encoding = SerializationEncoding::default();
    let mut payload: Option<Bytes> = None;

    for pair in fields.as_chunks::<2>().0 {
        let redis::Value::BulkString(key) = &pair[0] else {
            anyhow::bail!("Invalid stream field key: {stream_msg:?}");
        };

        match key.as_slice() {
            b"topic" => {
                let redis::Value::BulkString(bytes) = &pair[1] else {
                    anyhow::bail!("Invalid topic format: {stream_msg:?}");
                };
                topic = Some(
                    String::from_utf8(bytes.clone())
                        .map_err(|e| anyhow::anyhow!("Error parsing topic: {e}"))?,
                );
            }
            b"type" => {
                let redis::Value::BulkString(bytes) = &pair[1] else {
                    anyhow::bail!("Invalid type format: {stream_msg:?}");
                };
                type_name = Some(
                    String::from_utf8(bytes.clone())
                        .map_err(|e| anyhow::anyhow!("Error parsing type: {e}"))?,
                );
            }
            key if key == PAYLOAD_KIND_FIELD.as_bytes() => {
                let redis::Value::BulkString(bytes) = &pair[1] else {
                    anyhow::bail!("Invalid payload kind format: {stream_msg:?}");
                };
                let value = std::str::from_utf8(bytes)
                    .map_err(|e| anyhow::anyhow!("Error parsing payload kind: {e}"))?;
                anyhow::ensure!(
                    value == PAYLOAD_KIND_TYPED,
                    "Unknown payload kind '{value}'"
                );
                typed_payload = true;
            }
            b"encoding" => {
                let redis::Value::BulkString(bytes) = &pair[1] else {
                    anyhow::bail!("Invalid encoding format: {stream_msg:?}");
                };
                let value = std::str::from_utf8(bytes)
                    .map_err(|e| anyhow::anyhow!("Error parsing encoding: {e}"))?;
                encoding = value
                    .parse()
                    .map_err(|e| anyhow::anyhow!("Error parsing encoding: {e}"))?;
            }
            b"payload" => {
                let redis::Value::BulkString(bytes) = &pair[1] else {
                    anyhow::bail!("Invalid payload format: {stream_msg:?}");
                };
                payload = Some(Bytes::copy_from_slice(bytes));
            }
            _ => {}
        }
    }

    let Some(topic) = topic else {
        anyhow::bail!("Stream message missing topic: {stream_msg:?}");
    };
    let Some(payload) = payload else {
        anyhow::bail!("Stream message missing payload: {stream_msg:?}");
    };
    let payload_type = match type_name {
        Some(type_name) if typed_payload => BusPayloadType::from_typed_name(&type_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown typed payload '{type_name}'"))?,
        Some(type_name) => BusPayloadType::from_name(&type_name),
        None if typed_payload => {
            anyhow::bail!("Typed stream message missing type: {stream_msg:?}")
        }
        None => BusPayloadType::Custom(Ustr::default()),
    };

    Ok(BusMessage::with_str_topic(
        topic,
        payload_type,
        payload,
        encoding,
    ))
}

async fn run_heartbeat(
    heartbeat_interval_secs: u16,
    signal: Arc<AtomicBool>,
    pub_tx: PublishSender,
    sources: StreamSources,
) {
    log_task_started("heartbeat");
    log::debug!("Heartbeat at {heartbeat_interval_secs} second intervals");

    let heartbeat_interval = Duration::from_secs(u64::from(heartbeat_interval_secs));
    let heartbeat_timer = tokio::time::interval(heartbeat_interval);

    let check_interval = Duration::from_millis(100);
    let check_timer = tokio::time::interval(check_interval);

    tokio::pin!(heartbeat_timer);
    tokio::pin!(check_timer);

    loop {
        if signal.load(Ordering::Relaxed) {
            log::debug!("Received heartbeat terminate signal");
            break;
        }

        tokio::select! {
            _ = heartbeat_timer.tick() => {
                // Numbered before its payload is made, like every entry of the stream
                match sources.reserve(Ustr::from(HEARTBEAT_TOPIC)) {
                    Ok(source) => {
                        if let Err(e) = pub_tx.send(create_heartbeat_msg(source)) {
                            // We expect an error if the channel is closed during shutdown
                            log::debug!("Error sending heartbeat: {e}");
                        }
                    }
                    Err(e) => log::error!("{e}"),
                }
            },
            _ = check_timer.tick() => {}
        }
    }

    log_task_stopped("heartbeat");
}

fn create_heartbeat_msg(source: BusMessageSource) -> BusMessage {
    let payload = Bytes::from(
        Timestamp::now()
            .display_with_offset(Offset::UTC)
            .to_string()
            .into_bytes(),
    );
    let mut message = BusMessage::with_str_topic(
        HEARTBEAT_TOPIC,
        BusPayloadType::Custom(Ustr::default()),
        payload,
        SerializationEncoding::default(),
    );
    message.source = Some(source);
    message
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        msgbus::{
            MessageBus, ShareableMessageHandler, external_egress_from_backing,
            external_io_from_backing, get_message_bus, publish_any, publish_quote, set_message_bus,
            subscribe_any,
        },
        testing::{wait_until, wait_until_async},
    };
    use nautilus_core::UnixNanos;
    use nautilus_model::data::{CustomData, CustomDataTrait, DataType, HasTsInit, QuoteTick};
    use redis::{ToRedisArgs, Value};
    use rstest::*;
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[rstest]
    fn test_default_redis_message_bus_config() {
        let config = RedisMessageBusConfig::default();

        assert_eq!(config.host, None);
        assert_eq!(config.port, None);
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
        assert!(!config.ssl);
        assert_eq!(config.connection_timeout, 20);
        assert_eq!(config.response_timeout, 20);
        assert_eq!(config.number_of_retries, 100);
        assert_eq!(config.exponent_base, 2);
        assert_eq!(config.max_delay, 1000);
        assert_eq!(config.factor, 2);
    }

    #[rstest]
    fn test_deserialize_redis_message_bus_config() {
        let config_json = json!({
            "host": "localhost",
            "port": 6379,
            "username": "user",
            "password": "pass",
            "ssl": true,
            "connection_timeout": 30,
            "response_timeout": 10,
            "number_of_retries": 3,
            "exponent_base": 2,
            "max_delay": 10,
            "factor": 2
        });

        let config: RedisMessageBusConfig = serde_json::from_value(config_json).unwrap();

        assert_eq!(config.host, Some("localhost".to_string()));
        assert_eq!(config.port, Some(6379));
        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
        assert!(config.ssl);
        assert_eq!(config.connection_timeout, 30);
        assert_eq!(config.response_timeout, 10);
        assert_eq!(config.number_of_retries, 3);
        assert_eq!(config.exponent_base, 2);
        assert_eq!(config.max_delay, 10);
        assert_eq!(config.factor, 2);
    }

    #[rstest]
    fn test_deserialize_redis_message_bus_config_rejects_type_selector() {
        let config_json = json!({
            "type": "redis",
        });

        let error = serde_json::from_value::<RedisMessageBusConfig>(config_json).unwrap_err();

        assert!(error.to_string().contains("unknown field `type`"));
    }

    #[rstest]
    fn test_bus_message_fields_distinguish_custom_and_typed_names() {
        let custom = BusMessage::with_str_topic(
            "external.test",
            BusPayloadType::Custom(Ustr::from("TradingCommand")),
            Bytes::from_static(b"custom"),
            SerializationEncoding::Json,
        );
        let typed = BusMessage::with_str_topic(
            "external.test",
            BusPayloadType::TradingCommand,
            Bytes::from_static(b"typed"),
            SerializationEncoding::Json,
        );

        let custom_fields = bus_message_fields(&custom, "json");
        let typed_fields = bus_message_fields(&typed, "json");
        let custom_fields = custom_fields.as_slice();
        let typed_fields = typed_fields.as_slice();

        assert_eq!(custom_fields.len(), 4);
        assert_eq!(typed_fields.len(), 5);
        assert_eq!(
            custom_fields
                .iter()
                .find(|(key, _)| *key == "type")
                .expect("custom fields must include type")
                .1,
            b"TradingCommand"
        );
        assert_eq!(
            typed_fields
                .iter()
                .find(|(key, _)| *key == "type")
                .expect("typed fields must include type")
                .1,
            b"TradingCommand"
        );
        assert!(
            custom_fields
                .iter()
                .all(|(key, _)| *key != PAYLOAD_KIND_FIELD)
        );
        assert_eq!(
            typed_fields
                .iter()
                .find(|(key, _)| *key == PAYLOAD_KIND_FIELD)
                .expect("typed fields must include payload kind")
                .1,
            PAYLOAD_KIND_TYPED.as_bytes()
        );
    }

    #[rstest]
    fn test_decode_bus_message_valid() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"type".to_vec()),
            Value::BulkString(b"QuoteTick".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
            Value::BulkString(b"encoding".to_vec()),
            Value::BulkString(b"msgpack".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert_eq!(msg.topic, "topic1");
        assert_eq!(msg.payload_type, BusPayloadType::QuoteTick);
        assert_eq!(msg.encoding, SerializationEncoding::MsgPack);
        assert_eq!(msg.payload, Bytes::from("data1"));
    }

    #[rstest]
    fn test_decode_bus_message_defaults_legacy_headers() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert_eq!(msg.topic, "topic1");
        assert_eq!(msg.payload_type, BusPayloadType::Custom(Ustr::default()));
        assert_eq!(msg.encoding, SerializationEncoding::Json);
        assert_eq!(msg.payload, Bytes::from("data1"));
    }

    #[rstest]
    fn test_decode_bus_message_reads_chrono_heartbeat() {
        let heartbeat =
            include_str!("../../test_data/redis_msgbus_heartbeat_chrono.txt").trim_end();
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(HEARTBEAT_TOPIC.as_bytes().to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(heartbeat.as_bytes().to_vec()),
        ]);

        let msg = decode_bus_message(&stream_msg).unwrap();
        let timestamp = std::str::from_utf8(&msg.payload)
            .unwrap()
            .parse::<Timestamp>()
            .unwrap();

        assert_eq!(msg.topic, HEARTBEAT_TOPIC);
        assert_eq!(msg.payload_type, BusPayloadType::Custom(Ustr::default()));
        assert_eq!(msg.encoding, SerializationEncoding::Json);
        assert_eq!(msg.payload.as_ref(), heartbeat.as_bytes());
        assert_eq!(timestamp.as_nanosecond(), 1_785_805_323_456_789_000);
    }

    #[rstest]
    fn test_decode_bus_message_typed_name_without_kind_is_custom() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"type".to_vec()),
            Value::BulkString(b"TradingCommand".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert_eq!(
            msg.payload_type,
            BusPayloadType::Custom(Ustr::from("TradingCommand"))
        );
        assert_eq!(msg.encoding, SerializationEncoding::Json);
    }

    #[rstest]
    fn test_decode_bus_message_typed_kind_resolves_fixed_type() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"external.test".to_vec()),
            Value::BulkString(b"type".to_vec()),
            Value::BulkString(b"TradingCommand".to_vec()),
            Value::BulkString(PAYLOAD_KIND_FIELD.as_bytes().to_vec()),
            Value::BulkString(PAYLOAD_KIND_TYPED.as_bytes().to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let msg = decode_bus_message(&stream_msg).expect("typed message must decode");

        assert_eq!(msg.topic, "external.test");
        assert_eq!(msg.payload_type, BusPayloadType::TradingCommand);
        assert_eq!(msg.encoding, SerializationEncoding::Json);
        assert_eq!(msg.payload, Bytes::from_static(b"data1"));
    }

    #[rstest]
    fn test_decode_bus_message_rejects_invalid_typed_metadata() {
        let unknown_kind = stream_message_with_typed_metadata(
            Some("TradingCommand"),
            Value::BulkString(b"unknown".to_vec()),
        );
        let missing_type = stream_message_with_typed_metadata(
            None,
            Value::BulkString(PAYLOAD_KIND_TYPED.as_bytes().to_vec()),
        );
        let unknown_type = stream_message_with_typed_metadata(
            Some("UnknownPayload"),
            Value::BulkString(PAYLOAD_KIND_TYPED.as_bytes().to_vec()),
        );
        let invalid_kind =
            stream_message_with_typed_metadata(Some("TradingCommand"), Value::Int(42));
        let cases = [
            (&unknown_kind, "Unknown payload kind 'unknown'".to_string()),
            (
                &missing_type,
                format!("Typed stream message missing type: {missing_type:?}"),
            ),
            (
                &unknown_type,
                "Unknown typed payload 'UnknownPayload'".to_string(),
            ),
            (
                &invalid_kind,
                format!("Invalid payload kind format: {invalid_kind:?}"),
            ),
        ];

        for (stream_msg, expected) in cases {
            let error =
                decode_bus_message(stream_msg).expect_err("invalid typed metadata must not decode");

            assert_eq!(error.to_string(), expected);
        }
    }

    fn stream_message_with_typed_metadata(type_name: Option<&str>, payload_kind: Value) -> Value {
        let mut fields = vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"external.test".to_vec()),
        ];

        if let Some(type_name) = type_name {
            fields.extend([
                Value::BulkString(b"type".to_vec()),
                Value::BulkString(type_name.as_bytes().to_vec()),
            ]);
        }
        fields.extend([
            Value::BulkString(PAYLOAD_KIND_FIELD.as_bytes().to_vec()),
            payload_kind,
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);
        Value::Array(fields)
    }

    #[rstest]
    fn test_decode_bus_message_accepts_unordered_metadata_fields() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
            Value::BulkString(b"encoding".to_vec()),
            Value::BulkString(b"msgpack".to_vec()),
            Value::BulkString(b"type".to_vec()),
            Value::BulkString(b"TradeTick".to_vec()),
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
        ]);

        let msg = decode_bus_message(&stream_msg).unwrap();

        assert_eq!(msg.topic, "topic1");
        assert_eq!(msg.payload_type, BusPayloadType::TradeTick);
        assert_eq!(msg.encoding, SerializationEncoding::MsgPack);
        assert_eq!(msg.payload, Bytes::from("data1"));
    }

    #[rstest]
    fn test_decode_bus_message_rejects_invalid_encoding_header() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"encoding".to_vec()),
            Value::BulkString(b"invalid".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let error = decode_bus_message(&stream_msg).unwrap_err();

        assert!(
            error.to_string().contains("Error parsing encoding"),
            "{error:?}"
        );
    }

    #[rstest]
    fn test_decode_bus_message_missing_fields() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"0".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid stream message format: array([bulk-string('\"0\"'), bulk-string('\"topic1\"')])"
        );
    }

    #[rstest]
    fn test_decode_bus_message_invalid_topic_format() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::Int(42),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid topic format: array([bulk-string('\"topic\"'), int(42), bulk-string('\"payload\"'), bulk-string('\"data1\"')])"
        );
    }

    #[rstest]
    fn test_decode_bus_message_invalid_type_format() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"type".to_vec()),
            Value::Int(42),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid type format: array([bulk-string('\"topic\"'), bulk-string('\"topic1\"'), bulk-string('\"type\"'), int(42), bulk-string('\"payload\"'), bulk-string('\"data1\"')])"
        );
    }

    #[rstest]
    fn test_decode_bus_message_invalid_encoding_format() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"encoding".to_vec()),
            Value::Int(42),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid encoding format: array([bulk-string('\"topic\"'), bulk-string('\"topic1\"'), bulk-string('\"encoding\"'), int(42), bulk-string('\"payload\"'), bulk-string('\"data1\"')])"
        );
    }

    #[rstest]
    fn test_decode_bus_message_invalid_payload_format() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::Int(42),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid payload format: array([bulk-string('\"topic\"'), bulk-string('\"topic1\"'), bulk-string('\"payload\"'), int(42)])"
        );
    }

    #[rstest]
    fn test_decode_bus_message_invalid_stream_msg_format() {
        let stream_msg = Value::BulkString(b"not an array".to_vec());

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid stream message format: bulk-string('\"not an array\"')"
        );
    }

    #[rstest]
    fn test_new_rejects_zero_heartbeat_interval() {
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            heartbeat_interval_secs: Some(0),
            ..Default::default()
        };

        let result = RedisMessageBusBacking::new(
            trader_id,
            instance_id,
            config,
            RedisMessageBusConfig::default(),
        );

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "heartbeat_interval_secs must be greater than 0"
        );
    }

    #[rstest]
    fn test_new_rejects_heartbeats_on_one_stream_for_all_topics() {
        let config = MessageBusConfig {
            stream_per_topic: false,
            heartbeat_interval_secs: Some(1),
            ..Default::default()
        };

        let result = RedisMessageBusBacking::new(
            TraderId::from("tester-001"),
            UUID4::new(),
            config,
            RedisMessageBusConfig::default(),
        );

        assert_eq!(
            result.unwrap_err().to_string(),
            "heartbeat_interval_secs needs stream_per_topic: on one stream for all topics a heartbeat can be written before an entry numbered ahead of it; set stream_per_topic, or unset heartbeat_interval_secs"
        );
    }

    #[rstest]
    fn test_stream_retry_delay_uses_config_bounds() {
        let config = RedisMessageBusConfig {
            factor: 10,
            exponent_base: 2,
            max_delay: 1,
            ..Default::default()
        };

        assert_eq!(stream_retry_delay(&config, 0), Duration::from_millis(10));
        assert_eq!(stream_retry_delay(&config, 1), Duration::from_millis(20));
        assert_eq!(stream_retry_delay(&config, 10), Duration::from_secs(1));
    }

    #[rstest]
    fn test_stream_error_retry_classification() {
        let dropped =
            redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        let client: redis::RedisError = (redis::ErrorKind::Client, "client error").into();

        assert!(is_retryable_stream_error(&dropped));
        assert!(!is_retryable_stream_error(&client));
    }

    #[rstest]
    #[case::loading("LOADING", StreamFault::Unavailable)]
    #[case::busy("BUSY", StreamFault::Unavailable)]
    #[case::master_down("MASTERDOWN", StreamFault::Unavailable)]
    #[case::cluster_down("CLUSTERDOWN", StreamFault::Unavailable)]
    #[case::try_again("TRYAGAIN", StreamFault::Unavailable)]
    #[case::read_only("READONLY", StreamFault::Unavailable)]
    #[case::misconf("MISCONF", StreamFault::Unavailable)]
    #[case::oom("OOM", StreamFault::Unavailable)]
    #[case::no_replicas("NOREPLICAS", StreamFault::Unavailable)]
    #[case::wrong_type("WRONGTYPE", StreamFault::Broken)]
    #[case::err("ERR", StreamFault::Broken)]
    #[case::no_perm("NOPERM", StreamFault::Broken)]
    fn test_stream_fault_classifies_server_replies(
        #[case] code: &str,
        #[case] expected: StreamFault,
    ) {
        let error = redis::make_extension_error(code.to_string(), Some("detail".to_string()));

        assert_eq!(stream_fault(&error), expected);
    }

    #[rstest]
    fn test_stream_fault_classifies_transport_and_reply_shape() {
        let dropped =
            redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        let timed_out = redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::TimedOut));
        let wrong_shape: redis::RedisError =
            (redis::ErrorKind::UnexpectedReturnType, "wrong shape").into();

        assert_eq!(stream_fault(&dropped), StreamFault::Unavailable);
        assert_eq!(stream_fault(&timed_out), StreamFault::Unavailable);
        assert_eq!(stream_fault(&wrong_shape), StreamFault::Broken);
    }

    #[rstest]
    #[case::beginning(StreamStart::Beginning, "0")]
    #[case::after_entry(StreamStart::After("5-1".to_string()), "5-1")]
    fn test_stream_start_initial_id(#[case] start: StreamStart, #[case] expected: &str) {
        assert_eq!(initial_stream_id(&start), expected);
    }

    #[rstest]
    fn test_stream_start_now_initial_id_is_current_time() {
        let clock = get_atomic_clock_realtime();
        let before = clock.get_time_ms();

        let initial_id: u64 = initial_stream_id(&StreamStart::Now).parse().unwrap();

        assert!((before..=clock.get_time_ms()).contains(&initial_id));
    }

    #[rstest]
    fn test_stream_read_options_count_entries_and_keep_block() {
        let args = redis::ToRedisArgs::to_redis_args(&stream_read_options());

        assert_eq!(
            args,
            vec![
                b"BLOCK".to_vec(),
                b"100".to_vec(),
                b"COUNT".to_vec(),
                STREAM_READ_BATCH_SIZE.to_string().into_bytes(),
            ]
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_from_beginning_reads_backlog_in_bounded_pages_then_tail() {
        let backlog_len = 2 * STREAM_READ_BATCH_SIZE + 1;
        let stream_keys = vec!["stream-0".to_string(), "stream-1".to_string()];
        let fake = FakeRedisStreams::start(&stream_keys, backlog_len).await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);
        let stream_signal = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn(stream_messages_from(
            tx,
            fake.config(),
            stream_keys.clone(),
            StreamStart::Beginning,
            Arc::clone(&stream_signal),
        ));

        let mut received: [Vec<String>; 2] = Default::default();
        receive_stream_payloads(&mut rx, &stream_keys, &mut received, 2 * backlog_len).await;
        // Appended only once the whole backlog was received
        fake.append(backlog_len + 1, "tail");
        receive_stream_payloads(&mut rx, &stream_keys, &mut received, 2 * backlog_len + 2).await;
        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap().unwrap();

        let expected: Vec<String> = (1..=backlog_len)
            .map(|i| i.to_string())
            .chain(std::iter::once("tail".to_string()))
            .collect();
        assert_eq!(received, [expected.clone(), expected]);
        let reads = fake.reads.lock().unwrap();
        assert!(
            reads
                .iter()
                .all(|read| read.count == Some(STREAM_READ_BATCH_SIZE))
        );
        let expected_pages = [
            ("0".to_string(), STREAM_READ_BATCH_SIZE),
            (
                format!("{STREAM_READ_BATCH_SIZE}-0"),
                STREAM_READ_BATCH_SIZE,
            ),
            (format!("{}-0", 2 * STREAM_READ_BATCH_SIZE), 1),
            (format!("{backlog_len}-0"), 1),
        ];

        for stream in 0..stream_keys.len() {
            let pages: Vec<_> = reads
                .iter()
                .filter(|read| read.replied[stream] > 0)
                .map(|read| (read.ids[stream].clone(), read.replied[stream]))
                .collect();
            assert_eq!(pages, expected_pages);
        }
    }

    async fn receive_stream_payloads(
        rx: &mut tokio::sync::mpsc::Receiver<BusMessage>,
        stream_keys: &[String],
        received: &mut [Vec<String>; 2],
        total: usize,
    ) {
        while received.iter().map(Vec::len).sum::<usize>() < total {
            let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("the entries should arrive")
                .expect("the stream task should keep the channel open");
            let stream = stream_keys
                .iter()
                .position(|key| msg.topic == key.as_str())
                .expect("every entry comes from one of the read streams");
            received[stream].push(String::from_utf8(msg.payload.to_vec()).unwrap());
        }
    }

    #[tokio::test]
    async fn test_wait_for_retry_delay_returns_false_when_signaled() {
        let stream_signal = Arc::new(AtomicBool::new(true));
        let signal = Arc::clone(&stream_signal);
        let fut = async move { wait_for_retry_delay(Duration::from_secs(30), &signal).await };

        let handle = tokio::spawn(fut);

        wait_until_async(|| async { handle.is_finished() }, Duration::from_secs(1)).await;

        assert!(!handle.await.unwrap());
    }

    #[rstest]
    #[case::loading("LOADING Redis is loading the dataset in memory")]
    #[case::master_down(
        "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."
    )]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_messages_retries_transaction_aborted_by_retryable_error(
        #[case] queued_error: &'static str,
    ) {
        let fake = FakeRedis::start(queued_error).await;
        let (tx, rx) = publish_channel();
        let backing = fake.config();

        let handle = tokio::spawn(async move {
            publish_messages(
                rx,
                TraderId::from("tester-001"),
                UUID4::new(),
                MessageBusConfig::default(),
                backing,
            )
            .await
        });

        tx.send(fake_bus_message()).unwrap();
        wait_until_async(
            || {
                let aborted = Arc::clone(&fake.aborted);
                async move { aborted.load(Ordering::SeqCst) > 0 }
            },
            Duration::from_secs(5),
        )
        .await;
        assert!(!handle.is_finished());

        fake.accept_writes.store(true, Ordering::SeqCst);
        wait_until_async(
            || {
                let committed = Arc::clone(&fake.committed);
                async move { committed.load(Ordering::SeqCst) > 0 }
            },
            Duration::from_secs(5),
        )
        .await;

        tx.send(BusMessage::new_close()).unwrap();
        let result = handle.await.unwrap();

        assert!(result.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_publish_messages_drops_a_batch_redis_refuses_and_writes_the_next() {
        capture_logs();
        let fake =
            FakeRedis::start("OOM command not allowed when used memory > 'maxmemory'.").await;
        let (tx, handle) = start_publisher(&fake);
        let messages = numbered_messages(3);

        tx.send(messages[0].clone()).unwrap();
        wait_until_async(
            || {
                let aborted = Arc::clone(&fake.aborted);
                async move { aborted.load(Ordering::SeqCst) > 0 }
            },
            Duration::from_secs(5),
        )
        .await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        tx.send(messages[1].clone()).unwrap();
        tx.send(messages[2].clone()).unwrap();
        wait_until_async(
            || {
                let written = Arc::clone(&fake.written);
                async move { written.lock().unwrap().len() == 2 }
            },
            Duration::from_secs(5),
        )
        .await;
        let finished_before_close = handle.is_finished();
        tx.send(BusMessage::new_close()).unwrap();
        let result = handle.await.unwrap();

        let logs = thread_logs();
        assert!(!finished_before_close);
        assert!(result.is_ok());
        assert_eq!(fake.aborted.load(Ordering::SeqCst), 1);
        assert_eq!(fake.written_sequences(), [2, 3]);
        assert_eq!(
            fake.write_records(&logs),
            [
                ("degraded".to_string(), false, 1, 0),
                ("connected".to_string(), true, 1, 0),
            ]
        );
        assert!(logs.iter().any(|(level, line)| *level == log::Level::Warn
            && line.starts_with("Dropped a batch of 1 stream entries Redis refused")
            && line.contains("OOM")));
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    fn test_way_out_never_reports_closed_after_a_write_error() {
        let fake = get_runtime().block_on(FakeRedis::start(
            "OOM command not allowed when used memory > 'maxmemory'.",
        ));
        let config = MessageBusConfig {
            stream_per_topic: false,
            ..Default::default()
        };
        let mut backing = RedisMessageBusBacking::new(
            TraderId::from("TESTER-007"),
            UUID4::new(),
            config,
            fake.config(),
        )
        .unwrap();
        // Numbered by the backing, which ages an entry from its reservation
        let numbered = || {
            let mut message = fake_bus_message();
            message.source = backing.reserve_source(message.topic).unwrap();
            message
        };

        backing.publish(numbered());
        wait_until(
            || fake.aborted.load(Ordering::SeqCst) > 0,
            Duration::from_secs(5),
        );
        std::thread::sleep(Duration::from_millis(100));
        let closed_after_the_error = backing.is_closed();
        fake.accept_writes.store(true, Ordering::SeqCst);
        backing.publish(numbered());
        wait_until(|| fake.written().len() == 1, Duration::from_secs(5));
        let closed_after_the_write = backing.is_closed();
        backing.close();

        assert!(!closed_after_the_error);
        assert!(!closed_after_the_write);
        assert_eq!(fake.written_sequences(), [2]);
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_retries_within_a_second_through_a_ten_minute_outage() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        let (tx, handle) = start_publisher(&fake);
        let messages = numbered_messages(3);

        tx.send(messages[0].clone()).unwrap();
        settle_until(|| fake.written_len() == 1).await;
        let mut clock = TaskClock::writing();
        fake.available.store(false, Ordering::SeqCst);
        let outage_start = tokio::time::Instant::now();
        tx.send(messages[1].clone()).unwrap();
        clock.drive(Duration::from_secs(540)).await;
        tx.send(messages[2].clone()).unwrap();
        clock.drive(Duration::from_secs(60)).await;
        let returned_at = tokio::time::Instant::now();
        fake.available.store(true, Ordering::SeqCst);
        clock
            .drive_until(|| fake.written_len() == 2, Duration::from_secs(5))
            .await;
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let attempts: Vec<tokio::time::Instant> = fake
            .connections
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|&at| at >= outage_start && at <= returned_at)
            .collect();
        let longest_wait = attempts
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .max()
            .unwrap();
        let first_write = fake
            .commits
            .lock()
            .unwrap()
            .iter()
            .copied()
            .find(|&at| at >= returned_at)
            .unwrap();
        assert!(result.is_ok());
        assert!(attempts.len() >= 500, "{} attempts", attempts.len());
        // The clock steps to the end of each retry wait and stands still through each attempt
        assert!(attempts[0] - outage_start <= MAX_PUBLISH_RETRY_DELAY);
        assert!(returned_at - *attempts.last().unwrap() <= MAX_PUBLISH_RETRY_DELAY);
        assert!(longest_wait <= MAX_PUBLISH_RETRY_DELAY, "{longest_wait:?}");
        assert!(first_write - returned_at <= MAX_PUBLISH_RETRY_DELAY);
        assert_eq!(fake.written_sequences(), [1, 3]);
        assert_eq!(
            fake.write_records(&logs),
            [
                ("connected".to_string(), true, 0, 0),
                ("degraded".to_string(), true, 0, 0),
                ("connected".to_string(), true, 0, 1),
            ]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_writes_from_the_first_entry_when_redis_is_absent_at_start() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        fake.available.store(false, Ordering::SeqCst);
        let mut clock = TaskClock::writing();
        let (tx, handle) = start_publisher(&fake);

        for message in numbered_messages(3) {
            tx.send(message).unwrap();
        }
        clock.drive(Duration::from_secs(30)).await;
        let written_while_absent = fake.written().len();
        let finished_while_absent = handle.is_finished();
        let returned_at = tokio::time::Instant::now();
        fake.available.store(true, Ordering::SeqCst);
        clock
            .drive_until(|| fake.written_len() == 3, Duration::from_secs(5))
            .await;
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let first_write = fake.commits.lock().unwrap()[0];
        assert!(result.is_ok());
        assert_eq!(written_while_absent, 0);
        assert!(!finished_while_absent);
        assert!(first_write - returned_at <= MAX_PUBLISH_RETRY_DELAY);
        assert_eq!(fake.written_sequences(), [1, 2, 3]);
        assert_eq!(
            fake.write_records(&logs),
            [
                ("degraded".to_string(), false, 0, 0),
                ("connected".to_string(), true, 0, 0),
            ]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_drops_an_entry_pending_past_the_age_bound() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        fake.available.store(false, Ordering::SeqCst);
        let (tx, handle) = start_publisher(&fake);
        let messages = numbered_messages(2);

        tx.send(messages[0].clone()).unwrap();
        drive(Duration::from_secs(181)).await;
        tx.send(messages[1].clone()).unwrap();
        fake.available.store(true, Ordering::SeqCst);
        drive_until(|| fake.written_len() == 1, Duration::from_secs(5)).await;
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        assert!(result.is_ok());
        assert_eq!(fake.written_sequences(), [2]);
        assert_eq!(
            fake.write_records(&logs),
            [
                ("degraded".to_string(), false, 0, 0),
                ("connected".to_string(), true, 1, 0),
            ]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_drops_the_oldest_unsent_beyond_the_count_bound() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        fake.stall_writes.store(true, Ordering::SeqCst);
        let (tx, handle) = start_publisher(&fake);
        let messages = numbered_messages(MAX_PENDING_ENTRIES + 1);

        tx.send(messages[0].clone()).unwrap();
        settle_until(|| !fake.attempted.lock().unwrap().is_empty()).await;
        for message in &messages[1..] {
            tx.send(message.clone()).unwrap();
        }
        // The drop is taken with the last message
        settle_until(|| !state_records(&thread_logs()).is_empty()).await;
        fake.stall_writes.store(false, Ordering::SeqCst);
        drive_until(|| fake.written_len() > 0, MAX_PUBLISH_ATTEMPT * 2).await;
        settle_until(|| fake.written_len() == MAX_PENDING_ENTRIES).await;
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let expected: Vec<u64> = std::iter::once(1)
            .chain(3..=u64::try_from(MAX_PENDING_ENTRIES + 1).unwrap())
            .collect();
        assert!(result.is_ok());
        assert_eq!(fake.written_sequences(), expected);
        assert_eq!(
            fake.write_records(&logs),
            [
                ("degraded".to_string(), false, 1, 0),
                ("connected".to_string(), true, 1, 0),
            ]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_abandons_a_batch_being_written_at_the_age_bound() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("LOADING Redis is loading the dataset in memory").await;
        let (tx, handle) = start_publisher(&fake);
        let messages = numbered_messages(2);

        tx.send(messages[0].clone()).unwrap();
        settle_until(|| fake.aborted.load(Ordering::SeqCst) > 0).await;
        drive(Duration::from_secs(100)).await;
        tx.send(messages[1].clone()).unwrap();
        drive(Duration::from_secs(81)).await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        drive_until(|| fake.written_len() == 1, Duration::from_secs(5)).await;
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let first_attempts: Vec<u64> = fake
            .attempted
            .lock()
            .unwrap()
            .iter()
            .map(FakeXadd::sequence)
            .filter(|&sequence| sequence == 1)
            .collect();
        assert!(result.is_ok());
        assert!(
            first_attempts.len() >= 100,
            "{} attempts",
            first_attempts.len()
        );
        assert_eq!(fake.written_sequences(), [2]);
        assert_eq!(
            fake.write_records(&logs),
            [
                ("degraded".to_string(), false, 0, 0),
                ("connected".to_string(), true, 0, 1),
            ]
        );
        assert!(logs.iter().any(|(level, line)| *level == log::Level::Warn
            && line.starts_with("Abandoned a batch of 1 stream entries")));
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    fn test_publish_sender_holds_the_pending_bound_while_the_task_does_not_run() {
        let (tx, rx) = publish_channel();

        for message in numbered_messages(MAX_PENDING_ENTRIES + 1) {
            tx.send(message).unwrap();
        }

        let pending = rx.pending.lock().unwrap();
        let oldest = pending.buffer.front().unwrap().message.source.as_ref();
        assert_eq!(pending.buffer.len(), MAX_PENDING_ENTRIES);
        assert_eq!(pending.discarded, 1);
        assert_eq!(oldest.unwrap().sequence, 2);
    }

    /// A payload's owner that records, when the payload is destroyed, how many messages were then
    /// waiting in `pending`, or `None` when another holder had them locked. It never panics, so a
    /// failing test unwinds through it.
    struct DestroyProbe {
        pending: std::sync::Weak<Mutex<PendingEntries>>,
        seen: Arc<Mutex<Vec<Option<usize>>>>,
    }

    impl AsRef<[u8]> for DestroyProbe {
        fn as_ref(&self) -> &[u8] {
            b"probe"
        }
    }

    impl Drop for DestroyProbe {
        fn drop(&mut self) {
            let waiting = self
                .pending
                .upgrade()
                .and_then(|pending| pending.try_lock().ok().map(|entries| entries.buffer.len()));
            self.seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(waiting);
        }
    }

    #[rstest]
    fn test_publish_sender_destroys_the_message_it_drops_after_its_hold() {
        let (tx, _rx) = publish_channel();
        let seen = Arc::new(Mutex::new(Vec::new()));

        for mut message in numbered_messages(MAX_PENDING_ENTRIES + 1) {
            message.payload = Bytes::from_owner(DestroyProbe {
                pending: Arc::downgrade(&tx.pending),
                seen: Arc::clone(&seen),
            });
            tx.send(message).unwrap();
        }

        let destroyed = seen.lock().unwrap().clone();
        assert_eq!(destroyed, [Some(MAX_PENDING_ENTRIES)]);
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_expires_a_batch_per_hold_and_destroys_it_after_the_hold() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.available.store(false, Ordering::SeqCst);
        let mut clock = TaskClock::writing();
        let (tx, rx) = publish_channel();
        let seen = Arc::new(Mutex::new(Vec::new()));

        for mut message in numbered_messages(MAX_PENDING_ENTRIES) {
            message.payload = Bytes::from_owner(DestroyProbe {
                pending: Arc::downgrade(&tx.pending),
                seen: Arc::clone(&seen),
            });
            tx.send(message).unwrap();
        }
        let handle = spawn_publisher(&fake, rx, false, None);
        clock.drive(MAX_PENDING_AGE + Duration::from_secs(1)).await;
        let destroyed = seen.lock().unwrap().clone();
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        // Every message one hold took out was destroyed after it, seeing the same messages waiting
        let per_hold: Vec<usize> = destroyed.chunk_by(|a, b| a == b).map(<[_]>::len).collect();
        assert!(result.is_ok());
        assert_eq!(destroyed.len(), MAX_PENDING_ENTRIES);
        assert!(
            destroyed.iter().all(Option::is_some),
            "{} destroyed under the lock",
            destroyed.iter().filter(|waiting| waiting.is_none()).count()
        );
        assert_eq!(per_hold.len(), MAX_PENDING_ENTRIES / PUBLISH_BATCH_SIZE);
        assert!(per_hold.iter().all(|&taken| taken == PUBLISH_BATCH_SIZE));
        assert_eq!(destroyed.last(), Some(&Some(0)));
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_stays_degraded_while_admission_drops_messages_between_writes() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        fake.execs_allowed.store(1, Ordering::SeqCst);
        let (tx, rx) = publish_channel();
        let mut messages =
            numbered_messages(MAX_PENDING_ENTRIES + 20 * PUBLISH_BATCH_SIZE).into_iter();
        let mut admitted = 0;
        let mut admit = |tx: &PublishSender| {
            tx.send(messages.next().unwrap()).unwrap();
            admitted += 1;
        };

        // The bound is full before the task runs, and before each of the next ten writes is
        // acknowledged admission drops a message again
        for _ in 0..=MAX_PENDING_ENTRIES {
            admit(&tx);
        }
        let handle = spawn_publisher(&fake, rx, false, None);
        for _ in 0..10 {
            settle_until(|| fake.exec_waiting.load(Ordering::SeqCst)).await;
            while tx.pending.lock().unwrap().discarded == 0 {
                admit(&tx);
            }
            fake.exec_waiting.store(false, Ordering::SeqCst);
            fake.execs_allowed.store(1, Ordering::SeqCst);
        }
        settle_until(|| fake.exec_waiting.load(Ordering::SeqCst)).await;
        let written_while_overloaded = fake.written_len();
        let records_while_overloaded = state_records(&thread_logs());
        fake.execs_allowed.store(usize::MAX, Ordering::SeqCst);
        settle_until(|| state_records(&thread_logs()).len() == 2).await;
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        assert!(result.is_ok());
        assert_eq!(written_while_overloaded, 11 * PUBLISH_BATCH_SIZE);
        assert_eq!(
            records_while_overloaded,
            [("degraded".to_string(), false, 1, 0)]
        );
        assert_eq!(
            fake.write_records(&logs),
            [
                ("degraded".to_string(), false, 1, 0),
                (
                    "connected".to_string(),
                    true,
                    admitted - fake.written_len(),
                    0
                ),
            ]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_drops_an_entry_admitted_past_the_age_bound_before_the_task_runs()
    {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        let (tx, rx) = publish_channel();
        let messages = numbered_messages(2);

        tx.send(messages[0].clone()).unwrap();
        drive(Duration::from_secs(181)).await;
        tx.send(messages[1].clone()).unwrap();
        let handle = spawn_publisher(&fake, rx, false, None);
        drive_until(|| fake.written_len() > 0, Duration::from_secs(5)).await;
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        assert!(result.is_ok());
        assert_eq!(fake.written_sequences(), [2]);
        assert_eq!(
            fake.write_records(&logs),
            [
                ("degraded".to_string(), false, 1, 0),
                ("connected".to_string(), true, 1, 0),
            ]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_connects_again_once_a_second_while_redis_is_out_of_memory() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake =
            FakeRedis::start("OOM command not allowed when used memory > 'maxmemory'.").await;
        let (tx, handle) = start_publisher(&fake);
        let mut refused_by_ten_seconds = 0;
        let mut connections_by_ten_seconds = 0;

        // One entry every 100 ms for 60 s
        for (i, message) in numbered_messages(600).into_iter().enumerate() {
            if i == 100 {
                refused_by_ten_seconds = fake.aborted.load(Ordering::SeqCst);
                connections_by_ten_seconds = fake.connections.lock().unwrap().len();
            }
            tx.send(message).unwrap();
            drive(Duration::from_millis(100)).await;
        }
        drive(Duration::from_secs(2)).await;
        let refused = fake.aborted.load(Ordering::SeqCst);
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let dropped: Vec<usize> = logs
            .iter()
            .filter_map(|(level, line)| {
                let rest = line.strip_prefix("Dropped a batch of ")?;
                assert_eq!(*level, log::Level::Warn, "{line}");
                Some(rest.split(' ').next().unwrap().parse().unwrap())
            })
            .collect();
        let connections = fake.connections.lock().unwrap().len() - connections_by_ten_seconds;
        let warnings = logs
            .iter()
            .filter(|(level, _)| *level == log::Level::Warn)
            .count();
        assert!(result.is_ok());
        // Each refusal costs the connection, and the next write connects again a second later
        assert!(
            connections <= 53,
            "{connections} connections from 10 s to 62 s"
        );
        assert!(
            connections >= 40,
            "{connections} connections from 10 s to 62 s"
        );
        assert!(
            refused - refused_by_ten_seconds <= 52,
            "{} writes from 10 s to 62 s",
            refused - refused_by_ten_seconds
        );
        assert_eq!(dropped.len(), refused);
        // One line for each refusal, and the one record
        assert_eq!(warnings, refused + 1);
        assert_eq!(dropped.iter().sum::<usize>(), 600);
        assert_eq!(
            fake.write_records(&logs),
            [("degraded".to_string(), false, 1, 0)]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_connects_again_after_a_demoted_primary_refuses_a_batch() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("READONLY You can't write against a read only replica.").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        // The first connection stays open on the old primary, now a replica refusing every batch
        fake.demoted_connections.store(1, Ordering::SeqCst);
        let (tx, handle) = start_publisher(&fake);
        let messages = numbered_messages(2);

        tx.send(messages[0].clone()).unwrap();
        settle_until(|| fake.aborted.load(Ordering::SeqCst) == 1).await;
        let refused_at = tokio::time::Instant::now();
        tx.send(messages[1].clone()).unwrap();
        drive_until(|| fake.written_len() == 1, Duration::from_secs(5)).await;
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let connections = fake.connections.lock().unwrap().clone();
        assert!(result.is_ok());
        assert_eq!(fake.aborted.load(Ordering::SeqCst), 1);
        assert_eq!(fake.written_sequences(), [2]);
        assert_eq!(connections.len(), 2);
        assert!(connections[1] - refused_at <= MAX_PUBLISH_RETRY_DELAY);
        assert_eq!(
            fake.write_records(&logs),
            [
                ("degraded".to_string(), false, 1, 0),
                ("connected".to_string(), true, 1, 0),
            ]
        );
        assert!(logs.iter().any(|(level, line)| *level == log::Level::Warn
            && line.starts_with("Dropped a batch of 1 stream entries Redis refused")
            && line.contains("You can't write against a read only replica")));
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_keeps_the_connection_while_redis_refuses_a_wrong_type_key() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake =
            FakeRedis::start("WRONGTYPE Operation against a key holding the wrong kind of value")
                .await;
        let (tx, handle) = start_publisher(&fake);

        // One entry every 100 ms for 10 s
        for message in numbered_messages(100) {
            tx.send(message).unwrap();
            drive(Duration::from_millis(100)).await;
        }
        drive(Duration::from_secs(2)).await;
        let refused = fake.aborted.load(Ordering::SeqCst);
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let errors = error_lines(&logs);
        assert!(result.is_ok());
        assert!(refused >= 5, "{refused} refused writes");
        assert_eq!(fake.connections.lock().unwrap().len(), 1);
        assert_eq!(fake.written_len(), 0);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].starts_with("Dropped a batch of "), "{errors:?}");
        assert_eq!(
            fake.write_records(&logs),
            [("degraded".to_string(), false, 1, 0)]
        );
    }

    #[rstest]
    #[case::aborted_before_execution(
        None,
        "Dropped a batch of 2 stream entries Redis refused",
        log::Level::Warn,
        (2, 0),
        &[]
    )]
    #[case::executed_in_part(
        Some("trader-TESTER-007:stream:data.b"),
        "Dropped a batch of 2 stream entries whose transaction failed, which may have been written in part",
        log::Level::Error,
        (0, 2),
        &["trader-TESTER-007:stream:data.a"]
    )]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_counts_a_failed_transaction_discarded_only_when_none_executed(
        #[case] wrong_type_key: Option<&str>,
        #[case] dropped_line: &str,
        #[case] dropped_level: log::Level,
        #[case] counts: (usize, usize),
        #[case] written_by_the_batch: &[&str],
    ) {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake =
            FakeRedis::start("OOM command not allowed when used memory > 'maxmemory'.").await;
        fake.accept_writes
            .store(wrong_type_key.is_some(), Ordering::SeqCst);
        *fake.wrong_type_key.lock().unwrap() = wrong_type_key.map(str::to_string);
        let (tx, rx) = publish_channel();
        let handle = spawn_publisher(&fake, rx, true, None);
        let message = |topic: &str| {
            BusMessage::with_str_topic(
                topic,
                BusPayloadType::QuoteTick,
                Bytes::from("test_payload"),
                SerializationEncoding::Json,
            )
        };

        // Admitted before the task runs, so both are one batch
        tx.send(message("data.a")).unwrap();
        tx.send(message("data.b")).unwrap();
        drive_until(
            || fake.aborted.load(Ordering::SeqCst) + fake.written_len() > 0,
            Duration::from_secs(5),
        )
        .await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        tx.send(message("data.a")).unwrap();
        drive_until(
            || fake.committed.load(Ordering::SeqCst) == 1,
            Duration::from_secs(5),
        )
        .await;
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let written: Vec<String> = fake.written().into_iter().map(|xadd| xadd.key).collect();
        let mut expected_written: Vec<&str> = written_by_the_batch.to_vec();
        expected_written.push("trader-TESTER-007:stream:data.a");
        let (discarded, uncertain) = counts;
        assert!(result.is_ok());
        assert_eq!(written, expected_written);
        assert_eq!(
            fake.write_records(&logs),
            [
                ("degraded".to_string(), false, discarded, uncertain),
                ("connected".to_string(), true, discarded, uncertain),
            ]
        );
        assert!(
            logs.iter()
                .any(|(level, line)| *level == dropped_level && line.starts_with(dropped_line)),
            "{logs:?}"
        );
        assert_eq!(
            error_lines(&logs).len(),
            usize::from(dropped_level == log::Level::Error)
        );
    }

    #[rstest]
    #[case::read_only("READONLY You can't write against a read only replica.")]
    #[case::misconf(
        "MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk."
    )]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_writes_the_batch_and_warns_when_redis_cannot_trim_now(
        #[case] trim_error: &'static str,
    ) {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        *fake.trim_error.lock().unwrap() = Some(trim_error);
        let (tx, rx) = publish_channel();
        let handle = spawn_publisher(&fake, rx, false, Some(60));

        for (written, message) in numbered_messages(2).into_iter().enumerate() {
            tx.send(message).unwrap();
            drive_until(|| fake.written_len() == written + 1, Duration::from_secs(5)).await;
        }
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        // The redis crate names a reply it knows by its kind, so the line is matched by its detail
        let (_, detail) = trim_error.split_once(' ').unwrap();
        let trim_warnings = logs
            .iter()
            .filter(|(level, line)| {
                *level == log::Level::Warn
                    && line.starts_with("Error trimming stream 'trader-TESTER-007:stream'")
                    && line.contains(detail)
            })
            .count();
        assert!(result.is_ok());
        assert_eq!(fake.written_sequences(), [1, 2]);
        assert_eq!(fake.trims.load(Ordering::SeqCst), 2);
        assert_eq!(trim_warnings, 2);
        assert_eq!(
            fake.write_records(&logs),
            [("connected".to_string(), true, 0, 0)]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_messages_reports_a_wrong_type_trim_key_once_at_error_level() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        *fake.trim_error.lock().unwrap() =
            Some("WRONGTYPE Operation against a key holding the wrong kind of value");
        let (tx, rx) = publish_channel();
        let handle = spawn_publisher(&fake, rx, false, Some(60));

        for (written, message) in numbered_messages(10).into_iter().enumerate() {
            tx.send(message).unwrap();
            drive_until(|| fake.written_len() == written + 1, Duration::from_secs(5)).await;
        }
        tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let errors = error_lines(&logs);
        assert!(result.is_ok());
        assert_eq!(fake.written_sequences(), (1..=10).collect::<Vec<u64>>());
        assert_eq!(fake.trims.load(Ordering::SeqCst), 10);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(
            errors[0].starts_with("Error trimming stream 'trader-TESTER-007:stream'")
                && errors[0].contains("WRONGTYPE"),
            "{errors:?}"
        );
        assert_eq!(
            fake.write_records(&logs),
            [("connected".to_string(), true, 0, 0)]
        );
    }

    #[rstest]
    fn test_external_io_from_backing_takes_stream_receiver() {
        let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<BusMessage>(1);
        let backing = backing_with_stream_receiver(stream_rx);
        let message = BusMessage::with_str_topic(
            "events/data",
            BusPayloadType::QuoteTick,
            Bytes::from_static(b"payload"),
            SerializationEncoding::Json,
        );

        let (_egress, mut ingress) = external_io_from_backing(Box::new(backing));
        stream_tx.try_send(message.clone()).unwrap();
        let mut receiver = ingress.take_receiver().unwrap();
        let received = receiver.try_recv().unwrap();

        assert_eq!(received.topic, message.topic);
        assert_eq!(received.payload, message.payload);
        assert!(ingress.take_receiver().is_err());
    }

    #[rstest]
    #[case::one_key(false, ["trader-TESTER-007:stream"; 3], [1, 2, 3])]
    #[case::key_per_topic(
        true,
        [
            "trader-TESTER-007:stream:data.quotes.A",
            "trader-TESTER-007:stream:data.quotes.A",
            "trader-TESTER-007:stream:data.quotes.B",
        ],
        [1, 2, 1]
    )]
    fn test_way_out_numbers_entries_from_one_per_actual_key(
        #[case] stream_per_topic: bool,
        #[case] expected_keys: [&str; 3],
        #[case] expected_sequences: [u64; 3],
    ) {
        let fake = get_runtime().block_on(FakeRedis::start("unused"));
        fake.accept_writes.store(true, Ordering::SeqCst);
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            stream_per_topic,
            ..Default::default()
        };
        install_way_out(&fake, instance_id, config);

        publish_quote("data.quotes.A".into(), &QuoteTick::default());
        publish_quote("data.quotes.A".into(), &QuoteTick::default());
        publish_quote("data.quotes.B".into(), &QuoteTick::default());
        wait_until(|| fake.written().len() == 3, Duration::from_secs(5));
        close_way_out();

        let written = fake.written();
        let keys: Vec<&str> = written.iter().map(|xadd| xadd.key.as_str()).collect();
        let topics: Vec<Option<String>> = written.iter().map(|xadd| xadd.field("topic")).collect();
        let sequences: Vec<u64> = written.iter().map(FakeXadd::sequence).collect();
        assert_eq!(keys, expected_keys);
        assert_eq!(
            topics,
            ["data.quotes.A", "data.quotes.A", "data.quotes.B"]
                .map(|topic| Some(topic.to_string()))
        );
        assert_eq!(sequences, expected_sequences);
        for xadd in &written {
            assert_eq!(
                xadd.field(PUBLISHER_ID_FIELD).as_deref(),
                Some("TESTER-007")
            );
            assert_eq!(
                xadd.field(INSTANCE_ID_FIELD).as_deref(),
                Some(instance_id.as_str())
            );
        }
    }

    #[rstest]
    fn test_way_out_leaves_a_gap_for_a_publication_that_cannot_be_encoded() {
        let fake = get_runtime().block_on(FakeRedis::start("unused"));
        fake.accept_writes.store(true, Ordering::SeqCst);
        let config = MessageBusConfig {
            stream_per_topic: false,
            ..Default::default()
        };
        install_way_out(&fake, UUID4::new(), config);
        let unencodable = CustomData::new(
            Arc::new(UnencodableData),
            DataType::new("UnencodableData", None, None),
        );

        publish_quote("data.quotes.A".into(), &QuoteTick::default());
        publish_any("data.custom".into(), &unencodable);
        publish_quote("data.quotes.A".into(), &QuoteTick::default());
        wait_until(|| fake.written().len() == 2, Duration::from_secs(5));
        close_way_out();

        let written = fake.written();
        let sequences: Vec<u64> = written.iter().map(FakeXadd::sequence).collect();
        assert_eq!(sequences, [1, 3]);
        assert!(
            written
                .iter()
                .all(|xadd| xadd.field("topic").as_deref() == Some("data.quotes.A"))
        );
    }

    // The outer publication is numbered 1 and the one its encoding makes is refused before it is
    // numbered, so the next publication of the nested topic takes the next number of its key
    #[rstest]
    #[case::same_stream(false, "data.nested.same", "data.nested.same", &[1, 2])]
    #[case::another_stream(true, "data.nested.outer", "data.nested.other", &[1, 1])]
    fn test_way_out_refuses_a_publication_made_while_encoding_a_numbered_one(
        #[case] stream_per_topic: bool,
        #[case] outer: &'static str,
        #[case] nested: &'static str,
        #[case] expected_sequences: &[u64],
    ) {
        capture_logs();
        let fake = get_runtime().block_on(FakeRedis::start("unused"));
        fake.accept_writes.store(true, Ordering::SeqCst);
        let config = MessageBusConfig {
            stream_per_topic,
            ..Default::default()
        };
        install_way_out(&fake, UUID4::new(), config);

        publish_any(outer.into(), &NestingData::nesting(nested).custom());
        publish_quote(nested.into(), &QuoteTick::default());
        wait_until(|| fake.written_len() >= 2, Duration::from_secs(5));
        close_way_out();

        let written = fake.written();
        let topics: Vec<Option<String>> = written.iter().map(|xadd| xadd.field("topic")).collect();
        assert_eq!(fake.written_sequences(), expected_sequences);
        assert_eq!(topics, [Some(outer.to_string()), Some(nested.to_string())]);
        assert_eq!(nesting_depth(&written[0]), Some(1));
        assert_eq!(error_lines(&thread_logs()), [refusal_line(outer, nested)]);
    }

    // A handler of the refused publication publishes again, still inside the outer encoding
    #[rstest]
    fn test_way_out_refuses_every_publication_made_while_encoding_a_numbered_one() {
        capture_logs();
        let fake = get_runtime().block_on(FakeRedis::start("unused"));
        fake.accept_writes.store(true, Ordering::SeqCst);
        let config = MessageBusConfig {
            stream_per_topic: false,
            ..Default::default()
        };
        install_way_out(&fake, UUID4::new(), config);
        subscribe_any(
            "data.three.b".into(),
            ShareableMessageHandler::from_any(|_: &dyn std::any::Any| {
                publish_quote("data.three.c".into(), &QuoteTick::default());
            }),
            None,
        );

        publish_any(
            "data.three.a".into(),
            &NestingData::nesting("data.three.b").custom(),
        );
        publish_quote("data.three.a".into(), &QuoteTick::default());
        wait_until(|| fake.written_len() >= 2, Duration::from_secs(5));
        close_way_out();

        assert_eq!(fake.written_sequences(), [1, 2]);
        assert_eq!(nesting_depth(&fake.written()[0]), Some(1));
        assert_eq!(
            error_lines(&thread_logs()),
            [
                refusal_line("data.three.a", "data.three.c"),
                refusal_line("data.three.a", "data.three.b"),
            ]
        );
    }

    // Number 1's encoding has its nested publication refused, then fails: 1 is a hole, and the
    // next encoding is numbered and refuses its own nested publication again
    #[rstest]
    fn test_way_out_ends_the_refusal_with_an_encoding_that_fails() {
        capture_logs();
        let fake = get_runtime().block_on(FakeRedis::start("unused"));
        fake.accept_writes.store(true, Ordering::SeqCst);
        let config = MessageBusConfig {
            stream_per_topic: false,
            ..Default::default()
        };
        install_way_out(&fake, UUID4::new(), config);
        let nesting = NestingData::nesting("data.failing");

        let failing = NestingData {
            fails: true,
            ..nesting
        };
        publish_any("data.failing".into(), &failing.custom());
        publish_any("data.failing".into(), &nesting.custom());
        publish_quote("data.failing".into(), &QuoteTick::default());
        wait_until(|| fake.written_len() >= 2, Duration::from_secs(5));
        close_way_out();

        let refusals: Vec<String> = error_lines(&thread_logs())
            .into_iter()
            .filter(|line| line.starts_with("Refused"))
            .collect();
        assert_eq!(fake.written_sequences(), [2, 3]);
        assert_eq!(nesting_depth(&fake.written()[0]), Some(1));
        assert_eq!(refusals, [refusal_line("data.failing", "data.failing")]);
    }

    #[rstest]
    fn test_way_out_logs_the_first_refusal_of_a_pair_of_topics_only() {
        capture_logs();
        let fake = get_runtime().block_on(FakeRedis::start("unused"));
        fake.accept_writes.store(true, Ordering::SeqCst);
        let config = MessageBusConfig {
            stream_per_topic: false,
            ..Default::default()
        };
        install_way_out(&fake, UUID4::new(), config);

        let many = NestingData {
            publications: 1_000,
            ..NestingData::nesting("data.many")
        };
        publish_any("data.many".into(), &many.custom());
        publish_quote("data.many".into(), &QuoteTick::default());
        wait_until(|| fake.written_len() >= 2, Duration::from_secs(5));
        close_way_out();

        assert_eq!(fake.written_sequences(), [1, 2]);
        assert_eq!(
            error_lines(&thread_logs()),
            [refusal_line("data.many", "data.many")]
        );
    }

    // The heartbeat's first tick is at once, and two more fall within a 2.5 s encoding
    #[rstest]
    fn test_way_out_takes_heartbeats_while_the_message_bus_encodes() {
        capture_logs();
        let fake = get_runtime().block_on(FakeRedis::start("unused"));
        fake.accept_writes.store(true, Ordering::SeqCst);
        let config = MessageBusConfig {
            stream_per_topic: true,
            heartbeat_interval_secs: Some(1),
            ..Default::default()
        };
        install_way_out(&fake, UUID4::new(), config);
        let is_slow = |xadd: &FakeXadd| xadd.field("topic").as_deref() == Some("data.slow");

        wait_until(|| fake.written_len() == 1, Duration::from_secs(5));
        let slow = NestingData {
            publications: 0,
            encoding_time: Duration::from_millis(2_500),
            ..NestingData::nesting("data.slow")
        };
        publish_any("data.slow".into(), &slow.custom());
        wait_until(
            || fake.written().iter().any(is_slow),
            Duration::from_secs(5),
        );
        close_way_out();

        let written = fake.written();
        let slow_at = written.iter().position(is_slow).unwrap();
        let heartbeats: Vec<u64> = written
            .iter()
            .filter(|xadd| !is_slow(xadd))
            .map(FakeXadd::sequence)
            .collect();
        assert!(slow_at >= 3, "{slow_at}");
        assert_eq!(written[slow_at].sequence(), 1);
        assert_eq!(
            heartbeats,
            (1..=heartbeats.len() as u64).collect::<Vec<u64>>()
        );
        assert_eq!(error_lines(&thread_logs()), Vec::<String>::new());
    }

    // Reserved at 0 s, its encoding ends and it is admitted at 181 s
    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_backing_drops_an_entry_reserved_past_the_age_bound_when_it_arrives() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        let (backing, handle) = backing_publishing_to(&fake, MessageBusConfig::default());

        let late = numbered_by(&backing, "data.quotes.A");
        drive(Duration::from_secs(181)).await;
        backing.publish(late);
        backing.publish(numbered_by(&backing, "data.quotes.A"));
        drive_until(|| fake.written_len() >= 1, Duration::from_secs(5)).await;
        backing.pub_tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        assert!(result.is_ok());
        assert_eq!(fake.written_sequences(), [2]);
        assert_eq!(
            fake.write_records(&logs),
            [("degraded".to_string(), false, 1, 0)]
        );
    }

    // The bus's entry is reserved at 0 s and admitted at 179 s behind a heartbeat admitted at
    // 100 s; the write at 200 s takes the heartbeat and drops the bus's entry
    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_task_drops_an_entry_past_the_age_bound_behind_a_younger_one() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        let config = MessageBusConfig {
            buffer_interval_ms: Some(200_000),
            ..Default::default()
        };
        let (backing, handle) = backing_publishing_to(&fake, config);

        let bus = numbered_by(&backing, "data.quotes.A");
        drive(Duration::from_secs(100)).await;
        backing.pub_tx.send(heartbeat_of(&backing)).unwrap();
        drive(Duration::from_secs(79)).await;
        backing.publish(bus);
        drive(Duration::from_secs(30)).await;
        backing.pub_tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let topics: Vec<Option<String>> = fake
            .written()
            .iter()
            .map(|xadd| xadd.field("topic"))
            .collect();
        assert!(result.is_ok());
        assert_eq!(topics, [Some(HEARTBEAT_TOPIC.to_string())]);
        assert_eq!(
            fake.write_records(&logs),
            [
                ("degraded".to_string(), false, 1, 0),
                ("connected".to_string(), true, 1, 0),
            ]
        );
    }

    // A batch taken at 130 s holds a heartbeat admitted at 100 s and the bus's entry reserved at
    // 0 s and admitted at 129 s; Redis takes no write until 185 s
    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_publish_task_abandons_a_batch_at_the_first_age_bound_of_its_entries() {
        capture_logs();
        let _clock = ManualClock::hold();
        let fake = FakeRedis::start("LOADING Redis is loading the dataset in memory").await;
        let config = MessageBusConfig {
            buffer_interval_ms: Some(130_000),
            ..Default::default()
        };
        let (backing, handle) = backing_publishing_to(&fake, config);

        let bus = numbered_by(&backing, "data.quotes.A");
        drive(Duration::from_secs(100)).await;
        backing.pub_tx.send(heartbeat_of(&backing)).unwrap();
        drive(Duration::from_secs(29)).await;
        backing.publish(bus);
        drive(Duration::from_secs(56)).await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        drive(Duration::from_secs(5)).await;
        backing.pub_tx.send(BusMessage::new_close()).unwrap();
        settle_until(|| handle.is_finished()).await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        assert!(result.is_ok());
        assert_eq!(fake.written_len(), 0);
        assert!(logs.iter().any(|(level, line)| *level == log::Level::Warn
            && line.starts_with("Abandoned a batch of 2 stream entries")));
    }

    #[rstest]
    #[case::heartbeats_on(Some(1), &[HEARTBEAT_TOPIC, "data.quotes.A"])]
    #[case::heartbeats_off(None, &["data.quotes.A"])]
    fn test_way_out_numbers_a_heartbeat_on_its_own_key(
        #[case] heartbeat_interval_secs: Option<u16>,
        #[case] expected_topics: &[&str],
    ) {
        let fake = get_runtime().block_on(FakeRedis::start("unused"));
        fake.accept_writes.store(true, Ordering::SeqCst);
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            stream_per_topic: true,
            heartbeat_interval_secs,
            ..Default::default()
        };
        install_way_out(&fake, instance_id, config);

        // The heartbeat's first tick is at once
        wait_until(
            || fake.written_len() == expected_topics.len() - 1,
            Duration::from_secs(5),
        );
        publish_quote("data.quotes.A".into(), &QuoteTick::default());
        wait_until(
            || fake.written_len() >= expected_topics.len(),
            Duration::from_secs(5),
        );
        close_way_out();

        // A later heartbeat may follow before the close
        let written = fake.written();
        let topics: Vec<Option<String>> = written[..expected_topics.len()]
            .iter()
            .map(|xadd| xadd.field("topic"))
            .collect();
        assert_eq!(
            topics,
            expected_topics
                .iter()
                .map(|topic| Some((*topic).to_string()))
                .collect::<Vec<_>>()
        );
        // Each topic is numbered from 1 on its own key
        let mut last_sequences: HashMap<String, u64> = HashMap::new();
        for xadd in &written {
            let last = last_sequences.entry(xadd.key.clone()).or_default();
            *last += 1;
            assert_eq!(xadd.sequence(), *last, "{}", xadd.key);
            assert_eq!(
                xadd.key,
                format!("trader-TESTER-007:stream:{}", xadd.field("topic").unwrap())
            );
            assert_eq!(
                xadd.field(PUBLISHER_ID_FIELD).as_deref(),
                Some("TESTER-007")
            );
            assert_eq!(
                xadd.field(INSTANCE_ID_FIELD).as_deref(),
                Some(instance_id.as_str())
            );
        }
    }

    #[rstest]
    fn test_way_out_entry_keeps_its_number_when_its_batch_is_retried() {
        let fake = get_runtime().block_on(FakeRedis::start(
            "LOADING Redis is loading the dataset in memory",
        ));
        let config = MessageBusConfig {
            stream_per_topic: false,
            ..Default::default()
        };
        install_way_out(&fake, UUID4::new(), config);

        publish_quote("data.quotes.A".into(), &QuoteTick::default());
        wait_until(
            || fake.aborted.load(Ordering::SeqCst) > 0,
            Duration::from_secs(5),
        );
        publish_quote("data.quotes.B".into(), &QuoteTick::default());
        fake.accept_writes.store(true, Ordering::SeqCst);
        wait_until(|| fake.written().len() == 2, Duration::from_secs(5));
        close_way_out();

        let written = fake.written();
        let first_attempts: Vec<u64> = fake
            .attempted
            .lock()
            .unwrap()
            .iter()
            .filter(|xadd| xadd.field("topic").as_deref() == Some("data.quotes.A"))
            .map(FakeXadd::sequence)
            .collect();
        let sequences: Vec<u64> = written.iter().map(FakeXadd::sequence).collect();
        assert!(first_attempts.len() >= 2);
        assert!(first_attempts.iter().all(|&sequence| sequence == 1));
        assert_eq!(sequences, [1, 2]);
        assert_eq!(written[0].field("topic").as_deref(), Some("data.quotes.A"));
    }

    #[rstest]
    fn test_new_backing_numbers_from_one_under_its_own_instance() {
        let fake = get_runtime().block_on(FakeRedis::start("unused"));
        let trader_id = TraderId::from("TESTER-007");
        let config = MessageBusConfig {
            stream_per_topic: false,
            ..Default::default()
        };
        let (first_instance, second_instance) = (UUID4::new(), UUID4::new());
        let mut first =
            RedisMessageBusBacking::new(trader_id, first_instance, config.clone(), fake.config())
                .unwrap();
        let topic = Ustr::from("data.quotes.A");

        let first_sources = [
            first.reserve_source(topic).unwrap().unwrap(),
            first.reserve_source(topic).unwrap().unwrap(),
        ];
        first.close();
        let second =
            RedisMessageBusBacking::new(trader_id, second_instance, config, fake.config()).unwrap();
        let second_source = second.reserve_source(topic).unwrap().unwrap();

        let stream = Ustr::from("trader-TESTER-007:stream");
        assert_eq!(
            first_sources,
            [1, 2].map(|sequence| BusMessageSource {
                publisher_id: trader_id,
                instance_id: first_instance,
                stream,
                sequence,
            })
        );
        assert_eq!(
            second_source,
            BusMessageSource {
                publisher_id: trader_id,
                instance_id: second_instance,
                stream,
                sequence: 1,
            }
        );
    }

    #[rstest]
    fn test_stream_sources_refuse_numbers_that_run_out() {
        let sources = StreamSources::new(
            TraderId::from("TESTER-007"),
            UUID4::new(),
            &MessageBusConfig {
                stream_per_topic: false,
                ..Default::default()
            },
        );
        let topic = Ustr::from("data.quotes.A");
        sources
            .last_sequences
            .lock()
            .unwrap()
            .insert(Ustr::from("trader-TESTER-007:stream"), u64::MAX - 1);

        let last = sources.reserve(topic).unwrap();
        let refused = sources.reserve(topic).unwrap_err();
        let refused_again = sources.reserve(topic).unwrap_err();

        assert_eq!(last.sequence, u64::MAX);
        assert_eq!(
            refused.to_string(),
            "Stream 'trader-TESTER-007:stream' has no sequence number left"
        );
        assert_eq!(refused_again.to_string(), refused.to_string());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_stream_entries_writes_the_entries_of_the_backing_on_an_explicit_key() {
        let fake = FakeRedis::start("unused").await;
        fake.accept_writes.store(true, Ordering::SeqCst);
        let mut con = create_redis_connection("test", &fake.config())
            .await
            .unwrap();
        let sources = StreamSources::new(
            TraderId::from("READER-007"),
            UUID4::new(),
            &MessageBusConfig {
                stream_per_topic: false,
                ..Default::default()
            },
        );
        let messages: Vec<BusMessage> = ["asks.first", "asks.second"]
            .map(|topic| {
                let mut message = BusMessage::with_str_topic(
                    topic,
                    BusPayloadType::RequestCommand,
                    Bytes::from(topic),
                    SerializationEncoding::MsgPack,
                );
                message.source = Some(sources.reserve(Ustr::from(topic)).unwrap());
                message
            })
            .to_vec();

        write_stream_entries(&mut con, "trader-DATA-007:asks", Some(100_000), &messages)
            .await
            .unwrap();
        write_stream_entries(&mut con, "trader-DATA-007:asks", None, &messages[1..])
            .await
            .unwrap();

        let written = fake.written();
        let expected_fields = |message: &BusMessage| {
            let source = message.source.unwrap();
            vec![
                ("topic".to_string(), message.topic.as_bytes().to_vec()),
                ("type".to_string(), b"RequestCommand".to_vec()),
                ("payload".to_string(), message.payload.to_vec()),
                ("encoding".to_string(), b"MsgPack".to_vec()),
                (
                    PAYLOAD_KIND_FIELD.to_string(),
                    PAYLOAD_KIND_TYPED.as_bytes().to_vec(),
                ),
                (PUBLISHER_ID_FIELD.to_string(), b"READER-007".to_vec()),
                (
                    INSTANCE_ID_FIELD.to_string(),
                    source.instance_id.as_str().as_bytes().to_vec(),
                ),
                (
                    SEQUENCE_FIELD.to_string(),
                    source.sequence.to_string().into_bytes(),
                ),
            ]
        };
        assert_eq!(written.len(), 3);
        assert!(
            written
                .iter()
                .all(|xadd| xadd.key == "trader-DATA-007:asks")
        );
        assert_eq!(
            written
                .iter()
                .map(|xadd| xadd.maxlen.as_deref())
                .collect::<Vec<_>>(),
            [Some("100000"), Some("100000"), None]
        );
        assert_eq!(written[0].fields, expected_fields(&messages[0]));
        assert_eq!(written[1].fields, expected_fields(&messages[1]));
        assert_eq!(written[2].fields, expected_fields(&messages[1]));
        assert_eq!(
            written.iter().map(FakeXadd::sequence).collect::<Vec<_>>(),
            [1, 2, 2]
        );
    }

    #[rstest]
    fn test_decode_stream_entry_reads_the_source_written_and_its_key() {
        let mut message = BusMessage::with_str_topic(
            "data.quotes.A",
            BusPayloadType::QuoteTick,
            Bytes::from_static(b"quote"),
            SerializationEncoding::Json,
        );
        let source = BusMessageSource {
            publisher_id: TraderId::from("TESTER-007"),
            instance_id: UUID4::new(),
            stream: Ustr::from("trader-TESTER-007:stream"),
            sequence: 37,
        };
        message.source = Some(source);
        let fields = bus_message_fields(&message, "json").to_redis_args();
        let stream_msg = Value::Array(fields.into_iter().map(Value::BulkString).collect());

        let decoded = decode_stream_entry("trader-TESTER-007:stream", &stream_msg).unwrap();
        let read_elsewhere = decode_stream_entry("trader-OTHER-001:stream", &stream_msg).unwrap();

        assert_eq!(decoded.topic, message.topic);
        assert_eq!(decoded.payload_type, message.payload_type);
        assert_eq!(decoded.payload, message.payload);
        assert_eq!(decoded.source, Some(source));
        assert_eq!(
            read_elsewhere.source.unwrap().stream,
            Ustr::from("trader-OTHER-001:stream")
        );
    }

    #[rstest]
    fn test_decode_stream_entry_without_source_fields_has_no_source() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"data.quotes.A".to_vec()),
            Value::BulkString(b"type".to_vec()),
            Value::BulkString(b"QuoteTick".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"quote".to_vec()),
            Value::BulkString(b"encoding".to_vec()),
            Value::BulkString(b"json".to_vec()),
        ]);

        let decoded = decode_stream_entry("trader-TESTER-007:stream", &stream_msg).unwrap();

        assert_eq!(decoded.topic, Ustr::from("data.quotes.A"));
        assert_eq!(decoded.source, None);
    }

    #[rstest]
    #[case::incomplete(&[(SEQUENCE_FIELD, "1")], "Stream message has an incomplete source")]
    #[case::bad_sequence(
        &[(PUBLISHER_ID_FIELD, "TESTER-007"), (INSTANCE_ID_FIELD, FAKE_INSTANCE_ID), (SEQUENCE_FIELD, "-1")],
        "Error parsing sequence"
    )]
    #[case::bad_instance(
        &[(PUBLISHER_ID_FIELD, "TESTER-007"), (INSTANCE_ID_FIELD, "start"), (SEQUENCE_FIELD, "1")],
        "Error parsing instance ID"
    )]
    fn test_decode_stream_entry_rejects_an_invalid_source(
        #[case] source_fields: &[(&str, &str)],
        #[case] expected_error: &str,
    ) {
        let mut fields = vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"data.quotes.A".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"quote".to_vec()),
        ];
        for (field, value) in source_fields {
            fields.push(Value::BulkString(field.as_bytes().to_vec()));
            fields.push(Value::BulkString(value.as_bytes().to_vec()));
        }

        let error = decode_stream_entry("trader-TESTER-007:stream", &Value::Array(fields))
            .unwrap_err()
            .to_string();

        assert!(error.starts_with(expected_error), "{error}");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_fill_each_source_with_the_key_read() {
        let stream_keys = vec![
            "trader-PUBLISHER-007:stream".to_string(),
            "trader-PUBLISHER-007:asks".to_string(),
        ];
        let fake = FakeRedisStreams::start(&stream_keys, 2).await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(10);
        let stream_signal = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn(stream_messages_from(
            tx,
            fake.config(),
            stream_keys.clone(),
            StreamStart::Beginning,
            Arc::clone(&stream_signal),
        ));

        let mut sources = Vec::new();
        for _ in 0..4 {
            let message = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                message.topic.as_str(),
                message.source.unwrap().stream.as_str()
            );
            sources.push(message.source.unwrap());
        }
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap().unwrap();

        sources.sort_by_key(|source| (source.stream, source.sequence));
        let expected: Vec<BusMessageSource> = [
            ("trader-PUBLISHER-007:asks", 1),
            ("trader-PUBLISHER-007:asks", 2),
            ("trader-PUBLISHER-007:stream", 1),
            ("trader-PUBLISHER-007:stream", 2),
        ]
        .map(|(stream, sequence)| BusMessageSource {
            publisher_id: TraderId::from(FAKE_PUBLISHER_ID),
            instance_id: UUID4::from_str(FAKE_INSTANCE_ID).unwrap(),
            stream: Ustr::from(stream),
            sequence,
        })
        .to_vec();
        assert_eq!(sources, expected);
    }

    #[rstest]
    #[tokio::test]
    async fn test_stream_messages_read_on_after_a_read_error_redis_does_not_retry() {
        capture_logs();
        let stream_keys = vec!["stream-0".to_string()];
        let fake = FakeRedisStreams::start(&stream_keys, 1).await;
        let (mut rx, stream_signal, handle) =
            start_reader(&fake, &stream_keys, StreamStart::Beginning);

        let first = receive_payload(&mut rx).await;
        // The key holds the wrong type for two reads, then is repaired
        fake.wrong_type_reads.store(2, Ordering::SeqCst);
        wait_until_async(
            || {
                let wrong_type_reads = Arc::clone(&fake.wrong_type_reads);
                async move { wrong_type_reads.load(Ordering::SeqCst) == 0 }
            },
            Duration::from_secs(5),
        )
        .await;
        fake.append(2, "2");
        let second = receive_payload(&mut rx).await;
        let finished_after_the_error = handle.is_finished();
        stream_signal.store(true, Ordering::Relaxed);
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let errors = error_lines(&logs);
        let wrong_type_at = fake.wrong_type_at.lock().unwrap().clone();
        assert!(result.is_ok());
        assert!(!finished_after_the_error);
        assert_eq!([first, second], ["1", "2"]);
        assert_eq!(
            state_records(&logs),
            [
                ("connected".to_string(), true, 0, 0),
                ("degraded".to_string(), true, 0, 0),
                ("connected".to_string(), true, 0, 0),
            ]
        );
        assert_read_records(&logs);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(
            errors[0].starts_with("Error reading from stream") && errors[0].contains("WRONGTYPE"),
            "{}",
            errors[0]
        );
        assert!(
            !logs.iter().any(|(level, line)| *level == log::Level::Warn
                && line.starts_with("Error reading from stream")),
            "{logs:?}"
        );
        // Read again on its connection, at most once a second
        assert!(wrong_type_at[1] - wrong_type_at[0] >= MAX_PUBLISH_RETRY_DELAY);
        assert_eq!(fake.connections.lock().unwrap().len(), 1);
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_stream_messages_connect_again_within_a_second_through_a_ten_minute_outage() {
        capture_logs();
        let _clock = ManualClock::hold();
        let stream_keys = vec!["stream-0".to_string()];
        let fake = FakeRedisStreams::start(&stream_keys, 1).await;
        let mut clock = TaskClock::reading(&fake);
        let (mut rx, stream_signal, handle) =
            start_reader(&fake, &stream_keys, StreamStart::Beginning);
        let mut received = Vec::new();

        clock
            .drive_until(
                || take_received(&mut rx, &mut received) == 1,
                Duration::from_secs(5),
            )
            .await;
        fake.available.store(false, Ordering::SeqCst);
        let outage_start = tokio::time::Instant::now();
        clock.drive(Duration::from_secs(600)).await;
        fake.append(2, "2");
        let returned_at = tokio::time::Instant::now();
        fake.available.store(true, Ordering::SeqCst);
        clock
            .drive_until(
                || take_received(&mut rx, &mut received) == 2,
                Duration::from_secs(5),
            )
            .await;
        let delivered_at = tokio::time::Instant::now();
        let finished_before_the_stop = handle.is_finished();
        stream_signal.store(true, Ordering::Relaxed);
        clock
            .drive_until(|| handle.is_finished(), Duration::from_secs(5))
            .await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let attempts: Vec<tokio::time::Instant> = fake
            .connections
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|&at| at >= outage_start && at <= returned_at)
            .collect();
        let longest_wait = attempts
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .max()
            .unwrap();
        assert!(result.is_ok());
        assert!(!finished_before_the_stop);
        assert!(attempts.len() >= 500, "{} attempts", attempts.len());
        // The clock moves only while the task waits, so each attempt is timed by the task's own
        // wait; the first follows the read the fake held when the outage began, answered on the
        // clock's next step
        assert!(attempts[0] - outage_start <= MAX_PUBLISH_RETRY_DELAY + CLOCK_STEP);
        assert!(returned_at - *attempts.last().unwrap() <= MAX_PUBLISH_RETRY_DELAY);
        assert!(longest_wait <= MAX_PUBLISH_RETRY_DELAY, "{longest_wait:?}");
        assert!(delivered_at - returned_at <= MAX_PUBLISH_RETRY_DELAY);
        assert_eq!(received, stream_payloads(&stream_keys, &["1", "2"]));
        assert_eq!(
            state_records(&logs),
            [
                ("connected".to_string(), true, 0, 0),
                ("degraded".to_string(), true, 0, 0),
                ("connected".to_string(), true, 0, 0),
            ]
        );
        assert_read_records(&logs);
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_stream_messages_deliver_in_order_when_redis_is_absent_at_start() {
        capture_logs();
        let _clock = ManualClock::hold();
        let stream_keys = vec!["stream-0".to_string()];
        let fake = FakeRedisStreams::start(&stream_keys, 0).await;
        fake.available.store(false, Ordering::SeqCst);
        let mut clock = TaskClock::reading(&fake);
        let (mut rx, stream_signal, handle) =
            start_reader(&fake, &stream_keys, StreamStart::Beginning);
        let mut received = Vec::new();

        clock.drive(Duration::from_secs(30)).await;
        let received_while_absent = take_received(&mut rx, &mut received);
        let finished_while_absent = handle.is_finished();
        for n in 1..=3 {
            fake.append(n, &n.to_string());
        }
        let returned_at = tokio::time::Instant::now();
        fake.available.store(true, Ordering::SeqCst);
        clock
            .drive_until(
                || take_received(&mut rx, &mut received) == 3,
                Duration::from_secs(5),
            )
            .await;
        let delivered_at = tokio::time::Instant::now();
        stream_signal.store(true, Ordering::Relaxed);
        clock
            .drive_until(|| handle.is_finished(), Duration::from_secs(5))
            .await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        assert!(result.is_ok());
        assert_eq!(received_while_absent, 0);
        assert!(!finished_while_absent);
        // The clock moves only while the task waits: the entries arrive at the end of its wait
        assert!(delivered_at - returned_at <= MAX_PUBLISH_RETRY_DELAY);
        assert_eq!(received, stream_payloads(&stream_keys, &["1", "2", "3"]));
        assert_eq!(
            state_records(&logs),
            [
                ("degraded".to_string(), false, 0, 0),
                ("connected".to_string(), true, 0, 0),
            ]
        );
        assert_read_records(&logs);
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_stream_messages_deliver_only_the_entry_written_during_an_outage_after_it() {
        capture_logs();
        let _clock = ManualClock::hold();
        let stream_keys = vec!["stream-0".to_string(), "stream-1".to_string()];
        let fake = FakeRedisStreams::start(&stream_keys, 2).await;
        let mut clock = TaskClock::reading(&fake);
        let (mut rx, stream_signal, handle) =
            start_reader(&fake, &stream_keys, StreamStart::Beginning);
        let mut received = Vec::new();

        clock
            .drive_until(
                || take_received(&mut rx, &mut received) == 4,
                Duration::from_secs(5),
            )
            .await;
        fake.available.store(false, Ordering::SeqCst);
        clock.drive(Duration::from_secs(3)).await;
        fake.append(3, "3");
        fake.available.store(true, Ordering::SeqCst);
        clock
            .drive_until(
                || take_received(&mut rx, &mut received) >= 6,
                Duration::from_secs(5),
            )
            .await;
        // Long enough for any entry delivered again to arrive
        clock.drive(Duration::from_secs(2)).await;
        take_received(&mut rx, &mut received);
        stream_signal.store(true, Ordering::Relaxed);
        clock
            .drive_until(|| handle.is_finished(), Duration::from_secs(5))
            .await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let per_stream: Vec<Vec<&str>> = stream_keys
            .iter()
            .map(|key| {
                received
                    .iter()
                    .filter(|(stream, _)| stream == key)
                    .map(|(_, payload)| payload.as_str())
                    .collect()
            })
            .collect();
        assert!(result.is_ok());
        assert_eq!(per_stream, [["1", "2", "3"], ["1", "2", "3"]]);
        assert_eq!(
            state_records(&logs),
            [
                ("connected".to_string(), true, 0, 0),
                ("degraded".to_string(), true, 0, 0),
                ("connected".to_string(), true, 0, 0),
            ]
        );
        assert_read_records(&logs);
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test]
    async fn test_stream_messages_skip_an_entry_that_cannot_be_decoded_and_deliver_the_next() {
        capture_logs();
        let stream_keys = vec!["stream-0".to_string()];
        let fake = FakeRedisStreams::start(&stream_keys, 0).await;
        fake.append(1, "1");
        fake.append(2, FAKE_UNDECODABLE);
        fake.append(3, "3");
        let (mut rx, stream_signal, handle) =
            start_reader(&fake, &stream_keys, StreamStart::Beginning);

        let first = receive_payload(&mut rx).await;
        let second = receive_payload(&mut rx).await;
        let finished_after_the_entry = handle.is_finished();
        stream_signal.store(true, Ordering::Relaxed);
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let errors = error_lines(&logs);
        assert!(result.is_ok());
        assert!(!finished_after_the_entry);
        assert_eq!([first, second], ["1", "3"]);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(
            errors[0].starts_with("Error decoding stream entry 2-0 of stream-0: ")
                && errors[0].contains("Error parsing sequence"),
            "{}",
            errors[0]
        );
        assert_eq!(
            state_records(&logs),
            [("connected".to_string(), true, 0, 0)]
        );
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_stream_messages_from_now_deliver_an_entry_written_before_the_first_connection() {
        capture_logs();
        let _clock = ManualClock::hold();
        let stream_keys = vec!["stream-0".to_string()];
        let fake = FakeRedisStreams::start(&stream_keys, 0).await;
        fake.available.store(false, Ordering::SeqCst);
        let mut clock = TaskClock::reading(&fake);
        let (mut rx, stream_signal, handle) = start_reader(&fake, &stream_keys, StreamStart::Now);
        let mut received = Vec::new();

        clock.drive(Duration::from_secs(30)).await;
        // Stamped after the task started, and before its first connection on the realtime clock
        let n = realtime_ms_after(realtime_ms());
        fake.append(n, "written while absent");
        realtime_ms_after(n);
        fake.available.store(true, Ordering::SeqCst);
        clock
            .drive_until(
                || take_received(&mut rx, &mut received) == 1,
                Duration::from_secs(5),
            )
            .await;
        stream_signal.store(true, Ordering::Relaxed);
        clock
            .drive_until(|| handle.is_finished(), Duration::from_secs(5))
            .await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        assert!(result.is_ok());
        assert_eq!(
            received,
            stream_payloads(&stream_keys, &["written while absent"])
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_stream_messages_connect_again_after_a_read_that_gets_no_reply() {
        capture_logs();
        let _clock = ManualClock::hold();
        let stream_keys = vec!["stream-0".to_string()];
        let fake = FakeRedisStreams::start(&stream_keys, 0).await;
        fake.silent_reads.store(1, Ordering::SeqCst);
        let mut clock = TaskClock::reading(&fake);
        let (mut rx, stream_signal, handle) =
            start_reader(&fake, &stream_keys, StreamStart::Beginning);
        let mut received = Vec::new();

        clock
            .drive_until(
                || failed_reads(&thread_logs()) == 1,
                Duration::from_secs(30),
            )
            .await;
        let degraded_at = tokio::time::Instant::now();
        let read_at = fake.silent_read_at.lock().unwrap().unwrap();
        fake.append(1, "1");
        clock
            .drive_until(
                || take_received(&mut rx, &mut received) == 1,
                Duration::from_secs(5),
            )
            .await;
        let delivered_at = tokio::time::Instant::now();
        stream_signal.store(true, Ordering::Relaxed);
        clock
            .drive_until(|| handle.is_finished(), Duration::from_secs(5))
            .await;
        let result = handle.await.unwrap();

        let logs = thread_logs();
        let failed: Vec<&String> = logs
            .iter()
            .filter(|(_, line)| line.starts_with("Error reading from stream"))
            .map(|(_, line)| line)
            .collect();
        assert!(result.is_ok());
        // Failed when no reply came within the read's blocking time plus the attempt bound (the
        // connection's own response timeout is 20 s); the clock moves in steps of `CLOCK_STEP`
        assert!(
            degraded_at - read_at >= MAX_STREAM_READ
                && degraded_at - read_at <= MAX_STREAM_READ + CLOCK_STEP,
            "{:?}",
            degraded_at - read_at
        );
        assert!(delivered_at - degraded_at <= MAX_PUBLISH_RETRY_DELAY);
        assert!(fake.connections.lock().unwrap().len() >= 2);
        assert_eq!(received, stream_payloads(&stream_keys, &["1"]));
        assert!(failed[0].contains("No reply within 5.1s"), "{}", failed[0]);
        assert_eq!(
            state_records(&logs),
            [
                ("degraded".to_string(), false, 0, 0),
                ("connected".to_string(), true, 0, 0),
            ]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    fn test_backing_starts_its_stream_task_again_after_it_ends_without_the_stop() {
        capture_logs();
        let stream_keys = vec!["trader-PUBLISHER-007:stream".to_string()];
        let fake = get_runtime().block_on(FakeRedisStreams::start(&stream_keys, 0));
        let mut backing = RedisMessageBusBacking::new(
            TraderId::from("TESTER-007"),
            UUID4::new(),
            MessageBusConfig::default(),
            fake.config(),
        )
        .unwrap();
        // The backing's stream task as `new` starts it, its first task failing after two entries
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel::<BusMessage>(100);
        let tasks = Arc::new(AtomicUsize::new(0));
        backing.stream_handle = Some(get_runtime().spawn(read_streams_until_stopped(
            stream_tx,
            fake.config(),
            stream_keys,
            Arc::clone(&backing.stream_signal),
            move |tx, config, stream_keys, positions, signal| {
                let first = tasks.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    if !first {
                        return read_streams(tx, config, stream_keys, positions, signal).await;
                    }
                    let (read_tx, mut read_rx) = tokio::sync::mpsc::channel(1);
                    let read = tokio::spawn(read_streams(
                        read_tx,
                        config,
                        stream_keys,
                        positions,
                        signal,
                    ));
                    for _ in 0..2 {
                        tx.send(read_rx.recv().await.unwrap()).await.unwrap();
                    }
                    read.abort();
                    panic!("the stream task fails after its second entry");
                }
            },
        )));

        // Read once: the task has taken the current time
        wait_until(
            || !fake.reads.lock().unwrap().is_empty(),
            Duration::from_secs(5),
        );
        let n = realtime_ms_after(realtime_ms());
        fake.append(n, "1");
        fake.append(n + 1, "2");
        let before_the_end = [
            get_runtime().block_on(receive_payload(&mut stream_rx)),
            get_runtime().block_on(receive_payload(&mut stream_rx)),
        ];
        wait_until(
            || task_ends(&every_thread_logs()).len() == 1,
            Duration::from_secs(5),
        );
        fake.append(n + 2, "3");
        let after_the_end = get_runtime().block_on(receive_payload(&mut stream_rx));
        let more = get_runtime().block_on(async {
            tokio::time::timeout(Duration::from_millis(500), stream_rx.recv()).await
        });
        backing.publish(fake_bus_message());
        wait_until(
            || !fake.written.lock().unwrap().is_empty(),
            Duration::from_secs(5),
        );
        let closed = backing.is_closed();
        backing.close();

        let ends = task_ends(&every_thread_logs());
        assert_eq!(before_the_end, ["1", "2"]);
        assert_eq!(after_the_end, "3");
        assert!(more.is_err(), "{more:?}");
        assert!(!closed);
        assert_eq!(fake.written.lock().unwrap().len(), 1);
        assert_eq!(ends.len(), 1, "{ends:?}");
        assert_eq!(ends[0].0, log::Level::Error);
        assert!(
            ends[0].1.contains("panicked") && ends[0].1.ends_with("starting it again in 1s"),
            "{}",
            ends[0].1
        );
    }

    /// The realtime clock in milliseconds.
    fn realtime_ms() -> usize {
        usize::try_from(get_atomic_clock_realtime().get_time_ms()).unwrap()
    }

    /// Waits until the realtime clock is past `ms` and returns it in milliseconds.
    fn realtime_ms_after(ms: usize) -> usize {
        while realtime_ms() <= ms {
            std::thread::sleep(Duration::from_millis(1));
        }
        realtime_ms()
    }

    /// How many reads of the stream task failed in `logs`.
    fn failed_reads(logs: &[(log::Level, String)]) -> usize {
        logs.iter()
            .filter(|(_, line)| line.starts_with("Error reading from stream"))
            .count()
    }

    /// The lines of `logs` on a stream task that ended without the stop.
    fn task_ends(logs: &[(log::Level, String)]) -> Vec<(log::Level, String)> {
        logs.iter()
            .filter(|(_, line)| line.starts_with("Task 'msgbus-stream' ended without the stop"))
            .cloned()
            .collect()
    }

    #[rstest]
    fn test_way_out_and_inbound_stay_open_after_a_read_error_redis_does_not_retry() {
        let stream_keys = vec!["trader-PUBLISHER-007:stream".to_string()];
        let fake = get_runtime().block_on(FakeRedisStreams::start(&stream_keys, 0));
        let config = MessageBusConfig {
            external_streams: Some(stream_keys),
            ..Default::default()
        };
        let mut backing = RedisMessageBusBacking::new(
            TraderId::from("TESTER-007"),
            UUID4::new(),
            config,
            fake.config(),
        )
        .unwrap();
        let mut stream_rx = backing.get_stream_receiver().unwrap();

        wait_until(
            || !fake.reads.lock().unwrap().is_empty(),
            Duration::from_secs(5),
        );
        fake.wrong_type_reads.store(1, Ordering::SeqCst);
        wait_until(
            || fake.wrong_type_reads.load(Ordering::SeqCst) == 0,
            Duration::from_secs(5),
        );
        // The backing reads from the current time on, so the entry's ID is later
        let n = usize::try_from(get_atomic_clock_realtime().get_time_ms()).unwrap() + 1_000_000;
        fake.append(n, "after the error");
        let message = get_runtime().block_on(async {
            tokio::time::timeout(Duration::from_secs(5), stream_rx.recv()).await
        });
        backing.publish(fake_bus_message());
        wait_until(
            || !fake.written.lock().unwrap().is_empty(),
            Duration::from_secs(5),
        );
        backing.close();

        let message = message
            .expect("the entry should arrive")
            .expect("the stream task should keep the inbound side open");
        let written = fake.written.lock().unwrap().clone();
        assert_eq!(message.payload, Bytes::from("after the error"));
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].field("payload").as_deref(), Some("test_payload"));
    }

    #[rstest]
    fn test_backing_stop_read_ends_the_read_and_keeps_the_way_out_open() {
        let stream_keys = vec!["trader-PUBLISHER-007:stream".to_string()];
        let fake = get_runtime().block_on(FakeRedisStreams::start(&stream_keys, 0));
        let config = MessageBusConfig {
            external_streams: Some(stream_keys),
            ..Default::default()
        };
        let mut backing = RedisMessageBusBacking::new(
            TraderId::from("TESTER-007"),
            UUID4::new(),
            config,
            fake.config(),
        )
        .unwrap();
        let mut stream_rx = backing.get_stream_receiver().unwrap();
        wait_until(
            || !fake.reads.lock().unwrap().is_empty(),
            Duration::from_secs(5),
        );

        let stopped_at = std::time::Instant::now();
        backing.stop_read();
        wait_until(
            || backing.stream_handle.as_ref().unwrap().is_finished(),
            MAX_STREAM_READ,
        );
        let ended_after = stopped_at.elapsed();
        backing.stop_read();
        // The backing read from the time it started, so the entry's ID is later
        fake.append(realtime_ms_after(realtime_ms()), "after the stop");
        let message = get_runtime().block_on(async {
            tokio::time::timeout(Duration::from_secs(1), stream_rx.recv()).await
        });
        backing.publish(fake_bus_message());
        wait_until(
            || !fake.written.lock().unwrap().is_empty(),
            Duration::from_secs(5),
        );
        let closed = backing.is_closed();
        let still_ended = backing.stream_handle.as_ref().unwrap().is_finished();
        backing.close();

        assert!(ended_after <= MAX_STREAM_READ, "{ended_after:?}");
        assert!(still_ended);
        assert!(
            matches!(message, Ok(None)),
            "the entry after the stop should not be delivered: {message:?}"
        );
        assert!(!closed);
        assert_eq!(fake.written.lock().unwrap().len(), 1);
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_backing_close_writes_what_is_pending_and_logs_its_counts_once() {
        capture_logs();
        let _clock = ManualClock::hold();
        let writer = FakeRedis::start("unused").await;
        writer.accept_writes.store(true, Ordering::SeqCst);
        let stream_keys = vec!["trader-PUBLISHER-007:stream".to_string()];
        let reader = FakeRedisStreams::start(&stream_keys, 0).await;
        let config = MessageBusConfig {
            stream_per_topic: true,
            heartbeat_interval_secs: Some(60),
            buffer_interval_ms: Some(10_000),
            external_streams: Some(stream_keys),
            ..Default::default()
        };
        let (mut backing, read_tasks) =
            backing_on_this_runtime(writer.config(), reader.config(), config);
        // The first heartbeat and three publications wait for the 10 s buffer interval
        drive_until(
            || {
                writer.connections.lock().unwrap().len() == 1
                    && !reader.reads.lock().unwrap().is_empty()
            },
            Duration::from_secs(5),
        )
        .await;
        for _ in 0..3 {
            backing.publish(fake_bus_message());
        }
        let pending = backing.pub_tx.pending.lock().unwrap().buffer.len();
        let tasks = backing_tasks(&backing);

        let took = close_on_clock(&mut backing).await;
        backing.close_async().await;

        let logs = thread_logs();
        assert_eq!(pending, 4);
        assert!(took.is_some_and(|took| took < CLOSE_DEADLINE), "{took:?}");
        assert_eq!(writer.written_len(), 4);
        assert!(tasks.iter().all(tokio::task::AbortHandle::is_finished));
        assert_eq!(Arc::strong_count(&read_tasks), 1, "a read task is left");
        assert_eq!(
            close_lines(&logs),
            [(
                log::Level::Info,
                "Closed the stream: 4 written, 0 discarded, 0 uncertain".to_string()
            )]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_backing_close_aborts_its_tasks_at_the_deadline_when_redis_never_answers_a_write()
    {
        capture_logs();
        let _clock = ManualClock::hold();
        let writer = FakeRedis::start("unused").await;
        writer.accept_writes.store(true, Ordering::SeqCst);
        let stream_keys = vec!["trader-PUBLISHER-007:stream".to_string()];
        let reader = FakeRedisStreams::start(&stream_keys, 0).await;
        let config = MessageBusConfig {
            stream_per_topic: true,
            heartbeat_interval_secs: Some(60),
            external_streams: Some(stream_keys),
            ..Default::default()
        };
        let (mut backing, read_tasks) =
            backing_on_this_runtime(writer.config(), reader.config(), config);
        drive_until(
            || writer.written_len() == 1 && !reader.reads.lock().unwrap().is_empty(),
            Duration::from_secs(5),
        )
        .await;
        writer.stall_writes.store(true, Ordering::SeqCst);
        backing.publish(fake_bus_message());
        drive_until(
            || writer.attempted.lock().unwrap().len() == 2,
            Duration::from_secs(5),
        )
        .await;
        // The write's own bound then ends 1 s before the close's deadline, and its retry is cut
        drive(Duration::from_secs(1)).await;
        backing.publish(fake_bus_message());
        backing.publish(fake_bus_message());
        let tasks = backing_tasks(&backing);

        let took = close_on_clock(&mut backing).await;

        let logs = thread_logs();
        assert_eq!(took, Some(CLOSE_DEADLINE));
        assert!(tasks.iter().all(tokio::task::AbortHandle::is_finished));
        assert_eq!(Arc::strong_count(&read_tasks), 1, "a read task is left");
        assert_eq!(writer.written_len(), 1);
        assert_eq!(
            close_lines(&logs),
            [(
                log::Level::Warn,
                "Closed the stream at the close's deadline of 5s: 1 written, 2 discarded, 1 uncertain"
                    .to_string()
            )]
        );
        assert_eq!(aborted_tasks(&logs), [MSGBUS_PUBLISH]);
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_backing_close_ends_a_read_redis_never_answers_within_the_deadline() {
        capture_logs();
        let _clock = ManualClock::hold();
        let writer = FakeRedis::start("unused").await;
        writer.accept_writes.store(true, Ordering::SeqCst);
        let stream_keys = vec!["trader-PUBLISHER-007:stream".to_string()];
        let reader = FakeRedisStreams::start(&stream_keys, 0).await;
        reader.silent_reads.store(1, Ordering::SeqCst);
        let config = MessageBusConfig {
            external_streams: Some(stream_keys),
            ..Default::default()
        };
        let (mut backing, read_tasks) =
            backing_on_this_runtime(writer.config(), reader.config(), config);
        // The first read is held, sent with the clock still since, so its own bound of 5.1 s ends
        // after the close's deadline
        settle_until(|| reader.silent_read_at.lock().unwrap().is_some()).await;
        let tasks = backing_tasks(&backing);

        let took = close_on_clock(&mut backing).await;

        let logs = thread_logs();
        assert_eq!(took, Some(CLOSE_DEADLINE));
        assert!(tasks.iter().all(tokio::task::AbortHandle::is_finished));
        assert_eq!(Arc::strong_count(&read_tasks), 1, "a read task is left");
        assert_eq!(
            close_lines(&logs),
            [(
                log::Level::Info,
                "Closed the stream: 0 written, 0 discarded, 0 uncertain".to_string()
            )]
        );
        assert_eq!(aborted_tasks(&logs), [MSGBUS_STREAM]);
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_backing_close_stops_its_drain_at_the_first_batch_redis_refuses() {
        capture_logs();
        let _clock = ManualClock::hold();
        let writer =
            FakeRedis::start("OOM command not allowed when used memory > 'maxmemory'.").await;
        let config = MessageBusConfig {
            stream_per_topic: false,
            buffer_interval_ms: Some(10_000),
            ..Default::default()
        };
        let (mut backing, _) = backing_on_this_runtime(writer.config(), writer.config(), config);
        drive_until(
            || writer.connections.lock().unwrap().len() == 1,
            Duration::from_secs(5),
        )
        .await;
        // Three batches wait for the 10 s buffer interval
        for _ in 0..3 * PUBLISH_BATCH_SIZE {
            backing.publish(fake_bus_message());
        }

        let took = close_on_clock(&mut backing).await;

        let logs = thread_logs();
        assert!(took.is_some_and(|took| took < CLOSE_DEADLINE), "{took:?}");
        assert_eq!(writer.aborted.load(Ordering::SeqCst), 1);
        assert_eq!(writer.connections.lock().unwrap().len(), 1);
        assert_eq!(writer.attempted.lock().unwrap().len(), PUBLISH_BATCH_SIZE);
        assert_eq!(
            close_lines(&logs),
            [(
                log::Level::Warn,
                "Closed the stream: 0 written, 3000 discarded, 0 uncertain".to_string()
            )]
        );
        assert_eq!(error_lines(&logs), Vec::<String>::new());
    }

    /// Returns a backing for trader `TESTER-007` with `config` as `new` makes it, its tasks spawned
    /// on this runtime: the publish task writing to `writer`, the stream task, when `config` names
    /// external streams, reading them from `reader`, and the heartbeat task when `config` sets its
    /// interval. Each read task holds a clone of the `Arc` returned with the backing.
    fn backing_on_this_runtime(
        writer: RedisMessageBusConfig,
        reader: RedisMessageBusConfig,
        config: MessageBusConfig,
    ) -> (RedisMessageBusBacking, Arc<()>) {
        let trader_id = TraderId::from("TESTER-007");
        let instance_id = UUID4::new();
        let sources = StreamSources::new(trader_id, instance_id, &config);
        let stream_keys = config.external_streams.clone().unwrap_or_default();
        let heartbeat_interval_secs = config.heartbeat_interval_secs;
        let (pub_tx, pub_rx) = publish_channel();
        let pub_handle = tokio::spawn(async move {
            if let Err(e) = publish_messages(pub_rx, trader_id, instance_id, config, writer).await {
                log_task_error(MSGBUS_PUBLISH, &e);
            }
        });

        let stream_signal = Arc::new(AtomicBool::new(false));
        let read_tasks = Arc::new(());
        let (stream_rx, stream_handle, read_tasks_ended) = if stream_keys.is_empty() {
            (None, None, None)
        } else {
            let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<BusMessage>(100);
            let running = Arc::clone(&read_tasks);
            let (task, ended) = stream_task(
                stream_tx,
                reader,
                stream_keys,
                Arc::clone(&stream_signal),
                move |tx, config, stream_keys, positions, signal| {
                    let running = Arc::clone(&running);
                    // Dropped with the task's future, aborted or not
                    async move {
                        read_streams(tx, config, stream_keys, positions, signal).await;
                        drop(running);
                    }
                },
            );
            (Some(stream_rx), Some(tokio::spawn(task)), Some(ended))
        };

        let heartbeat_signal = Arc::new(AtomicBool::new(false));
        let heartbeat_handle = heartbeat_interval_secs.map(|interval| {
            tokio::spawn(run_heartbeat(
                interval,
                Arc::clone(&heartbeat_signal),
                pub_tx.clone(),
                sources.clone(),
            ))
        });

        let backing = RedisMessageBusBacking {
            trader_id,
            instance_id,
            pub_tx,
            pub_handle: Some(pub_handle),
            stream_rx,
            stream_handle,
            read_tasks: read_tasks_ended,
            stream_signal,
            heartbeat_handle,
            heartbeat_signal,
            sources,
        };
        (backing, read_tasks)
    }

    /// The tasks of `backing`, to tell whether each has ended after the close takes them.
    fn backing_tasks(backing: &RedisMessageBusBacking) -> Vec<tokio::task::AbortHandle> {
        [
            &backing.pub_handle,
            &backing.stream_handle,
            &backing.heartbeat_handle,
        ]
        .into_iter()
        .flatten()
        .map(tokio::task::JoinHandle::abort_handle)
        .collect()
    }

    /// Closes `backing`, moving the clock on as [`drive`] does until the close's deadline and then
    /// holding it still until the close returns, so a close that ends at its deadline ends exactly
    /// there. Returns how long the close took on the clock, or `None` when it had not returned 5 s
    /// of real time after the deadline.
    async fn close_on_clock(backing: &mut RedisMessageBusBacking) -> Option<Duration> {
        let started = tokio::time::Instant::now();
        let deadline = started + CLOSE_DEADLINE;
        let took = Cell::new(None);
        let close = async {
            backing.close_async().await;
            took.set(Some(started.elapsed()));
        };
        let clock = async {
            run_sockets().await;

            while took.get().is_none() && tokio::time::Instant::now() < deadline {
                let step = CLOCK_STEP.min(deadline - tokio::time::Instant::now());
                tokio::time::advance(step).await;
                run_sockets().await;
            }
            let held_since = std::time::Instant::now(); // dst-ok: test timer; uses real time by design

            while took.get().is_none() && held_since.elapsed() < Duration::from_secs(5) {
                tokio::task::yield_now().await;
            }
        };

        tokio::select! {
            biased;
            () = close => {}
            () = clock => {}
        }
        took.get()
    }

    /// The close's count lines in `logs`.
    fn close_lines(logs: &[(log::Level, String)]) -> Vec<(log::Level, String)> {
        logs.iter()
            .filter(|(_, line)| line.starts_with("Closed the stream"))
            .cloned()
            .collect()
    }

    /// The tasks the close aborted, as its lines in `logs` name them.
    fn aborted_tasks(logs: &[(log::Level, String)]) -> Vec<String> {
        logs.iter()
            .filter_map(|(_, line)| {
                let rest = line.strip_prefix("Task '")?;
                let (task, rest) = rest.split_once('\'')?;
                (rest == " still running at the close's deadline, aborting it")
                    .then(|| task.to_string())
            })
            .collect()
    }

    /// Spawns `stream_messages_from` on `stream_keys` of `fake`, starting after `start`.
    fn start_reader(
        fake: &FakeRedisStreams,
        stream_keys: &[String],
        start: StreamStart,
    ) -> (
        tokio::sync::mpsc::Receiver<BusMessage>,
        Arc<AtomicBool>,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel::<BusMessage>(100);
        let stream_signal = Arc::new(AtomicBool::new(false));
        let handle = tokio::spawn(stream_messages_from(
            tx,
            fake.config(),
            stream_keys.to_vec(),
            start,
            Arc::clone(&stream_signal),
        ));
        (rx, stream_signal, handle)
    }

    async fn receive_payload(rx: &mut tokio::sync::mpsc::Receiver<BusMessage>) -> String {
        let message = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("the entry should arrive")
            .expect("the stream task should keep the channel open");
        String::from_utf8(message.payload.to_vec()).unwrap()
    }

    /// Takes every message waiting on `rx` into `received` as its key and payload, and returns how
    /// many were received in all.
    fn take_received(
        rx: &mut tokio::sync::mpsc::Receiver<BusMessage>,
        received: &mut Vec<(String, String)>,
    ) -> usize {
        while let Ok(message) = rx.try_recv() {
            received.push((
                message.topic.to_string(),
                String::from_utf8(message.payload.to_vec()).unwrap(),
            ));
        }
        received.len()
    }

    /// The key and payload of each of `payloads` read from the one stream of `stream_keys`.
    fn stream_payloads(stream_keys: &[String], payloads: &[&str]) -> Vec<(String, String)> {
        payloads
            .iter()
            .map(|payload| (stream_keys[0].clone(), (*payload).to_string()))
            .collect()
    }

    /// Asserts that each state record names the stream task's last read.
    fn assert_read_records(logs: &[(log::Level, String)]) {
        for (_, line) in logs.iter().filter(|(_, line)| line.starts_with("Stream ")) {
            assert!(line.contains(", last read "), "{line}");
        }
    }

    /// Installs a Redis backing for `fake`, as trader `TESTER-007` with `instance_id`, as this
    /// thread's message bus way out.
    fn install_way_out(fake: &FakeRedis, instance_id: UUID4, config: MessageBusConfig) {
        let backing = RedisMessageBusBacking::new(
            TraderId::from("TESTER-007"),
            instance_id,
            config,
            fake.config(),
        )
        .unwrap();
        let mut bus = MessageBus::default();
        bus.set_external_egress(
            external_egress_from_backing(Box::new(backing)),
            SerializationEncoding::Json,
        );
        set_message_bus(Rc::new(RefCell::new(bus)));
    }

    fn close_way_out() {
        get_message_bus().borrow_mut().close().unwrap();
    }

    /// Custom data whose JSON encoding always fails.
    #[derive(Debug)]
    struct UnencodableData;

    impl HasTsInit for UnencodableData {
        fn ts_init(&self) -> UnixNanos {
            UnixNanos::default()
        }
    }

    impl CustomDataTrait for UnencodableData {
        fn type_name(&self) -> &'static str {
            "UnencodableData"
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn ts_event(&self) -> UnixNanos {
            UnixNanos::default()
        }

        fn to_json(&self) -> anyhow::Result<String> {
            anyhow::bail!("UnencodableData has no JSON form")
        }

        fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
            Arc::new(Self)
        }

        fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
            other.as_any().is::<Self>()
        }
    }

    /// Custom data whose JSON encoding first publishes on `nested_topic`, `publications` times, the
    /// same data one depth less, then takes `encoding_time` and fails when `fails` is set.
    #[derive(Clone, Copy, Debug)]
    struct NestingData {
        depth: u64,
        nested_topic: &'static str,
        publications: u64,
        encoding_time: Duration,
        fails: bool,
    }

    impl NestingData {
        /// Data of depth 1 that publishes data of depth 0 on `nested_topic` while it is encoded.
        fn nesting(nested_topic: &'static str) -> Self {
            Self {
                depth: 1,
                nested_topic,
                publications: 1,
                encoding_time: Duration::ZERO,
                fails: false,
            }
        }

        fn custom(self) -> CustomData {
            CustomData::new(Arc::new(self), DataType::new("NestingData", None, None))
        }
    }

    impl HasTsInit for NestingData {
        fn ts_init(&self) -> UnixNanos {
            UnixNanos::default()
        }
    }

    impl CustomDataTrait for NestingData {
        fn type_name(&self) -> &'static str {
            "NestingData"
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn ts_event(&self) -> UnixNanos {
            UnixNanos::default()
        }

        fn to_json(&self) -> anyhow::Result<String> {
            if let Some(depth) = self.depth.checked_sub(1) {
                for _ in 0..self.publications {
                    let nested = Self {
                        depth,
                        fails: false,
                        ..*self
                    };
                    publish_any(self.nested_topic.into(), &nested.custom());
                }
            }
            std::thread::sleep(self.encoding_time);
            anyhow::ensure!(!self.fails, "NestingData has no JSON form");
            Ok(json!({ "depth": self.depth }).to_string())
        }

        fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
            Arc::new(*self)
        }

        fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
            other.as_any().is::<Self>()
        }
    }

    /// Returns the depth of the [`NestingData`] an entry carries, if it carries one.
    fn nesting_depth(xadd: &FakeXadd) -> Option<u64> {
        let payload: serde_json::Value = serde_json::from_str(&xadd.field("payload")?).ok()?;
        payload["payload"]["depth"].as_u64()
    }

    fn backing_with_stream_receiver(
        stream_rx: tokio::sync::mpsc::Receiver<BusMessage>,
    ) -> RedisMessageBusBacking {
        let (pub_tx, _pub_rx) = publish_channel();
        RedisMessageBusBacking {
            trader_id: TraderId::from("tester-001"),
            instance_id: UUID4::new(),
            pub_tx,
            pub_handle: None,
            stream_rx: Some(stream_rx),
            stream_handle: None,
            read_tasks: None,
            stream_signal: Arc::new(AtomicBool::new(false)),
            heartbeat_handle: None,
            heartbeat_signal: Arc::new(AtomicBool::new(false)),
            sources: StreamSources::new(
                TraderId::from("tester-001"),
                UUID4::new(),
                &MessageBusConfig::default(),
            ),
        }
    }

    fn fake_bus_message() -> BusMessage {
        BusMessage::with_str_topic(
            "test_topic",
            BusPayloadType::QuoteTick,
            Bytes::from("test_payload"),
            SerializationEncoding::Json,
        )
    }

    /// Spawns `publish_messages` for trader `TESTER-007` on one key against `fake`.
    fn start_publisher(
        fake: &FakeRedis,
    ) -> (PublishSender, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let (tx, rx) = publish_channel();
        (tx, spawn_publisher(fake, rx, false, None))
    }

    /// Spawns `publish_messages` for trader `TESTER-007` against `fake`, taking what `rx` admits,
    /// with a key per topic when `stream_per_topic` and the given `autotrim_mins`.
    fn spawn_publisher(
        fake: &FakeRedis,
        rx: PublishReceiver,
        stream_per_topic: bool,
        autotrim_mins: Option<u32>,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let config = MessageBusConfig {
            stream_per_topic,
            autotrim_mins,
            ..Default::default()
        };
        let backing = fake.config();
        tokio::spawn(async move {
            publish_messages(
                rx,
                TraderId::from("TESTER-007"),
                UUID4::new(),
                config,
                backing,
            )
            .await
        })
    }

    /// Returns a backing for trader `TESTER-007` with `config` whose publish task, spawned on this
    /// runtime, writes to `fake`.
    fn backing_publishing_to(
        fake: &FakeRedis,
        config: MessageBusConfig,
    ) -> (
        RedisMessageBusBacking,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let (pub_tx, rx) = publish_channel();
        let trader_id = TraderId::from("TESTER-007");
        let instance_id = UUID4::new();
        let sources = StreamSources::new(trader_id, instance_id, &config);
        let handle = tokio::spawn(publish_messages(
            rx,
            trader_id,
            instance_id,
            config,
            fake.config(),
        ));
        let backing = RedisMessageBusBacking {
            trader_id,
            instance_id,
            pub_tx,
            pub_handle: None,
            stream_rx: None,
            stream_handle: None,
            read_tasks: None,
            stream_signal: Arc::new(AtomicBool::new(false)),
            heartbeat_handle: None,
            heartbeat_signal: Arc::new(AtomicBool::new(false)),
            sources,
        };
        (backing, handle)
    }

    /// Returns a heartbeat numbered by `backing`, as its heartbeat task numbers it.
    fn heartbeat_of(backing: &RedisMessageBusBacking) -> BusMessage {
        create_heartbeat_msg(
            backing
                .sources
                .reserve(Ustr::from(HEARTBEAT_TOPIC))
                .unwrap(),
        )
    }

    /// Returns the line that logs the first refusal of a publication on `nested` made while a
    /// publication on `outer` is encoded.
    fn refusal_line(outer: &str, nested: &str) -> String {
        format!(
            "Refused the publication on '{nested}' made while a publication on '{outer}' was encoded for a numbered stream: a serializer must not publish; every later one of this pair is refused without a line"
        )
    }

    /// Returns a message on `topic` numbered by `backing`, as the message bus numbers it before
    /// encoding it.
    fn numbered_by(backing: &RedisMessageBusBacking, topic: &str) -> BusMessage {
        let mut message = fake_bus_message();
        message.topic = Ustr::from(topic);
        message.source = backing.reserve_source(message.topic).unwrap();
        message
    }

    /// Returns `count` messages numbered from 1 as the way out numbers them on one key.
    fn numbered_messages(count: usize) -> Vec<BusMessage> {
        let sources = StreamSources::new(
            TraderId::from("TESTER-007"),
            UUID4::new(),
            &MessageBusConfig {
                stream_per_topic: false,
                ..Default::default()
            },
        );
        (0..count)
            .map(|_| {
                let mut message = fake_bus_message();
                message.source = Some(sources.reserve(message.topic).unwrap());
                message
            })
            .collect()
    }

    /// How far [`drive`] moves the paused clock at a time.
    const CLOCK_STEP: Duration = Duration::from_millis(50);

    /// Holds tokio's auto-advance off while it lives, through a blocking task in flight, so a
    /// paused clock moves only by [`drive`] and the loopback sockets run without a timer firing
    /// between a request and its reply.
    struct ManualClock {
        _release: std::sync::mpsc::Sender<()>,
    }

    impl ManualClock {
        fn hold() -> Self {
            let (release, released) = std::sync::mpsc::channel::<()>();
            tokio::task::spawn_blocking(move || released.recv());
            Self { _release: release }
        }
    }

    /// Lets the tasks and their sockets run with the clock still.
    async fn run_sockets() {
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
    }

    /// Lets the tasks and their sockets run with the clock still until `done` holds, failing
    /// after 30 s of real time.
    async fn settle_until(mut done: impl FnMut() -> bool) {
        let start = std::time::Instant::now(); // dst-ok: test timer; uses real time by design

        while !done() {
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "Timeout settling"
            );
            tokio::task::yield_now().await;
        }
    }

    /// Moves the paused clock on by `duration` in steps of [`CLOCK_STEP`], letting the tasks and
    /// their sockets run after each step.
    async fn drive(duration: Duration) {
        let end = tokio::time::Instant::now() + duration;

        while tokio::time::Instant::now() < end {
            tokio::time::advance(CLOCK_STEP).await;
            run_sockets().await;
        }
    }

    /// Moves the paused clock on as [`drive`] does until `done` holds, failing after `limit`.
    async fn drive_until(mut done: impl FnMut() -> bool, limit: Duration) {
        let end = tokio::time::Instant::now() + limit;
        run_sockets().await;

        while !done() {
            assert!(
                tokio::time::Instant::now() < end,
                "Not done within {limit:?}"
            );
            tokio::time::advance(CLOCK_STEP).await;
            run_sockets().await;
        }
    }

    /// Moves the paused clock for one stream task reading a [`FakeRedisStreams`], or one publish
    /// task, and only while the task waits on a timer: on a read the fake holds, or in the wait its
    /// last retry line names. Before each step it lets the task and the sockets run with the clock
    /// still until the task waits again, so no request or reply in flight sees the clock move, and
    /// it steps to the end of each retry wait exactly. The task's retry lines are this thread's.
    struct TaskClock<'a> {
        /// The fake a stream task reads, which holds reads; none for a publish task.
        fake: Option<&'a FakeRedisStreams>,
        /// How many lines of every thread have been read for this thread's retry lines.
        scanned: usize,
        /// When the wait the last retry line names ends.
        wake: Option<tokio::time::Instant>,
        /// How many reads the fake had held when that wait began.
        held_before: usize,
    }

    impl<'a> TaskClock<'a> {
        /// A clock for a stream task reading `fake`.
        fn reading(fake: &'a FakeRedisStreams) -> Self {
            Self {
                fake: Some(fake),
                scanned: THREAD_LOGS.0.lock().unwrap().len(),
                wake: None,
                held_before: 0,
            }
        }

        /// A clock for a publish task, which waits on a timer here only in a retry wait.
        fn writing() -> Self {
            Self {
                fake: None,
                scanned: THREAD_LOGS.0.lock().unwrap().len(),
                wake: None,
                held_before: 0,
            }
        }

        /// Whether the task waits on a timer: in a retry wait not yet over, or on a read made after
        /// the last retry wait began that the fake holds.
        fn waits(&mut self) -> bool {
            let now = tokio::time::Instant::now();
            let thread = std::thread::current().id();
            {
                let logs = THREAD_LOGS.0.lock().unwrap();
                for (logged_on, _, line) in &logs[self.scanned..] {
                    if *logged_on != thread {
                        continue;
                    }
                    if let Some(delay) = retry_delay_of(line) {
                        self.wake = Some(now + delay);
                        self.held_before = self
                            .fake
                            .map_or(0, |fake| fake.reads_held.load(Ordering::SeqCst));
                    }
                }
                self.scanned = logs.len();
            }

            match self.wake {
                Some(wake) if now < wake => true,
                _ => self.fake.is_some_and(|fake| {
                    fake.read_held.load(Ordering::SeqCst)
                        && fake.reads_held.load(Ordering::SeqCst) > self.held_before
                }),
            }
        }

        /// Moves the clock on by [`CLOCK_STEP`], to `until` or to the end of the retry wait,
        /// whichever comes first.
        async fn step(&self, until: tokio::time::Instant) {
            let now = tokio::time::Instant::now();
            let mut step = CLOCK_STEP.min(until.saturating_duration_since(now));
            if let Some(wake) = self.wake.filter(|&wake| wake > now) {
                step = step.min(wake - now);
            }
            tokio::time::advance(step).await;
        }

        /// Moves the clock on by `duration`, the task waiting at its end.
        async fn drive(&mut self, duration: Duration) {
            let end = tokio::time::Instant::now() + duration;

            loop {
                settle_until(|| self.waits()).await;
                if tokio::time::Instant::now() >= end {
                    return;
                }
                self.step(end).await;
            }
        }

        /// Moves the clock on until `done` holds, failing after `limit`.
        async fn drive_until(&mut self, mut done: impl FnMut() -> bool, limit: Duration) {
            let end = tokio::time::Instant::now() + limit;

            loop {
                settle_until(|| done() || self.waits()).await;
                if done() {
                    return;
                }
                assert!(
                    tokio::time::Instant::now() < end,
                    "Not done within {limit:?}"
                );
                self.step(end).await;
            }
        }
    }

    /// The wait a stream or publish task's retry line names (`..., retrying in <delay> (...`), a
    /// whole number of milliseconds or seconds, as every wait of at most a second is.
    fn retry_delay_of(line: &str) -> Option<Duration> {
        let (_, rest) = line.split_once(", retrying in ")?;
        let (delay, _) = rest.split_once(" (")?;
        Some(match delay.strip_suffix("ms") {
            Some(millis) => Duration::from_millis(millis.parse().unwrap()),
            None => Duration::from_secs(delay.strip_suffix('s').unwrap().parse().unwrap()),
        })
    }

    /// The lines of every thread [`ThreadLogs`] captures.
    fn every_thread_logs() -> Vec<(log::Level, String)> {
        THREAD_LOGS
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|(_, level, line)| (*level, line.clone()))
            .collect()
    }

    /// The warning and error lines of every thread, and the information lines of this module (the
    /// close's count line). A test on a current-thread runtime runs the publish task and the fake
    /// Redis on its own thread, so it reads the lines of that thread.
    struct ThreadLogs(std::sync::Mutex<Vec<(std::thread::ThreadId, log::Level, String)>>);

    impl log::Log for ThreadLogs {
        fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
            metadata.level() <= log::Level::Warn
                || (metadata.level() == log::Level::Info
                    && Some(metadata.target()) == module_path!().strip_suffix("::tests"))
        }

        fn log(&self, record: &log::Record<'_>) {
            if self.enabled(record.metadata()) {
                self.0.lock().unwrap().push((
                    std::thread::current().id(),
                    record.level(),
                    record.args().to_string(),
                ));
            }
        }

        fn flush(&self) {}
    }

    static THREAD_LOGS: ThreadLogs = ThreadLogs(std::sync::Mutex::new(Vec::new()));
    static THREAD_LOGS_INSTALL: std::sync::Once = std::sync::Once::new();

    fn capture_logs() {
        THREAD_LOGS_INSTALL.call_once(|| {
            log::set_logger(&THREAD_LOGS).expect("no other logger in these tests");
            log::set_max_level(log::LevelFilter::Info);
        });
    }

    fn thread_logs() -> Vec<(log::Level, String)> {
        let thread = std::thread::current().id();
        THREAD_LOGS
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(logged_on, _, _)| *logged_on == thread)
            .map(|(_, level, line)| (*level, line.clone()))
            .collect()
    }

    fn error_lines(logs: &[(log::Level, String)]) -> Vec<String> {
        logs.iter()
            .filter(|(level, _)| *level == log::Level::Error)
            .map(|(_, line)| line.clone())
            .collect()
    }

    /// Returns each state record of the stream, all at warning level, as its state, whether it
    /// names a last write or read, and its discarded and uncertain counts.
    fn state_records(logs: &[(log::Level, String)]) -> Vec<(String, bool, usize, usize)> {
        logs.iter()
            .filter(|(_, line)| line.starts_with("Stream "))
            .map(|(level, line)| {
                assert_eq!(*level, log::Level::Warn, "{line}");
                let (head, values) = line.split_once(", last ").unwrap();
                // The publish task's last write or the stream task's last read
                let (_, values) = values.split_once(' ').unwrap();
                let link = head.split(' ').nth(1).unwrap().to_string();
                let [last_write, discarded, uncertain] =
                    values.split(", ").collect::<Vec<_>>().try_into().unwrap();
                (
                    link,
                    last_write != "none",
                    discarded
                        .strip_suffix(" discarded")
                        .unwrap()
                        .parse()
                        .unwrap(),
                    uncertain
                        .strip_suffix(" uncertain")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
            })
            .collect()
    }

    /// Returns the since and the last write or read each state record names.
    fn state_record_times(logs: &[(log::Level, String)]) -> Vec<(UnixNanos, Option<UnixNanos>)> {
        logs.iter()
            .filter(|(_, line)| line.starts_with("Stream "))
            .map(|(_, line)| {
                let (head, values) = line.split_once(", last ").unwrap();
                let since = head.split_once(" since ").unwrap().1;
                let (_, values) = values.split_once(' ').unwrap();
                let last = values.split(", ").next().unwrap();
                (
                    since.parse().unwrap(),
                    (last != "none").then(|| last.parse().unwrap()),
                )
            })
            .collect()
    }

    /// A loopback RESP server standing in for Redis, which answers every queued `XADD` with
    /// `queued_error` until `accept_writes` is set, so each `EXEC` aborts with `EXECABORT`. It
    /// records every `XADD` it receives in `attempted`, and those of committed transactions in
    /// `written`, with the time of each commit in `commits` (and on the realtime clock in
    /// `committed_at`) and of each accepted connection in `connections`. While `available` is
    /// unset it closes each connection at once, and while `stall_writes` is set it leaves each
    /// `EXEC` unanswered and uncommitted. Each `XTRIM` is counted in `trims` and answered with
    /// `trim_error` when set. An `EXEC` executes its `XADD`s on `wrong_type_key` in part: each of
    /// those is answered `WRONGTYPE` and not written, the others are written, and the transaction
    /// is not counted committed. The first `demoted_connections` connections answer every `XADD`
    /// with `queued_error` whatever `accept_writes`, as a primary demoted to a replica does on a
    /// connection it keeps open. It answers at most `execs_allowed` more `EXEC`s of transactions
    /// whose `XADD`s were queued; a later one waits, with `exec_waiting` set, until more are
    /// allowed.
    #[derive(Clone)]
    struct FakeRedis {
        port: u16,
        started_at: UnixNanos,
        accept_writes: Arc<AtomicBool>,
        demoted_connections: Arc<AtomicUsize>,
        execs_allowed: Arc<AtomicUsize>,
        exec_waiting: Arc<AtomicBool>,
        available: Arc<AtomicBool>,
        stall_writes: Arc<AtomicBool>,
        aborted: Arc<AtomicUsize>,
        committed: Arc<AtomicUsize>,
        trims: Arc<AtomicUsize>,
        trim_error: Arc<std::sync::Mutex<Option<&'static str>>>,
        wrong_type_key: Arc<std::sync::Mutex<Option<String>>>,
        attempted: Arc<std::sync::Mutex<Vec<FakeXadd>>>,
        written: Arc<std::sync::Mutex<Vec<FakeXadd>>>,
        commits: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
        committed_at: Arc<std::sync::Mutex<Vec<UnixNanos>>>,
        connections: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
    }

    impl FakeRedis {
        async fn start(queued_error: &'static str) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let fake = Self {
                port: listener.local_addr().unwrap().port(),
                started_at: get_atomic_clock_realtime().get_time_ns(),
                accept_writes: Arc::new(AtomicBool::new(false)),
                demoted_connections: Arc::new(AtomicUsize::new(0)),
                execs_allowed: Arc::new(AtomicUsize::new(usize::MAX)),
                exec_waiting: Arc::new(AtomicBool::new(false)),
                available: Arc::new(AtomicBool::new(true)),
                stall_writes: Arc::new(AtomicBool::new(false)),
                aborted: Arc::new(AtomicUsize::new(0)),
                committed: Arc::new(AtomicUsize::new(0)),
                trims: Arc::new(AtomicUsize::new(0)),
                trim_error: Arc::new(std::sync::Mutex::new(None)),
                wrong_type_key: Arc::new(std::sync::Mutex::new(None)),
                attempted: Arc::new(std::sync::Mutex::new(Vec::new())),
                written: Arc::new(std::sync::Mutex::new(Vec::new())),
                commits: Arc::new(std::sync::Mutex::new(Vec::new())),
                committed_at: Arc::new(std::sync::Mutex::new(Vec::new())),
                connections: Arc::new(std::sync::Mutex::new(Vec::new())),
            };
            let server = fake.clone();

            tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    let demoted = {
                        let mut connections = server.connections.lock().unwrap();
                        connections.push(tokio::time::Instant::now());
                        connections.len() <= server.demoted_connections.load(Ordering::SeqCst)
                    };

                    if !server.available.load(Ordering::SeqCst) {
                        continue; // Closing the socket at once fails the client's first command
                    }

                    tokio::spawn(server.clone().serve(socket, queued_error, demoted));
                }
            });

            fake
        }

        async fn serve(
            self,
            socket: tokio::net::TcpStream,
            queued_error: &'static str,
            demoted: bool,
        ) {
            // Each reply leaves at once, not held for the client's acknowledgement of the last
            socket.set_nodelay(true).unwrap();
            let (read, mut write) = socket.into_split();
            let mut reader = tokio::io::BufReader::new(read);
            let mut queued = 0;
            let mut queue_failed = false;
            let mut queued_xadds = Vec::new();

            while let Some(command) = read_resp_command(&mut reader).await {
                if !self.available.load(Ordering::SeqCst) {
                    break;
                }

                if command[0].eq_ignore_ascii_case(b"EXEC")
                    && !queue_failed
                    && self.stall_writes.load(Ordering::SeqCst)
                {
                    continue;
                }

                if command[0].eq_ignore_ascii_case(b"EXEC") && !queue_failed {
                    while self
                        .execs_allowed
                        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_err()
                    {
                        self.exec_waiting.store(true, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                    }
                }

                if command[0].eq_ignore_ascii_case(b"XADD") {
                    self.attempted
                        .lock()
                        .unwrap()
                        .push(FakeXadd::parse(&command));
                }

                let reply = match command[0].to_ascii_uppercase().as_slice() {
                    b"INFO" => {
                        let info = "redis_version:8.0.0\r\n";
                        format!("${}\r\n{info}\r\n", info.len())
                    }
                    b"MULTI" => {
                        queued = 0;
                        queue_failed = false;
                        queued_xadds.clear();
                        "+OK\r\n".to_string()
                    }
                    b"XADD" if self.accept_writes.load(Ordering::SeqCst) && !demoted => {
                        queued += 1;
                        queued_xadds.push(FakeXadd::parse(&command));
                        "+QUEUED\r\n".to_string()
                    }
                    b"XADD" => {
                        queue_failed = true;
                        format!("-{queued_error}\r\n")
                    }
                    b"EXEC" if queue_failed => {
                        self.aborted.fetch_add(1, Ordering::SeqCst);
                        "-EXECABORT Transaction discarded because of previous errors.\r\n"
                            .to_string()
                    }
                    b"EXEC" => {
                        let wrong_type_key = self.wrong_type_key.lock().unwrap().clone();
                        let mut replies = String::new();
                        let mut executed_in_part = false;

                        for xadd in queued_xadds.drain(..) {
                            if Some(&xadd.key) == wrong_type_key.as_ref() {
                                executed_in_part = true;
                                replies.push_str(
                                    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                                );
                            } else {
                                self.written.lock().unwrap().push(xadd);
                                replies.push_str("$3\r\n0-1\r\n");
                            }
                        }

                        if !executed_in_part {
                            self.committed.fetch_add(1, Ordering::SeqCst);
                            self.commits
                                .lock()
                                .unwrap()
                                .push(tokio::time::Instant::now());
                            self.committed_at
                                .lock()
                                .unwrap()
                                .push(get_atomic_clock_realtime().get_time_ns());
                        }
                        format!("*{queued}\r\n{replies}")
                    }
                    b"XTRIM" => {
                        self.trims.fetch_add(1, Ordering::SeqCst);
                        match *self.trim_error.lock().unwrap() {
                            Some(error) => format!("-{error}\r\n"),
                            None => ":0\r\n".to_string(),
                        }
                    }
                    _ => "+OK\r\n".to_string(),
                };

                if write.write_all(reply.as_bytes()).await.is_err() {
                    break;
                }
            }
        }

        fn config(&self) -> RedisMessageBusConfig {
            RedisMessageBusConfig {
                port: Some(self.port),
                ..Default::default()
            }
        }

        fn written(&self) -> Vec<FakeXadd> {
            self.written.lock().unwrap().clone()
        }

        fn written_len(&self) -> usize {
            self.written.lock().unwrap().len()
        }

        fn written_sequences(&self) -> Vec<u64> {
            self.written().iter().map(FakeXadd::sequence).collect()
        }

        /// Returns the publish task's state records in `logs` as [`state_records`] does, after
        /// checking the times they name on the realtime clock: each since lies between this fake's
        /// start and now, in order, and each last write is the reply to the last transaction this
        /// fake committed before the record (none before the first commit, otherwise at or after
        /// that commit and at or before the record).
        fn write_records(
            &self,
            logs: &[(log::Level, String)],
        ) -> Vec<(String, bool, usize, usize)> {
            let committed_at = self.committed_at.lock().unwrap().clone();
            let now = get_atomic_clock_realtime().get_time_ns();
            let mut earliest = self.started_at;

            for (since, last_write) in state_record_times(logs) {
                assert!(
                    earliest <= since && since <= now,
                    "since {since} not in [{earliest}, {now}]"
                );
                earliest = since;

                let last_commit = committed_at.iter().copied().filter(|&at| at <= since).max();
                match (last_commit, last_write) {
                    (None, None) => {}
                    (Some(commit), Some(written)) => assert!(
                        commit <= written && written <= since,
                        "last write {written} not in [{commit}, {since}]"
                    ),
                    other => panic!("last write and last commit differ: {other:?}"),
                }
            }

            state_records(logs)
        }
    }

    /// One `XADD key [MAXLEN ~ n] * field value ...` a [`FakeRedis`] received.
    #[derive(Clone, Debug)]
    struct FakeXadd {
        key: String,
        maxlen: Option<String>,
        fields: Vec<(String, Vec<u8>)>,
    }

    impl FakeXadd {
        fn parse(command: &[Vec<u8>]) -> Self {
            let text = |arg: &Vec<u8>| String::from_utf8(arg.clone()).unwrap();
            let (maxlen, id_at) = if command[2].eq_ignore_ascii_case(b"MAXLEN") {
                assert_eq!(command[3], b"~");
                (Some(text(&command[4])), 5)
            } else {
                (None, 2)
            };
            assert_eq!(command[id_at], b"*");

            Self {
                key: text(&command[1]),
                maxlen,
                fields: command[id_at + 1..]
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|[field, value]| (text(field), value.clone()))
                    .collect(),
            }
        }

        fn field(&self, name: &str) -> Option<String> {
            self.fields
                .iter()
                .find(|(field, _)| field == name)
                .map(|(_, value)| String::from_utf8(value.clone()).unwrap())
        }

        fn sequence(&self) -> u64 {
            self.field(SEQUENCE_FIELD).unwrap().parse().unwrap()
        }
    }

    const FAKE_PUBLISHER_ID: &str = "PUBLISHER-007";
    const FAKE_INSTANCE_ID: &str = "2d89666b-1a1e-4a75-b193-4eb3b454c757";

    /// The entries of each stream of a [`FakeRedisStreams`], as `(n, payload)` of the entry `<n>-0`.
    type FakeStreams = Vec<Vec<(usize, String)>>;

    /// One `XREAD` a [`FakeRedisStreams`] answered: its `COUNT`, the ID it read after in each
    /// stream, and how many entries of each stream the reply carried.
    struct FakeStreamRead {
        count: Option<usize>,
        ids: Vec<String>,
        replied: Vec<usize>,
    }

    /// The payload of an entry a [`FakeRedisStreams`] writes with a sequence that is not a number,
    /// so it cannot be decoded.
    const FAKE_UNDECODABLE: &str = "undecodable";

    /// A loopback RESP server standing in for Redis, which holds streams of entries `<n>-0`
    /// carrying the stream key as topic, `n` as payload and the source of [`FAKE_PUBLISHER_ID`],
    /// [`FAKE_INSTANCE_ID`] and number `n`, answers `XREAD` as Redis does, and records each read.
    /// It records the time of each accepted connection in `connections`. While `available` is unset
    /// it closes each connection at once, and while `wrong_type_reads` is above zero it answers
    /// that many reads with Redis's `WRONGTYPE` error, which the client does not retry, at the times
    /// in `wrong_type_at`. While `silent_reads` is above zero it leaves that many reads unanswered,
    /// the last at `silent_read_at`. `read_held` is set while the last read it received is held
    /// unanswered (empty, until its 10 ms nil reply, or silent), and `reads_held` counts the reads
    /// it held. It queues each `XADD` of a transaction and records it in `written` at `EXEC`.
    #[derive(Clone)]
    struct FakeRedisStreams {
        port: u16,
        streams: Arc<std::sync::Mutex<FakeStreams>>,
        reads: Arc<std::sync::Mutex<Vec<FakeStreamRead>>>,
        available: Arc<AtomicBool>,
        wrong_type_reads: Arc<AtomicUsize>,
        wrong_type_at: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
        silent_reads: Arc<AtomicUsize>,
        silent_read_at: Arc<std::sync::Mutex<Option<tokio::time::Instant>>>,
        read_held: Arc<AtomicBool>,
        reads_held: Arc<AtomicUsize>,
        written: Arc<std::sync::Mutex<Vec<FakeXadd>>>,
        connections: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
    }

    impl FakeRedisStreams {
        async fn start(stream_keys: &[String], backlog_len: usize) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let backlog: Vec<_> = (1..=backlog_len).map(|n| (n, n.to_string())).collect();
            let fake = Self {
                port: listener.local_addr().unwrap().port(),
                streams: Arc::new(std::sync::Mutex::new(vec![backlog; stream_keys.len()])),
                reads: Arc::new(std::sync::Mutex::new(Vec::new())),
                available: Arc::new(AtomicBool::new(true)),
                wrong_type_reads: Arc::new(AtomicUsize::new(0)),
                wrong_type_at: Arc::new(std::sync::Mutex::new(Vec::new())),
                silent_reads: Arc::new(AtomicUsize::new(0)),
                silent_read_at: Arc::new(std::sync::Mutex::new(None)),
                read_held: Arc::new(AtomicBool::new(false)),
                reads_held: Arc::new(AtomicUsize::new(0)),
                written: Arc::new(std::sync::Mutex::new(Vec::new())),
                connections: Arc::new(std::sync::Mutex::new(Vec::new())),
            };
            let (keys, server) = (stream_keys.to_vec(), fake.clone());

            tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    server
                        .connections
                        .lock()
                        .unwrap()
                        .push(tokio::time::Instant::now());

                    if !server.available.load(Ordering::SeqCst) {
                        continue; // Closing the socket at once fails the client's first command
                    }

                    tokio::spawn(server.clone().serve(socket, keys.clone()));
                }
            });

            fake
        }

        async fn serve(self, socket: tokio::net::TcpStream, keys: Vec<String>) {
            // Each reply leaves at once, not held for the client's acknowledgement of the last
            socket.set_nodelay(true).unwrap();
            let (read, mut write) = socket.into_split();
            let mut reader = tokio::io::BufReader::new(read);
            let mut queued_xadds = Vec::new();

            while let Some(command) = read_resp_command(&mut reader).await {
                if !self.available.load(Ordering::SeqCst) {
                    break;
                }

                if command[0].eq_ignore_ascii_case(b"XREAD") {
                    self.read_held.store(false, Ordering::SeqCst);

                    if self
                        .silent_reads
                        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
                    {
                        *self.silent_read_at.lock().unwrap() = Some(tokio::time::Instant::now());
                        self.hold_read();
                        continue;
                    }
                }

                let reply = match command[0].to_ascii_uppercase().as_slice() {
                    b"INFO" => {
                        let info = "redis_version:8.0.0\r\n";
                        format!("${}\r\n{info}\r\n", info.len())
                    }
                    b"XREAD"
                        if self
                            .wrong_type_reads
                            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                            .is_ok() =>
                    {
                        self.wrong_type_at
                            .lock()
                            .unwrap()
                            .push(tokio::time::Instant::now());
                        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                            .to_string()
                    }
                    b"XREAD" => {
                        let reply = xread_reply(&command, &keys, &self.streams, &self.reads);
                        if reply.is_none() {
                            // An empty blocking read times out with a nil reply
                            self.hold_read();
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            self.read_held.store(false, Ordering::SeqCst);
                        }
                        reply.unwrap_or_else(|| "*-1\r\n".to_string())
                    }
                    b"MULTI" => {
                        queued_xadds.clear();
                        "+OK\r\n".to_string()
                    }
                    b"XADD" => {
                        queued_xadds.push(FakeXadd::parse(&command));
                        "+QUEUED\r\n".to_string()
                    }
                    b"EXEC" => {
                        let replies = "$3\r\n0-1\r\n".repeat(queued_xadds.len());
                        let len = queued_xadds.len();
                        self.written.lock().unwrap().append(&mut queued_xadds);
                        format!("*{len}\r\n{replies}")
                    }
                    b"XTRIM" => ":0\r\n".to_string(),
                    _ => "+OK\r\n".to_string(),
                };

                if write.write_all(reply.as_bytes()).await.is_err() {
                    break;
                }
            }
        }

        fn hold_read(&self) {
            self.reads_held.fetch_add(1, Ordering::SeqCst);
            self.read_held.store(true, Ordering::SeqCst);
        }

        fn append(&self, n: usize, payload: &str) {
            for stream in self.streams.lock().unwrap().iter_mut() {
                stream.push((n, payload.to_string()));
            }
        }

        fn config(&self) -> RedisMessageBusConfig {
            RedisMessageBusConfig {
                port: Some(self.port),
                ..Default::default()
            }
        }
    }

    /// Answers `XREAD [BLOCK ms] [COUNT n] STREAMS keys... ids...` with each stream's entries after
    /// its ID, at most `COUNT` of each, recording the read; `None` when no stream has any.
    fn xread_reply(
        command: &[Vec<u8>],
        keys: &[String],
        streams: &std::sync::Mutex<FakeStreams>,
        reads: &std::sync::Mutex<Vec<FakeStreamRead>>,
    ) -> Option<String> {
        use std::fmt::Write;

        let args: Vec<String> = command
            .iter()
            .map(|arg| String::from_utf8(arg.clone()).unwrap())
            .collect();
        let count = args
            .iter()
            .position(|arg| arg.eq_ignore_ascii_case("COUNT"))
            .map(|at| args[at + 1].parse::<usize>().unwrap());
        let at = args
            .iter()
            .position(|arg| arg.eq_ignore_ascii_case("STREAMS"))
            .unwrap();
        let (read_keys, ids) = args[at + 1..].split_at((args.len() - at - 1) / 2);
        let bulk = |value: &str| format!("${}\r\n{value}\r\n", value.len());
        let streams = streams.lock().unwrap();
        let mut replied = vec![0; keys.len()];
        let mut reply = String::new();

        for (key, id) in read_keys.iter().zip(ids) {
            let stream = keys.iter().position(|known| known == key).unwrap();
            let after: usize = id.split('-').next().unwrap().parse().unwrap();
            let entries: Vec<_> = streams[stream]
                .iter()
                .filter(|(n, _)| *n > after)
                .take(count.unwrap_or(usize::MAX))
                .collect();
            if entries.is_empty() {
                continue;
            }
            replied[stream] = entries.len();
            write!(reply, "*2\r\n{}*{}\r\n", bulk(key), entries.len()).unwrap();
            for (n, payload) in entries {
                let sequence = if payload == FAKE_UNDECODABLE {
                    "not-a-number".to_string()
                } else {
                    n.to_string()
                };
                let fields = [
                    bulk("topic"),
                    bulk(key),
                    bulk("payload"),
                    bulk(payload),
                    bulk(PUBLISHER_ID_FIELD),
                    bulk(FAKE_PUBLISHER_ID),
                    bulk(INSTANCE_ID_FIELD),
                    bulk(FAKE_INSTANCE_ID),
                    bulk(SEQUENCE_FIELD),
                    bulk(&sequence),
                ];
                write!(
                    reply,
                    "*2\r\n{}*10\r\n{}",
                    bulk(&format!("{n}-0")),
                    fields.concat()
                )
                .unwrap();
            }
        }
        let streams_replied = replied.iter().filter(|&&len| len > 0).count();
        reads.lock().unwrap().push(FakeStreamRead {
            count,
            ids: ids.to_vec(),
            replied,
        });
        (streams_replied > 0).then(|| format!("*{streams_replied}\r\n{reply}"))
    }

    async fn read_resp_command(
        reader: &mut tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
    ) -> Option<Vec<Vec<u8>>> {
        let mut line = String::new();
        if reader.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        let count: usize = line.trim_end().strip_prefix('*')?.parse().ok()?;
        let mut command = Vec::with_capacity(count);

        for _ in 0..count {
            line.clear();
            reader.read_line(&mut line).await.ok()?;
            let len: usize = line.trim_end().strip_prefix('$')?.parse().ok()?;
            let mut arg = vec![0; len + 2];
            reader.read_exact(&mut arg).await.ok()?;
            arg.truncate(len);
            command.push(arg);
        }

        Some(command)
    }
}

#[cfg(target_os = "linux")] // Run Redis tests on Linux platforms only
#[cfg(test)]
mod serial_tests {
    use std::{
        sync::{atomic::AtomicUsize, mpsc},
        thread,
    };

    #[cfg(feature = "python")]
    use nautilus_common::python::msgbus::get_global_msgbus_factory_registry;
    use nautilus_common::{
        enums::Environment,
        msgbus::{self, TypedHandler},
        testing::wait_until_async,
    };
    use nautilus_live::{
        builder::LiveNodeBuilder,
        config::{LiveExecutionEngineConfig, LiveNodeConfig},
    };
    use nautilus_model::data::{QuoteTick, TradeTick};
    #[cfg(feature = "python")]
    use pyo3::{Py, Python, types::PyModule};
    use redis::aio::ConnectionManager;
    use rstest::*;

    use super::*;

    #[fixture]
    async fn redis_connection() -> ConnectionManager {
        let config = RedisMessageBusConfig {
            connection_timeout: 1,
            number_of_retries: 0,
            ..Default::default()
        };
        create_redis_connection(MSGBUS_STREAM, &config)
            .await
            .expect("A running Redis service is required for this test")
    }

    fn redis_msgbus_factory(config: RedisMessageBusConfig) -> Box<dyn MessageBusBackingFactory> {
        #[cfg(feature = "python")]
        {
            Python::initialize();
            Python::attach(|py| {
                let module = PyModule::new(py, "infrastructure").unwrap();
                crate::python::infrastructure(py, &module).unwrap();
                let factory = Py::new(py, config).unwrap().into_any();

                get_global_msgbus_factory_registry()
                    .extract(py, factory)
                    .unwrap()
            })
        }

        #[cfg(not(feature = "python"))]
        Box::new(config)
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_terminate_signal(#[future] redis_connection: ConnectionManager) {
        let _con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let stream_key = get_stream_key(trader_id, instance_id, &config);
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = Arc::clone(&stream_signal);

        // Start the message streaming task
        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        stream_signal.store(true, Ordering::Relaxed);
        let _ = rx.recv().await; // Wait for the tx to close

        // Shutdown and cleanup
        rx.close();
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_when_receiver_closed(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let stream_key = get_stream_key(trader_id, instance_id, &config);
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = Arc::clone(&stream_signal);

        // Use a message ID in the future, as streaming begins
        // around the timestamp the task is spawned.
        let clock = get_atomic_clock_realtime();
        let future_id = (clock.get_time_ms() + 1_000_000).to_string();

        // Publish test message
        let _: () = con
            .xadd(
                stream_key,
                future_id,
                &[("topic", "topic1"), ("payload", "data1")],
            )
            .await
            .unwrap();

        // Immediately close channel
        rx.close();

        // Start the message streaming task
        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        // Shutdown and cleanup
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages(#[future] redis_connection: ConnectionManager) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let stream_key = get_stream_key(trader_id, instance_id, &config);
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = Arc::clone(&stream_signal);

        // Use a message ID in the future, as streaming begins
        // around the timestamp the task is spawned.
        let clock = get_atomic_clock_realtime();
        let future_id = (clock.get_time_ms() + 1_000_000).to_string();

        // Publish test message
        let _: () = con
            .xadd(
                stream_key,
                future_id,
                &[("topic", "topic1"), ("payload", "data1")],
            )
            .await
            .unwrap();

        // Start the message streaming task
        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        // Receive and verify the message
        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "topic1");
        assert_eq!(msg.payload, Bytes::from("data1"));

        // Shutdown and cleanup
        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_skips_preexisting_entries(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let suffix = UUID4::new();
        let stream_key = format!("test:stream:no-backlog:{suffix}");
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = Arc::clone(&stream_signal);

        let _: () = con
            .xadd(
                &stream_key,
                "1-0",
                &[("topic", "preexisting"), ("payload", "old")],
            )
            .await
            .unwrap();

        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        let future_id = (get_atomic_clock_realtime().get_time_ms() + 1_000_000).to_string();
        let _: () = con
            .xadd(
                &stream_key,
                future_id,
                &[("topic", "live"), ("payload", "new")],
            )
            .await
            .unwrap();

        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;

        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();
        let _: usize = con.del(stream_key).await.unwrap();

        assert_eq!(msg.topic, "live");
        assert_eq!(msg.payload, Bytes::from("new"));
    }

    async fn read_backlog_then_live(
        con: &mut ConnectionManager,
        start: StreamStart,
    ) -> Vec<(String, Bytes)> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);
        let stream_key = format!("test:stream:start:{}", UUID4::new());
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = Arc::clone(&stream_signal);

        for (id, payload) in [("1-0", "first"), ("2-0", "second")] {
            let _: () = con
                .xadd(
                    &stream_key,
                    id,
                    &[("topic", "backlog"), ("payload", payload)],
                )
                .await
                .unwrap();
        }

        let handle = tokio::spawn(async move {
            stream_messages_from(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                start,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        let future_id = (get_atomic_clock_realtime().get_time_ms() + 1_000_000).to_string();
        let _: () = con
            .xadd(
                &stream_key,
                future_id,
                &[("topic", "live"), ("payload", "new")],
            )
            .await
            .unwrap();

        let mut received = Vec::new();
        loop {
            let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
            let is_live = msg.topic == "live";
            received.push((msg.topic.to_string(), msg.payload));
            if is_live {
                break;
            }
        }

        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();
        let _: usize = con.del(stream_key).await.unwrap();

        received
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_from_beginning_replays_backlog_then_tails(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;

        let received = read_backlog_then_live(&mut con, StreamStart::Beginning).await;

        assert_eq!(
            received,
            vec![
                ("backlog".to_string(), Bytes::from("first")),
                ("backlog".to_string(), Bytes::from("second")),
                ("live".to_string(), Bytes::from("new")),
            ]
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_from_now_skips_backlog(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;

        let received = read_backlog_then_live(&mut con, StreamStart::Now).await;

        assert_eq!(received, vec![("live".to_string(), Bytes::from("new"))]);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_from_beginning_pages_backlog_of_each_stream(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);
        let suffix = UUID4::new();
        let stream_keys = vec![
            format!("test:stream:paged:{suffix}:0"),
            format!("test:stream:paged:{suffix}:1"),
        ];
        let topics = ["paged-0", "paged-1"];
        let backlog_len = 2 * STREAM_READ_BATCH_SIZE + 1;
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = Arc::clone(&stream_signal);

        for (stream_key, topic) in stream_keys.iter().zip(topics) {
            let mut pipe = redis::pipe();
            for i in 1..=backlog_len {
                pipe.xadd(
                    stream_key,
                    format!("{i}-0"),
                    &[("topic", topic.to_string()), ("payload", i.to_string())],
                )
                .ignore();
            }
            let _: () = pipe.query_async(&mut con).await.unwrap();
        }

        let external_streams = stream_keys.clone();
        let handle = tokio::spawn(async move {
            stream_messages_from(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                StreamStart::Beginning,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        let mut received: [Vec<String>; 2] = Default::default();
        let mut receive_until = async |total: usize| {
            while received.iter().map(Vec::len).sum::<usize>() < total {
                let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                    .await
                    .expect("the backlog and the live entries should arrive")
                    .expect("the stream task should keep the channel open");
                let stream = topics
                    .iter()
                    .position(|topic| msg.topic == *topic)
                    .expect("every entry comes from one of the read streams");
                received[stream].push(String::from_utf8(msg.payload.to_vec()).unwrap());
            }
        };
        receive_until(2 * backlog_len).await;

        // Appended only once the whole backlog was received, so a tail read takes them
        let future_id = (get_atomic_clock_realtime().get_time_ms() + 1_000_000).to_string();
        for (stream_key, topic) in stream_keys.iter().zip(topics) {
            let _: () = con
                .xadd(
                    stream_key,
                    &future_id,
                    &[("topic", topic), ("payload", "live")],
                )
                .await
                .unwrap();
        }
        receive_until(2 * backlog_len + 2).await;

        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();
        for stream_key in &stream_keys {
            let _: usize = con.del(stream_key).await.unwrap();
        }

        let expected: Vec<String> = (1..=backlog_len)
            .map(|i| i.to_string())
            .chain(std::iter::once("live".to_string()))
            .collect();
        assert_eq!(received[0], expected);
        assert_eq!(received[1], expected);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_skips_malformed_entry(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let suffix = UUID4::new();
        let stream_key = format!("test:stream:malformed:{suffix}");
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = Arc::clone(&stream_signal);

        let clock = get_atomic_clock_realtime();
        let base_id = clock.get_time_ms() + 1_000_000;

        let _: () = con
            .xadd(
                &stream_key,
                format!("{}", base_id + 1),
                &[("topic", "missing-payload")],
            )
            .await
            .unwrap();
        let _: () = con
            .xadd(
                &stream_key,
                format!("{}", base_id + 2),
                &[("topic", "valid"), ("payload", "data")],
            )
            .await
            .unwrap();

        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;

        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();

        assert_eq!(msg.topic, "valid");
        assert_eq!(msg.payload, Bytes::from("data"));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_read_on_after_an_unrecoverable_read_error(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let suffix = UUID4::new();
        let stream_key = format!("test:stream:wrong-type:{suffix}");
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = Arc::clone(&stream_signal);

        let _: () = con.set(&stream_key, "not-a-stream").await.unwrap();
        let failed_before = failed_stream_reads(&con).await;

        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
        });

        // The key is repaired only once Redis has refused a read of it
        wait_until_async(
            || {
                let con = con.clone();
                async move { failed_stream_reads(&con).await > failed_before }
            },
            Duration::from_secs(5),
        )
        .await;
        let finished_on_the_error = handle.is_finished();
        let _: () = con.del(&stream_key).await.unwrap();
        let future_id = (get_atomic_clock_realtime().get_time_ms() + 1_000_000).to_string();
        let _: () = con
            .xadd(
                &stream_key,
                future_id,
                &[("topic", "live"), ("payload", "after")],
            )
            .await
            .unwrap();
        let msg = receive_bus_message(&mut rx, Duration::from_secs(5)).await;

        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        let result = handle.await.unwrap();
        let _: () = con.del(&stream_key).await.unwrap();

        assert!(!finished_on_the_error);
        assert!(result.is_ok());
        assert_eq!(msg.payload, Bytes::from("after"));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_connection_returns_none_when_signaled() {
        let config = RedisMessageBusConfig {
            port: Some(1),
            connection_timeout: 20,
            ..Default::default()
        };
        let stream_signal = Arc::new(AtomicBool::new(true));
        let signal = Arc::clone(&stream_signal);
        let handle = tokio::spawn(async move { connect_stream_connection(&config, &signal).await });

        wait_until_async(|| async { handle.is_finished() }, Duration::from_secs(1)).await;

        assert!(handle.await.unwrap().unwrap().is_none());
    }

    #[rstest]
    #[case::market_data(BusPayloadType::QuoteTick)]
    #[case::typed(BusPayloadType::TradingCommand)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_messages(
        #[future] redis_connection: ConnectionManager,
        #[case] payload_type: BusPayloadType,
    ) {
        let mut con = redis_connection.await;
        let (tx, rx) = publish_channel();

        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);

        // Start the publish_messages task
        let handle = tokio::spawn(async move {
            publish_messages(
                rx,
                trader_id,
                instance_id,
                config,
                RedisMessageBusConfig::default(),
            )
            .await
            .unwrap();
        });

        // Send a test message
        let msg = BusMessage::with_str_topic(
            "test_topic",
            payload_type,
            Bytes::from("test_payload"),
            SerializationEncoding::Json,
        );
        tx.send(msg).unwrap();

        // Wait until the message is published to Redis
        wait_until_async(
            || {
                let mut con = con.clone();
                let stream_key = stream_key.clone();
                async move {
                    let messages: RedisStreamBulk =
                        con.xread(&[&stream_key], &["0"]).await.unwrap();
                    !messages.is_empty()
                }
            },
            Duration::from_secs(3),
        )
        .await;

        // Verify the message was published to Redis
        let messages: RedisStreamBulk = con.xread(&[&stream_key], &["0"]).await.unwrap();
        assert_eq!(messages.len(), 1);
        let stream_msgs = messages[0].get(&stream_key).unwrap();
        let stream_msg_array = &stream_msgs[0].values().next().unwrap();
        let decoded_message = decode_bus_message(stream_msg_array).unwrap();
        assert_eq!(decoded_message.topic, "test_topic");
        assert_eq!(decoded_message.payload_type, payload_type);
        assert_eq!(decoded_message.encoding, SerializationEncoding::Json);
        assert_eq!(decoded_message.payload, Bytes::from("test_payload"));

        // Stop publishing task
        let msg = BusMessage::new_close();
        tx.send(msg).unwrap();

        // Shutdown and cleanup
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_messages_applies_autotrim_maxlen(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, rx) = publish_channel();
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            buffer_interval_ms: Some(10),
            autotrim_mins: Some(60),
            autotrim_maxlen: Some(10),
            use_instance_id: true,
            stream_per_topic: false,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);

        let handle = tokio::spawn(async move {
            publish_messages(
                rx,
                trader_id,
                instance_id,
                config,
                RedisMessageBusConfig::default(),
            )
            .await
            .unwrap();
        });
        let msg = BusMessage::with_str_topic(
            "test_topic",
            BusPayloadType::QuoteTick,
            Bytes::from_static(b"test_payload"),
            SerializationEncoding::Json,
        );

        // The integration service uses Redis's default stream node limits,
        // so these writes span multiple macro nodes.
        let messages_sent = 250;

        for _ in 0..messages_sent {
            tx.send(msg.clone()).unwrap();
        }
        tx.send(BusMessage::new_close()).unwrap();
        handle.await.unwrap();

        let stream_len: usize = con.xlen(&stream_key).await.unwrap();
        let _: usize = con.del(stream_key).await.unwrap();

        assert!(stream_len >= 10);
        assert!(stream_len < messages_sent);
    }

    #[rstest]
    #[case::immediate(None)]
    #[case::buffered(Some(10))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_messages_retries_buffer_after_connection_loss(
        #[future] redis_connection: ConnectionManager,
        #[case] buffer_interval_ms: Option<u32>,
    ) {
        let mut con = redis_connection.await;
        let proxy = RedisProxy::start().await;
        let (tx, rx) = publish_channel();
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            buffer_interval_ms,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);
        let backing = proxy.config();

        let handle = tokio::spawn(async move {
            publish_messages(rx, trader_id, instance_id, config, backing).await
        });

        tx.send(test_bus_message("first")).unwrap();
        wait_for_stream_len(&con, &stream_key, 1).await;

        // The next write fails on the dropped connection and new connections are closed at once,
        // so the backlog builds up and is written in several batches once the proxy accepts again
        proxy.interrupt().await;
        let backlog: Vec<String> = (0..2_500).map(|i| format!("backlog-{i}")).collect();
        for topic in &backlog {
            tx.send(test_bus_message(topic)).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(read_stream_topics(&mut con, &stream_key).await, ["first"]);
        assert!(!handle.is_finished());

        proxy.resume();
        wait_for_last_topic(&con, &stream_key, backlog.last().unwrap()).await;
        tx.send(test_bus_message("last")).unwrap();
        wait_for_last_topic(&con, &stream_key, "last").await;

        let topics = read_stream_topics(&mut con, &stream_key).await;
        tx.send(BusMessage::new_close()).unwrap();
        let result = handle.await.unwrap();
        let _: usize = con.del(&stream_key).await.unwrap();

        // Seeing `first` on the stream does not show that its acknowledgement reached the
        // publisher before the interruption, and if it did not, `first` is written again.
        let mut expected = vec!["first".to_string()];
        if topics.get(1).is_some_and(|topic| topic == "first") {
            expected.push("first".to_string());
        }
        expected.extend(backlog);
        expected.push("last".to_string());
        assert_eq!(topics, expected);
        assert!(result.is_ok());
    }

    #[rstest]
    #[case::no_autotrim(None)]
    #[case::autotrim(Some(60))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_messages_close_returns_during_outage(
        #[future] redis_connection: ConnectionManager,
        #[case] autotrim_mins: Option<u32>,
    ) {
        let mut con = redis_connection.await;
        let proxy = RedisProxy::start().await;
        let (tx, rx) = publish_channel();
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();

        // A stream per topic, so the write after the interruption is due a trim
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: true,
            autotrim_mins,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);
        let first_key = format!("{stream_key}:first");
        let second_key = format!("{stream_key}:second");
        let backing = proxy.config();

        let handle = tokio::spawn(async move {
            publish_messages(rx, trader_id, instance_id, config, backing).await
        });

        tx.send(test_bus_message("first")).unwrap();
        wait_for_stream_len(&con, &first_key, 1).await;

        proxy.interrupt().await;
        tx.send(test_bus_message("second")).unwrap();
        tx.send(BusMessage::new_close()).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("close should return during an outage")
            .unwrap();
        let second_len: usize = con.xlen(&second_key).await.unwrap();
        let _: usize = con.del(&[&first_key, &second_key]).await.unwrap();

        // Seeing `first` on the stream does not show that its acknowledgement reached the
        // publisher before the interruption, and if it did not, `first` is still buffered.
        let error = result.unwrap_err().to_string();
        let unpublished = ["1", "2"].map(|count| {
            format!("Failed to publish buffered messages before closing ({count} unpublished)")
        });
        assert_eq!(second_len, 0);
        assert!(unpublished.contains(&error), "{error}");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_messages_drops_a_batch_with_an_unrecoverable_write_error(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, rx) = publish_channel();
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);

        let _: () = con.set(&stream_key, "not-a-stream").await.unwrap();

        let handle = tokio::spawn(async move {
            publish_messages(
                rx,
                trader_id,
                instance_id,
                config,
                RedisMessageBusConfig::default(),
            )
            .await
        });

        // The write fails with `WRONGTYPE`, which is not retried, so its batch is dropped. A key of
        // the wrong type takes no entry, so the batch ends only by that refusal, and the key is
        // repaired only once it has ended.
        tx.send(test_bus_message("first")).unwrap();
        wait_until_async(
            || {
                let pending = Arc::clone(&tx.pending);
                async move {
                    let pending = pending.lock().unwrap();
                    pending.buffer.is_empty() && pending.batch_len == 0
                }
            },
            Duration::from_secs(5),
        )
        .await;
        let finished_after_the_error = handle.is_finished();
        let _: usize = con.del(&stream_key).await.unwrap();

        tx.send(test_bus_message("second")).unwrap();
        wait_for_stream_len(&con, &stream_key, 1).await;
        let topics = read_stream_topics(&mut con, &stream_key).await;
        tx.send(BusMessage::new_close()).unwrap();
        let result = handle.await.unwrap();
        let _: usize = con.del(&stream_key).await.unwrap();

        assert!(!finished_after_the_error);
        assert_eq!(topics, ["second"]);
        assert!(result.is_ok());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_live_nodes_publish_and_ingest_external_redis_stream(
        #[future] redis_connection: ConnectionManager,
    ) {
        let _con = redis_connection.await;
        let redis_config = RedisMessageBusConfig::default();
        let trader_a = TraderId::from("NODEA-001");
        let instance_a = UUID4::new();
        let node_a_msgbus = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_a, instance_a, &node_a_msgbus);
        let node_b_msgbus = MessageBusConfig {
            external_streams: Some(vec![stream_key]),
            stream_per_topic: false,
            ..Default::default()
        };
        let quote = QuoteTick::default();
        let trade = TradeTick::default();
        let ready_topic = "test.quotes.ready";
        let (built_tx, built_rx) = mpsc::channel::<()>();
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (quote_tx, quote_rx) = mpsc::channel::<QuoteTick>();
        let (trade_tx, trade_rx) = mpsc::channel::<TradeTick>();

        let node_b = thread::spawn({
            let redis_config = redis_config.clone();
            move || -> anyhow::Result<()> {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()?;

                runtime.block_on(async move {
                    let config = LiveNodeConfig {
                        environment: Environment::Sandbox,
                        trader_id: TraderId::from("NODEB-001"),
                        msgbus: Some(node_b_msgbus),
                        exec_engine: LiveExecutionEngineConfig {
                            reconciliation: false,
                            ..Default::default()
                        },
                        delay_post_stop: Duration::ZERO,
                        timeout_connection: Duration::from_millis(500),
                        timeout_disconnection: Duration::from_millis(500),
                        ..Default::default()
                    };
                    let mut node = LiveNodeBuilder::from_config(config)?
                        .with_external_msgbus_factory(redis_msgbus_factory(redis_config))
                        .build()?;
                    let handle = node.handle();
                    let quote_handler = TypedHandler::from({
                        let quote_tx = quote_tx.clone();
                        let handle = handle.clone();
                        move |quote: &QuoteTick| {
                            let _ = quote_tx.send(*quote);
                            handle.stop();
                        }
                    });
                    let trade_handler = TypedHandler::from(move |trade: &TradeTick| {
                        let _ = trade_tx.send(*trade);
                    });

                    let ready_handler = TypedHandler::from(move |_: &QuoteTick| {
                        let _ = ready_tx.send(());
                    });

                    msgbus::subscribe_quotes(ready_topic.into(), ready_handler, None);
                    msgbus::subscribe_quotes("data.quotes.*".into(), quote_handler, None);
                    msgbus::subscribe_trades("data.trades.*".into(), trade_handler, None);
                    msgbus::get_message_bus()
                        .borrow_mut()
                        .add_streaming_type(BusPayloadType::QuoteTick);
                    built_tx.send(())?;
                    let result = tokio::time::timeout(Duration::from_secs(10), node.run()).await;
                    msgbus::get_message_bus().borrow_mut().dispose();

                    match result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e),
                        Err(e) => anyhow::bail!("node B timed out: {e}"),
                    }
                })
            }
        });

        built_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("node B should build and register subscriptions");

        let node_a = thread::spawn(move || -> anyhow::Result<()> {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;

            runtime.block_on(async move {
                let config = LiveNodeConfig {
                    environment: Environment::Sandbox,
                    trader_id: trader_a,
                    instance_id: Some(instance_a),
                    msgbus: Some(node_a_msgbus),
                    exec_engine: LiveExecutionEngineConfig {
                        reconciliation: false,
                        ..Default::default()
                    },
                    delay_post_stop: Duration::ZERO,
                    timeout_connection: Duration::from_millis(500),
                    timeout_disconnection: Duration::from_millis(500),
                    ..Default::default()
                };
                let _node = LiveNodeBuilder::from_config(config)?
                    .with_external_msgbus_factory(Box::new(redis_config))
                    .build()?;

                // A running node does not guarantee its Redis reader has set its initial cursor
                wait_until_async(
                    || {
                        let ready = ready_rx.try_recv().is_ok();

                        if !ready {
                            msgbus::publish_quote(ready_topic.into(), &quote);
                        }

                        async move { ready }
                    },
                    Duration::from_secs(5),
                )
                .await;

                msgbus::publish_trade("data.trades.TEST".into(), &trade);
                msgbus::publish_quote("data.quotes.TEST".into(), &quote);
                msgbus::get_message_bus().borrow_mut().dispose();

                Ok(())
            })
        });

        node_a
            .join()
            .expect("node A thread should not panic")
            .expect("node A should publish externally");
        let received_quote = quote_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("node B should republish the registered quote type");
        node_b
            .join()
            .expect("node B thread should not panic")
            .expect("node B should ingest and stop cleanly");

        assert_eq!(received_quote, quote);
        assert!(
            trade_rx.try_recv().is_err(),
            "unregistered trade type should not republish internally"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_multiple_streams(#[future] redis_connection: ConnectionManager) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        // Setup multiple stream keys
        let suffix = UUID4::new();
        let stream_key1 = format!("test:stream:{suffix}:1");
        let stream_key2 = format!("test:stream:{suffix}:2");
        let external_streams = vec![stream_key1.clone(), stream_key2.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = Arc::clone(&stream_signal);

        let clock = get_atomic_clock_realtime();
        let base_id = clock.get_time_ms() + 1_000_000;

        // Start streaming task
        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        // Publish to stream 1 at higher ID
        let _: () = con
            .xadd(
                &stream_key1,
                format!("{}", base_id + 100),
                &[("topic", "stream1-first"), ("payload", "data")],
            )
            .await
            .unwrap();

        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "stream1-first");

        // Publish to stream 2 at lower ID (tests independent cursor tracking)
        let _: () = con
            .xadd(
                &stream_key2,
                format!("{}", base_id + 50),
                &[("topic", "stream2-second"), ("payload", "data")],
            )
            .await
            .unwrap();

        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "stream2-second");

        // Shutdown and cleanup
        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_interleaved_at_different_rates(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        // Setup multiple stream keys
        let suffix = UUID4::new();
        let stream_key1 = format!("test:stream:interleaved:{suffix}:1");
        let stream_key2 = format!("test:stream:interleaved:{suffix}:2");
        let stream_key3 = format!("test:stream:interleaved:{suffix}:3");
        let external_streams = vec![
            stream_key1.clone(),
            stream_key2.clone(),
            stream_key3.clone(),
        ];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = Arc::clone(&stream_signal);

        let clock = get_atomic_clock_realtime();
        let base_id = clock.get_time_ms() + 1_000_000;

        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        // Stream 1 advances with high ID
        let _: () = con
            .xadd(
                &stream_key1,
                format!("{}", base_id + 100),
                &[("topic", "s1m1"), ("payload", "data")],
            )
            .await
            .unwrap();
        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "s1m1");

        // Stream 2 gets message at lower ID - would be skipped with global cursor
        let _: () = con
            .xadd(
                &stream_key2,
                format!("{}", base_id + 50),
                &[("topic", "s2m1"), ("payload", "data")],
            )
            .await
            .unwrap();
        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "s2m1");

        // Stream 3 gets message at even lower ID
        let _: () = con
            .xadd(
                &stream_key3,
                format!("{}", base_id + 25),
                &[("topic", "s3m1"), ("payload", "data")],
            )
            .await
            .unwrap();
        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "s3m1");

        // Shutdown and cleanup
        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_close() {
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let mut db = RedisMessageBusBacking::new(
            trader_id,
            instance_id,
            config,
            RedisMessageBusConfig::default(),
        )
        .unwrap();

        // Close the message bus backing (test should not hang)
        MessageBusBacking::close(&mut db);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_heartbeat_task() {
        let (tx, rx) = publish_channel();
        let signal = Arc::new(AtomicBool::new(false));
        let sources = StreamSources::new(
            TraderId::from("tester-001"),
            UUID4::new(),
            &MessageBusConfig::default(),
        );

        // Start the heartbeat task with a short interval
        let handle = tokio::spawn(run_heartbeat(1, Arc::clone(&signal), tx, sources));

        let heartbeat = receive_admitted_bus_message(&rx, Duration::from_secs(2)).await;

        // Stop the heartbeat task
        signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();

        // Ensure heartbeats were sent
        assert_eq!(heartbeat.topic, HEARTBEAT_TOPIC);
    }

    async fn receive_bus_message(
        rx: &mut tokio::sync::mpsc::Receiver<BusMessage>,
        timeout: Duration,
    ) -> BusMessage {
        let mut received = None;

        wait_until_async(
            || {
                if received.is_none() {
                    received = rx.try_recv().ok();
                }

                let has_received = received.is_some();
                async move { has_received }
            },
            timeout,
        )
        .await;

        received.unwrap()
    }

    async fn receive_admitted_bus_message(rx: &PublishReceiver, timeout: Duration) -> BusMessage {
        let mut received = None;

        wait_until_async(
            || {
                if received.is_none() {
                    received = rx
                        .pending
                        .lock()
                        .unwrap()
                        .buffer
                        .pop_front()
                        .map(|pending| pending.message);
                }

                let has_received = received.is_some();
                async move { has_received }
            },
            timeout,
        )
        .await;

        received.unwrap()
    }

    fn test_bus_message(topic: &str) -> BusMessage {
        BusMessage::with_str_topic(
            topic,
            BusPayloadType::QuoteTick,
            Bytes::from("test_payload"),
            SerializationEncoding::Json,
        )
    }

    /// Returns how many `XREAD` calls the local Redis has failed since it started (its
    /// `commandstats`).
    async fn failed_stream_reads(con: &ConnectionManager) -> u64 {
        let mut con = con.clone();
        let info: String = redis::cmd("INFO")
            .arg("commandstats")
            .query_async(&mut con)
            .await
            .unwrap();
        info.lines()
            .find_map(|line| line.strip_prefix("cmdstat_xread:"))
            .and_then(|stats| {
                stats
                    .split(',')
                    .find_map(|stat| stat.strip_prefix("failed_calls="))
            })
            .map_or(0, |count| count.trim().parse().unwrap())
    }

    async fn wait_for_stream_len(con: &ConnectionManager, stream_key: &str, len: usize) {
        wait_until_async(
            || {
                let mut con = con.clone();
                let stream_key = stream_key.to_string();
                async move {
                    let stream_len: usize = con.xlen(&stream_key).await.unwrap();
                    stream_len >= len
                }
            },
            Duration::from_secs(5),
        )
        .await;
    }

    async fn wait_for_last_topic(con: &ConnectionManager, stream_key: &str, topic: &str) {
        wait_until_async(
            || {
                let mut con = con.clone();
                let stream_key = stream_key.to_string();
                let topic = topic.to_string();
                async move {
                    read_stream_topics(&mut con, &stream_key).await.last() == Some(&topic)
                }
            },
            Duration::from_secs(5),
        )
        .await;
    }

    async fn read_stream_topics(con: &mut ConnectionManager, stream_key: &str) -> Vec<String> {
        let messages: RedisStreamBulk = con.xread(&[stream_key], &["0"]).await.unwrap();
        messages
            .iter()
            .flat_map(HashMap::values)
            .flatten()
            .flat_map(HashMap::values)
            .map(|fields| decode_bus_message(fields).unwrap().topic.to_string())
            .collect()
    }

    /// A loopback TCP proxy to the local Redis that can drop its connections and close new ones.
    struct RedisProxy {
        port: u16,
        accepting: Arc<AtomicBool>,
        open_connections: Arc<AtomicUsize>,
        disconnect_tx: Arc<tokio::sync::watch::Sender<u64>>,
    }

    impl RedisProxy {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let accepting = Arc::new(AtomicBool::new(true));
            let open_connections = Arc::new(AtomicUsize::new(0));
            let disconnect_tx = Arc::new(tokio::sync::watch::channel(0_u64).0);

            let accepting_clone = Arc::clone(&accepting);
            let open_connections_clone = Arc::clone(&open_connections);
            let disconnect_tx_clone = Arc::clone(&disconnect_tx);

            tokio::spawn(async move {
                while let Ok((mut inbound, _)) = listener.accept().await {
                    // Subscribe before checking, so an interrupt cannot miss this connection
                    let mut disconnect_rx = disconnect_tx_clone.subscribe();

                    if !accepting_clone.load(Ordering::SeqCst) {
                        continue; // Closing the socket at once fails the client's first command
                    }

                    let open_connections = Arc::clone(&open_connections_clone);
                    open_connections.fetch_add(1, Ordering::SeqCst);

                    tokio::spawn(async move {
                        if let Ok(mut outbound) =
                            tokio::net::TcpStream::connect("127.0.0.1:6379").await
                        {
                            tokio::select! {
                                _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                                _ = disconnect_rx.changed() => {}
                            }
                        }
                        open_connections.fetch_sub(1, Ordering::SeqCst);
                    });
                }
            });

            Self {
                port,
                accepting,
                open_connections,
                disconnect_tx,
            }
        }

        fn config(&self) -> RedisMessageBusConfig {
            RedisMessageBusConfig {
                port: Some(self.port),
                ..Default::default()
            }
        }

        /// Drops every open connection and closes new ones at once until [`Self::resume`].
        async fn interrupt(&self) {
            self.accepting.store(false, Ordering::SeqCst);
            self.disconnect_tx.send_modify(|count| *count += 1);

            wait_until_async(
                || {
                    let open_connections = Arc::clone(&self.open_connections);
                    async move { open_connections.load(Ordering::SeqCst) == 0 }
                },
                Duration::from_secs(1),
            )
            .await;
        }

        fn resume(&self) {
            self.accepting.store(true, Ordering::SeqCst);
        }
    }
}
