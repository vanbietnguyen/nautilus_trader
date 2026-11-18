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
//! and heartbeats. Messages are sent via an unbounded `tokio::sync::mpsc`
//! channel to the publish task, which buffers and writes them to Redis
//! streams. Each background task owns its own Redis connection created on
//! the Nautilus runtime.
//!
//! Handles are stored as `Option<JoinHandle>` for idempotent shutdown via
//! `close_async()`. The synchronous `close()` uses `block_in_place` to
//! bridge into the async shutdown path and must be called from outside any
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
//! refused, never wrapped. Heartbeat entries carry no source.
//!
//! The read side fills [`BusMessage::source`] from these fields, with the key the entry was read
//! from as its stream. An entry without them reads as having no source.

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    fmt::Debug,
    str::FromStr,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use bytes::Bytes;
use futures::stream::Stream;
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
    UUID4,
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
    pub_tx: tokio::sync::mpsc::UnboundedSender<BusMessage>,
    pub_handle: Option<tokio::task::JoinHandle<()>>,
    stream_rx: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
    stream_handle: Option<tokio::task::JoinHandle<()>>,
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
    /// Returns an error if the heartbeat interval is configured as zero seconds.
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

        let external_streams = config.external_streams.clone().unwrap_or_default();
        let heartbeat_interval_secs = config.heartbeat_interval_secs;
        let publish = backing.clone();
        let sources = StreamSources::new(trader_id, instance_id, &config);

        let (pub_tx, pub_rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();

        // Create publish task (start the runtime here for now)
        let pub_handle = Some(get_runtime().spawn(async move {
            if let Err(e) = publish_messages(pub_rx, trader_id, instance_id, config, publish).await
            {
                log_task_error(MSGBUS_PUBLISH, &e);
            }
        }));

        // Conditionally create stream task and channel if external streams configured
        let stream_signal = Arc::new(AtomicBool::new(false));
        let (stream_rx, stream_handle) = if external_streams.is_empty() {
            (None, None)
        } else {
            let stream_signal_clone = Arc::clone(&stream_signal);
            let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<BusMessage>(100_000);
            (
                Some(stream_rx),
                Some(get_runtime().spawn(async move {
                    if let Err(e) = stream_messages(
                        stream_tx,
                        backing.clone(),
                        external_streams,
                        stream_signal_clone,
                    )
                    .await
                    {
                        log_task_error(MSGBUS_STREAM, &e);
                    }
                })),
            )
        };

        // Create heartbeat task
        let heartbeat_signal = Arc::new(AtomicBool::new(false));
        let heartbeat_handle = if let Some(heartbeat_interval_secs) = heartbeat_interval_secs {
            let signal = Arc::clone(&heartbeat_signal);
            let pub_tx_clone = pub_tx.clone();

            Some(get_runtime().spawn(async move {
                run_heartbeat(heartbeat_interval_secs, signal, pub_tx_clone).await;
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
    fn reserve_source(&self, topic: Ustr) -> anyhow::Result<Option<BusMessageSource>> {
        self.sources.reserve(topic).map(Some)
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

    /// Closes the message bus backing.
    fn close(&mut self) {
        log::debug!("Closing");

        self.stream_signal.store(true, Ordering::Relaxed);
        self.heartbeat_signal.store(true, Ordering::Relaxed);

        if !self.pub_tx.is_closed() {
            let msg = BusMessage::new_close();

            if let Err(e) = self.pub_tx.send(msg) {
                log::warn!("Failed to send close message: {e:?}");
            }
        }

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

    pub async fn close_async(&mut self) {
        await_handle(self.pub_handle.take(), MSGBUS_PUBLISH).await;
        await_handle(self.stream_handle.take(), MSGBUS_STREAM).await;
        await_handle(self.heartbeat_handle.take(), MSGBUS_HEARTBEAT).await;
    }
}

/// Gives the source of each entry one backing writes.
///
/// A source names the backing's trader ID and instance ID, the key the entry is written on, and
/// its number: the last number of that key plus one, from 1. The last number of each key is kept
/// for the backing's life, and a key whose numbers ran out refuses every later entry.
#[derive(Debug)]
struct StreamSources {
    trader_id: TraderId,
    instance_id: UUID4,
    stream_key: String,
    stream_per_topic: bool,
    last_sequences: Mutex<AHashMap<Ustr, u64>>,
}

impl StreamSources {
    fn new(trader_id: TraderId, instance_id: UUID4, config: &MessageBusConfig) -> Self {
        Self {
            trader_id,
            instance_id,
            stream_key: get_stream_key(trader_id, instance_id, config),
            stream_per_topic: config.stream_per_topic,
            last_sequences: Mutex::new(AHashMap::new()),
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

/// Returns the key an entry of `topic` is written on: `stream_key`, or with one stream per topic,
/// `<stream_key>:<topic>`.
fn stream_entry_key<'a>(stream_key: &'a str, stream_per_topic: bool, topic: &str) -> Cow<'a, str> {
    if stream_per_topic {
        Cow::Owned(format!("{stream_key}:{topic}"))
    } else {
        Cow::Borrowed(stream_key)
    }
}

/// Publishes messages received on `rx` to Redis streams for the given `trader_id` and `instance_id`, using `config`.
///
/// Buffered messages are written in batches of at most 1,000 per `MULTI`/`EXEC`, and each batch
/// is only removed once its `EXEC` is acknowledged. On a retryable write error the task keeps
/// receiving, waits out the stream reader's backoff, and retries the same messages on a fresh
/// connection, so a batch whose reply was lost can be written twice.
///
/// # Errors
///
/// Returns an error if:
/// - The backing configuration is missing in `config`.
/// - Establishing the initial Redis connection fails.
/// - A Redis write fails with a non-retryable error.
/// - The buffered messages cannot be written when the task closes.
pub async fn publish_messages(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<BusMessage>,
    trader_id: TraderId,
    instance_id: UUID4,
    config: MessageBusConfig,
    backing: RedisMessageBusConfig,
) -> anyhow::Result<()> {
    log_task_started(MSGBUS_PUBLISH);

    let con = create_redis_connection(MSGBUS_PUBLISH, &backing).await?;
    let stream_key = get_stream_key(trader_id, instance_id, &config);

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

    let mut writer = StreamWriter {
        con: Some(con),
        // Fresh connections fail fast, so this task's backoff governs rather than the
        // connection manager's retry schedule, and a close is not held up behind it.
        reconnect_config: RedisMessageBusConfig {
            number_of_retries: 0,
            ..backing
        },
        stream_key,
        stream_per_topic: config.stream_per_topic,
        autotrim_duration,
        autotrim_maxlen,
        last_trim_index: HashMap::new(),
        buffer: VecDeque::new(),
        write_error_count: 0,
    };

    // Buffering
    let buffer_interval = Duration::from_millis(u64::from(config.buffer_interval_ms.unwrap_or(0)));

    // A sleep used to trigger periodic flushing of the buffer.
    // When `buffer_interval` is zero we skip using the timer and flush immediately
    // after every message.
    let flush_timer = tokio::time::sleep(buffer_interval);
    tokio::pin!(flush_timer);

    // A sleep used to retry the buffer after a retryable write error,
    // only polled while a retry is pending.
    let retry_timer = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(retry_timer);

    loop {
        let retry_delay = tokio::select! {
            maybe_msg = rx.recv() => {
                let Some(msg) = maybe_msg else {
                    log::debug!("Channel hung up");
                    break;
                };

                if msg.topic == CLOSE_TOPIC {
                    log::debug!("Received close message");
                    break;
                }

                writer.buffer.push_back(msg);

                // Immediate flush mode, unless a retry is pending
                if buffer_interval.is_zero() && !writer.is_retrying() {
                    writer.flush().await?
                } else {
                    None
                }
            }
            // Only poll the timer when the interval is non-zero. This avoids
            // unnecessarily waking the task when immediate flushing is enabled.
            () = &mut flush_timer, if !buffer_interval.is_zero() => {
                let retry_delay = if writer.is_retrying() {
                    None
                } else {
                    writer.flush().await?
                };

                // Schedule the next tick
                flush_timer.as_mut().reset(tokio::time::Instant::now() + buffer_interval);
                retry_delay
            }
            () = &mut retry_timer, if writer.is_retrying() => writer.flush().await?,
        };

        if let Some(retry_delay) = retry_delay {
            retry_timer
                .as_mut()
                .reset(tokio::time::Instant::now() + retry_delay);
        }
    }

    // Drain any remaining messages, with a single attempt
    if writer.flush().await?.is_some() {
        anyhow::bail!(
            "Failed to publish buffered messages before closing ({} unpublished)",
            writer.buffer.len()
        );
    }

    log_task_stopped(MSGBUS_PUBLISH);
    Ok(())
}

/// Holds the publish task's buffer and connection.
///
/// Messages leave the buffer only once their `EXEC` is acknowledged. A retryable write error
/// drops the connection, so the next attempt opens a fresh one, as the stream reader does.
struct StreamWriter {
    con: Option<ConnectionManager>,
    reconnect_config: RedisMessageBusConfig,
    stream_key: String,
    stream_per_topic: bool,
    autotrim_duration: Option<Duration>,
    autotrim_maxlen: Option<usize>,
    last_trim_index: HashMap<String, usize>,
    buffer: VecDeque<BusMessage>,
    write_error_count: usize,
}

impl StreamWriter {
    /// Returns whether a failed write is waiting to be retried.
    const fn is_retrying(&self) -> bool {
        self.write_error_count > 0
    }

    /// Writes the buffer in atomic pipelines of at most [`PUBLISH_BATCH_SIZE`] messages, removing
    /// each batch once it is acknowledged.
    ///
    /// Returns the delay before the next attempt when a retryable error left messages buffered.
    ///
    /// # Errors
    ///
    /// Returns an error if a write fails with a non-retryable error.
    async fn flush(&mut self) -> anyhow::Result<Option<Duration>> {
        while !self.buffer.is_empty() {
            let mut con = match self.con.take() {
                Some(con) => con,
                None => {
                    match create_redis_connection(MSGBUS_PUBLISH, &self.reconnect_config).await {
                        Ok(con) => con,
                        Err(e) => {
                            log::error!("Error reconnecting to publish: {e:?}");
                            return Ok(Some(self.next_retry_delay()));
                        }
                    }
                }
            };

            let count = self.buffer.len().min(PUBLISH_BATCH_SIZE);
            let result = write_buffer(
                &mut con,
                &self.stream_key,
                self.stream_per_topic,
                self.autotrim_duration,
                self.autotrim_maxlen,
                &mut self.last_trim_index,
                self.buffer.range(..count),
            )
            .await;

            match result {
                Ok(()) => {
                    self.buffer.drain(..count);
                    self.con = Some(con);
                    self.write_error_count = 0;
                }
                Err(e) if is_retryable_write_error(&e) => {
                    log::error!("Error publishing to stream: {e:?}");
                    return Ok(Some(self.next_retry_delay()));
                }
                Err(e) => anyhow::bail!("Error publishing to stream: {e:?}"),
            }
        }

        Ok(None)
    }

    fn next_retry_delay(&mut self) -> Duration {
        let retry_delay = stream_retry_delay(&self.reconnect_config, self.write_error_count);
        self.write_error_count = self.write_error_count.saturating_add(1);
        retry_delay
    }
}

async fn write_buffer(
    conn: &mut redis::aio::ConnectionManager,
    stream_key: &str,
    stream_per_topic: bool,
    autotrim_duration: Option<Duration>,
    autotrim_maxlen: Option<usize>,
    last_trim_index: &mut HashMap<String, usize>,
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
                log::error!("Error trimming stream '{stream_key}': {e}");
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

/// Streams messages from Redis streams and sends them over the provided `tx` channel.
///
/// Reads start from the current time; see [`stream_messages_from`] to read entries already
/// held in the streams.
///
/// # Errors
///
/// Returns an error if:
/// - Establishing the Redis connection fails before the terminate signal is received.
/// - A Redis read operation returns a non-retryable error.
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
/// so the hand-over from history to live loses and repeats nothing.
///
/// # Errors
///
/// Returns an error if:
/// - Establishing the Redis connection fails before the terminate signal is received.
/// - A Redis read operation returns a non-retryable error.
pub async fn stream_messages_from(
    tx: tokio::sync::mpsc::Sender<BusMessage>,
    config: RedisMessageBusConfig,
    stream_keys: Vec<String>,
    start: StreamStart,
    stream_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    log_task_started(MSGBUS_STREAM);

    let Some(mut con) = connect_stream_connection(&config, &stream_signal).await? else {
        log_task_stopped(MSGBUS_STREAM);
        return Ok(());
    };

    let mut read_error_count = 0;

    let stream_keys = &stream_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<&str>>();

    log::debug!("Listening to streams: [{}]", stream_keys.join(", "));

    let initial_id = initial_stream_id(&start);

    let mut last_ids: HashMap<String, String> = stream_keys
        .iter()
        .map(|&key| (key.to_string(), initial_id.clone()))
        .collect();

    let opts = stream_read_options();

    'outer: loop {
        if stream_signal.load(Ordering::Relaxed) {
            log::debug!("Received streaming terminate signal");
            break;
        }

        let ids: Vec<String> = stream_keys
            .iter()
            .map(|&key| last_ids[key].clone())
            .collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();

        let result: Result<RedisStreamBulk, _> =
            con.xread_options(&[&stream_keys], &[&id_refs], &opts).await;

        match result {
            Ok(stream_bulk) => {
                read_error_count = 0;

                if stream_bulk.is_empty() {
                    // Timeout occurred: no messages received
                    continue;
                }

                for entry in &stream_bulk {
                    for (stream_key, stream_msgs) in entry {
                        for stream_msg in stream_msgs {
                            for (id, array) in stream_msg {
                                last_ids.insert(stream_key.clone(), id.clone());

                                match decode_stream_entry(stream_key, array) {
                                    Ok(msg) => {
                                        if let Err(e) = tx.send(msg).await {
                                            log::debug!("Channel closed: {e:?}");
                                            break 'outer; // End streaming
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("{e:?}");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                if !is_retryable_stream_error(&e) {
                    anyhow::bail!("Error reading from stream: {e:?}");
                }

                log::error!("Error reading from stream: {e:?}");

                let Some(reconnected) =
                    reconnect_stream_connection(&config, &stream_signal, &mut read_error_count)
                        .await?
                else {
                    break;
                };
                con = reconnected;
            }
        }
    }

    log_task_stopped(MSGBUS_STREAM);
    Ok(())
}

/// Returns the options of each stream read: wait at most 100 ms for new entries and take at most
/// [`STREAM_READ_BATCH_SIZE`] entries from each stream.
fn stream_read_options() -> StreamReadOptions {
    StreamReadOptions::default()
        .block(100)
        .count(STREAM_READ_BATCH_SIZE)
}

async fn connect_stream_connection(
    config: &RedisMessageBusConfig,
    stream_signal: &Arc<AtomicBool>,
) -> anyhow::Result<Option<ConnectionManager>> {
    let connect = create_redis_connection(MSGBUS_STREAM, config);
    let terminate = wait_for_stream_signal(stream_signal);

    tokio::pin!(connect);
    tokio::pin!(terminate);

    tokio::select! {
        result = &mut connect => result.map(Some),
        () = &mut terminate => Ok(None),
    }
}

async fn reconnect_stream_connection(
    config: &RedisMessageBusConfig,
    stream_signal: &Arc<AtomicBool>,
    read_error_count: &mut usize,
) -> anyhow::Result<Option<ConnectionManager>> {
    loop {
        let retry_delay = stream_retry_delay(config, *read_error_count);
        *read_error_count = (*read_error_count).saturating_add(1);

        if !wait_for_retry_delay(retry_delay, stream_signal).await {
            return Ok(None);
        }

        match connect_stream_connection(config, stream_signal).await {
            Ok(Some(con)) => return Ok(Some(con)),
            Ok(None) => return Ok(None),
            Err(e) => {
                log::error!("Error reconnecting to stream: {e:?}");
            }
        }
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
    pub_tx: tokio::sync::mpsc::UnboundedSender<BusMessage>,
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
                let heartbeat = create_heartbeat_msg();
                if let Err(e) = pub_tx.send(heartbeat) {
                    // We expect an error if the channel is closed during shutdown
                    log::debug!("Error sending heartbeat: {e}");
                }
            },
            _ = check_timer.tick() => {}
        }
    }

    log_task_stopped("heartbeat");
}

fn create_heartbeat_msg() -> BusMessage {
    let payload = Bytes::from(
        Timestamp::now()
            .display_with_offset(Offset::UTC)
            .to_string()
            .into_bytes(),
    );
    BusMessage::with_str_topic(
        HEARTBEAT_TOPIC,
        BusPayloadType::Custom(Ustr::default()),
        payload,
        SerializationEncoding::default(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        msgbus::{
            MessageBus, external_egress_from_backing, external_io_from_backing, get_message_bus,
            publish_any, publish_quote, set_message_bus,
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
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
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
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_messages_returns_transaction_aborted_by_unrecoverable_error() {
        let fake = FakeRedis::start("ERR syntax error").await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
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

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("an unrecoverable aborted transaction should end the task")
            .unwrap();

        assert_eq!(fake.aborted.load(Ordering::SeqCst), 1);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Error publishing to stream")
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

    fn backing_with_stream_receiver(
        stream_rx: tokio::sync::mpsc::Receiver<BusMessage>,
    ) -> RedisMessageBusBacking {
        let (pub_tx, _pub_rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
        RedisMessageBusBacking {
            trader_id: TraderId::from("tester-001"),
            instance_id: UUID4::new(),
            pub_tx,
            pub_handle: None,
            stream_rx: Some(stream_rx),
            stream_handle: None,
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

    /// A loopback RESP server standing in for Redis, which answers every queued `XADD` with
    /// `queued_error` until `accept_writes` is set, so each `EXEC` aborts with `EXECABORT`. It
    /// records every `XADD` it receives in `attempted`, and those of committed transactions in
    /// `written`.
    struct FakeRedis {
        port: u16,
        accept_writes: Arc<AtomicBool>,
        aborted: Arc<AtomicUsize>,
        committed: Arc<AtomicUsize>,
        attempted: Arc<std::sync::Mutex<Vec<FakeXadd>>>,
        written: Arc<std::sync::Mutex<Vec<FakeXadd>>>,
    }

    impl FakeRedis {
        async fn start(queued_error: &'static str) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let accept_writes = Arc::new(AtomicBool::new(false));
            let aborted = Arc::new(AtomicUsize::new(0));
            let committed = Arc::new(AtomicUsize::new(0));
            let attempted = Arc::new(std::sync::Mutex::new(Vec::new()));
            let written = Arc::new(std::sync::Mutex::new(Vec::new()));

            let accept_writes_clone = Arc::clone(&accept_writes);
            let aborted_clone = Arc::clone(&aborted);
            let committed_clone = Arc::clone(&committed);
            let attempted_clone = Arc::clone(&attempted);
            let written_clone = Arc::clone(&written);

            tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    let accept_writes = Arc::clone(&accept_writes_clone);
                    let aborted = Arc::clone(&aborted_clone);
                    let committed = Arc::clone(&committed_clone);
                    let attempted = Arc::clone(&attempted_clone);
                    let written = Arc::clone(&written_clone);

                    tokio::spawn(async move {
                        let (read, mut write) = socket.into_split();
                        let mut reader = tokio::io::BufReader::new(read);
                        let mut queued = 0;
                        let mut queue_failed = false;
                        let mut queued_xadds = Vec::new();

                        while let Some(command) = read_resp_command(&mut reader).await {
                            if command[0].eq_ignore_ascii_case(b"XADD") {
                                attempted.lock().unwrap().push(FakeXadd::parse(&command));
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
                                b"XADD" if accept_writes.load(Ordering::SeqCst) => {
                                    queued += 1;
                                    queued_xadds.push(FakeXadd::parse(&command));
                                    "+QUEUED\r\n".to_string()
                                }
                                b"XADD" => {
                                    queue_failed = true;
                                    format!("-{queued_error}\r\n")
                                }
                                b"EXEC" if queue_failed => {
                                    aborted.fetch_add(1, Ordering::SeqCst);
                                    "-EXECABORT Transaction discarded because of previous errors.\r\n"
                                        .to_string()
                                }
                                b"EXEC" => {
                                    committed.fetch_add(1, Ordering::SeqCst);
                                    written.lock().unwrap().append(&mut queued_xadds);
                                    format!("*{queued}\r\n{}", "$3\r\n0-1\r\n".repeat(queued))
                                }
                                _ => "+OK\r\n".to_string(),
                            };

                            if write.write_all(reply.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            });

            Self {
                port,
                accept_writes,
                aborted,
                committed,
                attempted,
                written,
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

    /// A loopback RESP server standing in for Redis, which holds streams of entries `<n>-0`
    /// carrying the stream key as topic, `n` as payload and the source of [`FAKE_PUBLISHER_ID`],
    /// [`FAKE_INSTANCE_ID`] and number `n`, answers `XREAD` as Redis does, and records each read.
    struct FakeRedisStreams {
        port: u16,
        streams: Arc<std::sync::Mutex<FakeStreams>>,
        reads: Arc<std::sync::Mutex<Vec<FakeStreamRead>>>,
    }

    impl FakeRedisStreams {
        async fn start(stream_keys: &[String], backlog_len: usize) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let backlog: Vec<_> = (1..=backlog_len).map(|n| (n, n.to_string())).collect();
            let streams = Arc::new(std::sync::Mutex::new(vec![backlog; stream_keys.len()]));
            let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
            let (keys, streams_clone, reads_clone) = (
                stream_keys.to_vec(),
                Arc::clone(&streams),
                Arc::clone(&reads),
            );

            tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    let (keys, streams, reads) = (
                        keys.clone(),
                        Arc::clone(&streams_clone),
                        Arc::clone(&reads_clone),
                    );

                    tokio::spawn(async move {
                        let (read, mut write) = socket.into_split();
                        let mut reader = tokio::io::BufReader::new(read);

                        while let Some(command) = read_resp_command(&mut reader).await {
                            let reply = match command[0].to_ascii_uppercase().as_slice() {
                                b"INFO" => {
                                    let info = "redis_version:8.0.0\r\n";
                                    format!("${}\r\n{info}\r\n", info.len())
                                }
                                b"XREAD" => {
                                    let reply = xread_reply(&command, &keys, &streams, &reads);
                                    if reply.is_none() {
                                        // An empty blocking read times out with a nil reply
                                        tokio::time::sleep(Duration::from_millis(10)).await;
                                    }
                                    reply.unwrap_or_else(|| "*-1\r\n".to_string())
                                }
                                _ => "+OK\r\n".to_string(),
                            };

                            if write.write_all(reply.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            });

            Self {
                port,
                streams,
                reads,
            }
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
                    bulk(&n.to_string()),
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
    async fn test_stream_messages_returns_unrecoverable_read_error(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, _rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let suffix = UUID4::new();
        let stream_key = format!("test:stream:wrong-type:{suffix}");
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));

        let _: () = con.set(&stream_key, "not-a-stream").await.unwrap();

        let result = stream_messages(
            tx,
            RedisMessageBusConfig::default(),
            external_streams,
            stream_signal,
        )
        .await;

        let _: () = con.del(&stream_key).await.unwrap();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Error reading from stream")
        );
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
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();

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
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
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
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
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
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
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
    async fn test_publish_messages_returns_unrecoverable_write_error(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
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

        tx.send(test_bus_message("first")).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("a non-retryable write error should end the task")
            .unwrap();
        let _: usize = con.del(&stream_key).await.unwrap();

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Error publishing to stream")
        );
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
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
        let signal = Arc::new(AtomicBool::new(false));

        // Start the heartbeat task with a short interval
        let handle = tokio::spawn(run_heartbeat(1, Arc::clone(&signal), tx));

        let heartbeat = receive_unbounded_bus_message(&mut rx, Duration::from_secs(2)).await;

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

    async fn receive_unbounded_bus_message(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<BusMessage>,
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

    fn test_bus_message(topic: &str) -> BusMessage {
        BusMessage::with_str_topic(
            topic,
            BusPayloadType::QuoteTick,
            Bytes::from("test_payload"),
            SerializationEncoding::Json,
        )
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
