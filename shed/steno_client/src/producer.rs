/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * Copyright (c) Zentria OÜ.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Bounded, lossy background producer. Everything on the wire happens on the
//! producer's own runtime; callers only ever touch a queue or a oneshot.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crossbeam::queue::ArrayQueue;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tonic::Code;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;

use crate::proto::Message as ProtoMessage;
use crate::proto::WriteMessagesRequest;
use crate::proto::WriteResult;
use crate::proto::steno_client::StenoClient;

/// Largest request body we send. Individual events are already capped at 1 MiB
/// by the sink; this keeps a batch comfortably under gRPC's default 4 MiB.
const MAX_BATCH_BYTES: usize = 3 * 1024 * 1024;
/// Upper bound on any single retry sleep.
const MAX_BACKOFF: Duration = Duration::from_secs(10);
/// How often the background loop drains the queue.
const DRAIN_INTERVAL: Duration = Duration::from_millis(500);

/// A message destined for the server.
#[derive(Clone, Debug, Default)]
pub struct Message {
    /// Stream name; buck2 uses "buck2_events".
    pub category: String,
    /// The payload itself.
    pub message: Vec<u8>,
    /// Partition key; buck2 hashes the trace id so one invocation stays together.
    pub message_key: Option<i64>,
}

impl From<Message> for ProtoMessage {
    fn from(m: Message) -> ProtoMessage {
        ProtoMessage {
            category: m.category,
            payload: m.message,
            message_key: m.message_key,
        }
    }
}

/// Field names are the internal client's; the daemon fills them from
/// `[buck2] event_log_*` buckconfig keys.
#[derive(Debug)]
pub struct ScribeConfig {
    /// Capacity of the background queue. Beyond it, `offer` drops.
    pub buffer_size: usize,
    /// Base of the exponential backoff between attempts.
    pub retry_backoff: Duration,
    /// Attempts after the first.
    pub retry_attempts: usize,
    /// Maximum messages per request; `None` is bounded only by bytes.
    pub message_batch_size: Option<usize>,
    /// Per-request deadline, connect included.
    pub thrift_timeout: Duration,
}

impl Default for ScribeConfig {
    fn default() -> Self {
        Self {
            buffer_size: 100,
            retry_backoff: Duration::from_millis(500),
            retry_attempts: 5,
            message_batch_size: None,
            thrift_timeout: Duration::from_secs(1),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ProducerCounters {
    pub successes: u64,
    pub failures_invalid_request: u64,
    pub failures_unauthorized: u64,
    pub failures_rate_limited: u64,
    pub failures_pushed_back: u64,
    pub failures_enqueue_failed: u64,
    pub failures_internal_error: u64,
    pub failures_timed_out: u64,
    pub failures_unknown: u64,
    pub queue_depth: u64,
    pub dropped: u64,
    pub bytes_written: u64,
}

impl ProducerCounters {
    pub fn failures(&self) -> u64 {
        let ProducerCounters {
            successes: _,
            failures_invalid_request,
            failures_unauthorized,
            failures_rate_limited,
            failures_pushed_back,
            failures_enqueue_failed,
            failures_internal_error,
            failures_timed_out,
            failures_unknown,
            queue_depth: _,
            dropped: _,
            bytes_written: _,
        } = self;
        *failures_invalid_request
            + *failures_unauthorized
            + *failures_rate_limited
            + *failures_pushed_back
            + *failures_enqueue_failed
            + *failures_internal_error
            + *failures_timed_out
            + *failures_unknown
    }
}

#[derive(Default)]
struct Counters {
    successes: AtomicU64,
    failures_invalid_request: AtomicU64,
    failures_unauthorized: AtomicU64,
    failures_rate_limited: AtomicU64,
    failures_pushed_back: AtomicU64,
    failures_enqueue_failed: AtomicU64,
    failures_internal_error: AtomicU64,
    failures_timed_out: AtomicU64,
    failures_unknown: AtomicU64,
    dropped: AtomicU64,
    bytes_written: AtomicU64,
}

impl Counters {
    fn bump(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

/// Why a message (or a whole request) did not land, and whether trying again
/// can change that.
#[derive(Clone, Copy, Debug)]
enum Failure {
    Invalid,
    Unauthorized,
    RateLimited,
    PushedBack,
    Internal,
    TimedOut,
    Unknown,
}

impl Failure {
    fn is_retryable(self) -> bool {
        match self {
            Failure::Invalid | Failure::Unauthorized => false,
            Failure::RateLimited
            | Failure::PushedBack
            | Failure::Internal
            | Failure::TimedOut
            | Failure::Unknown => true,
        }
    }

    fn from_status(status: &tonic::Status) -> Failure {
        match status.code() {
            Code::DeadlineExceeded | Code::Cancelled => Failure::TimedOut,
            Code::Unauthenticated | Code::PermissionDenied => Failure::Unauthorized,
            Code::InvalidArgument | Code::OutOfRange => Failure::Invalid,
            Code::ResourceExhausted => Failure::RateLimited,
            Code::Unavailable => Failure::PushedBack,
            Code::Internal | Code::DataLoss | Code::Unimplemented => Failure::Internal,
            _ => Failure::Unknown,
        }
    }

    fn from_result(result: WriteResult) -> Option<Failure> {
        match result {
            WriteResult::Ok => None,
            WriteResult::TryLater => Some(Failure::PushedBack),
            WriteResult::Invalid => Some(Failure::Invalid),
            WriteResult::Unauthorized => Some(Failure::Unauthorized),
            WriteResult::RateLimited => Some(Failure::RateLimited),
            WriteResult::Internal | WriteResult::Unspecified => Some(Failure::Internal),
        }
    }

    fn count(self, counters: &Counters, n: u64) {
        let counter = match self {
            Failure::Invalid => &counters.failures_invalid_request,
            Failure::Unauthorized => &counters.failures_unauthorized,
            Failure::RateLimited => &counters.failures_rate_limited,
            Failure::PushedBack => &counters.failures_pushed_back,
            Failure::Internal => &counters.failures_internal_error,
            Failure::TimedOut => &counters.failures_timed_out,
            Failure::Unknown => &counters.failures_unknown,
        };
        Counters::bump(counter, n);
    }
}

type Urgent = (Vec<Message>, oneshot::Sender<anyhow::Result<()>>);

pub(crate) struct Producer {
    endpoint: String,
    config: ScribeConfig,
    queue: ArrayQueue<Message>,
    urgent: mpsc::UnboundedSender<Urgent>,
    counters: Counters,
}

impl Producer {
    pub(crate) fn new(
        endpoint: String,
        config: ScribeConfig,
    ) -> (Producer, mpsc::UnboundedReceiver<Urgent>) {
        let (urgent, rx) = mpsc::unbounded_channel();
        let queue = ArrayQueue::new(config.buffer_size.max(1));
        let producer = Producer {
            endpoint,
            config,
            queue,
            urgent,
            counters: Counters::default(),
        };
        (producer, rx)
    }

    pub(crate) fn offer(&self, message: Message) {
        if self.queue.push(message).is_err() {
            Counters::bump(&self.counters.dropped, 1);
        }
    }

    pub(crate) fn export_counters(&self) -> ProducerCounters {
        let c = &self.counters;
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        ProducerCounters {
            successes: load(&c.successes),
            failures_invalid_request: load(&c.failures_invalid_request),
            failures_unauthorized: load(&c.failures_unauthorized),
            failures_rate_limited: load(&c.failures_rate_limited),
            failures_pushed_back: load(&c.failures_pushed_back),
            failures_enqueue_failed: load(&c.failures_enqueue_failed),
            failures_internal_error: load(&c.failures_internal_error),
            failures_timed_out: load(&c.failures_timed_out),
            failures_unknown: load(&c.failures_unknown),
            queue_depth: self.queue.len() as u64,
            dropped: load(&c.dropped),
            bytes_written: load(&c.bytes_written),
        }
    }

    /// Worst case wall time of one retried send. Callers on the client's exit
    /// path wait at most this long.
    fn send_deadline(&self) -> Duration {
        let intervals = retry_intervals(&self.config);
        self.config.thrift_timeout * intervals.len() as u32 + intervals.iter().sum::<Duration>()
    }

    pub(crate) async fn send_messages_now(&self, messages: Vec<Message>) -> anyhow::Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let (reply, done) = oneshot::channel();
        if self.urgent.send((messages, reply)).is_err() {
            Counters::bump(&self.counters.failures_enqueue_failed, 1);
            anyhow::bail!("steno producer is not running");
        }
        // The wire has its own per-request deadline; this one guards against
        // the producer thread itself wedging, which must never hang a command.
        match tokio::time::timeout(self.send_deadline() + Duration::from_secs(1), done).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => anyhow::bail!("steno producer dropped the request"),
            Err(_) => {
                Counters::bump(&self.counters.failures_timed_out, 1);
                anyhow::bail!("steno producer did not answer in time")
            }
        }
    }

    fn drain(&self) -> Vec<Message> {
        let limit = self.config.message_batch_size.unwrap_or(usize::MAX);
        let mut out = Vec::new();
        let mut bytes = 0;
        while out.len() < limit && bytes < MAX_BATCH_BYTES {
            match self.queue.pop() {
                Some(m) => {
                    bytes += m.message.len();
                    out.push(m);
                }
                None => break,
            }
        }
        out
    }
}

/// Sleep before each attempt, the internal client's way: the first attempt is
/// immediate, then Fibonacci multiples of the base. `retry_attempts` is the
/// total number of attempts. At the defaults (5, 500 ms) a dead server costs
/// 3.5 s of sleeping, which is what the client's exit path can afford.
fn retry_intervals(config: &ScribeConfig) -> Vec<Duration> {
    let attempts = config.retry_attempts.max(1);
    let mut out = vec![Duration::ZERO];
    let (mut a, mut b) = (config.retry_backoff, config.retry_backoff);
    while out.len() < attempts {
        out.push(a.min(MAX_BACKOFF));
        let next = a.saturating_add(b);
        a = b;
        b = next;
    }
    out
}

fn connect(endpoint: &str, config: &ScribeConfig) -> anyhow::Result<StenoClient<Channel>> {
    let mut builder = Endpoint::from_shared(endpoint.to_owned())?
        .connect_timeout(config.thrift_timeout)
        .timeout(config.thrift_timeout)
        .tcp_nodelay(true);
    if endpoint.starts_with("https://") {
        builder = builder.tls_config(ClientTlsConfig::new().with_enabled_roots())?;
    }
    // Lazy: a server that is down costs a failed request, never a failed startup.
    let channel = builder.connect_lazy();
    Ok(StenoClient::new(channel).max_encoding_message_size(MAX_BATCH_BYTES * 2))
}

/// Splits `messages` into requests that respect the batch limits.
fn chunk(messages: Vec<Message>, config: &ScribeConfig) -> Vec<Vec<Message>> {
    let limit = config.message_batch_size.unwrap_or(usize::MAX).max(1);
    let mut chunks: Vec<Vec<Message>> = Vec::new();
    let mut current = Vec::new();
    let mut bytes = 0;
    for m in messages {
        if !current.is_empty()
            && (current.len() >= limit || bytes + m.message.len() > MAX_BATCH_BYTES)
        {
            chunks.push(std::mem::take(&mut current));
            bytes = 0;
        }
        bytes += m.message.len();
        current.push(m);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// One attempt at delivering `messages`. Returns the messages worth retrying.
async fn send_once(
    client: &mut StenoClient<Channel>,
    counters: &Counters,
    config: &ScribeConfig,
    messages: Vec<Message>,
) -> Vec<Message> {
    let mut retry = Vec::new();
    for batch in chunk(messages, config) {
        let request = WriteMessagesRequest {
            messages: batch.iter().cloned().map(ProtoMessage::from).collect(),
        };
        match client.write_messages(request).await {
            Ok(response) => {
                let results = response.into_inner().results;
                for (i, message) in batch.into_iter().enumerate() {
                    let result = results
                        .get(i)
                        .copied()
                        .and_then(|r| WriteResult::try_from(r).ok())
                        .unwrap_or(WriteResult::Internal);
                    match Failure::from_result(result) {
                        None => {
                            Counters::bump(&counters.successes, 1);
                            Counters::bump(&counters.bytes_written, message.message.len() as u64);
                        }
                        Some(failure) => {
                            failure.count(counters, 1);
                            if failure.is_retryable() {
                                retry.push(message);
                            }
                        }
                    }
                }
            }
            Err(status) => {
                let failure = Failure::from_status(&status);
                tracing::debug!("steno write failed: {} ({:?})", status.message(), failure);
                failure.count(counters, batch.len() as u64);
                if failure.is_retryable() {
                    retry.extend(batch);
                }
            }
        }
    }
    retry
}

async fn send_with_retry(
    client: &mut StenoClient<Channel>,
    producer: &Producer,
    messages: Vec<Message>,
) -> anyhow::Result<()> {
    let total = messages.len();
    let mut pending = messages;
    let intervals = retry_intervals(&producer.config);
    for interval in &intervals {
        if !interval.is_zero() {
            tokio::time::sleep(*interval).await;
        }
        pending = send_once(client, &producer.counters, &producer.config, pending).await;
        if pending.is_empty() {
            return Ok(());
        }
    }
    anyhow::bail!(
        "steno: gave up on {} of {} messages after {} attempts",
        pending.len(),
        total,
        intervals.len()
    )
}

/// Drives the producer. Runs for the life of the process on its own runtime.
pub(crate) async fn producer_loop(
    producer: Arc<Producer>,
    mut urgent: mpsc::UnboundedReceiver<Urgent>,
) {
    let mut client = match connect(&producer.endpoint, &producer.config) {
        Ok(client) => client,
        Err(e) => {
            // A malformed endpoint is a configuration error, not a reason to
            // fail builds: keep answering callers so nothing blocks.
            tracing::warn!(
                "steno: not sending events, bad {}: {:#}",
                crate::ENDPOINT_ENV,
                e
            );
            loop {
                match urgent.recv().await {
                    Some((_, reply)) => {
                        let _ = reply.send(Err(anyhow::anyhow!("steno endpoint is misconfigured")));
                    }
                    None => return,
                }
            }
        }
    };

    let mut tick = tokio::time::interval(DRAIN_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            urgent_message = urgent.recv() => {
                match urgent_message {
                    Some((messages, reply)) => {
                        let result = send_with_retry(&mut client, &producer, messages).await;
                        let _ = reply.send(result);
                    }
                    None => return,
                }
            }
            _ = tick.tick() => {
                let batch = producer.drain();
                if !batch.is_empty()
                    && let Err(e) = send_with_retry(&mut client, &producer, batch).await
                {
                    tracing::debug!("{:#}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(n: usize) -> Message {
        Message {
            category: "t".to_owned(),
            message: vec![0; n],
            message_key: None,
        }
    }

    #[test]
    fn chunks_respect_count_and_bytes() {
        let config = ScribeConfig {
            message_batch_size: Some(2),
            ..ScribeConfig::default()
        };
        let chunks = chunk(vec![message(1), message(1), message(1)], &config);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), vec![2, 1]);

        let config = ScribeConfig::default();
        let chunks = chunk(
            vec![message(MAX_BATCH_BYTES - 1), message(2), message(1)],
            &config,
        );
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), vec![1, 2]);

        // A single oversized message still goes out on its own rather than being lost here.
        let chunks = chunk(vec![message(MAX_BATCH_BYTES + 1)], &config);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn retry_intervals_are_fibonacci_from_zero() {
        let ms = Duration::from_millis;
        let config = ScribeConfig::default();
        assert_eq!(
            retry_intervals(&config),
            vec![ms(0), ms(500), ms(500), ms(1000), ms(1500)]
        );
        let config = ScribeConfig {
            retry_attempts: 0,
            ..ScribeConfig::default()
        };
        assert_eq!(retry_intervals(&config), vec![ms(0)]);
        let config = ScribeConfig {
            retry_attempts: 40,
            ..ScribeConfig::default()
        };
        assert!(retry_intervals(&config).iter().all(|d| *d <= MAX_BACKOFF));
    }

    #[test]
    fn offer_drops_when_full() {
        let (producer, _rx) = Producer::new(
            "http://127.0.0.1:1".to_owned(),
            ScribeConfig {
                buffer_size: 2,
                ..ScribeConfig::default()
            },
        );
        producer.offer(message(1));
        producer.offer(message(1));
        producer.offer(message(1));
        let counters = producer.export_counters();
        assert_eq!(counters.queue_depth, 2);
        assert_eq!(counters.dropped, 1);
    }
}
