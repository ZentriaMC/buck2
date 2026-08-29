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

//! Drop-in replacement for the internal `scribe_client` crate that ships buck2
//! events to a steno server over gRPC.
//!
//! The public surface (`ScribeClient`, `Message`, `ScribeConfig`,
//! `ProducerCounters`) is kept identical so `buck2_events::sink::scribe` is
//! compiled unchanged.
//!
//! Configuration is by environment, because the client process and the daemon
//! both construct a client and only the daemon has a buckconfig in hand:
//!
//! - `BUCK2_STENO_URL`: the server, `http://host:port` or `https://host`.
//!   `buck2_events::sink::remote` refuses to build a sink when it is unset, so
//!   without it buck2 behaves exactly like the stock open source build.

mod producer;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use fbinit::FacebookInit;
pub use producer::Message;
pub use producer::ProducerCounters;
pub use producer::ScribeConfig;
use tokio::runtime::Builder;

use crate::producer::Producer;

pub mod proto {
    tonic::include_proto!("zentria.steno.v1");
}

/// Environment variable naming the steno server.
pub const ENDPOINT_ENV: &str = "BUCK2_STENO_URL";

static PRODUCER: OnceLock<Arc<Producer>> = OnceLock::new();
static PRODUCER_INIT: Mutex<()> = Mutex::new(());

/// Initialises the producer that backs all clients in this process. Like the
/// internal client, the first configuration wins and later ones are ignored.
fn initialize(config: ScribeConfig) -> anyhow::Result<&'static Producer> {
    if let Some(producer) = PRODUCER.get() {
        return Ok(producer);
    }
    let _guard = PRODUCER_INIT.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(producer) = PRODUCER.get() {
        return Ok(producer);
    }

    let endpoint = std::env::var(ENDPOINT_ENV).map_err(|_| {
        anyhow::anyhow!("{ENDPOINT_ENV} is not set; steno sink cannot be constructed")
    })?;
    let (producer, urgent) = Producer::new(endpoint, config);
    let producer = Arc::new(producer);

    // Own runtime on its own thread, same reasoning as the internal client:
    // loading and analysis do blocking work on the main runtime's threads, and
    // a starved drain loop turns straight into dropped events.
    std::thread::Builder::new()
        .name("steno-producer".to_owned())
        .spawn({
            let producer = producer.clone();
            move || {
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("steno producer runtime");
                runtime.block_on(producer::producer_loop(producer, urgent));
            }
        })?;

    let _ = PRODUCER.set(producer);
    Ok(PRODUCER.get().expect("just set"))
}

/// A client that hands messages to the process-wide producer.
pub struct ScribeClient {
    producer: &'static Producer,
}

impl ScribeClient {
    pub fn new(_fb: FacebookInit, config: ScribeConfig) -> anyhow::Result<ScribeClient> {
        let producer = initialize(config)?;
        Ok(ScribeClient { producer })
    }

    pub fn export_counters(&self) -> ProducerCounters {
        self.producer.export_counters()
    }

    /// Offers a message to the background queue. Never blocks; drops when the
    /// queue is full and counts the drop.
    pub fn offer(&self, message: Message) {
        self.producer.offer(message);
    }

    /// Sends `messages` ahead of the queue and waits for the outcome, bounded
    /// by the configured timeout and retry budget.
    pub async fn send_messages_now(&self, messages: Vec<Message>) -> anyhow::Result<()> {
        self.producer.send_messages_now(messages).await
    }
}
