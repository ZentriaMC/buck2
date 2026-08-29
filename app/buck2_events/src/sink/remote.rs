/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! A Sink for forwarding events directly to Remote service.
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use fbinit::FacebookInit;

#[cfg(any(fbcode_build, feature = "steno"))]
mod fbcode {
    pub use scribe_client::ScribeConfig;

    pub use crate::sink::scribe::RemoteEventSink;
    pub(crate) use crate::sink::scribe::scribe_category;
}

#[cfg(not(any(fbcode_build, feature = "steno")))]
mod fbcode {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::BuckEvent;
    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;

    pub enum RemoteEventSink {}

    impl RemoteEventSink {
        pub async fn send_now(&self, _event: BuckEvent) -> buck2_error::Result<()> {
            Ok(())
        }
        pub async fn send_messages_now(&self, _events: Vec<BuckEvent>) -> buck2_error::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl EventSink for RemoteEventSink {
        fn send(&self, _event: Event) {}
    }

    impl EventSinkWithStats for RemoteEventSink {
        fn to_event_sync(self: Arc<Self>) -> Arc<dyn EventSink> {
            self as _
        }

        fn stats(&self) -> EventSinkStats {
            match *self {}
        }
    }

    #[derive(Default)]
    pub struct ScribeConfig {
        pub buffer_size: usize,
        pub retry_backoff: Duration,
        pub retry_attempts: usize,
        pub message_batch_size: Option<usize>,
        pub thrift_timeout: Duration,
    }
}

pub use fbcode::*;

fn new_remote_event_sink_if_fbcode(
    fb: FacebookInit,
    config: ScribeConfig,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    #[cfg(fbcode_build)]
    {
        Ok(Some(RemoteEventSink::new(fb, scribe_category()?, config)?))
    }
    #[cfg(all(not(fbcode_build), feature = "steno"))]
    {
        // Opt-in per environment. Unset means stock behaviour; an error here
        // would fail daemon startup, so absence is `None`, not `Err`.
        if buck2_core::buck2_env!("BUCK2_STENO_URL")?.is_none() {
            return Ok(None);
        }
        Ok(Some(RemoteEventSink::new(fb, scribe_category()?, config)?))
    }
    #[cfg(not(any(fbcode_build, feature = "steno")))]
    {
        let _ = (fb, config);
        Ok(None)
    }
}

/// Where a person can look at this invocation, when events are shipped
/// somewhere with a UI. `BUCK2_STENO_UI_URL` is the base; the run page is
/// `<base>/runs/<trace id>`.
pub fn build_ui_url(trace_id: &str) -> Option<String> {
    #[cfg(feature = "steno")]
    {
        let base = buck2_core::buck2_env!("BUCK2_STENO_UI_URL")
            .ok()
            .flatten()?;
        Some(format!("{}/runs/{}", base.trim_end_matches('/'), trace_id))
    }
    #[cfg(not(feature = "steno"))]
    {
        let _ = trace_id;
        None
    }
}

pub fn new_remote_event_sink_if_enabled(
    fb: FacebookInit,
    config: ScribeConfig,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    if is_enabled() {
        new_remote_event_sink_if_fbcode(fb, config)
    } else {
        Ok(None)
    }
}

/// Whether or not remote event logging is enabled for this process. It must be explicitly disabled via `disable()`.
static REMOTE_EVENT_SINK_ENABLED: AtomicBool = AtomicBool::new(true);

/// Returns whether this process should actually write to remote sink, even if it is fully supported by the platform and
/// binary.
pub fn is_enabled() -> bool {
    REMOTE_EVENT_SINK_ENABLED.load(Ordering::Relaxed)
}

/// Disables remote event logging for this process. Remote event logging must be disabled explicitly on startup, otherwise it is
/// on by default.
pub fn disable() {
    REMOTE_EVENT_SINK_ENABLED.store(false, Ordering::Relaxed);
}
