# steno_client

Zentria's replacement for Meta's internal `scribe_client`: buck2's event sink (`app/buck2_events/src/sink/scribe.rs`,
unchanged from upstream) hands it batches of encoded `buck.data.BuckEvent`s, and it ships them to a
[steno](https://github.com/ZentriaMC/steno) server over gRPC.

The crate keeps the internal client's public surface (`ScribeClient`, `Message`, `ScribeConfig`, `ProducerCounters`)
and is wired in as `scribe_client` through a Cargo rename in the workspace manifest, so the sink compiles verbatim.

## How the fork differs from upstream

- `app/buck2_events`: feature `steno` compiles the internal sink (`sink/scribe.rs`) against this crate. The gates are
  `cfg(any(fbcode_build, feature = "steno"))` at four sites in `sink.rs` and `sink/remote.rs`. With the feature the
  sink sends the complete event stream (upstream's whitelist in `should_send_event` is bypassed) so steno can
  reconstruct event logs.
- `app/buck2`: feature `steno` is on by default; `cargo build --no-default-features` gives stock buck2.
- `sink/remote.rs::build_ui_url` and three console sites print `Buck UI: <BUCK2_STENO_UI_URL>/runs/<trace id>`
  in place of `Build ID:` when that variable is set.
- `Cargo.lock` is committed (upstream ignores it) so the nix build is reproducible.

## Configuration

Both the client process and the daemon construct a sink, and only the daemon has a buckconfig, so it is by
environment:

| | |
|---|---|
| `BUCK2_STENO_URL` | `http://host:port` or `https://host`. Unset: no sink is built and buck2 behaves exactly like the stock open source build. Checked in `sink/remote.rs` before anything is constructed, so an absent endpoint is `None`, never an error that would fail daemon startup. |
| `BUCK2_STENO_UI_URL` | Base of the run page, for the `Buck UI:` line. |
| `BUCK2_SCRIBE_CATEGORY` | Stream name, default `buck2_events`. |

Queue and retry knobs come from the same buckconfig keys the internal sink uses: `[buck2] event_log_buffer_size`
(10000), `event_log_retry_backoff_duration_ms` (500), `event_log_retry_attempts` (5), `event_log_message_batch_size`.

## Behaviour

Same shape as the internal producer: one producer per process on its own thread and single-thread runtime (the main
runtime does blocking work during load and analysis and would starve the drain loop), a bounded queue that drops
and counts when full, a 500 ms drain, exponential backoff capped at 10 s, and per-message results mapped onto the
same counters `buck2 status` and rage report.

A build never waits on steno. `offer` cannot block, and `send_now` (used for the invocation record on the client's
exit path) is bounded by `thrift_timeout × (attempts + 1) + backoff` even if the producer thread wedges.

Requests over 3 MiB are split; individual events are already capped at 1 MiB by the sink.

## Building buck2 with buck2

The BUCK graph builds too (`buck2 build //:buck2`), with the steno feature: `app/buck2_events/BUCK` sets
`features = ["steno"]` and depends on `//buck2/shed/steno_client:steno_client`, whose crate is named `scribe_client`
because the shim discards `named_deps`.

Two things to know:

- `.buckconfig.d/common.buckconfig` uses the prelude **bundled in the buck2 binary that runs the build**, so bootstrap
  with a binary built from this tree (`cargo build --release --bin buck2`, then `target/release/buck2 build //:buck2`).
  A release binary from another date carries a different prelude snapshot and fails in unrelated packages.
- Generate third-party rules with the pinned `./bootstrap/reindeer --third-party-dir shim/third-party/rust buckify`.
  The fork carries four shim fixes upstream's fresh resolve needed on macOS: the stale `logos-codegen` buildscript fixup
  removed, an `objc2` fixup (no build script, `CARGO_PKG_VERSION` provided), `h2` added to the shim manifest for
  `re_grpc`, and `elf_sections` swallowed in the shim's `rust_binary`.
- The prelude builds Rust at opt-level 0 unless a target says otherwise, which makes a ~500 MB buck2. This fork's
  prelude adds `prelude//rust/constraints:opt_level` (a unified `constraint`, values `0 1 2 3 s z`, default
  `unspecified`) and a hidden `_rustc_opt_level_flags` attribute on every rust rule that `select`s on it in the
  *target* configuration (`prelude/decls/rust_common.bzl`, consumed in `prelude/rust/build.bzl` between toolchain and
  target flags). It is deliberately not on the toolchain: toolchains are configured for the execution platform and
  cannot see target constraints. Use it as a modifier — `buck2 build '//:buck2?prelude//rust/constraints:opt_level[3]'`,
  `set_cfg_modifiers` in a PACKAGE, or buckconfig-backed modifiers — which needs a cfg constructor; the fork's root
  `PACKAGE` installs the shim's. Because the prelude is bundled, every binary built from this tree carries it.

## What the sink keeps that Scribe drops

`sink/smart_truncate_event.rs` is tuned for Meta's Scuba tables: stderr of successful actions and stdout/stderr of
tests become `<<omitted>>`, and every string is cut at 20 KiB. Under the steno feature successful actions keep their
stderr and tests keep their streams, and the per-string cap is 512 KiB (the 1 MiB per-event guard still applies).
Test output proper arrives in `TestResult.details` (`---- STDOUT ---- / ---- STDERR ----` sections, per test, with
status, duration and peak memory); `TestRunEnd.command_report` carries the command, digest and timings but the runner
consumes its streams, so those are empty even in the local event log. Argv and env of *successful* actions are
omitted by the daemon itself (`buck2_build_api/src/actions/calculation.rs::command_details`), not by the sink; failed
actions carry them in full.
