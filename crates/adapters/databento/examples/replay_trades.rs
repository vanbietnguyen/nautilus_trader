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

//! Example making one `replay_start` trades request through the Databento data client and
//! printing its counts and timings.
//!
//! The client is configured with the venue not taken from the exchange and one-digit futures
//! years, so `--instrument` reads like `MNQZ6.GLBX` (dataset `GLBX.MDP3`). The request carries
//! `replay_start`, `schema = "tbbo"` and `price_precision = 2`, so it is served by a live gateway
//! replay and never calls the historical API. With `--with-quotes` the client first subscribes
//! quotes for the same instrument on its ordinary feed session, a second concurrent gateway
//! session on the same key.
//!
//! The client decodes the replay with `TsEventSource::Exchange`: a trade's `ts_event` is the
//! record header's exchange (matching-engine) time, or its `ts_recv` when the header time is
//! undefined, and its `ts_init` is its `ts_recv`. So `first_ts_event_ms`, `last_ts_event_ms`,
//! `min_event_offset_ms` and `halt_hour_trades` read exchange time; `first_ts_init_ms`,
//! `last_ts_init_ms`, `ts_init_non_decreasing` and `ts_init_decreases` read receive time; and
//! `max_init_minus_event_ns` is the largest receive time less exchange time.
//!
//! Output, on stdout, holds counts and timings only: never a price, size, side, quote or trade
//! row, and no raw timestamp other than as an offset from `--start-ns`.
//! - The adapter's `info` lines, including its one summary line at the replay's end
//!   (`Replay of <id> ended: ...`), through the workspace logger at `info`.
//! - One line of this program's own `key=value` pairs, printed last, after the logger has
//!   flushed.
//!
//! The API key is read from `--key-file` (a file or a pipe), held only in memory and never
//! printed; no flag or environment variable takes its value.
//!
//! Run with: `cargo run --example databento-replay-trades --package nautilus-databento --
//! --key-file <path> --instrument MNQZ6.GLBX --start-ns <unix nanos> [--with-quotes]`

use std::{
    fmt::Display,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    process::ExitCode,
    str::FromStr,
    time::Duration,
};

use clap::Parser;
use jiff::{Timestamp, tz::TimeZone};
use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    logging::{init_logging, logger::LoggerConfig},
    messages::{
        DataEvent, DataResponse,
        data::{RequestTrades, SubscribeQuotes},
    },
};
use nautilus_core::{
    Params, UUID4, UnixNanos, datetime::get_timezone, string::secret::SecretString,
    time::get_atomic_clock_realtime,
};
use nautilus_databento::{
    data::{DatabentoDataClient, DatabentoDataClientConfig},
    decode::{DatabentoDecodeConfig, TsEventSource},
};
use nautilus_model::{
    data::Data,
    identifiers::{ClientId, InstrumentId, TraderId, futures::FuturesYearDigits},
};
use serde_json::json;
use tokio::{sync::mpsc::UnboundedReceiver, time::Instant};
use zeroize::Zeroize;

const CLIENT_ID: &str = "DATABENTO-REPLAY";
const TRADER_ID: &str = "REPLAY-001";
const NANOS_PER_MILLI: i128 = 1_000_000;
// The New York hour of the daily CME Globex halt, [17:00, 18:00)
const HALT_HOUR: i8 = 17;

/// Makes one `replay_start` trades request and prints its counts and timings.
#[derive(Debug, Parser)]
#[command(name = "databento-replay-trades")]
struct Args {
    /// File (or pipe) the API key is read from; the key is never printed.
    #[arg(long)]
    key_file: PathBuf,
    /// Instrument to replay, e.g. MNQZ6.GLBX.
    // `FromStr` refuses a malformed ID; clap would otherwise pick the panicking `From<&str>`
    #[arg(long, value_parser = InstrumentId::from_str)]
    instrument: InstrumentId,
    /// Replay start (the session open), UNIX nanoseconds.
    #[arg(long)]
    start_ns: u64,
    /// Subscribe quotes for the same instrument on the client's feed session first.
    #[arg(long)]
    with_quotes: bool,
    /// This program's wait for the completed response or a failure, in seconds.
    #[arg(long, default_value_t = 150)]
    bound_secs: u64,
}

/// How the request ended, as this program saw it.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// A completed response arrived without a failure.
    Completed,
    /// The request failed with the adapter's reason.
    Failed(String),
    /// The client refused the request before sending it.
    Refused(String),
    /// The program could not set up the client or the request.
    Error(String),
    /// Neither a completed response nor a failure arrived within `--bound-secs`.
    Bound,
    /// The data event channel closed before the request ended.
    ChannelClosed,
}

impl Outcome {
    const fn word(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed(_) => "failed",
            Self::Refused(_) => "refused",
            Self::Error(_) => "error",
            Self::Bound => "bound",
            Self::ChannelClosed => "channel_closed",
        }
    }

    fn reason(&self) -> Option<&str> {
        match self {
            Self::Failed(reason) | Self::Refused(reason) | Self::Error(reason) => Some(reason),
            Self::Completed | Self::Bound | Self::ChannelClosed => None,
        }
    }
}

/// The receive (`ts_init`) and exchange (`ts_event`) times of one trade, in UNIX nanoseconds.
#[derive(Clone, Copy, Debug)]
struct TradeTimes {
    ts_init: u64,
    ts_event: u64,
}

/// Counts and timings over one replay request's events; keeps no price, size or side.
#[derive(Debug)]
struct ReplayTally {
    start_ns: u64,
    new_york: TimeZone,
    envelopes: u64,
    partial_envelopes: u64,
    trades: u64,
    first: Option<TradeTimes>,
    last: Option<TradeTimes>,
    ts_init_decreases: u64,
    halt_hour_trades: u64,
    min_event_offset_ns: Option<i128>,
    max_init_minus_event_ns: Option<i128>,
    quotes: u64,
}

impl ReplayTally {
    fn new(start_ns: u64, new_york: TimeZone) -> Self {
        Self {
            start_ns,
            new_york,
            envelopes: 0,
            partial_envelopes: 0,
            trades: 0,
            first: None,
            last: None,
            ts_init_decreases: 0,
            halt_hour_trades: 0,
            min_event_offset_ns: None,
            max_init_minus_event_ns: None,
            quotes: 0,
        }
    }

    fn envelope(&mut self, completed: bool) {
        self.envelopes += 1;
        if !completed {
            self.partial_envelopes += 1;
        }
    }

    fn trade(&mut self, ts_init: u64, ts_event: u64) {
        let times = TradeTimes { ts_init, ts_event };
        self.trades += 1;
        if self.last.is_some_and(|last| ts_init < last.ts_init) {
            self.ts_init_decreases += 1;
        }
        if self.in_halt_hour(ts_event) {
            self.halt_hour_trades += 1;
        }
        let event_offset = i128::from(ts_event) - i128::from(self.start_ns);
        self.min_event_offset_ns = Some(
            self.min_event_offset_ns
                .map_or(event_offset, |min| min.min(event_offset)),
        );
        let init_minus_event = i128::from(ts_init) - i128::from(ts_event);
        self.max_init_minus_event_ns = Some(
            self.max_init_minus_event_ns
                .map_or(init_minus_event, |max| max.max(init_minus_event)),
        );
        self.first.get_or_insert(times);
        self.last = Some(times);
    }

    fn quote(&mut self) {
        self.quotes += 1;
    }

    fn in_halt_hour(&self, ts_event: u64) -> bool {
        Timestamp::from_nanosecond(i128::from(ts_event))
            .is_ok_and(|timestamp| self.new_york.to_datetime(timestamp).hour() == HALT_HOUR)
    }

    /// Whole milliseconds from `start_ns`, rounded down, so any time before it is negative.
    fn offset_ms(&self, ts: u64) -> i128 {
        (i128::from(ts) - i128::from(self.start_ns)).div_euclid(NANOS_PER_MILLI)
    }
}

impl Display for ReplayTally {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let offset = |times: Option<TradeTimes>, pick: fn(TradeTimes) -> u64| {
            times.map(|times| self.offset_ms(pick(times)))
        };
        write!(
            f,
            "envelopes={} partial_envelopes={} trades={}",
            self.envelopes, self.partial_envelopes, self.trades
        )?;
        write_field(f, "first_ts_init_ms", offset(self.first, |t| t.ts_init))?;
        write_field(f, "first_ts_event_ms", offset(self.first, |t| t.ts_event))?;
        write_field(f, "last_ts_init_ms", offset(self.last, |t| t.ts_init))?;
        write_field(f, "last_ts_event_ms", offset(self.last, |t| t.ts_event))?;
        write!(
            f,
            " ts_init_non_decreasing={} ts_init_decreases={} halt_hour_trades={}",
            self.ts_init_decreases == 0,
            self.ts_init_decreases,
            self.halt_hour_trades
        )?;
        write_field(
            f,
            "min_event_offset_ms",
            self.min_event_offset_ns
                .map(|ns| ns.div_euclid(NANOS_PER_MILLI)),
        )?;
        write_field(f, "max_init_minus_event_ns", self.max_init_minus_event_ns)?;
        write!(f, " quotes={}", self.quotes)
    }
}

fn write_field(
    f: &mut std::fmt::Formatter<'_>,
    name: &str,
    value: Option<impl Display>,
) -> std::fmt::Result {
    match value {
        Some(value) => write!(f, " {name}={value}"),
        None => write!(f, " {name}=none"),
    }
}

/// The program's one output line.
#[derive(Debug)]
struct Report {
    outcome: Outcome,
    tally: ReplayTally,
    wall_ms: Option<u128>,
}

impl Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "outcome={} {}", self.outcome.word(), self.tally)?;
        write_field(f, "wall_ms", self.wall_ms)?;
        // Last, quoted: the reason is free text
        write_field(
            f,
            "failure",
            self.outcome.reason().map(|r| format!("{r:?}")),
        )
    }
}

/// Reads the API key from `path` in one read to the end, trimmed.
///
/// No error names the key's content.
fn read_key(path: &Path) -> anyhow::Result<SecretString> {
    let mut raw = String::new();
    let read = File::open(path).and_then(|mut file| file.read_to_string(&mut raw));
    let key = SecretString::from(raw.trim());
    raw.zeroize();
    read.map_err(|e| anyhow::anyhow!("Failed to read the key file {}: {e}", path.display()))?;
    anyhow::ensure!(
        !key.expose_secret().is_empty(),
        "The key file {} is empty",
        path.display()
    );
    Ok(key)
}

fn client_config(key: &SecretString) -> DatabentoDataClientConfig {
    let mut config = DatabentoDataClientConfig::new(
        key.expose_secret(),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("publishers.json"),
        false, // use_exchange_as_venue
        true,  // bars_timestamp_on_close
    );
    config.futures_year_digits = FuturesYearDigits::One;
    config.decode_config = Some(DatabentoDecodeConfig {
        ts_event_source: TsEventSource::Exchange,
        ..DatabentoDecodeConfig::default()
    });
    config
}

fn replay_params(start_ns: u64) -> Params {
    Params::new(
        [
            ("replay_start".to_owned(), json!(start_ns)),
            ("schema".to_owned(), json!("tbbo")),
            ("price_precision".to_owned(), json!(2)),
        ]
        .into(),
    )
}

fn price_precision_params() -> Params {
    Params::new([("price_precision".to_owned(), json!(2))].into())
}

/// Builds the client on its own data event channel, then connects and subscribes.
async fn set_up(
    args: &Args,
) -> anyhow::Result<(DatabentoDataClient, UnboundedReceiver<DataEvent>)> {
    let key = read_key(&args.key_file)?;
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(sender);

    let client_id = ClientId::from(CLIENT_ID);
    let mut client =
        DatabentoDataClient::new(client_id, client_config(&key), get_atomic_clock_realtime())?;
    drop(key);
    client.connect().await?;

    if args.with_quotes {
        client.subscribe_quotes(SubscribeQuotes::new(
            args.instrument,
            Some(client_id),
            None,
            UUID4::new(),
            get_atomic_clock_realtime().get_time_ns(),
            None,
            Some(price_precision_params()),
        ))?;
    }

    Ok((client, receiver))
}

/// Reads events until the request's completed response, its failure, or `deadline`.
async fn read_until_end(
    receiver: &mut UnboundedReceiver<DataEvent>,
    request_id: UUID4,
    deadline: Instant,
    tally: &mut ReplayTally,
) -> Outcome {
    loop {
        let event = match tokio::time::timeout_at(deadline, receiver.recv()).await {
            Err(_) => return Outcome::Bound,
            Ok(None) => return Outcome::ChannelClosed,
            Ok(Some(event)) => event,
        };

        match event {
            DataEvent::Data(Data::Quote(_)) => tally.quote(),
            DataEvent::ResponseEnvelope(envelope) if *envelope.correlation_id() == request_id => {
                tally.envelope(envelope.completed());
                if let Some(DataResponse::Trades(response)) = envelope.response() {
                    for trade in response.data.iter() {
                        tally.trade(trade.ts_init.as_u64(), trade.ts_event.as_u64());
                    }
                }
                if envelope.completed() {
                    return envelope
                        .failure_reason()
                        .map_or(Outcome::Completed, |reason| {
                            Outcome::Failed(reason.to_owned())
                        });
                }
            }
            DataEvent::RequestFailed(failed) if failed.request_id == request_id => {
                return Outcome::Failed(failed.reason);
            }
            _ => {}
        }
    }
}

async fn run(args: &Args, tally: &mut ReplayTally) -> (Outcome, Option<u128>) {
    let (mut client, mut receiver) = match set_up(args).await {
        Ok(set_up) => set_up,
        Err(e) => return (Outcome::Error(format!("{e:#}")), None),
    };

    let request_id = UUID4::new();
    let request = RequestTrades::new(
        args.instrument,
        Some(UnixNanos::from(args.start_ns).to_datetime_utc()),
        None,
        None,
        Some(ClientId::from(CLIENT_ID)),
        request_id,
        get_atomic_clock_realtime().get_time_ns(),
        Some(replay_params(args.start_ns)),
    );
    let sent = Instant::now();
    let outcome = match client.request_trades(request) {
        Ok(()) => {
            let deadline = sent + Duration::from_secs(args.bound_secs);
            read_until_end(&mut receiver, request_id, deadline, tally).await
        }
        Err(e) => Outcome::Refused(format!("{e:#}")),
    };
    let wall_ms = sent.elapsed().as_millis();

    if let Err(e) = client.disconnect().await {
        log::warn!("Disconnect failed: {e:#}");
    }

    (outcome, Some(wall_ms))
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    // The adapter's `info` lines on stdout; no `debug` line, which would show market data
    let log_guard = match init_logging(
        TraderId::from(TRADER_ID),
        UUID4::new(),
        LoggerConfig {
            stdout_level: log::LevelFilter::Info,
            is_colored: false,
            ..Default::default()
        },
        Default::default(),
    ) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("Failed to initialize logging: {e:#}");
            return ExitCode::FAILURE;
        }
    };

    let new_york = match get_timezone("America/New_York") {
        Ok(new_york) => new_york,
        Err(e) => {
            eprintln!("Failed to load the America/New_York timezone: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut tally = ReplayTally::new(args.start_ns, new_york);
    let (outcome, wall_ms) = run(&args, &mut tally).await;

    // Flushes the adapter's lines first, so this program's line is the last one
    drop(log_guard);
    let report = Report {
        outcome,
        tally,
        wall_ms,
    };
    println!("{report}");

    if report.outcome == Outcome::Completed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use jiff::civil::date;
    use rstest::rstest;

    use super::*;

    const MS: u64 = 1_000_000;

    fn utc_ns(year: i16, month: i8, day: i8, hour: i8, minute: i8, nanos: i32) -> u64 {
        let ns = date(year, month, day)
            .at(hour, minute, 0, nanos)
            .to_zoned(TimeZone::UTC)
            .unwrap()
            .timestamp()
            .as_nanosecond();
        u64::try_from(ns).unwrap()
    }

    fn tally(start_ns: u64) -> ReplayTally {
        ReplayTally::new(start_ns, get_timezone("America/New_York").unwrap())
    }

    #[rstest]
    fn test_client_config_decodes_replays_on_exchange_time() {
        let config = client_config(&SecretString::from("db-made-up-key"));

        assert_eq!(
            config.decode_config.map(|config| config.ts_event_source),
            Some(TsEventSource::Exchange)
        );
    }

    #[rstest]
    fn test_parses_flags_with_default_bound() {
        let args = Args::try_parse_from([
            "databento-replay-trades",
            "--key-file",
            "/dev/stdin",
            "--instrument",
            "MNQZ6.GLBX",
            "--start-ns",
            "1794783600000000000",
        ])
        .unwrap();

        assert_eq!(args.key_file, PathBuf::from("/dev/stdin"));
        assert_eq!(args.instrument, InstrumentId::from("MNQZ6.GLBX"));
        assert_eq!(args.start_ns, 1_794_783_600_000_000_000);
        assert!(!args.with_quotes);
        assert_eq!(args.bound_secs, 150);
    }

    #[rstest]
    fn test_parses_with_quotes_and_bound() {
        let args = Args::try_parse_from([
            "databento-replay-trades",
            "--key-file",
            "key",
            "--instrument",
            "MNQZ6.GLBX",
            "--start-ns",
            "1",
            "--with-quotes",
            "--bound-secs",
            "30",
        ])
        .unwrap();

        assert!(args.with_quotes);
        assert_eq!(args.bound_secs, 30);
    }

    #[rstest]
    #[case::no_key_file(&["--instrument", "MNQZ6.GLBX", "--start-ns", "1"][..])]
    #[case::negative_start(&["--key-file", "k", "--instrument", "MNQZ6.GLBX", "--start-ns", "-1"][..])]
    #[case::no_venue(&["--key-file", "k", "--instrument", "MNQZ6", "--start-ns", "1"][..])]
    #[case::key_value_flag(&["--key", "abc", "--key-file", "k", "--instrument", "MNQZ6.GLBX", "--start-ns", "1"][..])]
    fn test_refuses_bad_flags(#[case] flags: &[&str]) {
        let argv = std::iter::once("databento-replay-trades").chain(flags.iter().copied());

        assert!(Args::try_parse_from(argv).is_err());
    }

    #[rstest]
    fn test_reads_key_trimmed_and_never_debug_prints_it() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "  db-made-up-key-0123456789  ").unwrap();

        let key = read_key(file.path()).unwrap();

        assert_eq!(key.expose_secret(), "db-made-up-key-0123456789");
        assert!(!format!("{key:?}").contains("made-up"));
        assert!(!format!("{:?}", client_config(&key)).contains("made-up"));
    }

    #[rstest]
    fn test_refuses_an_empty_key_file() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "   ").unwrap();

        let error = read_key(file.path()).unwrap_err();

        assert!(error.to_string().contains("is empty"), "{error}");
    }

    #[rstest]
    fn test_tally_counts_envelopes_trades_and_offsets() {
        let start = 1_000_000 * MS;
        let mut tally = tally(start);
        tally.envelope(false);
        tally.trade(start + 2 * MS, start + MS);
        tally.trade(start + 5 * MS + 1, start + 3 * MS);
        tally.envelope(false);
        tally.trade(start + 9 * MS, start + 8 * MS + 999_999);
        tally.envelope(true);
        tally.quote();
        tally.quote();

        assert_eq!(
            tally.to_string(),
            "envelopes=3 partial_envelopes=2 trades=3 first_ts_init_ms=2 first_ts_event_ms=1 \
             last_ts_init_ms=9 last_ts_event_ms=8 ts_init_non_decreasing=true ts_init_decreases=0 \
             halt_hour_trades=0 min_event_offset_ms=1 max_init_minus_event_ns=2000001 quotes=2"
        );
    }

    #[rstest]
    fn test_tally_counts_ts_init_decreases_and_negative_offsets() {
        let start = 1_000_000 * MS;
        let mut tally = tally(start);
        tally.trade(start + 5 * MS, start - 1);
        tally.trade(start + 4 * MS, start + 4 * MS);
        tally.trade(start + 4 * MS, start + 4 * MS);
        tally.trade(start + 3 * MS, start + 3 * MS + 10);

        assert_eq!(tally.ts_init_decreases, 2);
        assert_eq!(tally.min_event_offset_ns, Some(-1));
        assert_eq!(tally.max_init_minus_event_ns, Some(i128::from(5 * MS) + 1));
        let line = tally.to_string();
        assert!(line.contains(" first_ts_event_ms=-1 "), "{line}");
        assert!(line.contains(" min_event_offset_ms=-1 "), "{line}");
        assert!(
            line.contains(" ts_init_non_decreasing=false ts_init_decreases=2 "),
            "{line}"
        );
    }

    #[rstest]
    fn test_tally_without_trades_marks_times_none() {
        let mut tally = tally(1);
        tally.envelope(true);

        assert_eq!(
            tally.to_string(),
            "envelopes=1 partial_envelopes=0 trades=0 first_ts_init_ms=none \
             first_ts_event_ms=none last_ts_init_ms=none last_ts_event_ms=none \
             ts_init_non_decreasing=true ts_init_decreases=0 halt_hour_trades=0 \
             min_event_offset_ms=none max_init_minus_event_ns=none quotes=0"
        );
    }

    // New York daylight saving time ends 2026-11-01: 17:00 New York is 21:00 UTC on 2026-10-30
    // (EDT) and 22:00 UTC on 2026-11-02 (EST)
    #[rstest]
    #[case::edt_before_halt(utc_ns(2026, 10, 30, 20, 59, 999_999_999), false)]
    #[case::edt_halt_opens(utc_ns(2026, 10, 30, 21, 0, 0), true)]
    #[case::edt_halt_last_ns(utc_ns(2026, 10, 30, 21, 59, 999_999_999), true)]
    #[case::edt_reopen(utc_ns(2026, 10, 30, 22, 0, 0), false)]
    #[case::edt_est_halt_hour_in_utc(utc_ns(2026, 10, 30, 22, 30, 0), false)]
    #[case::est_edt_halt_hour_in_utc(utc_ns(2026, 11, 2, 21, 30, 0), false)]
    #[case::est_halt_opens(utc_ns(2026, 11, 2, 22, 0, 0), true)]
    #[case::est_halt_last_ns(utc_ns(2026, 11, 2, 22, 59, 999_999_999), true)]
    #[case::est_reopen(utc_ns(2026, 11, 2, 23, 0, 0), false)]
    fn test_tally_counts_trades_in_the_new_york_halt_hour(
        #[case] ts_event: u64,
        #[case] in_halt: bool,
    ) {
        let mut tally = tally(ts_event);
        tally.trade(ts_event, ts_event);

        assert_eq!(tally.halt_hour_trades, u64::from(in_halt));
    }

    #[rstest]
    fn test_report_ends_with_quoted_failure_reason() {
        let report = Report {
            outcome: Outcome::Failed("Replay of MNQZ6.GLBX: exceeded the replay bound".to_owned()),
            tally: tally(1),
            wall_ms: Some(120_004),
        };

        let line = report.to_string();

        assert!(line.starts_with("outcome=failed envelopes=0 "), "{line}");
        assert!(
            line.ends_with(
                " quotes=0 wall_ms=120004 failure=\"Replay of MNQZ6.GLBX: exceeded the replay bound\""
            ),
            "{line}"
        );
    }

    #[rstest]
    fn test_report_of_a_completed_request_has_no_failure() {
        let report = Report {
            outcome: Outcome::Completed,
            tally: tally(1),
            wall_ms: Some(4_500),
        };

        assert!(
            report
                .to_string()
                .ends_with(" quotes=0 wall_ms=4500 failure=none")
        );
    }
}
