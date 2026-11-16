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

//! Integration tests for trades requests served by a live gateway replay.
//!
//! Drives `DataClient::request_trades` with the `replay_start` param against the mock LSG
//! (Live Streaming Gateway) server.

use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use databento::dbn;
use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    messages::{
        DataEvent, DataResponse,
        data::{DataRequestFailed, DataResponseEnvelope, RequestTrades},
    },
};
use nautilus_core::{Params, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_databento::{
    data::{DatabentoDataClient, DatabentoDataClientConfig},
    decode::{DatabentoDecodeConfig, TsEventSource},
};
use nautilus_model::{
    data::TradeTick,
    identifiers::{ClientId, InstrumentId, futures::FuturesYearDigits},
    types::Quantity,
};
use rstest::rstest;
use serde_json::json;

use crate::common::{
    TEST_DATASET, TEST_KEY, mbp1_msg, mock_server::MockLsgServer, symbol_mapping_msg, system_msg,
};

const INSTRUMENT_ID: u32 = 1;
const RAW_SYMBOL: &str = "ESM4";
const REQUEST_INSTRUMENT_ID: &str = "ESM4.GLBX";
const REPLAY_START: u64 = 1_717_423_200_000_000_000;
// The adapter's lag bound, one second
const LAG_BOUND_NS: u64 = 1_000_000_000;
// The adapter's idle time, 5000 ms
const IDLE: Duration = Duration::from_millis(5_000);
const RECV_TIMEOUT: Duration = Duration::from_secs(5);
// An idle end must arrive by then; one re-armed by a heartbeat a second before it would not
const IDLE_END_WITHIN: Duration = Duration::from_millis(7_000);

fn replay_client(
    gateway_addr: String,
) -> (
    DatabentoDataClient,
    tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) {
    replay_client_with_bound(gateway_addr, None)
}

fn replay_client_with_bound(
    gateway_addr: String,
    replay_bound_secs: Option<u64>,
) -> (
    DatabentoDataClient,
    tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) {
    replay_client_with(gateway_addr, replay_bound_secs, None)
}

fn replay_client_with(
    gateway_addr: String,
    replay_bound_secs: Option<u64>,
    decode_config: Option<DatabentoDecodeConfig>,
) -> (
    DatabentoDataClient,
    tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(sender);

    let mut config = DatabentoDataClientConfig::new(
        TEST_KEY,
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("publishers.json"),
        false,
        true,
    );
    config.live_gateway_addr = Some(gateway_addr);
    config.futures_year_digits = FuturesYearDigits::One;
    config.replay_bound_secs = replay_bound_secs;
    config.decode_config = decode_config;
    let client = DatabentoDataClient::new(
        ClientId::from("DATABENTO-REPLAY"),
        config,
        get_atomic_clock_realtime(),
    )
    .unwrap();

    (client, receiver)
}

fn replay_request() -> RequestTrades {
    let params = Params::new(
        [
            ("replay_start".to_owned(), json!(REPLAY_START)),
            ("price_precision".to_owned(), json!(2)),
        ]
        .into(),
    );

    RequestTrades::new(
        InstrumentId::from(REQUEST_INSTRUMENT_ID),
        Some(UnixNanos::from(REPLAY_START).to_datetime_utc()),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::from(37),
        Some(params),
    )
}

fn tbbo_trade(ts_recv: u64, size: u32) -> dbn::Mbp1Msg {
    let mut msg = mbp1_msg(INSTRUMENT_ID, 100_000_000_000, 100_250_000_000, b'T');
    msg.hd.ts_event = ts_recv;
    msg.ts_recv = ts_recv;
    msg.price = 100_250_000_000;
    msg.size = size;
    msg
}

fn replay_completed_msg() -> dbn::SystemMsg {
    system_msg("Replay completed", dbn::SystemCode::ReplayCompleted as u8)
}

fn heartbeat_msg() -> dbn::SystemMsg {
    system_msg("Heartbeat", dbn::SystemCode::Heartbeat as u8)
}

/// Returns a time an hour from now, past the clock's time when a request arrives.
fn an_hour_from_now() -> u64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    u64::try_from((now + Duration::from_secs(3_600)).as_nanos()).unwrap()
}

/// Queues one replay session: the subscription, the start, then `records`, and then goes quiet.
fn serve_session(server: &MockLsgServer, records: impl IntoIterator<Item = dbn::Mbp1Msg>) {
    server.authenticate();
    server.expect_subscription();
    server.start();
    server.send_record(symbol_mapping_msg(INSTRUMENT_ID, RAW_SYMBOL));

    for record in records {
        server.send_record(record);
    }
}

/// Queues one replay session: the subscription, the start, then `records` and `ReplayCompleted`.
fn serve_replay(server: &MockLsgServer, records: impl IntoIterator<Item = dbn::Mbp1Msg>) {
    serve_session(server, records);
    server.send_record(replay_completed_msg());
}

/// Receives response envelopes until the completed one.
async fn recv_until_completed(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) -> Vec<DataResponseEnvelope> {
    recv_until_completed_by(receiver, tokio::time::Instant::now() + RECV_TIMEOUT).await
}

/// Receives response envelopes until the completed one, which must arrive by `deadline`.
async fn recv_until_completed_by(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    deadline: tokio::time::Instant,
) -> Vec<DataResponseEnvelope> {
    let mut envelopes = Vec::new();

    loop {
        let event = tokio::time::timeout_at(deadline, receiver.recv())
            .await
            .expect("timed out waiting for a response envelope")
            .expect("channel closed");
        let DataEvent::ResponseEnvelope(envelope) = event else {
            panic!("expected a response envelope, was {event:?}");
        };
        let completed = envelope.completed();
        envelopes.push(envelope);

        if completed {
            return envelopes;
        }
    }
}

/// Receives the request's failure, which must be its next event and arrive within `within`.
async fn recv_failure(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    within: Duration,
) -> DataRequestFailed {
    let event = tokio::time::timeout(within, receiver.recv())
        .await
        .expect("timed out waiting for the request failure")
        .expect("channel closed");
    let DataEvent::RequestFailed(failed) = event else {
        panic!("expected a request failure, was {event:?}");
    };
    failed
}

fn trade_sizes(envelopes: &[DataResponseEnvelope]) -> Vec<Quantity> {
    envelopes
        .iter()
        .flat_map(envelope_trades)
        .map(|trade| trade.size)
        .collect()
}

fn envelope_trades(envelope: &DataResponseEnvelope) -> Vec<TradeTick> {
    let Some(DataResponse::Trades(response)) = envelope.response() else {
        panic!("expected a trades response, was {:?}", envelope.response());
    };
    response.data.to_vec()
}

#[rstest]
#[tokio::test]
async fn test_replay_subscribes_tbbo_from_lag_bound_before_replay_start() {
    let server = MockLsgServer::new(TEST_DATASET).await;
    let (client, mut receiver) = replay_client(server.addr());
    serve_replay(&server, [tbbo_trade(REPLAY_START, 1)]);

    client.request_trades(replay_request()).unwrap();
    recv_until_completed(&mut receiver).await;

    let lines = server.subscription_lines();
    assert_eq!(
        lines.len(),
        1,
        "expected one subscription line, was {lines:?}"
    );
    let fields: Vec<&str> = lines[0].split('|').collect();
    assert!(fields.contains(&"schema=tbbo"), "{fields:?}");
    assert!(fields.contains(&"stype_in=raw_symbol"), "{fields:?}");
    assert!(
        fields.contains(&format!("symbols={RAW_SYMBOL}").as_str()),
        "{fields:?}"
    );
    let start = format!("start={}", REPLAY_START - LAG_BOUND_NS);
    assert!(fields.contains(&start.as_str()), "{fields:?}");

    server.stop().await;
}

#[rstest]
#[tokio::test]
async fn test_replay_discards_trades_received_before_replay_start() {
    let server = MockLsgServer::new(TEST_DATASET).await;
    let (client, mut receiver) = replay_client(server.addr());
    serve_replay(
        &server,
        [
            tbbo_trade(REPLAY_START - LAG_BOUND_NS, 1),
            tbbo_trade(REPLAY_START - 1, 2),
            tbbo_trade(REPLAY_START, 3),
            tbbo_trade(REPLAY_START + 1, 4),
            tbbo_trade(REPLAY_START + 500_000_000, 5),
        ],
    );

    client.request_trades(replay_request()).unwrap();
    let envelopes = recv_until_completed(&mut receiver).await;

    let trades: Vec<TradeTick> = envelopes.iter().flat_map(envelope_trades).collect();
    let sizes: Vec<Quantity> = trades.iter().map(|trade| trade.size).collect();
    assert_eq!(
        sizes,
        [3, 4, 5].map(Quantity::from).to_vec(),
        "trades received before replay_start must be absent, the rest in order"
    );
    let ts_inits: Vec<UnixNanos> = trades.iter().map(|trade| trade.ts_init).collect();
    assert_eq!(
        ts_inits,
        [REPLAY_START, REPLAY_START + 1, REPLAY_START + 500_000_000]
            .map(UnixNanos::from)
            .to_vec(),
    );
    for trade in &trades {
        assert_eq!(
            trade.instrument_id,
            InstrumentId::from(REQUEST_INSTRUMENT_ID)
        );
    }

    server.stop().await;
}

#[rstest]
#[tokio::test]
async fn test_replay_completed_sends_one_completed_envelope_after_partials() {
    let server = MockLsgServer::new(TEST_DATASET).await;
    let (client, mut receiver) = replay_client(server.addr());
    serve_replay(
        &server,
        [tbbo_trade(REPLAY_START, 1), tbbo_trade(REPLAY_START + 1, 2)],
    );
    let request = replay_request();
    let request_id = request.request_id;

    client.request_trades(request).unwrap();
    let envelopes = recv_until_completed(&mut receiver).await;

    let (completed, partials) = envelopes.split_last().unwrap();
    assert!(!partials.is_empty(), "expected partial envelopes first");
    assert!(partials.iter().all(|envelope| !envelope.completed()));
    assert_eq!(
        partials
            .iter()
            .map(|e| envelope_trades(e).len())
            .sum::<usize>(),
        2
    );
    assert!(envelope_trades(completed).is_empty());
    for envelope in &envelopes {
        assert_eq!(envelope.correlation_id(), &request_id);
        assert_eq!(envelope.failure_reason(), None);
    }

    let after = tokio::time::timeout(Duration::from_millis(200), receiver.recv()).await;
    assert!(
        after.is_err(),
        "expected no event after completion, was {after:?}"
    );

    server.stop().await;
}

#[rstest]
#[tokio::test]
async fn test_replay_ends_idle_when_the_gateway_goes_quiet() {
    let server = MockLsgServer::new(TEST_DATASET).await;
    let (client, mut receiver) = replay_client(server.addr());
    serve_session(
        &server,
        [tbbo_trade(REPLAY_START, 1), tbbo_trade(REPLAY_START + 1, 2)],
    );
    let sent = tokio::time::Instant::now();

    client.request_trades(replay_request()).unwrap();
    let envelopes = recv_until_completed_by(&mut receiver, sent + IDLE_END_WITHIN).await;

    assert!(
        sent.elapsed() >= IDLE,
        "the idle end came before the idle time"
    );
    assert_eq!(trade_sizes(&envelopes), [1, 2].map(Quantity::from).to_vec());
    let (completed, _) = envelopes.split_last().unwrap();
    assert!(envelope_trades(completed).is_empty());
    assert!(
        envelopes
            .iter()
            .all(|envelope| envelope.failure_reason().is_none())
    );

    server.stop().await;
}

#[rstest]
#[tokio::test]
async fn test_replay_heartbeats_do_not_rearm_the_idle_end() {
    let server = MockLsgServer::new(TEST_DATASET).await;
    let (client, mut receiver) = replay_client(server.addr());
    serve_session(
        &server,
        [tbbo_trade(REPLAY_START, 1), tbbo_trade(REPLAY_START + 1, 2)],
    );
    let sent = tokio::time::Instant::now();

    client.request_trades(replay_request()).unwrap();
    // Heartbeats one to four seconds into the quiet: re-armed, the idle end would come at nine
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        server.send_record(heartbeat_msg());
    }
    let envelopes = recv_until_completed_by(&mut receiver, sent + IDLE_END_WITHIN).await;

    assert!(
        sent.elapsed() >= IDLE,
        "the idle end came before the idle time"
    );
    assert_eq!(trade_sizes(&envelopes), [1, 2].map(Quantity::from).to_vec());

    server.stop().await;
}

#[rstest]
#[tokio::test]
async fn test_replay_ends_at_bound_and_discards_the_crossing_record() {
    let server = MockLsgServer::new(TEST_DATASET).await;
    let (client, mut receiver) = replay_client(server.addr());
    let crossing = an_hour_from_now();
    serve_session(
        &server,
        [
            tbbo_trade(REPLAY_START, 1),
            tbbo_trade(REPLAY_START + 1, 2),
            tbbo_trade(crossing, 3),
            tbbo_trade(crossing + 1, 4),
        ],
    );
    let sent = tokio::time::Instant::now();

    client.request_trades(replay_request()).unwrap();
    // Before the idle time, so the end is the bound's
    let envelopes = recv_until_completed_by(&mut receiver, sent + IDLE / 2).await;

    assert_eq!(
        trade_sizes(&envelopes),
        [1, 2].map(Quantity::from).to_vec(),
        "the record at or after the bound and those after it must be absent"
    );
    let (completed, _) = envelopes.split_last().unwrap();
    assert!(envelope_trades(completed).is_empty());

    server.stop().await;
}

#[rstest]
#[tokio::test]
async fn test_replay_bound_drops_a_session_that_never_authenticates() {
    let server = MockLsgServer::new(TEST_DATASET).await;
    let (client, mut receiver) = replay_client_with_bound(server.addr(), Some(1));
    let closed = server.accept_until_closed();

    client.request_trades(replay_request()).unwrap();
    let failed = recv_failure(&mut receiver, Duration::from_secs(3)).await;

    assert!(
        failed.reason.contains("replay bound of 1 s"),
        "{}",
        failed.reason
    );
    assert!(
        failed.reason.contains("replay_bound_secs"),
        "{}",
        failed.reason
    );
    tokio::time::timeout(Duration::from_secs(2), closed)
        .await
        .expect("the gateway did not see the session dropped")
        .unwrap();

    server.stop().await;
}

#[rstest]
#[tokio::test]
async fn test_replay_fails_on_a_record_received_before_the_previous() {
    let server = MockLsgServer::new(TEST_DATASET).await;
    let (client, mut receiver) = replay_client(server.addr());
    serve_replay(
        &server,
        [
            tbbo_trade(REPLAY_START + 10, 1),
            tbbo_trade(REPLAY_START + 5, 2),
        ],
    );

    client.request_trades(replay_request()).unwrap();
    let failed = recv_failure(&mut receiver, RECV_TIMEOUT).await;

    assert!(
        failed.reason.contains("ts_recv is below the previous"),
        "{}",
        failed.reason
    );

    server.stop().await;
}

#[rstest]
#[tokio::test]
async fn test_replay_gateway_error_fails_with_its_code_name() {
    let server = MockLsgServer::new(TEST_DATASET).await;
    let (client, mut receiver) = replay_client(server.addr());
    serve_session(&server, [tbbo_trade(REPLAY_START, 1)]);
    server.send_record(dbn::ErrorMsg::new(
        1_000_000_000,
        Some(dbn::ErrorCode::ReplayDataAgedOut),
        "Replay data aged out",
        true,
    ));

    client.request_trades(replay_request()).unwrap();
    let failed = recv_failure(&mut receiver, RECV_TIMEOUT).await;

    assert!(
        failed.reason.contains("ReplayDataAgedOut"),
        "{}",
        failed.reason
    );

    server.stop().await;
}

fn exchange_time_config() -> DatabentoDecodeConfig {
    DatabentoDecodeConfig {
        ts_event_source: TsEventSource::Exchange,
        ..DatabentoDecodeConfig::default()
    }
}

// The made-up trade's receive time, and an exchange time 1.5 µs before it
const TS_RECV: u64 = REPLAY_START + 500_000_000;
const TS_EXCHANGE: u64 = TS_RECV - 1_500;

#[rstest]
#[case::exchange_time(Some(exchange_time_config()), TS_EXCHANGE, TS_EXCHANGE)]
#[case::exchange_time_undefined_header(Some(exchange_time_config()), dbn::UNDEF_TIMESTAMP, TS_RECV)]
#[case::receive_time(Some(DatabentoDecodeConfig::default()), TS_EXCHANGE, TS_RECV)]
#[case::no_decode_config(None, TS_EXCHANGE, TS_RECV)]
#[tokio::test]
async fn test_replay_stamps_ts_event_by_the_client_decode_config(
    #[case] decode_config: Option<DatabentoDecodeConfig>,
    #[case] header_ts_event: u64,
    #[case] expected_ts_event: u64,
) {
    let server = MockLsgServer::new(TEST_DATASET).await;
    let (client, mut receiver) = replay_client_with(server.addr(), None, decode_config);
    let mut trade = tbbo_trade(TS_RECV, 1);
    trade.hd.ts_event = header_ts_event;
    serve_replay(&server, [trade]);

    client.request_trades(replay_request()).unwrap();
    let envelopes = recv_until_completed(&mut receiver).await;

    let trades: Vec<TradeTick> = envelopes.iter().flat_map(envelope_trades).collect();
    let times: Vec<(UnixNanos, UnixNanos)> = trades
        .iter()
        .map(|trade| (trade.ts_event, trade.ts_init))
        .collect();
    assert_eq!(
        times,
        [(UnixNanos::from(expected_ts_event), UnixNanos::from(TS_RECV))],
        "(ts_event, ts_init) of the replayed trade"
    );

    server.stop().await;
}
