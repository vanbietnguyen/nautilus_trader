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

use std::{path::PathBuf, time::Duration};

use databento::dbn;
use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    messages::{
        DataEvent, DataResponse,
        data::{DataResponseEnvelope, RequestTrades},
    },
};
use nautilus_core::{Params, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_databento::data::{DatabentoDataClient, DatabentoDataClientConfig};
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
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

fn replay_client(
    gateway_addr: String,
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

/// Queues one replay session: the subscription, the start, then `records` and `ReplayCompleted`.
fn serve_replay(server: &MockLsgServer, records: impl IntoIterator<Item = dbn::Mbp1Msg>) {
    server.authenticate();
    server.expect_subscription();
    server.start();
    server.send_record(symbol_mapping_msg(INSTRUMENT_ID, RAW_SYMBOL));

    for record in records {
        server.send_record(record);
    }
    server.send_record(replay_completed_msg());
}

/// Receives response envelopes until the completed one.
async fn recv_until_completed(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) -> Vec<DataResponseEnvelope> {
    let mut envelopes = Vec::new();

    loop {
        let event = tokio::time::timeout(RECV_TIMEOUT, receiver.recv())
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
