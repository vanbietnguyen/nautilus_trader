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

use nautilus_common::{
    actor::{DataActor, DataActorCore, data_actor::DataActorConfig},
    component::Component,
    messages::data::ReleaseKeptBars,
    nautilus_actor,
};
use nautilus_model::{enums::BarIntervalType, identifiers::ActorId};

use super::*;

#[rstest]
fn test_request_scoped_bar_aggregator_runs_alongside_live_subscription(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str());
    let live_subscribe = DataCommand::Subscribe(SubscribeCommand::Bars(SubscribeBars::new(
        bar_type,
        Some(client_id),
        Some(venue),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )));
    data_engine.execute(live_subscribe);

    let make_trade = |ts: u64, trade_id: &str| {
        TradeTick::new(
            instrument_id,
            Price::from("0.65000"),
            Quantity::from("1000"),
            AggressorSide::Buy,
            TradeId::new(trade_id),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    };

    data_engine.process_data(Data::Trade(make_trade(1_000, "live-1")));
    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(1_000)),
    );

    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": false,
    }))
    .unwrap();

    let request = RequestTrades::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params.clone()),
    );
    data_engine.execute(DataCommand::Request(RequestCommand::Trades(
        request.clone(),
    )));

    assert_eq!(
        recorder.borrow().last(),
        Some(&DataCommand::Request(RequestCommand::Trades(request))),
    );

    data_engine.response(DataResponse::Trades(TradesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![make_trade(2_000, "historical-1")],
        None,
        None,
        UnixNanos::from(2_000),
        Some(params),
    )));

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(2_000)),
        "request-scoped aggregator must process the historical response",
    );

    data_engine.process_data(Data::Trade(make_trade(3_000, "live-2")));

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(3_000)),
        "live aggregator must remain subscribed after request cleanup",
    );
}

#[rstest]
fn test_request_scoped_quote_bar_aggregators_handle_multiple_bar_types(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let one_tick = BarType::from(format!("{instrument_id}-1-TICK-BID-INTERNAL").as_str());
    let two_tick = BarType::from(format!("{instrument_id}-2-TICK-BID-INTERNAL").as_str());
    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [one_tick.to_string(), two_tick.to_string()],
        "update_subscriptions": false,
    }))
    .unwrap();

    let request = RequestQuotes::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params.clone()),
    );
    data_engine.execute(DataCommand::Request(RequestCommand::Quotes(request)));

    let make_quote = |ts: u64, bid: &str| {
        QuoteTick::new(
            instrument_id,
            Price::from(bid),
            Price::from("0.65010"),
            Quantity::from("1000"),
            Quantity::from("1000"),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    };

    data_engine.response(DataResponse::Quotes(QuotesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![make_quote(1_000, "0.65000"), make_quote(2_000, "0.65001")],
        None,
        None,
        UnixNanos::from(2_000),
        Some(params),
    )));

    assert_eq!(
        cache.borrow().bar(&one_tick).map(|bar| bar.ts_event),
        Some(UnixNanos::from(2_000)),
    );
    assert_eq!(
        cache.borrow().bar(&two_tick).map(|bar| bar.ts_event),
        Some(UnixNanos::from(2_000)),
    );
}

#[rstest]
fn test_request_scoped_bar_aggregation_deduplicates_bar_types(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-3-TICK-BID-INTERNAL").as_str());

    let params = || -> Params {
        serde_json::from_value(json!({
        "bar_types": [bar_type.to_string(), bar_type.to_string()],
        "update_subscriptions": false,
        }))
        .unwrap()
    };

    let request_id = UUID4::new();

    let request = RequestQuotes::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params()),
    );
    data_engine.execute(DataCommand::Request(RequestCommand::Quotes(request)));

    let make_quote = |ts: u64, bid: &str| {
        QuoteTick::new(
            instrument_id,
            Price::from(bid),
            Price::from("0.65010"),
            Quantity::from("1000"),
            Quantity::from("1000"),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    };

    data_engine.response(DataResponse::Quotes(QuotesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![make_quote(1_000, "0.65000"), make_quote(2_000, "0.65001")],
        None,
        None,
        UnixNanos::from(2_000),
        Some(params()),
    )));

    assert_eq!(cache.borrow().bar(&bar_type), None);

    let request_id = UUID4::new();

    let request = RequestQuotes::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params()),
    );
    data_engine.execute(DataCommand::Request(RequestCommand::Quotes(request)));

    data_engine.response(DataResponse::Quotes(QuotesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![
            make_quote(1_000, "0.65000"),
            make_quote(2_000, "0.65001"),
            make_quote(3_000, "0.65002"),
        ],
        None,
        None,
        UnixNanos::from(3_000),
        Some(params()),
    )));

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(3_000)),
    );
}

#[rstest]
fn test_request_scoped_bar_aggregation_does_not_publish_to_live_topic(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str());
    let (handler, saver) = get_typed_message_saving_handler::<Bar>(None);
    let topic = switchboard::get_bars_topic(bar_type);
    msgbus::subscribe_bars(topic.into(), handler, None);

    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": false,
    }))
    .unwrap();

    let request = RequestTrades::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params.clone()),
    );
    data_engine.execute(DataCommand::Request(RequestCommand::Trades(request)));

    let trade = TradeTick::new(
        instrument_id,
        Price::from("0.65000"),
        Quantity::from("1000"),
        AggressorSide::Buy,
        TradeId::new("historical-1"),
        UnixNanos::from(1_000),
        UnixNanos::from(1_000),
    );
    data_engine.response(DataResponse::Trades(TradesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![trade],
        None,
        None,
        UnixNanos::from(1_000),
        Some(params),
    )));

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(1_000)),
    );
    assert!(saver.get_messages().is_empty());
}

#[rstest]
fn test_request_scoped_time_bar_aggregation_handles_trade_response(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    // The engine clock reads the time the answer carries, so its end covers the last trade
    let clock = data_engine_clock_at(1_000_000_000);
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-1-SECOND-LAST-INTERNAL").as_str());
    let (handler, saver) = get_typed_message_saving_handler::<Bar>(None);
    let topic = switchboard::get_bars_topic(bar_type);
    msgbus::subscribe_bars(topic.into(), handler, None);

    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": false,
    }))
    .unwrap();

    let request = RequestTrades::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params.clone()),
    );
    data_engine.execute(DataCommand::Request(RequestCommand::Trades(request)));

    let make_trade = |ts: u64, trade_id: &str| {
        TradeTick::new(
            instrument_id,
            Price::from("0.65000"),
            Quantity::from("1000"),
            AggressorSide::Buy,
            TradeId::new(trade_id),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    };

    // A completed envelope certifies that the answer holds every record of its window
    data_engine.response_envelope(DataResponseEnvelope::new(
        DataResponse::Trades(TradesResponse::new(
            request_id,
            client_id,
            instrument_id,
            vec![
                make_trade(0, "historical-1"),
                make_trade(1_000_000_000, "historical-2"),
            ],
            None,
            None,
            UnixNanos::from(1_000_000_000),
            Some(params),
        )),
        true,
        None,
    ));

    // The close at the last trade's stamp stays armed until the end, the dispatch time (1 s)
    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(1_000_000_000))
    );
    assert!(saver.get_messages().is_empty());
}

#[rstest]
fn test_request_scoped_composite_bar_aggregator_handles_bar_response(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let composite =
        BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL@1-TICK-EXTERNAL").as_str());
    let source = composite.composite();
    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [composite.to_string()],
        "update_subscriptions": false,
    }))
    .unwrap();

    let request = RequestBars::new(
        source,
        None,
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params.clone()),
    );
    data_engine.execute(DataCommand::Request(RequestCommand::Bars(request)));

    let bar = Bar::new(
        source,
        Price::from("0.65000"),
        Price::from("0.65000"),
        Price::from("0.65000"),
        Price::from("0.65000"),
        Quantity::from("1000"),
        UnixNanos::from(1_000),
        UnixNanos::from(1_000),
    );
    data_engine.response(DataResponse::Bars(BarsResponse::new(
        request_id,
        client_id,
        source,
        vec![bar],
        None,
        None,
        UnixNanos::from(1_000),
        Some(params),
    )));

    // Aggregated bars are cached under the standard bar type (v1 parity)
    assert_eq!(
        cache
            .borrow()
            .bar(&composite.standard())
            .map(|bar| bar.ts_event),
        Some(UnixNanos::from(1_000)),
    );
}

#[rstest]
fn test_update_subscriptions_request_aggregator_can_be_started_live_after_response(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str());
    let (handler, saver) = get_typed_message_saving_handler::<Bar>(None);
    let topic = switchboard::get_bars_topic(bar_type);
    msgbus::subscribe_bars(topic.into(), handler, None);

    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": true,
    }))
    .unwrap();

    let request = RequestTrades::new(
        instrument_id,
        Some(UnixNanos::default().to_datetime_utc()),
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params.clone()),
    );
    data_engine.execute(DataCommand::Request(RequestCommand::Trades(request)));

    let make_trade = |ts: u64, trade_id: &str| {
        TradeTick::new(
            instrument_id,
            Price::from("0.65000"),
            Quantity::from("1000"),
            AggressorSide::Buy,
            TradeId::new(trade_id),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    };

    data_engine.response(DataResponse::Trades(TradesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![make_trade(1_000, "historical-1")],
        None,
        None,
        UnixNanos::from(1_000),
        Some(params),
    )));

    assert!(saver.get_messages().is_empty());

    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Bars(
        SubscribeBars::new(
            bar_type,
            Some(client_id),
            Some(venue),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ),
    )));
    data_engine.process_data(Data::Trade(make_trade(2_000, "live-1")));

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(2_000)),
    );
    let messages = saver.get_messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].ts_event, UnixNanos::from(2_000));
}

#[rstest]
fn test_update_subscriptions_request_aggregator_can_subscribe_before_response(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str());
    let (handler, saver) = get_typed_message_saving_handler::<Bar>(None);
    let topic = switchboard::get_bars_topic(bar_type);
    msgbus::subscribe_bars(topic.into(), handler, None);

    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": true,
    }))
    .unwrap();

    let request = RequestTrades::new(
        instrument_id,
        Some(UnixNanos::default().to_datetime_utc()),
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params.clone()),
    );
    data_engine.execute(DataCommand::Request(RequestCommand::Trades(request)));

    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Bars(
        SubscribeBars::new(
            bar_type,
            Some(client_id),
            Some(venue),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ),
    )));

    let make_trade = |ts: u64, trade_id: &str| {
        TradeTick::new(
            instrument_id,
            Price::from("0.65000"),
            Quantity::from("1000"),
            AggressorSide::Buy,
            TradeId::new(trade_id),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    };

    data_engine.response(DataResponse::Trades(TradesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![make_trade(1_000, "historical-1")],
        None,
        None,
        UnixNanos::from(1_000),
        Some(params),
    )));

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(1_000)),
    );
    assert!(saver.get_messages().is_empty());

    data_engine.process_data(Data::Trade(make_trade(2_000, "live-1")));

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(2_000)),
    );
    let messages = saver.get_messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].ts_event, UnixNanos::from(2_000));
}

#[rstest]
fn test_update_subscriptions_request_aggregator_replays_live_quotes_after_response(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    const SEC: u64 = 1_000_000_000;
    let t0 = 1_700_000_040 * SEC;

    let _ = stub_msgbus;
    let virtual_clock = Rc::new(RefCell::new(VirtualClock::new()));
    virtual_clock
        .borrow_mut()
        .set_time(UnixNanos::from(t0 + 30 * SEC));
    let clock: Rc<RefCell<dyn Clock>> = virtual_clock;
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-1-MINUTE-MID-INTERNAL").as_str());
    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": true,
    }))
    .unwrap();

    data_engine.execute(DataCommand::Request(RequestCommand::Quotes(
        RequestQuotes::new(
            instrument_id,
            Some(UnixNanos::from(t0 - 300 * SEC).to_datetime_utc()),
            Some(UnixNanos::from(t0 - 1).to_datetime_utc()),
            None,
            Some(client_id),
            request_id,
            UnixNanos::from(t0 + 30 * SEC),
            Some(params.clone()),
        ),
    )));
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Bars(
        SubscribeBars::new(
            bar_type,
            Some(client_id),
            Some(venue),
            UUID4::new(),
            UnixNanos::from(t0 + 30 * SEC),
            None,
            None,
        ),
    )));

    let make_quote = |ts: u64, bid: &str, ask: &str| {
        QuoteTick::new(
            instrument_id,
            Price::from(bid),
            Price::from(ask),
            Quantity::from("1000"),
            Quantity::from("1000"),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    };

    // A stale quote and a quote after the history end arrive while the response is in flight
    data_engine.process_data(Data::Quote(make_quote(t0 - 2 * SEC, "0.64000", "0.64010")));
    data_engine.process_data(Data::Quote(make_quote(t0 + 29 * SEC, "0.66000", "0.66010")));
    assert!(cache.borrow().bar(&bar_type).is_none());

    let history: Vec<QuoteTick> = (1..=30)
        .map(|i| make_quote(t0 - 300 * SEC + i * 10 * SEC - 1, "0.65000", "0.65010"))
        .collect();
    data_engine.response_envelope(DataResponseEnvelope::new(
        DataResponse::Quotes(QuotesResponse::new(
            request_id,
            client_id,
            instrument_id,
            history,
            None,
            None,
            UnixNanos::from(t0 + 30 * SEC),
            Some(params),
        )),
        true,
        None,
    ));

    // The held quote closes the last historical bar and the stale quote is not applied
    let cache_ref = cache.borrow();
    let bars = cache_ref.bars(&bar_type).unwrap();
    assert_eq!(bars.len(), 5);
    assert_eq!(bars[0].ts_event, UnixNanos::from(t0));
    assert_eq!(bars[0].close, Price::from("0.65005"));
    assert_eq!(bars[0].low, Price::from("0.65005"));
}

#[rstest]
fn test_update_subscriptions_request_aggregator_replays_live_trades_after_response(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-2-TICK-LAST-INTERNAL").as_str());
    let (handler, saver) = get_typed_message_saving_handler::<Bar>(None);
    let topic = switchboard::get_bars_topic(bar_type);
    msgbus::subscribe_bars(topic.into(), handler, None);

    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": true,
    }))
    .unwrap();

    data_engine.execute(DataCommand::Request(RequestCommand::Trades(
        RequestTrades::new(
            instrument_id,
            Some(UnixNanos::default().to_datetime_utc()),
            None,
            None,
            Some(client_id),
            request_id,
            UnixNanos::default(),
            Some(params.clone()),
        ),
    )));
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Bars(
        SubscribeBars::new(
            bar_type,
            Some(client_id),
            Some(venue),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ),
    )));

    let make_trade = |ts: u64, price: &str, trade_id: &str| {
        TradeTick::new(
            instrument_id,
            Price::from(price),
            Quantity::from("1000"),
            AggressorSide::Buy,
            TradeId::new(trade_id),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    };

    // A live trade arrives while the response is in flight and must not displace the history
    data_engine.process_data(Data::Trade(make_trade(5_000, "0.67000", "live-1")));
    assert!(cache.borrow().bar(&bar_type).is_none());
    assert!(saver.get_messages().is_empty());

    data_engine.response(DataResponse::Trades(TradesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![
            make_trade(1_000, "0.65000", "historical-1"),
            make_trade(2_000, "0.65010", "historical-2"),
            make_trade(3_000, "0.65020", "historical-3"),
            make_trade(4_000, "0.65030", "historical-4"),
        ],
        None,
        None,
        UnixNanos::from(5_000),
        Some(params),
    )));

    {
        let cache_ref = cache.borrow();
        let bars = cache_ref.bars(&bar_type).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].close, Price::from("0.65030"));
        assert_eq!(bars[1].close, Price::from("0.65010"));
    }
    assert!(saver.get_messages().is_empty());

    // The held live trade opens the next bar, which completes with the next live trade
    data_engine.process_data(Data::Trade(make_trade(6_000, "0.67010", "live-2")));

    let messages = saver.get_messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].open, Price::from("0.67000"));
    assert_eq!(messages[0].close, Price::from("0.67010"));
    assert_eq!(messages[0].ts_event, UnixNanos::from(6_000));
}

/// A kept quotes request naming a 5-MINUTE bar built from 1-MINUTE bars, with its source in either
/// order, is refused by name before dispatch: no client is asked, the requester gets the reason
/// in one terminal.
#[rstest]
#[case::composite_first(true)]
#[case::source_first(false)]
fn test_update_subscriptions_request_naming_a_composite_is_refused_by_name_before_dispatch(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] composite_first: bool,
) {
    const SEC: u64 = 1_000_000_000;
    // Aligned to a 5-minute boundary
    let boundary = 1_700_000_100 * SEC;

    let _ = stub_msgbus;
    let virtual_clock = Rc::new(RefCell::new(VirtualClock::new()));
    virtual_clock
        .borrow_mut()
        .set_time(UnixNanos::from(boundary + 10 * SEC));
    let clock: Rc<RefCell<dyn Clock>> = virtual_clock;
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let source = BarType::from(format!("{instrument_id}-1-MINUTE-MID-INTERNAL").as_str());
    let composite =
        BarType::from(format!("{instrument_id}-5-MINUTE-MID-INTERNAL@1-MINUTE-INTERNAL").as_str());
    let bar_types = if composite_first {
        vec![composite, source]
    } else {
        vec![source, composite]
    };

    let params: Params = serde_json::from_value(json!({
        "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "update_subscriptions": true,
    }))
    .unwrap();
    let request = RequestCommand::Quotes(RequestQuotes::new(
        instrument_id,
        Some(UnixNanos::from(boundary - 300 * SEC).to_datetime_utc()),
        Some(UnixNanos::from(boundary - 60 * SEC).to_datetime_utc()),
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::from(boundary + 10 * SEC),
        Some(params),
    ));

    let (result, failures) = execute_recording_failures(&mut data_engine, request);

    let error = result.unwrap_err().to_string();
    assert!(
        error.starts_with("Cannot keep the bar builders of an `update_subscriptions` request"),
        "{error}"
    );
    assert!(
        error.contains(&format!("{composite} is built from another bar")),
        "{error}"
    );
    assert_eq!(failures, vec![Some(error)]);
    assert!(recorder.borrow().is_empty(), "{:?}", recorder.borrow());
}

#[rstest]
fn test_request_bar_aggregation_rejects_running_update_subscription_aggregator(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str());
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Bars(
        SubscribeBars::new(
            bar_type,
            Some(client_id),
            Some(venue),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ),
    )));

    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": true,
    }))
    .unwrap();

    let request = RequestTrades::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(params),
    );

    let result = data_engine.execute_request(RequestCommand::Trades(request));

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already running"));
}

#[rstest]
fn test_request_bar_aggregation_rejects_external_bar_type(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-1-TICK-LAST-EXTERNAL").as_str());
    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
    }))
    .unwrap();

    let request = RequestTrades::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(params),
    );

    let result = data_engine.execute_request(RequestCommand::Trades(request));

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("must be internally aggregated")
    );
}

#[rstest]
fn test_request_bar_aggregation_cleans_up_after_dispatch_failure(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let bar_type = BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str());
    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": false,
    }))
    .unwrap();

    let request = RequestTrades::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params.clone()),
    );

    let result = data_engine.execute_request(RequestCommand::Trades(request.clone()));

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("no client found"));

    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    data_engine
        .execute_request(RequestCommand::Trades(request))
        .unwrap();

    let trade = TradeTick::new(
        instrument_id,
        Price::from("0.65000"),
        Quantity::from("1000"),
        AggressorSide::Buy,
        TradeId::new("historical-1"),
        UnixNanos::from(1_000),
        UnixNanos::from(1_000),
    );
    data_engine.response(DataResponse::Trades(TradesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![trade],
        None,
        None,
        UnixNanos::from(1_000),
        Some(params),
    )));

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(1_000)),
    );
}

#[rstest]
fn test_request_bar_aggregation_reset_clears_pending_aggregators(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let bar_type = BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str());

    let params = || -> Params {
        serde_json::from_value(json!({
            "bar_types": [bar_type.to_string()],
            "update_subscriptions": false,
        }))
        .unwrap()
    };

    let request = RequestTrades::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(params()),
    );
    data_engine
        .execute_request(RequestCommand::Trades(request))
        .unwrap();

    data_engine.reset();

    let request_id = UUID4::new();

    let request = RequestTrades::new(
        instrument_id,
        None,
        None,
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params()),
    );
    data_engine
        .execute_request(RequestCommand::Trades(request))
        .unwrap();

    let trade = TradeTick::new(
        instrument_id,
        Price::from("0.65000"),
        Quantity::from("1000"),
        AggressorSide::Buy,
        TradeId::new("historical-1"),
        UnixNanos::from(1_000),
        UnixNanos::from(1_000),
    );
    data_engine.response(DataResponse::Trades(TradesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![trade],
        None,
        None,
        UnixNanos::from(1_000),
        Some(params()),
    )));

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(1_000)),
    );
}

#[rstest]
fn test_execute_request_data(
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    data_engine: Rc<RefCell<DataEngine>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let req = RequestCustomData {
        client_id,
        data_type: DataType::new("X", None, None),
        start: None,
        end: None,
        limit: None,
        request_id: UUID4::new(),
        ts_init: UnixNanos::default(),
        params: None,
    };

    let cmd = DataCommand::Request(RequestCommand::Data(req));
    data_engine.execute(cmd.clone());

    assert_eq!(recorder.borrow()[0], cmd);
}

#[rstest]
fn test_execute_request_instrument(
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    data_engine: Rc<RefCell<DataEngine>>,
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let req = RequestInstrument::new(
        audusd_sim.id,
        None,
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    let cmd = DataCommand::Request(RequestCommand::Instrument(req));
    data_engine.execute(cmd.clone());

    assert_eq!(recorder.borrow()[0], cmd);
}

#[rstest]
fn test_execute_request_instruments(
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    data_engine: Rc<RefCell<DataEngine>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let req = RequestInstruments::new(
        None,
        None,
        Some(client_id),
        Some(venue),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    let cmd = DataCommand::Request(RequestCommand::Instruments(req));
    data_engine.execute(cmd.clone());

    assert_eq!(recorder.borrow()[0], cmd);
}

#[rstest]
fn test_execute_request_book_snapshot(
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    data_engine: Rc<RefCell<DataEngine>>,
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let req = RequestBookSnapshot::new(
        audusd_sim.id,
        None, // depth
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        None, // params
    );
    let cmd = DataCommand::Request(RequestCommand::BookSnapshot(req));
    data_engine.execute(cmd.clone());

    assert_eq!(recorder.borrow()[0], cmd);
}

#[rstest]
fn test_execute_request_quotes(
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    data_engine: Rc<RefCell<DataEngine>>,
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let req = RequestQuotes::new(
        audusd_sim.id,
        None, // start
        None, // end
        None, // limit
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        None, // params
    );
    let cmd = DataCommand::Request(RequestCommand::Quotes(req));
    data_engine.execute(cmd.clone());

    assert_eq!(recorder.borrow()[0], cmd);
}

#[rstest]
fn test_execute_request_trades(
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    data_engine: Rc<RefCell<DataEngine>>,
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let req = RequestTrades::new(
        audusd_sim.id,
        None, // start
        None, // end
        None, // limit
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        None, // params
    );
    let cmd = DataCommand::Request(RequestCommand::Trades(req));
    data_engine.execute(cmd.clone());

    assert_eq!(recorder.borrow()[0], cmd);
}

#[rstest]
fn test_execute_request_funding_rates(
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    data_engine: Rc<RefCell<DataEngine>>,
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let req = RequestFundingRates::new(
        audusd_sim.id,
        None, // start
        None, // end
        None, // limit
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        None, // params
    );
    let cmd = DataCommand::Request(RequestCommand::FundingRates(req));
    data_engine.execute(cmd.clone());

    assert_eq!(recorder.borrow()[0], cmd);
}

#[rstest]
fn test_execute_request_bars(
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    data_engine: Rc<RefCell<DataEngine>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let req = RequestBars::new(
        BarType::from("AUD/USD.SIM-1-MINUTE-LAST-INTERNAL"),
        None, // start
        None, // end
        None, // limit
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        None, // params
    );
    let cmd = DataCommand::Request(RequestCommand::Bars(req));
    data_engine.execute(cmd.clone());

    assert_eq!(recorder.borrow()[0], cmd);
}

#[rstest]
fn test_execute_request_order_book_depth(
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    data_engine: Rc<RefCell<DataEngine>>,
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let req = RequestBookDepth::new(
        audusd_sim.id,
        None,                                 // start
        None,                                 // end
        None,                                 // limit
        Some(NonZeroUsize::new(10).unwrap()), // depth
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        None, // params
    );
    let cmd = DataCommand::Request(RequestCommand::BookDepth(req));
    data_engine.execute(cmd.clone());

    assert_eq!(recorder.borrow()[0], cmd);
}

#[cfg(feature = "defi")]
#[rstest]
fn test_execute_defi_request_pool_snapshot(
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let instrument_id =
        InstrumentId::from("0x11b815efB8f581194ae79006d24E0d814B7697F6.Arbitrum:UniswapV3");

    let request = RequestPoolSnapshot::new(
        instrument_id,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    let cmd = DataCommand::DefiRequest(DefiRequestCommand::PoolSnapshot(request));
    data_engine.execute(cmd.clone());

    // Verify command was forwarded to the client
    assert_eq!(recorder.borrow().len(), 1);
    assert_eq!(recorder.borrow().as_slice(), std::slice::from_ref(&cmd));
}

#[cfg(feature = "defi")]
#[rstest]
fn test_setup_pool_updater_requests_snapshot(
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let instrument_id =
        InstrumentId::from("0x11b815efB8f581194ae79006d24E0d814B7697F6.Arbitrum:UniswapV3");

    let subscribe_pool = SubscribePool::new(
        instrument_id,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    let cmd = DataCommand::DefiSubscribe(DefiSubscribeCommand::Pool(subscribe_pool));
    data_engine.execute(cmd.clone());

    // Verify two commands were recorded:
    // 1. The SubscribePool command (forwarded to client first)
    // 2. The RequestPoolSnapshot command (automatically sent by setup_pool_updater after)
    let recorded = recorder.borrow();
    assert_eq!(
        recorded.len(),
        2,
        "Expected 2 commands (SubscribePool and RequestPoolSnapshot)"
    );

    // First command should be the SubscribePool (forwarded before snapshot request)
    assert_eq!(recorded[0], cmd);

    // Second command should be RequestPoolSnapshot
    match &recorded[1] {
        DataCommand::DefiRequest(DefiRequestCommand::PoolSnapshot(request)) => {
            assert_eq!(request.instrument_id, instrument_id);
            assert_eq!(request.client_id, Some(client_id));
        }
        _ => panic!(
            "Expected second command to be RequestPoolSnapshot, was: {:?}",
            recorded[1]
        ),
    }
}

#[cfg(feature = "defi")]
#[rstest]
fn test_pool_snapshot_request_routing_by_client_id(
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
) {
    let mut data_engine = data_engine.borrow_mut();

    // Register two clients
    let client_id_1 = ClientId::from("CLIENT1");
    let venue_1 = Venue::from("VENUE1");
    let recorder_1: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::clone(&clock),
        Rc::clone(&cache),
        client_id_1,
        venue_1,
        None,
        &recorder_1,
        &mut data_engine,
    );

    let client_id_2 = ClientId::from("CLIENT2");
    let venue_2 = Venue::from("VENUE2");
    let recorder_2: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        clock,
        cache,
        client_id_2,
        venue_2,
        None,
        &recorder_2,
        &mut data_engine,
    );

    let instrument_id =
        InstrumentId::from("0x11b815efB8f581194ae79006d24E0d814B7697F6.Arbitrum:UniswapV3");

    // Request snapshot with specific client_id
    let request = RequestPoolSnapshot::new(
        instrument_id,
        Some(client_id_1),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    let cmd = DataCommand::DefiRequest(DefiRequestCommand::PoolSnapshot(request));
    data_engine.execute(cmd.clone());

    // Verify request was routed to CLIENT1 only
    assert_eq!(recorder_1.borrow().len(), 1);
    assert_eq!(recorder_1.borrow()[0], cmd);
    assert_eq!(recorder_2.borrow().len(), 0);
}

#[rstest]
fn test_unsubscribe_book_snapshots_removes_only_requested_interval(
    audusd_sim: CurrencyPair,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let data_engine = create_snapshot_test_engine(Rc::clone(&clock), Rc::clone(&cache));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::clone(&clock),
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine.borrow_mut(),
    );

    let _ = cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim.clone()));

    let fast_interval_ms = NonZeroUsize::new(100).unwrap();
    let slow_interval_ms = NonZeroUsize::new(200).unwrap();
    let fast_topic = switchboard::get_book_snapshots_topic(audusd_sim.id, fast_interval_ms);
    let slow_topic = switchboard::get_book_snapshots_topic(audusd_sim.id, slow_interval_ms);
    let (fast_handler, fast_saver) = get_typed_message_saving_handler::<OrderBook>(None);
    let (slow_handler, slow_saver) = get_typed_message_saving_handler::<OrderBook>(None);
    msgbus::subscribe_book_snapshots(fast_topic.into(), fast_handler, None);
    msgbus::subscribe_book_snapshots(slow_topic.into(), slow_handler, None);

    execute_book_snapshot_subscribe(
        &data_engine,
        audusd_sim.id,
        client_id,
        venue,
        fast_interval_ms,
    );
    execute_book_snapshot_subscribe(
        &data_engine,
        audusd_sim.id,
        client_id,
        venue,
        slow_interval_ms,
    );
    process_book_delta(&data_engine, audusd_sim.id);
    advance_clock_and_dispatch(&clock, 500_000_000);

    let fast_count = fast_saver.get_messages().len();
    let slow_count = slow_saver.get_messages().len();

    execute_book_snapshot_unsubscribe(
        &data_engine,
        audusd_sim.id,
        client_id,
        venue,
        fast_interval_ms,
    );

    assert_eq!(recorder.borrow().len(), 1);

    process_book_delta(&data_engine, audusd_sim.id);
    advance_clock_and_dispatch(&clock, 500_000_000);

    assert_eq!(fast_saver.get_messages().len(), fast_count);
    assert!(slow_saver.get_messages().len() > slow_count);

    execute_book_snapshot_unsubscribe(
        &data_engine,
        audusd_sim.id,
        client_id,
        venue,
        slow_interval_ms,
    );

    let recorded = recorder.borrow();
    assert_eq!(recorded.len(), 2);
    assert!(matches!(
        &recorded[1],
        DataCommand::Unsubscribe(UnsubscribeCommand::BookDeltas(cmd)) if cmd.single_instrument_id().unwrap() == audusd_sim.id
    ));
    drop(recorded);

    let slow_count = slow_saver.get_messages().len();
    process_book_delta(&data_engine, audusd_sim.id);
    advance_clock_and_dispatch(&clock, 500_000_000);
    assert_eq!(slow_saver.get_messages().len(), slow_count);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_trades_with_bar_types_param_sets_up_aggregation_through_streaming_path(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);

    let _catalog_dir = register_trade_catalog_with_trades(
        &mut data_engine,
        "agg-trades",
        &[split_trade(instrument_id, 2_000, "agg-1")],
        Some((1_000, 2_000)),
    );

    let bar_type = BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str());
    let params: Params = serde_json::from_value(json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": false,
    }))
    .unwrap();

    let parent_id = UUID4::new();
    let req = RequestCommand::Trades(RequestTrades::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        Some(params),
    ));
    data_engine.execute_request(req).unwrap();

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(2_000)),
        "request-scoped aggregator must consume the catalog-sourced trade",
    );
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[derive(Debug, PartialEq)]
enum ResponsePart {
    Trades(UUID4, Vec<TradeId>),
    Bars(UUID4, BarType, Vec<UnixNanos>),
}

/// Registers a response handler for `request_id` that records every trades and bars part.
fn record_response_parts(request_id: UUID4) -> Rc<RefCell<Vec<ResponsePart>>> {
    let parts = Rc::new(RefCell::new(Vec::new()));
    let recorded = Rc::clone(&parts);
    let handler = TypedHandler::from(move |envelope: &DataResponseEnvelope| {
        if let Some(DataResponse::Trades(resp)) = envelope.response() {
            let trade_ids = resp.data.iter().map(|trade| trade.trade_id).collect();
            recorded
                .borrow_mut()
                .push(ResponsePart::Trades(resp.correlation_id, trade_ids));
        } else if let Some(DataResponse::Bars(resp)) = envelope.response() {
            let bar_ts = resp.data.iter().map(|bar| bar.ts_event).collect();
            recorded.borrow_mut().push(ResponsePart::Bars(
                resp.correlation_id,
                resp.bar_type,
                bar_ts,
            ));
        }
    });
    msgbus::register_response_handler(&request_id, handler);
    parts
}

/// Requests trades aggregated into a `RANGE_INCLUSIVE` and a `RANGE` bar type, answers with four
/// trades (the third completes a bar of each type) and returns the recorded response parts.
fn respond_to_range_aggregation_request(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
    interleave: Option<bool>,
) -> (Vec<ResponsePart>, UUID4, [BarType; 2], Rc<RefCell<Cache>>) {
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );

    let range_inclusive =
        BarType::from(format!("{instrument_id}-10-RANGE_INCLUSIVE-LAST-INTERNAL").as_str());
    let range = BarType::from(format!("{instrument_id}-10-RANGE-LAST-INTERNAL").as_str());
    let mut params = json!({
        "bar_types": [range_inclusive.to_string(), range.to_string()],
        "update_subscriptions": false,
    });
    if let Some(interleave) = interleave {
        params["interleave"] = json!(interleave);
    }
    let params: Params = serde_json::from_value(params).unwrap();

    let request_id = UUID4::new();
    let parts = record_response_parts(request_id);
    data_engine.execute(DataCommand::Request(RequestCommand::Trades(
        RequestTrades::new(
            instrument_id,
            None,
            None,
            None,
            Some(client_id),
            request_id,
            UnixNanos::default(),
            Some(params.clone()),
        ),
    )));

    let make_trade = |price: &str, ts: u64, trade_id: &str| {
        TradeTick::new(
            instrument_id,
            Price::from(price),
            Quantity::from("1000"),
            AggressorSide::Buy,
            TradeId::new(trade_id),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    };
    data_engine.response(DataResponse::Trades(TradesResponse::new(
        request_id,
        client_id,
        instrument_id,
        vec![
            make_trade("0.65000", 1_000, "t1"),
            make_trade("0.65009", 2_000, "t2"),
            make_trade("0.65011", 3_000, "t3"),
            make_trade("0.65012", 4_000, "t4"),
        ],
        None,
        None,
        UnixNanos::from(4_000),
        Some(params),
    )));

    let parts = parts.take();
    (parts, request_id, [range_inclusive, range], cache)
}

fn trade_ids(ids: &[&str]) -> Vec<TradeId> {
    ids.iter().map(|id| TradeId::new(*id)).collect()
}

#[rstest]
fn test_interleaved_trades_request_orders_parts_by_bar_convention(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;

    let (parts, request_id, [range_inclusive, range], cache) =
        respond_to_range_aggregation_request(audusd_sim, client_id, venue, Some(true));

    let completed = vec![UnixNanos::from(3_000)];
    assert_eq!(
        parts,
        vec![
            ResponsePart::Trades(request_id, trade_ids(&["t1", "t2"])),
            ResponsePart::Bars(request_id, range, completed.clone()),
            ResponsePart::Trades(request_id, trade_ids(&["t3"])),
            ResponsePart::Bars(request_id, range_inclusive, completed),
            ResponsePart::Trades(request_id, trade_ids(&["t4"])),
        ]
    );
    assert_eq!(
        cache.borrow().bar(&range_inclusive).map(|bar| bar.ts_event),
        Some(UnixNanos::from(3_000))
    );
    assert_eq!(
        cache.borrow().bar(&range).map(|bar| bar.ts_event),
        Some(UnixNanos::from(3_000))
    );
}

#[rstest]
#[case::absent(None)]
#[case::disabled(Some(false))]
fn test_trades_request_without_interleave_sends_one_response(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] interleave: Option<bool>,
) {
    let _ = stub_msgbus;

    let (parts, request_id, _, _) =
        respond_to_range_aggregation_request(audusd_sim, client_id, venue, interleave);

    assert_eq!(
        parts,
        vec![ResponsePart::Trades(
            request_id,
            trade_ids(&["t1", "t2", "t3", "t4"])
        )]
    );
}

/// What a subscriber is given, in order: a trade or a bar, each with the number of bars of every
/// watched bar type the cache holds at its callback.
#[derive(Clone, Debug, PartialEq)]
enum OrderedDelivery {
    Trade(TradeId, Vec<usize>),
    Bar(BarType, UnixNanos, Vec<usize>),
}

fn cached_bars(cache: &Rc<RefCell<Cache>>, watched: &[BarType]) -> Vec<usize> {
    watched
        .iter()
        .map(|bar_type| cache.borrow().bar_count(bar_type))
        .collect()
}

fn order_engine(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
) -> (DataEngine, Rc<RefCell<Cache>>) {
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);
    let test_clock: Rc<RefCell<VirtualClock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        test_clock,
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut data_engine,
    );
    (data_engine, cache)
}

/// The order tests' trades: (price, `ts_init`, size, trade ID), a second after the epoch (a
/// timer started at the epoch itself takes the clock's time as its start). With a 10-increment
/// range, the third and the fifth each complete a `RANGE_INCLUSIVE` bar; the third, of 3,000,
/// completes two 1,500-`VOLUME` bars; 1-ms time bars close before the third, the fifth and the
/// sixth.
const ORDER_TAPE: [(&str, u64, u64, &str); 6] = [
    ("0.65000", 1_000_450_000, 1_000, "t1"),
    ("0.65009", 1_000_900_000, 1_000, "t2"),
    ("0.65011", 1_001_350_000, 3_000, "t3"),
    ("0.65011", 1_001_800_000, 1_000, "t4"),
    ("0.65021", 1_002_250_000, 1_000, "t5"),
    ("0.65022", 1_004_700_000, 1_000, "t6"),
];

fn order_tape(instrument_id: InstrumentId) -> Vec<TradeTick> {
    ORDER_TAPE
        .into_iter()
        .map(|(price, ts, size, trade_id)| make_trade(instrument_id, price, size, trade_id, ts))
        .collect()
}

/// Subscribes `bar_types` live, then processes the order tape live, each trade after the engine
/// clock's timers due by its `ts_init` (as a node's event loop runs them), and returns what a
/// subscriber to its trades and to every bar type, at the default priority as an actor, is given.
fn live_deliveries(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
    bar_types: &[BarType],
) -> Vec<OrderedDelivery> {
    let instrument_id = audusd_sim.id;
    let (mut data_engine, cache) = order_engine(audusd_sim, client_id, venue);
    for bar_type in bar_types {
        data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Bars(
            SubscribeBars::new(
                *bar_type,
                Some(client_id),
                Some(venue),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ),
        )));
    }

    let delivered = Rc::new(RefCell::new(Vec::new()));
    let trade_delivered = Rc::clone(&delivered);
    let trade_cache = Rc::clone(&cache);
    let watched = bar_types.to_vec();
    let trade_watched = watched.clone();
    let trade_handler = TypedHandler::from(move |trade: &TradeTick| {
        let cached = cached_bars(&trade_cache, &trade_watched);
        trade_delivered
            .borrow_mut()
            .push(OrderedDelivery::Trade(trade.trade_id, cached));
    });
    let trades_topic = switchboard::get_trades_topic(instrument_id);
    msgbus::subscribe_trades(trades_topic.into(), trade_handler, None);

    for bar_type in bar_types {
        let bar_delivered = Rc::clone(&delivered);
        let bar_cache = Rc::clone(&cache);
        let bar_watched = watched.clone();
        let bar_handler = TypedHandler::from(move |bar: &Bar| {
            let cached = cached_bars(&bar_cache, &bar_watched);
            bar_delivered.borrow_mut().push(OrderedDelivery::Bar(
                bar.bar_type,
                bar.ts_event,
                cached,
            ));
        });
        let bars_topic = switchboard::get_bars_topic(bar_type.standard());
        msgbus::subscribe_bars(bars_topic.into(), bar_handler, None);
    }

    for trade in order_tape(instrument_id) {
        let clock = Rc::clone(data_engine.clock());
        let handlers = {
            let mut clock = clock.borrow_mut();
            let clock = clock.as_any_mut().downcast_mut::<VirtualClock>().unwrap();
            let events = clock.advance_time(trade.ts_init, true);
            clock.match_handlers(events)
        };
        for handler in handlers {
            handler.callback.call(handler.event);
        }
        data_engine.process_data(Data::Trade(trade));
    }

    delivered.take()
}

/// How the history engine answers an interleaving request with the order tape.
#[derive(Clone, Copy, Debug)]
enum HistoryAnswer {
    /// The client answers it directly, in one envelope.
    Client,
    /// A catalog holding the tape answers it, as the one leg of its date-range plan.
    #[cfg(feature = "streaming")]
    Catalog,
}

/// Every way of [`HistoryAnswer`] this build serves.
fn history_answers() -> Vec<HistoryAnswer> {
    #[cfg(feature = "streaming")]
    let answers = vec![HistoryAnswer::Client, HistoryAnswer::Catalog];
    #[cfg(not(feature = "streaming"))]
    let answers = vec![HistoryAnswer::Client];
    answers
}

/// Answers an interleaving trades request for `bar_types` with the order tape, the way `answer`
/// says, and returns what its requester is given, part by part.
fn interleaved_deliveries(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
    bar_types: &[BarType],
    answer: HistoryAnswer,
) -> Vec<OrderedDelivery> {
    let instrument_id = audusd_sim.id;
    let (mut data_engine, cache) = order_engine(audusd_sim, client_id, venue);
    let params: Params = serde_json::from_value(json!({
        "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "update_subscriptions": false,
        "interleave": true,
    }))
    .unwrap();

    let request_id = UUID4::new();
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let recorded = Rc::clone(&delivered);
    let watched = bar_types.to_vec();
    let handler = TypedHandler::from(move |envelope: &DataResponseEnvelope| {
        if let Some(DataResponse::Trades(resp)) = envelope.response() {
            for trade in resp.data.iter() {
                let cached = cached_bars(&cache, &watched);
                recorded
                    .borrow_mut()
                    .push(OrderedDelivery::Trade(trade.trade_id, cached));
            }
        } else if let Some(DataResponse::Bars(resp)) = envelope.response() {
            for bar in resp.data.iter() {
                let cached = cached_bars(&cache, &watched);
                recorded.borrow_mut().push(OrderedDelivery::Bar(
                    bar.bar_type,
                    bar.ts_event,
                    cached,
                ));
            }
        }
    });
    msgbus::register_response_handler(&request_id, handler);

    let tape = order_tape(instrument_id);
    let (first, last) = (tape[0].ts_init, tape[tape.len() - 1].ts_init);
    #[cfg(not(feature = "streaming"))]
    let _ = first;
    let request = |start: Option<UnixNanos>, end: Option<UnixNanos>| {
        DataCommand::Request(RequestCommand::Trades(RequestTrades::new(
            instrument_id,
            start.as_ref().map(UnixNanos::to_datetime_utc),
            end.as_ref().map(UnixNanos::to_datetime_utc),
            None,
            Some(client_id),
            request_id,
            UnixNanos::default(),
            Some(params.clone()),
        )))
    };

    match answer {
        HistoryAnswer::Client => {
            data_engine.execute(request(None, None));
            data_engine.response(DataResponse::Trades(TradesResponse::new(
                request_id,
                client_id,
                instrument_id,
                tape,
                None,
                None,
                last,
                Some(params.clone()),
            )));
        }
        #[cfg(feature = "streaming")]
        HistoryAnswer::Catalog => {
            advance_clock_to(data_engine.clock(), 2 * NANOSECONDS_IN_SECOND);
            let _catalog_dir = register_trade_catalog_with_trades(
                &mut data_engine,
                "interleave-order",
                &tape,
                Some((first.as_u64(), last.as_u64())),
            );
            data_engine.execute(request(Some(first), Some(last)));
            assert_eq!(data_engine.request_pipeline_count(), 0);
        }
    }

    delivered.take()
}

fn delivered_trade(trade_id: &str, cached: &[usize]) -> OrderedDelivery {
    OrderedDelivery::Trade(TradeId::new(trade_id), cached.to_vec())
}

fn delivered_bar(bar_type: BarType, ts: u64, cached: &[usize]) -> OrderedDelivery {
    OrderedDelivery::Bar(bar_type.standard(), UnixNanos::from(ts), cached.to_vec())
}

#[rstest]
#[case::tick("2-TICK")]
#[case::volume("1500-VOLUME")]
#[case::value("1000-VALUE")]
#[case::tick_imbalance("2-TICK_IMBALANCE")]
#[case::tick_runs("2-TICK_RUNS")]
#[case::volume_imbalance("2000-VOLUME_IMBALANCE")]
#[case::volume_runs("2000-VOLUME_RUNS")]
#[case::value_imbalance("1000-VALUE_IMBALANCE")]
#[case::value_runs("1000-VALUE_RUNS")]
#[case::renko("5-RENKO")]
#[case::range("10-RANGE")]
#[case::range_inclusive("10-RANGE_INCLUSIVE")]
#[case::time("1-MILLISECOND")]
fn test_interleaved_bars_of_each_trade_fed_aggregation_reach_handlers_and_cache_as_live(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] spec: &str,
) {
    let _ = stub_msgbus;
    let bar_type = BarType::from(format!("{}-{spec}-LAST-INTERNAL", audusd_sim.id).as_str());

    let live = live_deliveries(audusd_sim.clone(), client_id, venue, &[bar_type]);

    assert!(
        live.iter()
            .any(|delivered| matches!(delivered, OrderedDelivery::Bar(..))),
        "the tape completes a {bar_type} bar: {live:?}"
    );
    for answer in history_answers() {
        let history =
            interleaved_deliveries(audusd_sim.clone(), client_id, venue, &[bar_type], answer);
        assert_eq!(history, live, "{answer:?}");
    }
}

#[rstest]
fn test_interleaved_trade_completing_two_bars_of_one_type_delivers_them_one_at_a_time_as_live(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let volume = BarType::from(format!("{}-1500-VOLUME-LAST-INTERNAL", audusd_sim.id).as_str());
    // t3, of 3,000, completes two bars: the first one's handler does not find the second cached
    let expected = vec![
        delivered_trade("t1", &[0]),
        delivered_bar(volume, 1_000_900_000, &[1]),
        delivered_trade("t2", &[1]),
        delivered_bar(volume, 1_001_350_000, &[2]),
        delivered_bar(volume, 1_001_350_000, &[3]),
        delivered_trade("t3", &[3]),
        delivered_bar(volume, 1_001_800_000, &[4]),
        delivered_trade("t4", &[4]),
        delivered_trade("t5", &[4]),
        delivered_bar(volume, 1_004_700_000, &[5]),
        delivered_trade("t6", &[5]),
    ];

    let live = live_deliveries(audusd_sim.clone(), client_id, venue, &[volume]);

    assert_eq!(live, expected);
    for answer in history_answers() {
        let history =
            interleaved_deliveries(audusd_sim.clone(), client_id, venue, &[volume], answer);
        assert_eq!(history, live, "{answer:?}");
    }
}

#[rstest]
fn test_interleaved_trade_handler_finds_in_the_cache_only_the_bars_live_has_cached(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let range_inclusive = BarType::from(format!("{id}-10-RANGE_INCLUSIVE-LAST-INTERNAL").as_str());
    let range = BarType::from(format!("{id}-10-RANGE-LAST-INTERNAL").as_str());
    let bar_types = [range_inclusive, range];
    // t3 completes a bar of each type: neither is in the cache for t1 and t2, and the
    // `RANGE_INCLUSIVE` one, which contains t3, follows it
    let expected = vec![
        delivered_trade("t1", &[0, 0]),
        delivered_trade("t2", &[0, 0]),
        delivered_bar(range, 1_001_350_000, &[0, 1]),
        delivered_trade("t3", &[0, 1]),
        delivered_bar(range_inclusive, 1_001_350_000, &[1, 1]),
        delivered_trade("t4", &[1, 1]),
        delivered_trade("t5", &[1, 1]),
        delivered_bar(range_inclusive, 1_002_250_000, &[2, 1]),
        delivered_bar(range, 1_004_700_000, &[2, 2]),
        delivered_trade("t6", &[2, 2]),
    ];

    let live = live_deliveries(audusd_sim.clone(), client_id, venue, &bar_types);

    assert_eq!(live, expected);
    for answer in history_answers() {
        let history =
            interleaved_deliveries(audusd_sim.clone(), client_id, venue, &bar_types, answer);
        assert_eq!(history, live, "{answer:?}");
    }
}

// Made-up clock times and trading-day opens (civil 18:00 New York) for the join's admission
const NY_2026_11_08_1800: u64 = 1_794_178_800_000_000_000;
const NY_2026_11_09_1700: u64 = 1_794_261_600_000_000_000;
const NY_2026_11_09_1800: u64 = 1_794_265_200_000_000_000;
const NY_2026_11_10_1730: u64 = 1_794_349_800_000_000_000;
const NY_2026_11_10_1800: u64 = 1_794_351_600_000_000_000;
const NY_2026_11_10_2000: u64 = 1_794_358_800_000_000_000;
// Around the end of daylight saving time, 2026-11-01 02:00 New York
const NY_2026_10_29_1800_EDT: u64 = 1_793_311_200_000_000_000;
const NY_2026_11_01_1700_EST: u64 = 1_793_570_400_000_000_000; // 2026-10-29 18:00 EDT + 72 h
const NY_2026_11_01_1800_EST: u64 = 1_793_574_000_000_000_000;
const NY_2026_11_01_2000_EST: u64 = 1_793_581_200_000_000_000;
const JOIN_DEADLINE_AFTER_NS: u64 = 120 * NANOSECONDS_IN_SECOND;
const CUT_WARMUP_REFUSED: &str = "Cannot admit a `cut_warmup` request";

struct JoinFixture {
    engine: Rc<RefCell<DataEngine>>,
    recorder: Rc<RefCell<Vec<DataCommand>>>,
    instrument_id: InstrumentId,
    bar_type: BarType,
    client_id: ClientId,
}

/// An engine whose clock reads `now_ns`, with a recording mock client.
fn join_fixture(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
    now_ns: u64,
) -> JoinFixture {
    let mut virtual_clock = VirtualClock::new();
    let _ = virtual_clock.advance_time(UnixNanos::from(now_ns), true);
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(virtual_clock));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));

    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();

    let engine = Rc::new(RefCell::new(DataEngine::new(
        clock,
        Rc::clone(&cache),
        None,
    )));
    DataEngine::register_msgbus_handlers(&engine);
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut engine.borrow_mut(),
    );

    JoinFixture {
        engine,
        recorder,
        instrument_id,
        bar_type: BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str()),
        client_id,
    }
}

/// The parameters of an admissible joined request at `now_ns`.
fn joined_params(bar_type: BarType, now_ns: u64, session_opens: &[u64]) -> Value {
    json!({
        "cut_warmup": true,
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": true,
        "deadline": now_ns + JOIN_DEADLINE_AFTER_NS,
        "schema": "tbbo",
        "session_opens": session_opens,
    })
}

/// A trades request with `params`. A joined request (`session_opens`) starts at its first session
/// open: its builders are kept by request, which needs a `start`.
fn trades_request(
    instrument_ids: Vec<InstrumentId>,
    end: Option<u64>,
    client_id: ClientId,
    params: Value,
) -> RequestCommand {
    let start = params["session_opens"][0]
        .as_u64()
        .map(|start| UnixNanos::from(start).to_datetime_utc());
    RequestCommand::Trades(RequestTrades::new(
        instrument_ids,
        start,
        end.map(|end| UnixNanos::from(end).to_datetime_utc()),
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(serde_json::from_value(params).unwrap()),
    ))
}

/// `request`, a trades request, with `start` (UNIX nanoseconds): one whose builders are kept by
/// request (`update_subscriptions`) needs one.
fn starting_at(request: RequestCommand, start: u64) -> RequestCommand {
    let RequestCommand::Trades(mut cmd) = request else {
        panic!("a trades request: {request:?}");
    };
    cmd.start = Some(UnixNanos::from(start).to_datetime_utc());
    RequestCommand::Trades(cmd)
}

/// Executes `request`; returns its error and the failure reasons of the terminal envelopes the
/// requester received.
fn execute_recording_failures(
    engine: &mut DataEngine,
    request: RequestCommand,
) -> (anyhow::Result<()>, Vec<Option<String>>) {
    let failures = Rc::new(RefCell::new(Vec::new()));
    let recorded = Rc::clone(&failures);
    msgbus::register_response_handler(
        request.request_id(),
        TypedHandler::from(move |envelope: &DataResponseEnvelope| {
            recorded
                .borrow_mut()
                .push(envelope.failure_reason().map(str::to_owned));
        }),
    );
    let result = engine.execute_request(request);
    (result, failures.take())
}

/// Asserts `request` is refused on the command with a reason containing `reason`, the requester
/// receives that reason in one terminal, and no client is asked anything.
fn assert_cut_warmup_refused(fixture: &JoinFixture, request: RequestCommand, reason: &str) {
    let (result, failures) = execute_recording_failures(&mut fixture.engine.borrow_mut(), request);
    let error = result
        .expect_err("a `cut_warmup` request failing a condition must be refused")
        .to_string();
    assert!(error.starts_with(CUT_WARMUP_REFUSED), "{error}");
    assert!(error.contains(reason), "{error}");
    assert_eq!(failures, vec![Some(error)]);
    assert!(
        fixture.recorder.borrow().is_empty(),
        "a refused request reaches no client: {:?}",
        fixture.recorder.borrow()
    );
}

/// Asserts `request` was admitted and its plan dispatched: the client received exactly one
/// command, the replay tail, open-ended from the current trading day's open.
#[cfg(feature = "streaming")]
fn assert_reaches_join_dispatcher(fixture: &JoinFixture, request: RequestCommand) {
    let current_open = joined_current_open(&request);
    let (result, failures) = execute_recording_failures(&mut fixture.engine.borrow_mut(), request);
    result.unwrap();
    assert!(failures.is_empty(), "{failures:?}");
    assert_replay_tail(fixture, current_open);
}

/// Asserts `request` was admitted and reached the join's dispatcher, which refuses it without
/// the `streaming` feature; no client was asked anything.
#[cfg(not(feature = "streaming"))]
fn assert_reaches_join_dispatcher(fixture: &JoinFixture, request: RequestCommand) {
    let (result, failures) = execute_recording_failures(&mut fixture.engine.borrow_mut(), request);
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("the join's plan needs the `streaming` feature"),
        "{error}"
    );
    assert_eq!(failures, vec![Some(error)]);
    assert!(fixture.recorder.borrow().is_empty());
}

/// The last `session_opens` of a joined `request`: the current trading day's open, `H`.
#[cfg(feature = "streaming")]
fn joined_current_open(request: &RequestCommand) -> u64 {
    let RequestCommand::Trades(cmd) = request else {
        panic!("a joined request is a trades request");
    };
    cmd.params.as_ref().unwrap()["session_opens"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .as_u64()
        .unwrap()
}

/// Asserts the client received exactly one command, the join's replay tail: a trades request
/// from `current_open` with no end and `replay_start = current_open`. Returns its request ID.
#[cfg(feature = "streaming")]
fn assert_replay_tail(fixture: &JoinFixture, current_open: u64) -> UUID4 {
    let recorded = fixture.recorder.borrow();
    let [DataCommand::Request(RequestCommand::Trades(tail))] = &recorded[..] else {
        panic!("the client receives the replay tail alone: {recorded:?}");
    };
    assert_eq!(tail.instrument_ids, vec![fixture.instrument_id]);
    assert_eq!(
        tail.start,
        Some(UnixNanos::from(current_open).to_datetime_utc())
    );
    assert_eq!(tail.end, None);
    let params = tail.params.as_ref().unwrap();
    assert_eq!(params.get_u64("replay_start"), Some(current_open));
    assert_eq!(params.get_str("schema"), Some("tbbo"));
    tail.request_id
}

#[rstest]
#[case::after_the_open(NY_2026_11_10_2000, [NY_2026_11_09_1800, NY_2026_11_10_1800])]
#[case::before_the_open(NY_2026_11_10_1730, [NY_2026_11_08_1800, NY_2026_11_09_1800])]
#[case::across_daylight_saving_end(
    NY_2026_11_01_2000_EST,
    [NY_2026_10_29_1800_EDT, NY_2026_11_01_1800_EST]
)]
fn test_admitted_cut_warmup_request_reaches_the_join_dispatcher(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] now_ns: u64,
    #[case] session_opens: [u64; 2],
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, now_ns);
    let request = trades_request(
        vec![fixture.instrument_id],
        None,
        client_id,
        joined_params(fixture.bar_type, now_ns, &session_opens),
    );

    assert_reaches_join_dispatcher(&fixture, request);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_admitted_cut_warmup_request_reaches_the_join_dispatcher_with_a_catalog(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let _catalog_dir = register_trade_catalog_with_trades(
        &mut fixture.engine.borrow_mut(),
        "join-admission",
        &[split_trade(fixture.instrument_id, 2_000, "catalog-1")],
        Some((1_000, 2_000)),
    );
    let request = trades_request(
        vec![fixture.instrument_id],
        None,
        client_id,
        joined_params(
            fixture.bar_type,
            NY_2026_11_10_2000,
            &[NY_2026_11_09_1800, NY_2026_11_10_1800],
        ),
    );

    // The schema bypass of the catalog path would send this request to the client
    assert_reaches_join_dispatcher(&fixture, request);
}

#[rstest]
fn test_cut_warmup_refused_for_a_request_other_than_trades(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let params = joined_params(
        fixture.bar_type,
        NY_2026_11_10_2000,
        &[NY_2026_11_09_1800, NY_2026_11_10_1800],
    );
    let request = RequestCommand::Quotes(RequestQuotes::new(
        fixture.instrument_id,
        None,
        None,
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(serde_json::from_value(params).unwrap()),
    ));

    assert_cut_warmup_refused(
        &fixture,
        request,
        "only a trades request can set `cut_warmup`",
    );
}

#[rstest]
fn test_cut_warmup_refused_for_a_batch(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let request = trades_request(
        vec![fixture.instrument_id, InstrumentId::from("EUR/USD.SIM")],
        None,
        client_id,
        joined_params(
            fixture.bar_type,
            NY_2026_11_10_2000,
            &[NY_2026_11_09_1800, NY_2026_11_10_1800],
        ),
    );

    assert_cut_warmup_refused(
        &fixture,
        request,
        "it must name exactly one instrument (batches are refused), received 2",
    );
}

#[rstest]
#[case::absent(None, "it must ask for `bar_types`")]
#[case::empty(Some(&[][..]), "it must ask for `bar_types`")]
#[case::external(
    Some(&["1-TICK-LAST-EXTERNAL"][..]),
    "is not an internal, non-composite LAST bar type"
)]
#[case::composite(
    Some(&["1-TICK-LAST-INTERNAL@1-TICK-EXTERNAL"][..]),
    "is not an internal, non-composite LAST bar type"
)]
#[case::bid(Some(&["1-TICK-BID-INTERNAL"][..]), "is not an internal, non-composite LAST bar type")]
fn test_cut_warmup_refused_without_internal_last_bar_types(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] bar_type_specs: Option<&[&str]>,
    #[case] reason: &str,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let mut params = joined_params(
        fixture.bar_type,
        NY_2026_11_10_2000,
        &[NY_2026_11_09_1800, NY_2026_11_10_1800],
    );
    let instrument_id = fixture.instrument_id;
    match bar_type_specs {
        None => {
            params.as_object_mut().unwrap().remove("bar_types");
        }
        Some(specs) => {
            params["bar_types"] = json!(
                specs
                    .iter()
                    .map(|spec| format!("{instrument_id}-{spec}"))
                    .collect::<Vec<_>>()
            );
        }
    }
    let request = trades_request(vec![instrument_id], None, client_id, params);

    assert_cut_warmup_refused(&fixture, request, reason);
}

#[rstest]
#[case::absent(None)]
#[case::disabled(Some(false))]
fn test_cut_warmup_refused_without_update_subscriptions(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] update_subscriptions: Option<bool>,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let mut params = joined_params(
        fixture.bar_type,
        NY_2026_11_10_2000,
        &[NY_2026_11_09_1800, NY_2026_11_10_1800],
    );
    match update_subscriptions {
        None => {
            params
                .as_object_mut()
                .unwrap()
                .remove("update_subscriptions");
        }
        Some(value) => params["update_subscriptions"] = json!(value),
    }
    let request = trades_request(vec![fixture.instrument_id], None, client_id, params);

    assert_cut_warmup_refused(&fixture, request, "it must set `update_subscriptions`");
}

#[rstest]
#[case::absent(None, "it must give a `deadline` in UNIX nanoseconds")]
#[case::at_the_clock(Some(NY_2026_11_10_2000), "is not after the clock's time")]
fn test_cut_warmup_refused_without_a_valid_deadline(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] deadline: Option<u64>,
    #[case] reason: &str,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let mut params = joined_params(
        fixture.bar_type,
        NY_2026_11_10_2000,
        &[NY_2026_11_09_1800, NY_2026_11_10_1800],
    );
    match deadline {
        None => {
            params.as_object_mut().unwrap().remove("deadline");
        }
        Some(value) => params["deadline"] = json!(value),
    }
    let request = trades_request(vec![fixture.instrument_id], None, client_id, params);

    assert_cut_warmup_refused(&fixture, request, reason);
}

#[rstest]
fn test_cut_warmup_refused_with_an_end(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let request = trades_request(
        vec![fixture.instrument_id],
        Some(NY_2026_11_10_2000),
        client_id,
        joined_params(
            fixture.bar_type,
            NY_2026_11_10_2000,
            &[NY_2026_11_09_1800, NY_2026_11_10_1800],
        ),
    );

    assert_cut_warmup_refused(&fixture, request, "it must be open-ended (no end)");
}

#[rstest]
#[case::absent(None)]
#[case::trades(Some("trades"))]
fn test_cut_warmup_refused_without_the_tbbo_schema(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] schema: Option<&str>,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let mut params = joined_params(
        fixture.bar_type,
        NY_2026_11_10_2000,
        &[NY_2026_11_09_1800, NY_2026_11_10_1800],
    );
    match schema {
        None => {
            params.as_object_mut().unwrap().remove("schema");
        }
        Some(value) => params["schema"] = json!(value),
    }
    let request = trades_request(vec![fixture.instrument_id], None, client_id, params);

    assert_cut_warmup_refused(&fixture, request, "it must set `schema` to `tbbo`");
}

#[rstest]
#[case::absent(
    NY_2026_11_10_2000,
    None,
    "`session_opens` must be an array of UNIX nanoseconds"
)]
#[case::empty(
    NY_2026_11_10_2000,
    Some(&[][..]),
    "`session_opens` must name the current trading day's open"
)]
#[case::repeated(
    NY_2026_11_10_2000,
    Some(&[NY_2026_11_10_1800, NY_2026_11_10_1800][..]),
    "`session_opens` must be strictly increasing"
)]
#[case::decreasing(
    NY_2026_11_10_2000,
    Some(&[NY_2026_11_10_1800, NY_2026_11_09_1800][..]),
    "`session_opens` must be strictly increasing"
)]
#[case::not_at_eighteen(
    NY_2026_11_10_2000,
    Some(&[NY_2026_11_09_1700, NY_2026_11_10_1800][..]),
    "not at 18:00 America/New_York"
)]
#[case::added_hours_across_daylight_saving_end(
    NY_2026_11_01_2000_EST,
    Some(&[NY_2026_10_29_1800_EDT, NY_2026_11_01_1700_EST][..]),
    "not at 18:00 America/New_York"
)]
#[case::previous_day_last(
    NY_2026_11_10_2000,
    Some(&[NY_2026_11_08_1800, NY_2026_11_09_1800][..]),
    "is not the current trading day's open"
)]
#[case::open_still_ahead(
    NY_2026_11_10_1730,
    Some(&[NY_2026_11_09_1800, NY_2026_11_10_1800][..]),
    "is not the current trading day's open"
)]
fn test_cut_warmup_refused_without_verified_session_opens(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] now_ns: u64,
    #[case] session_opens: Option<&[u64]>,
    #[case] reason: &str,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, now_ns);
    let mut params = joined_params(fixture.bar_type, now_ns, session_opens.unwrap_or_default());
    if session_opens.is_none() {
        params.as_object_mut().unwrap().remove("session_opens");
    }
    let request = trades_request(vec![fixture.instrument_id], None, client_id, params);

    assert_cut_warmup_refused(&fixture, request, reason);
}

#[rstest]
fn test_cut_warmup_refused_when_not_a_boolean(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let mut params = joined_params(
        fixture.bar_type,
        NY_2026_11_10_2000,
        &[NY_2026_11_09_1800, NY_2026_11_10_1800],
    );
    params["cut_warmup"] = json!("true");
    let request = trades_request(vec![fixture.instrument_id], None, client_id, params);

    assert_cut_warmup_refused(&fixture, request, "`cut_warmup` must be a boolean");
}

#[rstest]
#[case::open_end(true, "it must be open-ended (no end)")]
#[case::no_bar_types_given(false, "it must ask for `bar_types`")]
fn test_cut_warmup_refused_before_request_aggregators(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] with_end: bool,
    #[case] reason: &str,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    fixture
        .engine
        .borrow_mut()
        .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
            SubscribeBars::new(
                fixture.bar_type,
                Some(client_id),
                Some(venue),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ),
        )));
    fixture.recorder.borrow_mut().clear();

    let mut params = joined_params(
        fixture.bar_type,
        NY_2026_11_10_2000,
        &[NY_2026_11_09_1800, NY_2026_11_10_1800],
    );
    if !with_end {
        params.as_object_mut().unwrap().remove("bar_types");
    }
    let request = trades_request(
        vec![fixture.instrument_id],
        with_end.then_some(NY_2026_11_10_2000),
        client_id,
        params,
    );

    // Preparing the request aggregators first would fail on the running live aggregator, or,
    // without `bar_types`, take the ordinary path to the client
    assert_cut_warmup_refused(&fixture, request, reason);
}

#[rstest]
fn test_trades_request_with_cut_warmup_false_takes_the_ordinary_path(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let request = trades_request(
        vec![fixture.instrument_id],
        None,
        client_id,
        json!({ "cut_warmup": false, "bar_types": [fixture.bar_type.to_string()] }),
    );

    fixture
        .engine
        .borrow_mut()
        .execute_request(request.clone())
        .unwrap();

    assert_eq!(
        fixture.recorder.borrow().last(),
        Some(&DataCommand::Request(request))
    );
}

// More made-up trading-day opens and times for the join's plan
#[cfg(feature = "streaming")]
const NY_2026_11_11_1800: u64 = 1_794_438_000_000_000_000;
#[cfg(feature = "streaming")]
const NY_2026_11_12_1800: u64 = 1_794_524_400_000_000_000;
const NY_2026_11_12_2000: u64 = 1_794_531_600_000_000_000;
#[cfg(feature = "streaming")]
const NY_2026_10_30_1800_EDT: u64 = 1_793_397_600_000_000_000;
#[cfg(feature = "streaming")]
const NY_2026_10_31_1730_EDT: u64 = 1_793_482_200_000_000_000;
#[cfg(feature = "streaming")]
const NY_2026_10_31_1800_EDT: u64 = 1_793_484_000_000_000_000;
#[cfg(feature = "streaming")]
const NY_2026_11_01_1730_EST: u64 = 1_793_572_200_000_000_000;
#[cfg(feature = "streaming")]
const ONE_HOUR_NS: u64 = 3_600 * NANOSECONDS_IN_SECOND;

/// One answer envelope the requester of a join received.
#[cfg(feature = "streaming")]
#[derive(Debug)]
struct JoinAnswer {
    trade_ids: Vec<TradeId>,
    end: Option<UnixNanos>,
    failure: Option<String>,
}

#[cfg(feature = "streaming")]
fn record_join_answers(request_id: UUID4) -> Rc<RefCell<Vec<JoinAnswer>>> {
    let answers = Rc::new(RefCell::new(Vec::new()));
    let recorded = Rc::clone(&answers);
    msgbus::register_response_handler(
        &request_id,
        TypedHandler::from(move |envelope: &DataResponseEnvelope| {
            let (trade_ids, end) = match envelope.response() {
                Some(DataResponse::Trades(resp)) => (
                    resp.data.iter().map(|trade| trade.trade_id).collect(),
                    resp.end,
                ),
                _ => (Vec::new(), None),
            };
            recorded.borrow_mut().push(JoinAnswer {
                trade_ids,
                end,
                failure: envelope.failure_reason().map(str::to_owned),
            });
        }),
    );
    answers
}

/// Registers a made-up trades catalog holding `rows` (`ts_init`, trade ID).
#[cfg(feature = "streaming")]
fn register_join_catalog(
    fixture: &JoinFixture,
    label: &str,
    rows: &[(u64, &str)],
) -> CatalogTempDir {
    let mut trades = rows
        .iter()
        .map(|(ts, id)| split_trade(fixture.instrument_id, *ts, id))
        .collect::<Vec<_>>();
    trades.sort_by_key(|trade| trade.ts_init);
    let first = trades.first().unwrap().ts_init.as_u64();
    let last = trades.last().unwrap().ts_init.as_u64();
    register_trade_catalog_with_trades(
        &mut fixture.engine.borrow_mut(),
        label,
        &trades,
        Some((first, last)),
    )
}

/// Executes a joined request over `session_opens` at `now_ns` with the parent's `start`, checks
/// its replay tail, answers the tail with `tail` in one part and then completes it, and returns
/// every answer the requester received. Without a `start` the request starts at its first session
/// open: its builders are kept by request, which needs one.
#[cfg(feature = "streaming")]
fn run_join(
    fixture: &JoinFixture,
    now_ns: u64,
    session_opens: &[u64],
    start: Option<u64>,
    tail: &[(u64, &str)],
) -> Vec<JoinAnswer> {
    let params = joined_params(fixture.bar_type, now_ns, session_opens);
    let request_id = UUID4::new();
    let request = RequestCommand::Trades(RequestTrades::new(
        vec![fixture.instrument_id],
        start
            .or(session_opens.first().copied())
            .map(|start| UnixNanos::from(start).to_datetime_utc()),
        None,
        None,
        Some(fixture.client_id),
        request_id,
        UnixNanos::default(),
        Some(serde_json::from_value(params).unwrap()),
    ));
    let answers = record_join_answers(request_id);
    let current_open = *session_opens.last().unwrap();

    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();
    let tail_id = assert_replay_tail(fixture, current_open);
    assert!(
        answers.borrow().is_empty(),
        "the join answers once its replay tail completes: {:?}",
        answers.borrow()
    );

    let part = |trades: Vec<TradeTick>, completed: bool| {
        DataResponseEnvelope::new(
            DataResponse::Trades(TradesResponse::new(
                tail_id,
                fixture.client_id,
                fixture.instrument_id,
                trades,
                Some(UnixNanos::from(current_open)),
                None,
                UnixNanos::from(now_ns),
                None,
            )),
            completed,
            None,
        )
    };
    let tail = tail
        .iter()
        .map(|(ts, id)| split_trade(fixture.instrument_id, *ts, id))
        .collect();
    fixture
        .engine
        .borrow_mut()
        .response_envelope(part(tail, false));
    assert!(answers.borrow().is_empty(), "{:?}", answers.borrow());
    fixture
        .engine
        .borrow_mut()
        .response_envelope(part(Vec::new(), true));

    answers.take()
}

#[cfg(feature = "streaming")]
fn assert_one_answer(answers: &[JoinAnswer], expected: &[&str], failure: Option<&[&str]>) {
    let [answer] = answers else {
        panic!("the join answers in one envelope: {answers:?}");
    };
    assert_eq!(answer.trade_ids, trade_ids(expected));
    assert_eq!(answer.end, None, "the rebuilt answer keeps its open end");
    match (failure, &answer.failure) {
        (None, None) => {}
        (Some(parts), Some(reason)) => {
            for part in parts {
                assert!(reason.contains(part), "{reason:?} lacks {part:?}");
            }
        }
        (expected, actual) => panic!("reason {actual:?}, expected {expected:?}"),
    }
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_join_answers_catalog_rows_then_the_replay_tail(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let _catalog_dir = register_join_catalog(
        &fixture,
        "join-catalog-then-tail",
        &[
            (NY_2026_11_09_1800 - 1, "before-the-depth"),
            (NY_2026_11_09_1800, "c1"),
            (NY_2026_11_11_1800 + ONE_HOUR_NS, "c2"),
            (NY_2026_11_12_1800 - 1, "c3"),
        ],
    );

    let answers = run_join(
        &fixture,
        NY_2026_11_12_2000,
        &[
            NY_2026_11_09_1800,
            NY_2026_11_10_1800,
            NY_2026_11_11_1800,
            NY_2026_11_12_1800,
        ],
        None,
        // Stamped after the request was dispatched
        &[(NY_2026_11_12_2000 + NANOSECONDS_IN_SECOND, "t1")],
    );

    assert_one_answer(&answers, &["c1", "c2", "c3", "t1"], None);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_join_keeps_current_day_catalog_rows_outside_the_catalog_leg(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let _catalog_dir = register_join_catalog(
        &fixture,
        "join-current-day",
        &[
            (NY_2026_11_11_1800 + ONE_HOUR_NS, "c1"),
            (NY_2026_11_12_1800, "catalog-at-the-open"),
            (NY_2026_11_12_1800 + ONE_HOUR_NS, "catalog-today"),
        ],
    );

    // The tail runs although the catalog holds some of the current day
    let answers = run_join(
        &fixture,
        NY_2026_11_12_2000,
        &[NY_2026_11_11_1800, NY_2026_11_12_1800],
        None,
        &[
            (NY_2026_11_12_1800, "t-open"),
            (NY_2026_11_12_1800 + ONE_HOUR_NS, "t-today"),
        ],
    );

    assert_one_answer(&answers, &["c1", "t-open", "t-today"], None);
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::leading_seam(
    vec![NY_2026_11_09_1800, NY_2026_11_10_1800, NY_2026_11_11_1800],
    vec!["d09-open", "d09-last", "d10-open", "d10-last", "d11-open", "d11-last"],
    None
)]
#[case::interior_missing_day(
    vec![NY_2026_11_08_1800, NY_2026_11_09_1800, NY_2026_11_11_1800],
    vec!["d08-open", "d08-last", "d09-open", "d09-last", "d11-open", "d11-last"],
    Some("1 missing sessions (`missing_sessions`), the trading days opening 2026-11-10T18:00-05:00")
)]
#[case::trailing_missing_day(
    vec![NY_2026_11_08_1800, NY_2026_11_09_1800, NY_2026_11_10_1800],
    vec!["d08-open", "d08-last", "d09-open", "d09-last", "d10-open", "d10-last"],
    Some("1 missing sessions (`missing_sessions`), the trading days opening 2026-11-11T18:00-05:00")
)]
fn test_join_seams_serve_listed_days_and_name_missing_ones(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] past_opens: Vec<u64>,
    #[case] served: Vec<&str>,
    #[case] missing: Option<&str>,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    // Each day's first and last nanosecond, as a restamped session's rows can reach them
    let _catalog_dir = register_join_catalog(
        &fixture,
        "join-seams",
        &[
            (NY_2026_11_08_1800 - 1, "before-the-depth"),
            (NY_2026_11_08_1800, "d08-open"),
            (NY_2026_11_09_1800 - 1, "d08-last"),
            (NY_2026_11_09_1800, "d09-open"),
            (NY_2026_11_10_1800 - 1, "d09-last"),
            (NY_2026_11_10_1800, "d10-open"),
            (NY_2026_11_11_1800 - 1, "d10-last"),
            (NY_2026_11_11_1800, "d11-open"),
            (NY_2026_11_12_1800 - 1, "d11-last"),
        ],
    );
    let mut session_opens = past_opens;
    session_opens.push(NY_2026_11_12_1800);

    let answers = run_join(
        &fixture,
        NY_2026_11_12_2000,
        &session_opens,
        None,
        &[(NY_2026_11_12_1800 + 1, "t1")],
    );

    let mut expected = served;
    expected.push("t1");
    assert_one_answer(
        &answers,
        &expected,
        missing.map(|reason| vec![reason]).as_deref(),
    );
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::leading_seam(
    vec![NY_2026_11_09_1800, NY_2026_11_10_1800, NY_2026_11_11_1800],
    vec!["d09-open", "d09-last", "d10-open", "d10-last", "d11-open", "d11-last"],
    None
)]
#[case::interior_missing_day(
    vec![NY_2026_11_08_1800, NY_2026_11_09_1800, NY_2026_11_11_1800],
    vec!["d08-open", "d08-last", "d09-open", "d09-last", "d11-open", "d11-last"],
    Some("1 missing sessions (`missing_sessions`), the trading days opening 2026-11-10T18:00-05:00")
)]
#[case::trailing_missing_day(
    vec![NY_2026_11_08_1800, NY_2026_11_09_1800, NY_2026_11_10_1800],
    vec!["d08-open", "d08-last", "d09-open", "d09-last", "d10-open", "d10-last"],
    Some("1 missing sessions (`missing_sessions`), the trading days opening 2026-11-11T18:00-05:00")
)]
fn test_join_seams_under_restamping_serve_each_trade_once(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] past_opens: Vec<u64>,
    #[case] served: Vec<&str>,
    #[case] missing: Option<&str>,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    // Restamped rows: each `ts_init` one second after its `ts_event`, within its trading day, a
    // day's first row at its open by exchange time and its last at the day's last nanosecond
    let shift = NANOSECONDS_IN_SECOND;
    let restamped = |ts_event: u64, id: &str| {
        let mut trade = split_trade(fixture.instrument_id, ts_event + shift, id);
        trade.ts_event = UnixNanos::from(ts_event);
        trade
    };
    let _catalog_dir = register_trade_catalog_with_trades(
        &mut fixture.engine.borrow_mut(),
        "join-seams-restamped",
        &[
            restamped(NY_2026_11_08_1800 - 1 - shift, "before-the-depth"),
            restamped(NY_2026_11_08_1800, "d08-open"),
            restamped(NY_2026_11_09_1800 - 1 - shift, "d08-last"),
            restamped(NY_2026_11_09_1800, "d09-open"),
            restamped(NY_2026_11_10_1800 - 1 - shift, "d09-last"),
            restamped(NY_2026_11_10_1800, "d10-open"),
            restamped(NY_2026_11_11_1800 - 1 - shift, "d10-last"),
            restamped(NY_2026_11_11_1800, "d11-open"),
            restamped(NY_2026_11_12_1800 - 1 - shift, "d11-last"),
        ],
        Some((NY_2026_11_08_1800 - 1, NY_2026_11_12_1800 - 1)),
    );
    let mut session_opens = past_opens;
    session_opens.push(NY_2026_11_12_1800);

    let answers = run_join(
        &fixture,
        NY_2026_11_12_2000,
        &session_opens,
        None,
        &[(NY_2026_11_12_1800 + 1, "t1")],
    );

    let mut expected = served;
    expected.push("t1");
    assert_one_answer(
        &answers,
        &expected,
        missing.map(|reason| vec![reason]).as_deref(),
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_join_without_a_catalog_requests_the_replay_tail_alone(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);

    let answers = run_join(
        &fixture,
        NY_2026_11_12_2000,
        &[NY_2026_11_10_1800, NY_2026_11_11_1800, NY_2026_11_12_1800],
        None,
        &[(NY_2026_11_12_1800 + 1, "t1")],
    );

    assert_one_answer(
        &answers,
        &["t1"],
        Some(&[
            "no catalog is registered, so only the current session's replay was requested",
            "2 missing sessions (`missing_sessions`), the trading days opening \
             2026-11-10T18:00-05:00, 2026-11-11T18:00-05:00",
        ]),
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_join_days_follow_civil_boundaries_across_daylight_saving_end(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_01_2000_EST);
    // The 2026-10-31 day lasts 25 hours: to 2026-11-01 18:00 EST, not 17:00 EST
    let _catalog_dir = register_join_catalog(
        &fixture,
        "join-daylight-saving",
        &[
            (NY_2026_10_30_1800_EDT, "d30-open"),
            (NY_2026_10_31_1730_EDT, "d30-late"),
            (NY_2026_10_31_1800_EDT, "d31-open"),
            (NY_2026_11_01_1730_EST, "d31-late"),
        ],
    );

    let answers = run_join(
        &fixture,
        NY_2026_11_01_2000_EST,
        &[NY_2026_10_30_1800_EDT, NY_2026_11_01_1800_EST],
        None,
        &[(NY_2026_11_01_1800_EST + 1, "t1")],
    );

    assert_one_answer(
        &answers,
        &["d30-open", "d30-late", "t1"],
        Some(&[
            "1 missing sessions (`missing_sessions`), the trading days opening \
             2026-10-31T18:00-04:00",
        ]),
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_join_answer_keeps_the_open_end_when_trimmed_to_its_start(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let _catalog_dir = register_join_catalog(
        &fixture,
        "join-open-end",
        &[(NY_2026_11_11_1800 + ONE_HOUR_NS, "c1")],
    );

    // With a start the rebuilt answer is trimmed: to its start only, never to the catalog's end
    let answers = run_join(
        &fixture,
        NY_2026_11_12_2000,
        &[NY_2026_11_11_1800, NY_2026_11_12_1800],
        Some(NY_2026_11_11_1800),
        &[(NY_2026_11_12_2000 + NANOSECONDS_IN_SECOND, "t1")],
    );

    assert_one_answer(&answers, &["c1", "t1"], None);
}

#[cfg(feature = "streaming")]
const MS_NS: u64 = 1_000_000;

/// A trade of the release tests, its stamps `ts_event` and `ts_init` as nanoseconds after the
/// current trading day's open (2026-11-12 18:00 New York); its price identifies it in a bar.
#[cfg(feature = "streaming")]
fn release_trade(
    instrument_id: InstrumentId,
    ts_event: u64,
    ts_init: u64,
    price: &str,
    trade_id: &str,
) -> TradeTick {
    TradeTick::new(
        instrument_id,
        Price::from(price),
        Quantity::from("1000"),
        AggressorSide::Buy,
        TradeId::new(trade_id),
        UnixNanos::from(NY_2026_11_12_1800 + ts_event),
        UnixNanos::from(NY_2026_11_12_1800 + ts_init),
    )
}

/// Executes a joined request for `bar_types` at 2026-11-12 20:00 New York, interleaved or not,
/// subscribes each bar type live (its builder holds the live trades while the answer is in
/// flight), publishes `held` as live trades, then answers the replay tail with `answered` and
/// completes it.
#[cfg(feature = "streaming")]
fn run_joined_release(
    fixture: &JoinFixture,
    bar_types: &[BarType],
    interleave: bool,
    held: &[TradeTick],
    answered: Vec<TradeTick>,
) {
    run_joined_release_observed(fixture, bar_types, interleave, held, answered, |_| {});
}

/// Runs [`run_joined_release`], giving `observe` the request's ID once it is executed.
#[cfg(feature = "streaming")]
fn run_joined_release_observed(
    fixture: &JoinFixture,
    bar_types: &[BarType],
    interleave: bool,
    held: &[TradeTick],
    answered: Vec<TradeTick>,
    observe: impl FnOnce(UUID4),
) {
    // With no catalog registered the join's answer carries a reason, so it fails and its kept
    // builders are not handed over; a registered catalog leaves the answer the replay tail's
    let catalog_dir = CatalogTempDir::new("joined-release");
    fixture.engine.borrow_mut().register_catalog(
        Box::new(ParquetDataCatalog::new(
            catalog_dir.path(),
            None,
            None,
            None,
            None,
        )),
        None,
    );
    let mut params = joined_params(fixture.bar_type, NY_2026_11_12_2000, &[NY_2026_11_12_1800]);
    params["bar_types"] = json!(
        bar_types
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    params["interleave"] = json!(interleave);
    let request = trades_request(vec![fixture.instrument_id], None, fixture.client_id, params);
    let request_id = *request.request_id();
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();
    observe(request_id);
    let tail_id = assert_replay_tail(fixture, NY_2026_11_12_1800);

    for bar_type in bar_types {
        fixture
            .engine
            .borrow_mut()
            .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
                SubscribeBars::new(
                    *bar_type,
                    Some(fixture.client_id),
                    None,
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                ),
            )));
    }

    for trade in held {
        fixture
            .engine
            .borrow_mut()
            .process_data(Data::Trade(*trade));
    }

    fixture
        .engine
        .borrow_mut()
        .response_envelope(DataResponseEnvelope::new(
            DataResponse::Trades(TradesResponse::new(
                tail_id,
                fixture.client_id,
                fixture.instrument_id,
                answered,
                Some(UnixNanos::from(NY_2026_11_12_1800)),
                None,
                UnixNanos::from(NY_2026_11_12_2000),
                None,
            )),
            true,
            None,
        ));
}

/// The bars of `bar_type` in the cache, oldest first, as (close, `ts_init`).
fn built_bars(fixture: &JoinFixture, bar_type: BarType) -> Vec<(Price, UnixNanos)> {
    let engine = fixture.engine.borrow();
    let cache = engine.cache().borrow();
    let mut bars: Vec<_> = cache
        .bars(&bar_type)
        .unwrap_or_default()
        .iter()
        .map(|bar| (bar.close, bar.ts_init))
        .collect();
    bars.reverse();
    bars
}

#[cfg(feature = "streaming")]
fn at(price: &str, ts_init: u64) -> (Price, UnixNanos) {
    (
        Price::from(price),
        UnixNanos::from(NY_2026_11_12_1800 + ts_init),
    )
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_release_counts_a_trade_in_both_the_answer_and_the_held_stream_once(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let bar_type = fixture.bar_type;

    run_joined_release(
        &fixture,
        &[bar_type],
        false,
        &[
            // The answer's second trade, received live later
            release_trade(id, 110 * MS_NS, 170 * MS_NS, "0.65010", "a2"),
            release_trade(id, 120 * MS_NS, 180 * MS_NS, "0.65020", "h3"),
        ],
        vec![
            release_trade(id, 100 * MS_NS, 150 * MS_NS, "0.65000", "a1"),
            release_trade(id, 110 * MS_NS, 160 * MS_NS, "0.65010", "a2"),
        ],
    );

    assert_eq!(
        built_bars(&fixture, bar_type),
        vec![
            at("0.65000", 150 * MS_NS),
            at("0.65010", 160 * MS_NS),
            at("0.65020", 180 * MS_NS),
        ]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_release_drops_live_trades_in_the_cut_millisecond(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let bar_type = fixture.bar_type;

    // The cut is the answer's millisecond, 100 ms, though its trade is at its first nanosecond
    run_joined_release(
        &fixture,
        &[bar_type],
        false,
        &[release_trade(
            id,
            100 * MS_NS + 500_000,
            170 * MS_NS,
            "0.65010",
            "held-late",
        )],
        vec![release_trade(id, 100 * MS_NS, 150 * MS_NS, "0.65000", "a1")],
    );
    // After the release: a late trade of the same millisecond, then one after it
    fixture
        .engine
        .borrow_mut()
        .process_data(Data::Trade(release_trade(
            id,
            100 * MS_NS + 999_999,
            175 * MS_NS,
            "0.65020",
            "live-late",
        )));
    fixture
        .engine
        .borrow_mut()
        .process_data(Data::Trade(release_trade(
            id,
            101 * MS_NS,
            180 * MS_NS,
            "0.65030",
            "after",
        )));

    assert_eq!(
        built_bars(&fixture, bar_type),
        vec![at("0.65000", 150 * MS_NS), at("0.65030", 180 * MS_NS)]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_release_builds_a_held_trade_below_the_answer_at_the_floor(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let bar_type = fixture.bar_type;

    // Answered (100 ms, 200), held (101 ms, 199): past the cut, below the builder's last input
    run_joined_release(
        &fixture,
        &[bar_type],
        false,
        &[release_trade(
            id,
            101 * MS_NS,
            199 * MS_NS,
            "0.65010",
            "held",
        )],
        vec![release_trade(id, 100 * MS_NS, 200 * MS_NS, "0.65000", "a1")],
    );

    assert_eq!(
        built_bars(&fixture, bar_type),
        vec![at("0.65000", 200 * MS_NS), at("0.65010", 200 * MS_NS)]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_interleaved_joined_release_cuts_and_floors_the_held_trades(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let bar_type = fixture.bar_type;

    run_joined_release(
        &fixture,
        &[bar_type],
        true,
        &[
            release_trade(id, 100 * MS_NS + 500_000, 210 * MS_NS, "0.65010", "same-ms"),
            release_trade(id, 101 * MS_NS, 199 * MS_NS, "0.65020", "below-the-floor"),
        ],
        vec![release_trade(id, 100 * MS_NS, 200 * MS_NS, "0.65000", "a1")],
    );

    assert_eq!(
        built_bars(&fixture, bar_type),
        vec![at("0.65000", 200 * MS_NS), at("0.65020", 200 * MS_NS)]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_release_floor_rolls_with_each_admitted_trade(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let bar_type = fixture.bar_type;

    run_joined_release(
        &fixture,
        &[bar_type],
        false,
        &[
            release_trade(id, 101 * MS_NS, 300 * MS_NS, "0.65010", "h1"),
            // Above the answer's floor, below the trade the builder took before it
            release_trade(id, 102 * MS_NS, 250 * MS_NS, "0.65020", "h2"),
        ],
        vec![release_trade(id, 100 * MS_NS, 200 * MS_NS, "0.65000", "a1")],
    );
    // After the release the floor keeps rolling
    fixture
        .engine
        .borrow_mut()
        .process_data(Data::Trade(release_trade(
            id,
            103 * MS_NS,
            260 * MS_NS,
            "0.65030",
            "live",
        )));

    assert_eq!(
        built_bars(&fixture, bar_type),
        vec![
            at("0.65000", 200 * MS_NS),
            at("0.65010", 300 * MS_NS),
            at("0.65020", 300 * MS_NS),
            at("0.65030", 300 * MS_NS),
        ]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_release_puts_a_floored_trade_on_a_time_bar_boundary_in_the_later_interval(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let bar_type = BarType::from(format!("{id}-1-SECOND-LAST-INTERNAL").as_str());
    let second = NANOSECONDS_IN_SECOND;

    run_joined_release(
        &fixture,
        &[bar_type],
        false,
        &[
            // Its own stamp is in the first second, the floor is past that second's close
            release_trade(id, 1_000 * MS_NS, second - 1_000, "0.65090", "h1"),
            release_trade(id, 2_500 * MS_NS, 2_500 * MS_NS, "0.65020", "h2"),
        ],
        vec![
            release_trade(id, 400 * MS_NS, 500 * MS_NS, "0.65000", "a1"),
            release_trade(id, 999 * MS_NS, second + 1_000, "0.65010", "a2"),
        ],
    );

    let engine = fixture.engine.borrow();
    let cache = engine.cache().borrow();
    let mut bars = cache.bars(&bar_type).unwrap_or_default();
    bars.reverse();
    let bars: Vec<_> = bars
        .iter()
        .map(|bar| (bar.ts_event, bar.high, bar.close))
        .collect();
    // Then h2's bar, and the hand-over drives the closes through now (20:00) minus 1 ns: flat
    // bars each second through 19:59:59
    assert_eq!(
        bars[..3],
        [
            (
                UnixNanos::from(NY_2026_11_12_1800 + second),
                Price::from("0.65000"),
                Price::from("0.65000"),
            ),
            (
                UnixNanos::from(NY_2026_11_12_1800 + 2 * second),
                Price::from("0.65090"),
                Price::from("0.65090"),
            ),
            (
                UnixNanos::from(NY_2026_11_12_1800 + 3 * second),
                Price::from("0.65020"),
                Price::from("0.65020"),
            ),
        ]
    );
    assert_eq!(bars.len(), 7_199);
    assert_eq!(
        bars.last(),
        Some(&(
            UnixNanos::from(NY_2026_11_12_2000 - second),
            Price::from("0.65020"),
            Price::from("0.65020"),
        ))
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_release_gives_each_bar_type_its_own_floor(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let tick = fixture.bar_type;
    let volume = BarType::from(format!("{id}-1000-VOLUME-LAST-INTERNAL").as_str());

    run_joined_release(
        &fixture,
        &[tick, volume],
        false,
        &[
            release_trade(id, 101 * MS_NS, 199 * MS_NS, "0.65010", "h1"),
            release_trade(id, 102 * MS_NS, 400 * MS_NS, "0.65020", "h2"),
        ],
        vec![release_trade(id, 100 * MS_NS, 200 * MS_NS, "0.65000", "a1")],
    );

    // Each builder starts from the answer's floor, not from the other builder's rolled one
    let expected = vec![
        at("0.65000", 200 * MS_NS),
        at("0.65010", 200 * MS_NS),
        at("0.65020", 400 * MS_NS),
    ];
    assert_eq!(built_bars(&fixture, tick), expected);
    assert_eq!(built_bars(&fixture, volume), expected);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_release_leaves_the_original_trades_to_an_ordinary_subscriber(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let bar_type = fixture.bar_type;
    let (handler, saver) = get_typed_message_saving_handler::<TradeTick>(None);
    msgbus::subscribe_trades(switchboard::get_trades_topic(id).into(), handler, None);
    let held = release_trade(id, 101 * MS_NS, 199 * MS_NS, "0.65010", "held");
    let live = release_trade(id, 102 * MS_NS, 198 * MS_NS, "0.65020", "live");

    run_joined_release(
        &fixture,
        &[bar_type],
        false,
        &[held],
        vec![release_trade(id, 100 * MS_NS, 200 * MS_NS, "0.65000", "a1")],
    );
    fixture.engine.borrow_mut().process_data(Data::Trade(live));

    assert_eq!(saver.get_messages(), vec![held, live]);
    assert_eq!(
        built_bars(&fixture, bar_type),
        vec![
            at("0.65000", 200 * MS_NS),
            at("0.65010", 200 * MS_NS),
            at("0.65020", 200 * MS_NS),
        ]
    );
}

#[rstest]
fn test_update_subscriptions_request_without_cut_warmup_releases_without_the_cut_or_the_floor(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_10_2000);
    let id = fixture.instrument_id;
    let bar_type = fixture.bar_type;
    let trade = |ts_event: u64, ts_init: u64, price: &str, trade_id: &str| {
        TradeTick::new(
            id,
            Price::from(price),
            Quantity::from("1000"),
            AggressorSide::Buy,
            TradeId::new(trade_id),
            UnixNanos::from(ts_event),
            UnixNanos::from(ts_init),
        )
    };
    let params = json!({ "bar_types": [bar_type.to_string()], "update_subscriptions": true });
    let request = starting_at(trades_request(vec![id], None, client_id, params.clone()), 0);
    let request_id = *request.request_id();
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();
    fixture
        .engine
        .borrow_mut()
        .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
            SubscribeBars::new(
                bar_type,
                Some(client_id),
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ),
        )));

    // In the answer's millisecond, then below its last `ts_init`
    fixture.engine.borrow_mut().process_data(Data::Trade(trade(
        100_000_001,
        250,
        "0.65010",
        "same-ms",
    )));
    fixture.engine.borrow_mut().process_data(Data::Trade(trade(
        101_000_000,
        199,
        "0.65020",
        "below",
    )));
    fixture
        .engine
        .borrow_mut()
        .response(DataResponse::Trades(TradesResponse::new(
            request_id,
            client_id,
            id,
            vec![trade(100_000_000, 200, "0.65000", "a1")],
            None,
            None,
            UnixNanos::from(300),
            Some(serde_json::from_value(params).unwrap()),
        )));

    assert_eq!(
        built_bars(&fixture, bar_type),
        vec![
            (Price::from("0.65000"), UnixNanos::from(200)),
            (Price::from("0.65010"), UnixNanos::from(250)),
        ]
    );
}

/// A joined request (2026-11-12 18:00 to 20:00 New York) for 1-MINUTE bars completes with a1 at
/// 18:00:30 and keeps its builder; its answer fires no end of coverage. A kept request continuing
/// that replay from the next nanosecond after its dispatch is refused by name, asks no client
/// and changes nothing: a subscription then takes over the builder with a1's open bar, closed at
/// 18:01 in its catch-up.
#[cfg(feature = "streaming")]
#[rstest]
fn test_kept_request_continuing_a_joined_replay_is_refused_for_its_missing_end_of_coverage(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let catalog_dir = CatalogTempDir::new("joined-no-coverage");
    fixture.engine.borrow_mut().register_catalog(
        Box::new(ParquetDataCatalog::new(
            catalog_dir.path(),
            None,
            None,
            None,
            None,
        )),
        None,
    );
    let mut params = joined_params(minute, NY_2026_11_12_2000, &[NY_2026_11_12_1800]);
    params["interleave"] = json!(false);
    let joined = trades_request(vec![fixture.instrument_id], None, client_id, params);
    let (result, failures) = execute_recording_failures(&mut fixture.engine.borrow_mut(), joined);
    result.unwrap();
    let tail_id = assert_replay_tail(&fixture, NY_2026_11_12_1800);
    let a1 = release_trade(
        fixture.instrument_id,
        30 * NANOSECONDS_IN_SECOND,
        30 * NANOSECONDS_IN_SECOND,
        "1.0001",
        "a1",
    );
    fixture
        .engine
        .borrow_mut()
        .response_envelope(DataResponseEnvelope::new(
            DataResponse::Trades(TradesResponse::new(
                tail_id,
                client_id,
                fixture.instrument_id,
                vec![a1],
                Some(UnixNanos::from(NY_2026_11_12_1800)),
                None,
                UnixNanos::from(NY_2026_11_12_2000),
                None,
            )),
            true,
            None,
        ));

    let continuation = starting_at(
        trades_request(
            vec![fixture.instrument_id],
            None,
            client_id,
            json!({ "bar_types": [minute.to_string()], "update_subscriptions": true }),
        ),
        NY_2026_11_12_2000 + 1,
    );
    let (refused, refusals) =
        execute_recording_failures(&mut fixture.engine.borrow_mut(), continuation);
    let commands = fixture.recorder.borrow().len();
    fixture
        .engine
        .borrow_mut()
        .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
            SubscribeBars::new(
                minute,
                Some(client_id),
                None,
                UUID4::new(),
                UnixNanos::from(NY_2026_11_12_2000),
                None,
                None,
            ),
        )));

    let error = refused.unwrap_err().to_string();
    assert!(
        error.starts_with("Cannot keep the bar builders of an `update_subscriptions` request"),
        "{error}"
    );
    assert!(error.contains("fired no end of coverage"), "{error}");
    assert_eq!(refusals, vec![Some(error)]);
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(commands, 1);
    assert_eq!(
        built_bars(&fixture, minute).first(),
        Some(&at("1.0001", 60 * NANOSECONDS_IN_SECOND))
    );
}

/// One envelope the bus delivered for a request: its payload, as the parts recorder sees it,
/// and its completion and reason.
#[derive(Debug, PartialEq)]
struct Delivered {
    part: Option<ResponsePart>,
    completed: bool,
    failure: Option<String>,
}

fn part(part: ResponsePart) -> Delivered {
    Delivered {
        part: Some(part),
        completed: false,
        failure: None,
    }
}

fn terminal(part: Option<ResponsePart>, failure: Option<&str>) -> Delivered {
    Delivered {
        part,
        completed: true,
        failure: failure.map(str::to_owned),
    }
}

/// Records every response envelope the bus delivers, terminals included: the bus drops a
/// request's handler at its first terminal, so only a tap sees a second one.
#[derive(Default)]
struct ResponseTap(RefCell<Vec<(UUID4, Delivered)>>);

impl BusTap for ResponseTap {
    fn on_publish(&self, _topic: MStr<Topic>, _message: &dyn Any) {}

    fn on_send(&self, _endpoint: MStr<Endpoint>, _message: &dyn Any) {}

    fn on_response(&self, correlation_id: &UUID4, message: &dyn Any) {
        let Some(envelope) = message.downcast_ref::<DataResponseEnvelope>() else {
            return;
        };
        let id = *correlation_id;
        let part = match envelope.response() {
            Some(DataResponse::Trades(resp)) => Some(ResponsePart::Trades(
                id,
                resp.data.iter().map(|trade| trade.trade_id).collect(),
            )),
            Some(DataResponse::Bars(resp)) => Some(ResponsePart::Bars(
                id,
                resp.bar_type,
                resp.data.iter().map(|bar| bar.ts_event).collect(),
            )),
            _ => None,
        };
        self.0.borrow_mut().push((
            id,
            Delivered {
                part,
                completed: envelope.completed(),
                failure: envelope.failure_reason().map(str::to_owned),
            },
        ));
    }
}

/// A [`ResponseTap`] installed on this thread's bus until dropped.
struct TappedResponses(Rc<ResponseTap>);

impl TappedResponses {
    fn install() -> Self {
        let tap = Rc::new(ResponseTap::default());
        msgbus::set_bus_tap(Rc::clone(&tap) as Rc<dyn BusTap>);
        Self(tap)
    }

    /// Takes what was delivered for `request_id` so far, in order.
    fn take(&self, request_id: UUID4) -> Vec<Delivered> {
        let all = std::mem::take(&mut *self.0.0.borrow_mut());
        let (taken, kept): (Vec<_>, Vec<_>) =
            all.into_iter().partition(|(id, _)| *id == request_id);
        *self.0.0.borrow_mut() = kept;
        taken.into_iter().map(|(_, delivered)| delivered).collect()
    }
}

impl Drop for TappedResponses {
    fn drop(&mut self) {
        msgbus::clear_bus_tap();
    }
}

const SECOND_NS: u64 = NANOSECONDS_IN_SECOND;

fn second_bars(instrument_id: InstrumentId) -> BarType {
    BarType::from(format!("{instrument_id}-1-SECOND-LAST-INTERNAL").as_str())
}

/// Executes an interleaved trades request for `instrument_id` aggregated into `bar_types`.
fn execute_interleaved(
    fixture: &JoinFixture,
    instrument_id: InstrumentId,
    bar_types: &[BarType],
    update_subscriptions: bool,
) -> UUID4 {
    let params = json!({
        "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "update_subscriptions": update_subscriptions,
        "interleave": true,
    });
    let request = trades_request(vec![instrument_id], None, fixture.client_id, params);
    // Kept builders need a `start`; the trades are a few nanoseconds after the epoch
    let request = if update_subscriptions {
        starting_at(request, 0)
    } else {
        request
    };
    let request_id = *request.request_id();
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();
    request_id
}

/// Executes an interleaved trades request like [`execute_interleaved`], without
/// `update_subscriptions` and with an `end`: at a completed answer its replay fires the closes up
/// to `end`, not up to the fixture's engine clock, years later.
fn execute_interleaved_until(
    fixture: &JoinFixture,
    instrument_id: InstrumentId,
    bar_types: &[BarType],
    end: u64,
) -> UUID4 {
    let params = json!({
        "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "update_subscriptions": false,
        "interleave": true,
    });
    let request = trades_request(vec![instrument_id], Some(end), fixture.client_id, params);
    let request_id = *request.request_id();
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();
    request_id
}

/// Answers `request_id` with trades (`ts`, trade ID) of `instrument_id` in one envelope.
fn answer_trades(
    fixture: &JoinFixture,
    request_id: UUID4,
    instrument_id: InstrumentId,
    trades: &[(u64, &str)],
    completed: bool,
) -> DataResponseEnvelope {
    let trades: Vec<_> = trades
        .iter()
        .map(|(ts, id)| make_trade(instrument_id, "1.0000", 1, id, *ts))
        .collect();
    DataResponseEnvelope::new(
        DataResponse::Trades(TradesResponse::new(
            request_id,
            fixture.client_id,
            instrument_id,
            trades,
            None,
            None,
            UnixNanos::default(),
            None,
        )),
        completed,
        None,
    )
}

fn at_ns(ts: &[u64]) -> Vec<UnixNanos> {
    ts.iter().copied().map(UnixNanos::from).collect()
}

#[rstest]
fn test_interleaved_answer_keeps_its_failure_reason_on_its_one_terminal(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let tick = fixture.bar_type;
    let tap = TappedResponses::install();

    let request_id = execute_interleaved(&fixture, id, &[tick], false);
    let failed = answer_trades(&fixture, request_id, id, &[(1, "t1"), (2, "t2")], true)
        .with_failure("the source stopped".to_owned());
    fixture.engine.borrow_mut().response_envelope(failed);

    assert_eq!(
        tap.take(request_id),
        vec![
            part(ResponsePart::Bars(request_id, tick, at_ns(&[1]))),
            part(ResponsePart::Trades(request_id, trade_ids(&["t1"]))),
            part(ResponsePart::Bars(request_id, tick, at_ns(&[2]))),
            terminal(
                Some(ResponsePart::Trades(request_id, trade_ids(&["t2"]))),
                Some("the source stopped"),
            ),
        ]
    );
}

#[rstest]
fn test_interleaved_failure_after_a_part_delivers_its_bars_once_and_none_to_the_next_request(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let seconds = second_bars(id);
    let tick = fixture.bar_type;
    let tap = TappedResponses::install();

    let first = execute_interleaved(&fixture, id, &[seconds], false);
    let answer = answer_trades(
        &fixture,
        first,
        id,
        &[(SECOND_NS / 5, "a1"), (SECOND_NS + SECOND_NS / 5, "a2")],
        false,
    );
    fixture.engine.borrow_mut().response_envelope(answer);
    fixture
        .engine
        .borrow_mut()
        .response_envelope(DataResponseEnvelope::terminal(
            first,
            Some("the source stopped".to_owned()),
        ));

    let second = execute_interleaved(&fixture, id, &[tick], false);
    let answer = answer_trades(&fixture, second, id, &[(5 * SECOND_NS, "b1")], true);
    fixture.engine.borrow_mut().response_envelope(answer);

    assert_eq!(
        tap.take(second),
        vec![
            part(ResponsePart::Bars(second, tick, at_ns(&[5 * SECOND_NS]))),
            terminal(Some(ResponsePart::Trades(second, trade_ids(&["b1"]))), None),
        ]
    );
    // The bar closing at a2's instant fires only after a2, and a failure fires no close
    assert_eq!(
        tap.take(first),
        vec![
            part(ResponsePart::Trades(first, trade_ids(&["a1", "a2"]))),
            terminal(None, Some("the source stopped")),
        ]
    );
}

#[rstest]
fn test_interleaved_completion_delivers_its_clean_up_bars_then_one_terminal(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let seconds = second_bars(id);
    let tap = TappedResponses::install();

    let request_id = execute_interleaved_until(&fixture, id, &[seconds], 2 * SECOND_NS);
    let answer = answer_trades(
        &fixture,
        request_id,
        id,
        &[(SECOND_NS / 5, "a1"), (SECOND_NS + SECOND_NS / 5, "a2")],
        true,
    );
    fixture.engine.borrow_mut().response_envelope(answer);

    // The bar closing at a2's instant fires after a2, at the end of the answer's coverage
    assert_eq!(
        tap.take(request_id),
        vec![
            part(ResponsePart::Trades(request_id, trade_ids(&["a1", "a2"]))),
            part(ResponsePart::Bars(
                request_id,
                seconds,
                at_ns(&[SECOND_NS + SECOND_NS / 5])
            )),
            terminal(Some(ResponsePart::Trades(request_id, Vec::new())), None),
        ]
    );
}

#[rstest]
fn test_interleaved_failure_before_any_part_gives_held_trades_bars_to_the_subscription_not_the_answer(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let tick = fixture.bar_type;
    let tap = TappedResponses::install();
    let (handler, published) = get_typed_message_saving_handler::<Bar>(None);
    msgbus::subscribe_bars(switchboard::get_bars_topic(tick).into(), handler, None);

    let request_id = execute_interleaved(&fixture, id, &[tick], true);
    fixture
        .engine
        .borrow_mut()
        .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
            SubscribeBars::new(
                tick,
                Some(client_id),
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ),
        )));
    for (ts, trade_id) in [(10, "live-1"), (20, "live-2")] {
        fixture
            .engine
            .borrow_mut()
            .process_data(Data::Trade(make_trade(id, "1.0000", 1, trade_id, ts)));
    }
    fixture
        .engine
        .borrow_mut()
        .response_envelope(DataResponseEnvelope::terminal(
            request_id,
            Some("the source stopped".to_owned()),
        ));

    // The failed request's builder is removed: a new one of the subscription takes the held
    // trades and publishes their bars; the answer is the terminal alone
    assert_eq!(
        tap.take(request_id),
        vec![terminal(None, Some("the source stopped"))]
    );
    assert_eq!(
        published
            .get_messages()
            .iter()
            .map(|bar| bar.ts_event)
            .collect::<Vec<_>>(),
        at_ns(&[10, 20])
    );
}

#[rstest]
fn test_interleaved_dispatch_error_sends_one_terminal_and_no_bars(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();
    let mut engine = DataEngine::new(clock, cache, None);
    let tap = TappedResponses::install();
    let params = json!({
        "bar_types": [second_bars(id).to_string()],
        "update_subscriptions": false,
        "interleave": true,
    });
    let request = trades_request(vec![id], None, client_id, params);
    let request_id = *request.request_id();

    let error = engine.execute_request(request).unwrap_err().to_string();

    assert!(error.contains("no client found"), "{error}");
    assert_eq!(tap.take(request_id), vec![terminal(None, Some(&error))]);
}

#[rstest]
fn test_two_interleaved_requests_one_after_the_other_each_get_their_own_bars(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let seconds = second_bars(id);
    let tap = TappedResponses::install();

    let first = execute_interleaved_until(&fixture, id, &[seconds], 2 * SECOND_NS);
    let answer = answer_trades(
        &fixture,
        first,
        id,
        &[(SECOND_NS / 5, "a1"), (SECOND_NS + SECOND_NS / 5, "a2")],
        true,
    );
    fixture.engine.borrow_mut().response_envelope(answer);
    // The same trades again: each request's aggregators are its own
    let second = execute_interleaved_until(&fixture, id, &[seconds], 2 * SECOND_NS);
    let answer = answer_trades(
        &fixture,
        second,
        id,
        &[(SECOND_NS / 5, "b1"), (SECOND_NS + SECOND_NS / 5, "b2")],
        true,
    );
    fixture.engine.borrow_mut().response_envelope(answer);

    assert_eq!(
        tap.take(first),
        vec![
            part(ResponsePart::Trades(first, trade_ids(&["a1", "a2"]))),
            part(ResponsePart::Bars(
                first,
                seconds,
                at_ns(&[SECOND_NS + SECOND_NS / 5])
            )),
            terminal(Some(ResponsePart::Trades(first, Vec::new())), None),
        ]
    );
    assert_eq!(
        tap.take(second),
        vec![
            part(ResponsePart::Trades(second, trade_ids(&["b1", "b2"]))),
            part(ResponsePart::Bars(
                second,
                seconds,
                at_ns(&[SECOND_NS + SECOND_NS / 5])
            )),
            terminal(Some(ResponsePart::Trades(second, Vec::new())), None),
        ]
    );
}

#[rstest]
fn test_two_interleaved_requests_at_the_same_time_for_different_instruments_each_get_their_own_bars(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let gbpusd = gbpusd_sim();
    let (aud, gbp) = (fixture.instrument_id, gbpusd.id);
    fixture
        .engine
        .borrow()
        .cache()
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(gbpusd))
        .unwrap();
    let (aud_seconds, gbp_seconds) = (second_bars(aud), second_bars(gbp));
    let tap = TappedResponses::install();

    let first = execute_interleaved(&fixture, aud, &[aud_seconds], false);
    let second = execute_interleaved_until(&fixture, gbp, &[gbp_seconds], 3 * SECOND_NS);
    let (a1, a2) = (SECOND_NS / 5, SECOND_NS + SECOND_NS / 5);
    let (b1, b2, b3) = (
        SECOND_NS / 4,
        SECOND_NS + SECOND_NS / 4,
        2 * SECOND_NS + SECOND_NS / 4,
    );
    let answer = answer_trades(&fixture, first, aud, &[(a1, "a1"), (a2, "a2")], false);
    fixture.engine.borrow_mut().response_envelope(answer);
    let answer = answer_trades(&fixture, second, gbp, &[(b1, "b1"), (b2, "b2")], false);
    fixture.engine.borrow_mut().response_envelope(answer);
    fixture
        .engine
        .borrow_mut()
        .response_envelope(DataResponseEnvelope::terminal(
            first,
            Some("the source stopped".to_owned()),
        ));
    let answer = answer_trades(&fixture, second, gbp, &[(b3, "b3")], true);
    fixture.engine.borrow_mut().response_envelope(answer);

    // A bar closing at a trade's instant fires before the next trade, or at the end of coverage
    assert_eq!(
        tap.take(second),
        vec![
            part(ResponsePart::Trades(second, trade_ids(&["b1", "b2"]))),
            part(ResponsePart::Bars(second, gbp_seconds, at_ns(&[b2]))),
            part(ResponsePart::Trades(second, trade_ids(&["b3"]))),
            part(ResponsePart::Bars(second, gbp_seconds, at_ns(&[b3]))),
            terminal(Some(ResponsePart::Trades(second, Vec::new())), None),
        ]
    );
    // A failure fires no close after the last trade
    assert_eq!(
        tap.take(first),
        vec![
            part(ResponsePart::Trades(first, trade_ids(&["a1", "a2"]))),
            terminal(None, Some("the source stopped")),
        ]
    );
}

#[rstest]
fn test_interleaved_request_routes_three_partial_envelopes_to_its_entry_until_the_completed_one(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let seconds = second_bars(id);
    let tap = TappedResponses::install();

    let request_id =
        execute_interleaved_until(&fixture, id, &[seconds], 2 * SECOND_NS + SECOND_NS / 2);
    for (index, trade_id) in ["a1", "a2", "a3"].into_iter().enumerate() {
        let ts = index as u64 * SECOND_NS + SECOND_NS / 5;
        let answer = answer_trades(&fixture, request_id, id, &[(ts, trade_id)], false);
        fixture.engine.borrow_mut().response_envelope(answer);
    }
    let answer = answer_trades(&fixture, request_id, id, &[], true);
    fixture.engine.borrow_mut().response_envelope(answer);

    // A bar closing at a trade's instant fires before the next trade, or at the end of coverage
    let bars = |ts: u64| part(ResponsePart::Bars(request_id, seconds, at_ns(&[ts])));
    let trades = |id: &str| part(ResponsePart::Trades(request_id, trade_ids(&[id])));
    assert_eq!(
        tap.take(request_id),
        vec![
            trades("a1"),
            trades("a2"),
            bars(SECOND_NS + SECOND_NS / 5),
            trades("a3"),
            bars(2 * SECOND_NS + SECOND_NS / 5),
            terminal(Some(ResponsePart::Trades(request_id, Vec::new())), None),
        ]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_interleaved_request_answers_once_after_three_partial_envelopes_with_its_reason(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let tick = fixture.bar_type;
    let tap = TappedResponses::install();
    let mut params = joined_params(tick, NY_2026_11_12_2000, &[NY_2026_11_12_1800]);
    params["interleave"] = json!(true);
    let request = trades_request(vec![id], None, client_id, params);
    let request_id = *request.request_id();

    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();
    let tail_id = assert_replay_tail(&fixture, NY_2026_11_12_1800);
    let tail_part = |trades: Vec<TradeTick>, completed: bool| {
        DataResponseEnvelope::new(
            DataResponse::Trades(TradesResponse::new(
                tail_id,
                client_id,
                id,
                trades,
                Some(UnixNanos::from(NY_2026_11_12_1800)),
                None,
                UnixNanos::from(NY_2026_11_12_2000),
                None,
            )),
            completed,
            None,
        )
    };
    for (offset, trade_id) in [(1, "r1"), (2, "r2"), (3, "r3")] {
        let trade = split_trade(id, NY_2026_11_12_1800 + offset, trade_id);
        fixture
            .engine
            .borrow_mut()
            .response_envelope(tail_part(vec![trade], false));
        assert!(tap.take(request_id).is_empty(), "the join answers once");
    }
    fixture
        .engine
        .borrow_mut()
        .response_envelope(tail_part(Vec::new(), true));

    let delivered = tap.take(request_id);
    let bars = |offset: u64| {
        part(ResponsePart::Bars(
            request_id,
            tick,
            at_ns(&[NY_2026_11_12_1800 + offset]),
        ))
    };
    let trades = |id: &str| part(ResponsePart::Trades(request_id, trade_ids(&[id])));
    let [b1, r1, b2, r2, b3, last] = &delivered[..] else {
        panic!("three bars, two trades parts and one terminal: {delivered:?}");
    };
    assert_eq!(
        [b1, r1, b2, r2, b3],
        [&bars(1), &trades("r1"), &bars(2), &trades("r2"), &bars(3)]
    );
    assert_eq!(
        last.part,
        Some(ResponsePart::Trades(request_id, trade_ids(&["r3"])))
    );
    assert!(last.completed);
    let reason = last.failure.as_deref().unwrap_or_default();
    assert!(
        reason.contains("no catalog is registered"),
        "the join's reason reaches its terminal: {reason:?}"
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_interleaved_request_without_a_client_ends_with_one_terminal_and_its_reasons(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let mut virtual_clock = VirtualClock::new();
    let _ = virtual_clock.advance_time(UnixNanos::from(NY_2026_11_12_2000), true);
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(virtual_clock));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();
    let engine = Rc::new(RefCell::new(DataEngine::new(clock, cache, None)));
    DataEngine::register_msgbus_handlers(&engine);
    let tick = BarType::from(format!("{id}-1-TICK-LAST-INTERNAL").as_str());
    let tap = TappedResponses::install();
    let mut params = joined_params(tick, NY_2026_11_12_2000, &[NY_2026_11_12_1800]);
    params["interleave"] = json!(true);
    let request = trades_request(vec![id], None, client_id, params);
    let request_id = *request.request_id();

    engine.borrow_mut().execute_request(request).unwrap();

    let delivered = tap.take(request_id);
    let [only] = &delivered[..] else {
        panic!("one terminal: {delivered:?}");
    };
    assert_eq!((&only.part, only.completed), (&None, true));
    let reason = only.failure.as_deref().unwrap_or_default();
    assert!(reason.contains("no client found"), "{reason:?}");
    assert!(reason.contains("no catalog is registered"), "{reason:?}");
}

// The join's deadline: the engine's outer bound over all its legs, armed at dispatch
#[cfg(feature = "streaming")]
const JOIN_DEADLINE: u64 = NY_2026_11_12_2000 + JOIN_DEADLINE_AFTER_NS;

/// Moves the engine clock of `fixture` to `to_ns` and returns the timer handlers that fell due,
/// not yet run: the node's event loop runs them after the envelopes queued ahead of them.
#[cfg(feature = "streaming")]
fn move_join_clock(
    fixture: &JoinFixture,
    to_ns: u64,
) -> Vec<nautilus_common::timer::TimeEventHandler> {
    let clock = Rc::clone(fixture.engine.borrow().clock());
    let mut clock = clock.borrow_mut();
    let clock = clock.as_any_mut().downcast_mut::<VirtualClock>().unwrap();
    let events = clock.advance_time(UnixNanos::from(to_ns), true);
    clock.match_handlers(events)
}

#[cfg(feature = "streaming")]
fn run_timers(handlers: Vec<nautilus_common::timer::TimeEventHandler>) {
    for handler in handlers {
        handler.callback.call(handler.event);
    }
}

/// The time the engine clock's one timer is armed for, if any.
#[cfg(feature = "streaming")]
fn join_timer(fixture: &JoinFixture) -> Option<UnixNanos> {
    let engine = fixture.engine.borrow();
    let clock = engine.clock().borrow();
    let names = clock.timer_names();
    assert!(names.len() <= 1, "one deadline timer at most: {names:?}");
    names.first().and_then(|name| clock.next_time_ns(name))
}

/// Executes a joined request of `fixture`'s instrument at 2026-11-12 20:00 New York over
/// `session_opens`, interleaved or not; returns its request ID and its replay tail's.
#[cfg(feature = "streaming")]
fn execute_join(fixture: &JoinFixture, session_opens: &[u64], interleave: bool) -> (UUID4, UUID4) {
    let mut params = joined_params(fixture.bar_type, NY_2026_11_12_2000, session_opens);
    params["interleave"] = json!(interleave);
    let request = trades_request(vec![fixture.instrument_id], None, fixture.client_id, params);
    let request_id = *request.request_id();
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();
    let tail_id = assert_replay_tail(fixture, NY_2026_11_12_1800);
    (request_id, tail_id)
}

/// An envelope of the replay tail `tail_id` with trades (`ts`, trade ID).
#[cfg(feature = "streaming")]
fn tail_envelope(
    fixture: &JoinFixture,
    tail_id: UUID4,
    trades: &[(u64, &str)],
    completed: bool,
) -> DataResponseEnvelope {
    let trades: Vec<_> = trades
        .iter()
        .map(|(ts, id)| split_trade(fixture.instrument_id, *ts, id))
        .collect();
    DataResponseEnvelope::new(
        DataResponse::Trades(TradesResponse::new(
            tail_id,
            fixture.client_id,
            fixture.instrument_id,
            trades,
            Some(UnixNanos::from(NY_2026_11_12_1800)),
            None,
            UnixNanos::from(NY_2026_11_12_2000),
            None,
        )),
        completed,
        None,
    )
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::ordinary(false)]
#[case::interleaved(true)]
fn test_joined_request_expires_with_its_answered_part_rebuilt_open_and_one_terminal(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] interleave: bool,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let catalog_ts = NY_2026_11_11_1800 + ONE_HOUR_NS;
    let _catalog = register_join_catalog(&fixture, "deadline", &[(catalog_ts, "c1")]);
    let tap = TappedResponses::install();
    let (request_id, tail_id) = execute_join(
        &fixture,
        &[NY_2026_11_11_1800, NY_2026_11_12_1800],
        interleave,
    );
    let answers = record_join_answers(request_id);
    let tail_ts = NY_2026_11_12_1800 + MS_NS;
    fixture.engine.borrow_mut().response_envelope(tail_envelope(
        &fixture,
        tail_id,
        &[(tail_ts, "r1")],
        false,
    ));
    assert_eq!(join_timer(&fixture), Some(UnixNanos::from(JOIN_DEADLINE)));

    run_timers(move_join_clock(&fixture, JOIN_DEADLINE));

    let delivered = tap.take(request_id);
    let tick = fixture.bar_type;
    if interleave {
        let bars = |ts: u64| part(ResponsePart::Bars(request_id, tick, at_ns(&[ts])));
        assert_eq!(
            delivered,
            vec![
                bars(catalog_ts),
                part(ResponsePart::Trades(request_id, trade_ids(&["c1"]))),
                bars(tail_ts),
                terminal(
                    Some(ResponsePart::Trades(request_id, trade_ids(&["r1"]))),
                    Some("deadline"),
                ),
            ]
        );
        let answers = answers.take();
        assert!(
            answers.iter().all(|answer| answer.end.is_none()),
            "{answers:?}"
        );
    } else {
        assert_eq!(
            delivered,
            vec![terminal(
                Some(ResponsePart::Trades(request_id, trade_ids(&["c1", "r1"]))),
                Some("deadline"),
            )]
        );
        assert_one_answer(&answers.take(), &["c1", "r1"], Some(&["deadline"]));
        let built: Vec<_> = built_bars(&fixture, tick)
            .into_iter()
            .map(|(_, ts_init)| ts_init)
            .collect();
        assert_eq!(built, at_ns(&[catalog_ts, tail_ts]));
    }
    assert_eq!(join_timer(&fixture), None);
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_request_expiry_gives_a_held_back_subscription_a_new_builder_fed_its_held_trades(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let bar_type = fixture.bar_type;
    let tap = TappedResponses::install();
    let (request_id, tail_id) = execute_join(&fixture, &[NY_2026_11_12_1800], false);
    fixture
        .engine
        .borrow_mut()
        .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
            SubscribeBars::new(
                bar_type,
                Some(fixture.client_id),
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ),
        )));
    for trade in [
        // In the answered cut's millisecond: the answer holds it, the new builder does not
        release_trade(
            id,
            100 * MS_NS + MS_NS / 2,
            205 * MS_NS,
            "0.65010",
            "overlap",
        ),
        // Received below the held trade before it: the new builder's stale case
        release_trade(id, 101 * MS_NS, 150 * MS_NS, "0.65020", "below-the-floor"),
    ] {
        fixture.engine.borrow_mut().process_data(Data::Trade(trade));
    }
    // The tail answers one part and never completes
    fixture
        .engine
        .borrow_mut()
        .response_envelope(DataResponseEnvelope::new(
            DataResponse::Trades(TradesResponse::new(
                tail_id,
                fixture.client_id,
                id,
                vec![release_trade(id, 100 * MS_NS, 200 * MS_NS, "0.65000", "r1")],
                Some(UnixNanos::from(NY_2026_11_12_1800)),
                None,
                UnixNanos::from(NY_2026_11_12_2000),
                None,
            )),
            false,
            None,
        ));

    // The expiry removes the request's builder; the subscription's new builder takes the held
    // trades as they were held, with no cut or floor, and the later live trades
    run_timers(move_join_clock(&fixture, JOIN_DEADLINE));
    let later = release_trade(id, 102 * MS_NS, 210 * MS_NS, "0.65030", "later");
    fixture.engine.borrow_mut().process_data(Data::Trade(later));

    assert_eq!(
        built_bars(&fixture, bar_type),
        vec![
            at("0.65000", 200 * MS_NS),
            at("0.65010", 205 * MS_NS),
            at("0.65030", 210 * MS_NS),
        ]
    );
    let [only] = &tap.take(request_id)[..] else {
        panic!("one terminal at the deadline");
    };
    assert_eq!(
        (&only.part, only.completed),
        (
            &Some(ResponsePart::Trades(request_id, trade_ids(&["r1"]))),
            true
        )
    );
    let reason = only.failure.as_deref().unwrap_or_default();
    assert!(reason.starts_with("deadline; "), "{reason:?}");
    assert_eq!(join_timer(&fixture), None);
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::ordinary(false)]
#[case::interleaved(true)]
fn test_late_envelopes_after_expiry_are_dropped_and_counted_until_the_legs_terminal(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] interleave: bool,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let tap = TappedResponses::install();
    let (request_id, tail_id) = execute_join(&fixture, &[NY_2026_11_12_1800], interleave);

    run_timers(move_join_clock(&fixture, JOIN_DEADLINE));

    let [only] = &tap.take(request_id)[..] else {
        panic!("one terminal at the deadline");
    };
    assert_eq!((&only.part, only.completed), (&None, true));
    let reason = only.failure.as_deref().unwrap_or_default();
    assert!(reason.starts_with("deadline; "), "{reason:?}");

    let late_ts = NY_2026_11_12_1800 + MS_NS;
    let late = tail_envelope(&fixture, tail_id, &[(late_ts, "late")], false);
    fixture.engine.borrow_mut().response_envelope(late);
    assert_eq!(fixture.engine.borrow().late_envelopes(), 1);
    let leg_terminal = DataResponseEnvelope::terminal(tail_id, None);
    fixture.engine.borrow_mut().response_envelope(leg_terminal);
    assert_eq!(fixture.engine.borrow().late_envelopes(), 2);
    assert!(tap.take(tail_id).is_empty(), "late envelopes are dropped");

    // The leg's own terminal ended its suppression: a later envelope is no longer the join's
    let after = DataResponseEnvelope::terminal(tail_id, None);
    fixture.engine.borrow_mut().response_envelope(after);
    assert_eq!(fixture.engine.borrow().late_envelopes(), 2);
    assert_eq!(tap.take(tail_id).len(), 1);
    assert!(tap.take(request_id).is_empty());
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_envelope_queued_behind_the_deadline_is_dropped_and_counted(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let tap = TappedResponses::install();
    let (request_id, tail_id) = execute_join(&fixture, &[NY_2026_11_12_1800], false);
    let answered = tail_envelope(&fixture, tail_id, &[(NY_2026_11_12_1800 + 1, "r1")], false);
    fixture.engine.borrow_mut().response_envelope(answered);

    // The clock passed the deadline; its timer event is still queued behind an envelope
    let timers = move_join_clock(&fixture, JOIN_DEADLINE);
    let queued = tail_envelope(&fixture, tail_id, &[(NY_2026_11_12_1800 + 2, "r2")], true);
    fixture.engine.borrow_mut().response_envelope(queued);

    let [only] = &tap.take(request_id)[..] else {
        panic!("one terminal at the deadline");
    };
    assert_eq!(
        (&only.part, only.completed),
        (
            &Some(ResponsePart::Trades(request_id, trade_ids(&["r1"]))),
            true
        )
    );
    let reason = only.failure.as_deref().unwrap_or_default();
    assert!(reason.starts_with("deadline; "), "{reason:?}");
    assert_eq!(fixture.engine.borrow().late_envelopes(), 1);

    run_timers(timers);

    assert!(tap.take(request_id).is_empty(), "no second terminal");
    assert_eq!(join_timer(&fixture), None);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_completion_just_before_its_deadline_cancels_the_timer(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let catalog_ts = NY_2026_11_11_1800 + ONE_HOUR_NS;
    let _catalog = register_join_catalog(&fixture, "deadline", &[(catalog_ts, "c1")]);
    let tap = TappedResponses::install();
    let (request_id, tail_id) =
        execute_join(&fixture, &[NY_2026_11_11_1800, NY_2026_11_12_1800], false);
    let answers = record_join_answers(request_id);
    run_timers(move_join_clock(&fixture, JOIN_DEADLINE - 1));

    let completed = tail_envelope(&fixture, tail_id, &[(NY_2026_11_12_1800 + 1, "r1")], true);
    fixture.engine.borrow_mut().response_envelope(completed);

    assert_one_answer(&answers.take(), &["c1", "r1"], None);
    assert_eq!(join_timer(&fixture), None, "completion cancels the timer");
    assert_eq!(tap.take(request_id).len(), 1);

    run_timers(move_join_clock(&fixture, JOIN_DEADLINE + 1));

    assert!(tap.take(request_id).is_empty(), "no second terminal");
    assert_eq!(fixture.engine.borrow().late_envelopes(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_request_is_refused_when_its_deadline_timer_cannot_be_registered(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let mut virtual_clock = VirtualClock::new();
    let _ = virtual_clock.advance_time(UnixNanos::from(NY_2026_11_12_2000), true);
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(virtual_clock));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();
    // Not registered on the message bus: the engine holds no handle for a timer's callback
    let mut engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut engine,
    );
    let tick = BarType::from(format!("{id}-1-TICK-LAST-INTERNAL").as_str());
    let params = joined_params(tick, NY_2026_11_12_2000, &[NY_2026_11_12_1800]);
    let request = trades_request(vec![id], None, client_id, params);

    let (result, failures) = execute_recording_failures(&mut engine, request);

    let error = result.unwrap_err().to_string();
    assert!(error.contains("could not be armed"), "{error}");
    assert_eq!(failures, vec![Some(error)]);
    assert!(recorder.borrow().is_empty(), "{:?}", recorder.borrow());
    assert_eq!(engine.request_pipeline_count(), 0);
    assert_eq!(clock.borrow().timer_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_two_joined_requests_expire_each_at_its_own_deadline(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let gbpusd = gbpusd_sim();
    let (aud, gbp) = (fixture.instrument_id, gbpusd.id);
    fixture
        .engine
        .borrow()
        .cache()
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(gbpusd))
        .unwrap();
    let tap = TappedResponses::install();
    let (earlier, later) = (JOIN_DEADLINE - 60 * SECOND_NS, JOIN_DEADLINE);
    let execute = |instrument_id: InstrumentId, deadline: u64| {
        let tick = BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str());
        let mut params = joined_params(tick, NY_2026_11_12_2000, &[NY_2026_11_12_1800]);
        params["deadline"] = json!(deadline);
        let request = trades_request(vec![instrument_id], None, client_id, params);
        let request_id = *request.request_id();
        fixture
            .engine
            .borrow_mut()
            .execute_request(request)
            .unwrap();
        request_id
    };
    let (first, second) = (execute(gbp, later), execute(aud, earlier));
    assert_eq!(join_timer(&fixture), Some(UnixNanos::from(earlier)));
    let expired_once = |request_id: UUID4| {
        let delivered = tap.take(request_id);
        let [only] = &delivered[..] else {
            panic!("one terminal: {delivered:?}");
        };
        let reason = only.failure.as_deref().unwrap_or_default();
        assert!(
            only.completed && reason.starts_with("deadline; "),
            "{only:?}"
        );
    };

    run_timers(move_join_clock(&fixture, earlier));

    expired_once(second);
    assert!(tap.take(first).is_empty());
    assert_eq!(join_timer(&fixture), Some(UnixNanos::from(later)));

    run_timers(move_join_clock(&fixture, later));

    expired_once(first);
    assert!(tap.take(second).is_empty());
    assert_eq!(join_timer(&fixture), None);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_engine_reset_ends_join_deadlines_and_leg_suppressions(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let gbpusd = gbpusd_sim();
    let (aud, gbp) = (fixture.instrument_id, gbpusd.id);
    fixture
        .engine
        .borrow()
        .cache()
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(gbpusd))
        .unwrap();
    let tap = TappedResponses::install();
    let (earlier, later) = (JOIN_DEADLINE - 60 * SECOND_NS, JOIN_DEADLINE);
    let execute = |instrument_id: InstrumentId, deadline: u64| {
        let tick = BarType::from(format!("{instrument_id}-1-TICK-LAST-INTERNAL").as_str());
        let mut params = joined_params(tick, NY_2026_11_12_2000, &[NY_2026_11_12_1800]);
        params["deadline"] = json!(deadline);
        let request = trades_request(vec![instrument_id], None, client_id, params);
        let request_id = *request.request_id();
        fixture
            .engine
            .borrow_mut()
            .execute_request(request)
            .unwrap();
        request_id
    };
    let (_expired, pending) = (execute(aud, earlier), execute(gbp, later));
    let aud_tail = fixture
        .recorder
        .borrow()
        .iter()
        .find_map(|command| match command {
            DataCommand::Request(RequestCommand::Trades(tail)) if tail.instrument_ids == [aud] => {
                Some(tail.request_id)
            }
            _ => None,
        })
        .unwrap();
    run_timers(move_join_clock(&fixture, earlier));

    fixture.engine.borrow_mut().reset();
    run_timers(move_join_clock(&fixture, later));
    let leg_terminal = DataResponseEnvelope::terminal(aud_tail, None);
    fixture.engine.borrow_mut().response_envelope(leg_terminal);

    assert_eq!(
        fixture.engine.borrow().late_envelopes(),
        0,
        "no leg is suppressed"
    );
    assert!(
        tap.take(pending).is_empty(),
        "no deadline survives the reset"
    );
    assert_eq!(join_timer(&fixture), None);
}

/// What one envelope of a `quotes` request carried: its quotes (by `ts_init`), its trades (by
/// trade ID) or its bars (by `ts_event`), with its completion and reason.
#[cfg(feature = "streaming")]
#[derive(Debug, PartialEq)]
enum QuotedPart {
    Quotes(Vec<u64>),
    Trades(Vec<TradeId>),
    Bars(Vec<UnixNanos>),
    Nothing,
}

#[cfg(feature = "streaming")]
#[derive(Debug, PartialEq)]
struct QuotedDelivery {
    part: QuotedPart,
    completed: bool,
    failure: Option<String>,
}

#[cfg(feature = "streaming")]
fn quoted(part: QuotedPart) -> QuotedDelivery {
    QuotedDelivery {
        part,
        completed: false,
        failure: None,
    }
}

#[cfg(feature = "streaming")]
fn quoted_end(part: QuotedPart, failure: Option<&str>) -> QuotedDelivery {
    QuotedDelivery {
        part,
        completed: true,
        failure: failure.map(str::to_owned),
    }
}

/// Records every response envelope the bus delivers for one request, terminals included: what
/// each carried, and each whole envelope serialized.
#[cfg(feature = "streaming")]
struct QuotedTap {
    request_id: UUID4,
    delivered: RefCell<Vec<QuotedDelivery>>,
    envelopes: RefCell<Vec<Value>>,
}

#[cfg(feature = "streaming")]
impl BusTap for QuotedTap {
    fn on_publish(&self, _topic: MStr<Topic>, _message: &dyn Any) {}

    fn on_send(&self, _endpoint: MStr<Endpoint>, _message: &dyn Any) {}

    fn on_response(&self, correlation_id: &UUID4, message: &dyn Any) {
        let Some(envelope) = message.downcast_ref::<DataResponseEnvelope>() else {
            return;
        };

        if *correlation_id != self.request_id {
            return;
        }
        let part = match envelope.response() {
            Some(DataResponse::Quotes(resp)) => QuotedPart::Quotes(
                resp.data
                    .iter()
                    .map(|quote| quote.ts_init.as_u64())
                    .collect(),
            ),
            Some(DataResponse::Trades(resp)) => {
                QuotedPart::Trades(resp.data.iter().map(|trade| trade.trade_id).collect())
            }
            Some(DataResponse::Bars(resp)) => {
                QuotedPart::Bars(resp.data.iter().map(|bar| bar.ts_event).collect())
            }
            _ => QuotedPart::Nothing,
        };
        self.delivered.borrow_mut().push(QuotedDelivery {
            part,
            completed: envelope.completed(),
            failure: envelope.failure_reason().map(str::to_owned),
        });
        self.envelopes
            .borrow_mut()
            .push(serde_json::to_value(envelope).unwrap());
    }
}

/// A [`QuotedTap`] for `request_id` installed on this thread's bus until dropped.
#[cfg(feature = "streaming")]
struct QuotedResponses(Rc<QuotedTap>);

#[cfg(feature = "streaming")]
impl QuotedResponses {
    fn install(request_id: UUID4) -> Self {
        let tap = Rc::new(QuotedTap {
            request_id,
            delivered: RefCell::new(Vec::new()),
            envelopes: RefCell::new(Vec::new()),
        });
        msgbus::set_bus_tap(Rc::clone(&tap) as Rc<dyn BusTap>);
        Self(tap)
    }

    fn take(&self) -> Vec<QuotedDelivery> {
        self.0.delivered.take()
    }

    fn take_envelopes(&self) -> Vec<Value> {
        self.0.envelopes.take()
    }
}

#[cfg(feature = "streaming")]
impl Drop for QuotedResponses {
    fn drop(&mut self) {
        msgbus::clear_bus_tap();
    }
}

/// A print without an aggressor (the side the quote in force decides).
#[cfg(feature = "streaming")]
fn unsided_trade(instrument_id: InstrumentId, ts: u64, price: &str, trade_id: &str) -> TradeTick {
    TradeTick::new(
        instrument_id,
        Price::from(price),
        Quantity::from(1),
        AggressorSide::NoAggressor,
        TradeId::new(trade_id),
        UnixNanos::from(ts),
        UnixNanos::from(ts),
    )
}

/// One catalog run of a quoted catalog: its file interval, its quotes (`ts`, bid, ask) and its
/// trades (`ts`, trade ID).
#[cfg(feature = "streaming")]
struct QuotedRun<'a> {
    interval: (u64, u64),
    quotes: &'a [(u64, &'a str, &'a str)],
    trades: &'a [(u64, &'a str)],
}

/// Registers a made-up catalog holding quotes and trades, one file of each per run (a run with
/// no rows of a kind writes no file of it).
#[cfg(feature = "streaming")]
fn register_quoted_catalog(
    fixture: &JoinFixture,
    label: &str,
    runs: &[QuotedRun],
) -> CatalogTempDir {
    let catalog_dir = CatalogTempDir::new(label);
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);

    for run in runs {
        let (start, end) = (
            Some(UnixNanos::from(run.interval.0)),
            Some(UnixNanos::from(run.interval.1)),
        );
        let quotes: Vec<_> = run
            .quotes
            .iter()
            .map(|(ts, bid, ask)| make_quote(fixture.instrument_id, bid, ask, *ts))
            .collect();

        if !quotes.is_empty() {
            catalog.write_to_parquet(&quotes, start, end, None).unwrap();
        }

        // Each print is at the ask of the quote in force before it
        let trades: Vec<_> = run
            .trades
            .iter()
            .map(|(ts, id)| unsided_trade(fixture.instrument_id, *ts, "1.0002", id))
            .collect();

        if !trades.is_empty() {
            catalog.write_to_parquet(&trades, start, end, None).unwrap();
        }
    }
    fixture
        .engine
        .borrow_mut()
        .register_catalog(Box::new(catalog), None);
    catalog_dir
}

/// The quote counterexample's catalog: changing quotes, prints without an aggressor, a quote
/// and a trade at equal stamps, over two runs spliced at 2 000 ns (a quote and a trade at the
/// splice).
#[cfg(feature = "streaming")]
const COUNTEREXAMPLE: [QuotedRun<'static>; 2] = [
    QuotedRun {
        interval: (1_000, 1_999),
        quotes: &[(1_000, "1.0000", "1.0002"), (1_500, "1.0002", "1.0004")],
        trades: &[(1_000, "t1"), (1_200, "t2"), (1_500, "t3")],
    },
    QuotedRun {
        interval: (2_000, 2_999),
        quotes: &[(2_000, "1.0001", "1.0002"), (2_500, "1.0002", "1.0003")],
        trades: &[(2_000, "t4"), (2_700, "t5")],
    },
];

/// An ordinary trades request for the window `[start, end]` with `params`.
#[cfg(feature = "streaming")]
fn windowed_trades_request(
    fixture: &JoinFixture,
    (start, end): (u64, u64),
    params: Value,
) -> RequestCommand {
    RequestCommand::Trades(RequestTrades::new(
        vec![fixture.instrument_id],
        Some(UnixNanos::from(start).to_datetime_utc()),
        Some(UnixNanos::from(end).to_datetime_utc()),
        None,
        Some(fixture.client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(serde_json::from_value(params).unwrap()),
    ))
}

/// `request`, a trades request, routed to a client no engine registers.
#[cfg(feature = "streaming")]
fn without_a_client(request: RequestCommand) -> RequestCommand {
    let RequestCommand::Trades(mut cmd) = request else {
        panic!("a trades request: {request:?}");
    };
    cmd.client_id = Some(ClientId::new("ABSENT"));
    RequestCommand::Trades(cmd)
}

/// The failure reason of a `quotes` request whose window's part from `start` to `end` no
/// catalog file holds for `feeds` and no client resolves.
#[cfg(feature = "streaming")]
fn uncovered_part(start: u64, end: u64, feeds: &str) -> String {
    format!(
        "No source covers {} to {} of the request's window: no catalog file holds its {feeds} \
         and no client resolves",
        UnixNanos::from(start).to_datetime_utc(),
        UnixNanos::from(end).to_datetime_utc(),
    )
}

/// Executes `request` and returns every envelope its requester was sent.
#[cfg(feature = "streaming")]
fn run_quoted(fixture: &JoinFixture, request: RequestCommand) -> Vec<QuotedDelivery> {
    let tap = QuotedResponses::install(*request.request_id());
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();
    tap.take()
}

/// How a request of the quote tests builds bars.
#[cfg(feature = "streaming")]
#[derive(Clone, Copy, Debug)]
enum QuotedBars {
    None,
    Ticks,
    InterleavedTicks,
}

#[cfg(feature = "streaming")]
fn quoted_params(fixture: &JoinFixture, bars: QuotedBars, quotes: Option<bool>) -> Value {
    let mut params = match bars {
        QuotedBars::None => json!({}),
        QuotedBars::Ticks => json!({
            "bar_types": [fixture.bar_type.to_string()],
            "update_subscriptions": false,
        }),
        QuotedBars::InterleavedTicks => json!({
            "bar_types": [fixture.bar_type.to_string()],
            "update_subscriptions": false,
            "interleave": true,
        }),
    };

    if let Some(quotes) = quotes {
        params["quotes"] = json!(quotes);
    }
    params
}

#[cfg(feature = "streaming")]
fn quotes_at(ts: &[u64]) -> QuotedPart {
    QuotedPart::Quotes(ts.to_vec())
}

#[cfg(feature = "streaming")]
fn trades_of(ids: &[&str]) -> QuotedPart {
    QuotedPart::Trades(trade_ids(ids))
}

#[cfg(feature = "streaming")]
fn bars_at(ts: &[u64]) -> QuotedPart {
    QuotedPart::Bars(at_ns(ts))
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::builds_no_bars(QuotedBars::None)]
#[case::builds_tick_bars(QuotedBars::Ticks)]
#[case::interleaves_tick_bars(QuotedBars::InterleavedTicks)]
fn test_quotes_request_answers_quotes_and_trades_in_one_time_order_quote_first_on_a_tie(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] bars: QuotedBars,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let _catalog_dir = register_quoted_catalog(&fixture, "quotes-counterexample", &COUNTEREXAMPLE);
    let params = quoted_params(&fixture, bars, Some(true));
    let request = windowed_trades_request(&fixture, (1_000, 2_999), params);

    let delivered = run_quoted(&fixture, request);

    let expected = match bars {
        QuotedBars::None | QuotedBars::Ticks => vec![
            quoted(quotes_at(&[1_000])),
            quoted(trades_of(&["t1", "t2"])),
            quoted(quotes_at(&[1_500])),
            quoted(trades_of(&["t3"])),
            quoted(quotes_at(&[2_000])),
            quoted(trades_of(&["t4"])),
            quoted(quotes_at(&[2_500])),
            quoted_end(trades_of(&["t5"]), None),
        ],
        // A tick bar goes out before the trade that completes it; quotes build no LAST bar
        QuotedBars::InterleavedTicks => vec![
            quoted(quotes_at(&[1_000])),
            quoted(bars_at(&[1_000])),
            quoted(trades_of(&["t1"])),
            quoted(bars_at(&[1_200])),
            quoted(trades_of(&["t2"])),
            quoted(quotes_at(&[1_500])),
            quoted(bars_at(&[1_500])),
            quoted(trades_of(&["t3"])),
            quoted(quotes_at(&[2_000])),
            quoted(bars_at(&[2_000])),
            quoted(trades_of(&["t4"])),
            quoted(quotes_at(&[2_500])),
            quoted(bars_at(&[2_700])),
            quoted_end(trades_of(&["t5"]), None),
        ],
    };
    assert_eq!(delivered, expected);
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_quotes_request_ending_with_a_quote_completes_with_an_empty_trades_part(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let _catalog_dir = register_quoted_catalog(
        &fixture,
        "quotes-ending-with-a-quote",
        &[QuotedRun {
            interval: (1_000, 1_999),
            quotes: &[(1_000, "1.0000", "1.0002"), (1_900, "1.0001", "1.0003")],
            trades: &[(1_100, "t1")],
        }],
    );
    let params = quoted_params(&fixture, QuotedBars::None, Some(true));
    let request = windowed_trades_request(&fixture, (1_000, 1_999), params);

    assert_eq!(
        run_quoted(&fixture, request),
        vec![
            quoted(quotes_at(&[1_000])),
            quoted(trades_of(&["t1"])),
            quoted(quotes_at(&[1_900])),
            quoted_end(trades_of(&[]), None),
        ]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::builds_no_bars(QuotedBars::None)]
#[case::interleaves_tick_bars(QuotedBars::InterleavedTicks)]
fn test_quotes_request_with_incomplete_quote_coverage_fails_with_one_terminal(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] bars: QuotedBars,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);

    // The trades cover the window; the quotes miss 2 000 to 2 099 between their runs
    let _catalog_dir = register_quoted_catalog(
        &fixture,
        "quotes-incomplete",
        &[
            QuotedRun {
                interval: (1_000, 2_999),
                quotes: &[],
                trades: &[(1_000, "t1"), (2_050, "t2")],
            },
            QuotedRun {
                interval: (1_000, 1_999),
                quotes: &[(1_000, "1.0000", "1.0002")],
                trades: &[],
            },
            QuotedRun {
                interval: (2_100, 2_999),
                quotes: &[(2_100, "1.0001", "1.0003")],
                trades: &[],
            },
        ],
    );
    let params = quoted_params(&fixture, bars, Some(true));
    let request = without_a_client(windowed_trades_request(&fixture, (1_000, 2_999), params));

    // No client resolves: the records the catalog holds, then one terminal naming the gap
    let reason = uncovered_part(2_000, 2_099, "quotes");
    let expected = match bars {
        QuotedBars::None | QuotedBars::Ticks => vec![
            quoted(quotes_at(&[1_000])),
            quoted(trades_of(&["t1", "t2"])),
            quoted(quotes_at(&[2_100])),
            quoted_end(trades_of(&[]), Some(&reason)),
        ],
        QuotedBars::InterleavedTicks => vec![
            quoted(quotes_at(&[1_000])),
            quoted(bars_at(&[1_000])),
            quoted(trades_of(&["t1"])),
            quoted(bars_at(&[2_050])),
            quoted(trades_of(&["t2"])),
            quoted(quotes_at(&[2_100])),
            quoted_end(trades_of(&[]), Some(&reason)),
        ],
    };
    assert_eq!(run_quoted(&fixture, request), expected);
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
    assert!(fixture.recorder.borrow().is_empty());
}

/// Completes the replay tail `tail_id` of a joined request with no trades.
#[cfg(feature = "streaming")]
fn complete_empty_tail(fixture: &JoinFixture, tail_id: UUID4) {
    fixture
        .engine
        .borrow_mut()
        .response_envelope(DataResponseEnvelope::new(
            DataResponse::Trades(TradesResponse::new(
                tail_id,
                fixture.client_id,
                fixture.instrument_id,
                Vec::<TradeTick>::new(),
                Some(UnixNanos::from(NY_2026_11_12_1800)),
                None,
                UnixNanos::from(NY_2026_11_12_2000),
                None,
            )),
            true,
            None,
        ));
}

#[cfg(feature = "streaming")]
const QUOTES_REFUSED: &str = "Cannot answer the quotes of a `quotes` request";

/// A `quotes` request whose plan cannot carry the instrument's quotes over its whole window.
#[cfg(feature = "streaming")]
#[derive(Clone, Copy, Debug)]
enum Unquotable {
    /// No catalog is registered: its client would answer it.
    NoCatalog,
    /// It sets `skip_catalog_data`: one client leg answers it.
    SkipCatalogData,
    /// It sets a `schema`: its client answers it whole.
    Schema,
}

/// A catalog run outside every window of the tests that register it.
#[cfg(feature = "streaming")]
const OUTSIDE_THE_WINDOW: [QuotedRun<'static>; 1] = [QuotedRun {
    interval: (5_000, 5_999),
    quotes: &[(5_000, "1.0000", "1.0002")],
    trades: &[(5_100, "t1")],
}];

#[cfg(feature = "streaming")]
#[rstest]
#[case::no_catalog(
    Unquotable::NoCatalog,
    "no catalog is registered, so its plan has no catalog leg"
)]
#[case::skip_catalog_data(Unquotable::SkipCatalogData, "its plan has no catalog leg")]
#[case::schema(
    Unquotable::Schema,
    "its `schema` sends it to its client, so its plan has no catalog leg"
)]
fn test_quotes_request_whose_plan_cannot_carry_quotes_is_refused_before_any_dispatch(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] plan: Unquotable,
    #[case] reason: &str,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let runs: &[QuotedRun] = match plan {
        Unquotable::NoCatalog => &[],
        Unquotable::SkipCatalogData | Unquotable::Schema => &COUNTEREXAMPLE,
    };
    let _catalog_dir =
        (!runs.is_empty()).then(|| register_quoted_catalog(&fixture, "quotes-unquotable", runs));
    let mut params = quoted_params(&fixture, QuotedBars::Ticks, Some(true));
    match plan {
        Unquotable::SkipCatalogData => params["skip_catalog_data"] = json!(true),
        Unquotable::Schema => params["schema"] = json!("trades"),
        _ => {}
    }
    let request = windowed_trades_request(&fixture, (1_000, 2_999), params);

    let (result, failures) = execute_recording_failures(&mut fixture.engine.borrow_mut(), request);

    let error = result
        .expect_err("a `quotes` request its plan cannot carry must be refused")
        .to_string();
    assert_eq!(error, format!("{QUOTES_REFUSED}: {reason}"));
    assert_eq!(failures, vec![Some(error)]);
    assert!(
        fixture.recorder.borrow().is_empty(),
        "a refused request reaches no client: {:?}",
        fixture.recorder.borrow()
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_joined_quotes_request_is_refused_naming_its_replay_tail(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[values(true, false)] with_catalog: bool,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let hour = ONE_HOUR_NS;
    let _catalog_dir = with_catalog.then(|| {
        register_quoted_catalog(
            &fixture,
            "joined-quotes-refused",
            &[QuotedRun {
                interval: (NY_2026_11_11_1800, NY_2026_11_12_1800 - 1),
                quotes: &[(NY_2026_11_11_1800 + hour, "1.0000", "1.0002")],
                trades: &[(NY_2026_11_11_1800 + 2 * hour, "c1")],
            }],
        )
    });
    let mut params = joined_params(
        fixture.bar_type,
        NY_2026_11_12_2000,
        &[NY_2026_11_11_1800, NY_2026_11_12_1800],
    );
    params["quotes"] = json!(true);
    let request = trades_request(vec![fixture.instrument_id], None, client_id, params);

    let (result, failures) = execute_recording_failures(&mut fixture.engine.borrow_mut(), request);

    let tail = format!(
        "its leg from {NY_2026_11_12_1800} is its client's replay tail, which carries no quotes"
    );
    let reason = if with_catalog {
        tail
    } else {
        format!("its plan has no catalog leg; {tail}")
    };
    let error = result
        .expect_err("a joined `quotes` request must be refused")
        .to_string();
    assert_eq!(error, format!("{QUOTES_REFUSED}: {reason}"));
    assert_eq!(failures, vec![Some(error)]);
    assert!(
        fixture.recorder.borrow().is_empty(),
        "a refused request reaches no client: {:?}",
        fixture.recorder.borrow()
    );
    assert_eq!(join_timer(&fixture), None);
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// A shape of `quotes` request no catalog leg can serve whole, refused on its route.
#[cfg(feature = "streaming")]
#[derive(Clone, Copy, Debug)]
enum UnservedShape {
    /// Its `quotes` is not a boolean.
    NotBoolean,
    /// It names two instruments, the second with no catalog coverage.
    Batch,
    /// It sets a time-range pipeline with no child window.
    EmptyTimeRange,
    /// It sets a time-range pipeline with child windows.
    TimeRange,
    /// It sets continuous-future params.
    ContinuousFuture,
    /// Its client is external.
    ExternalClient,
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::not_boolean(UnservedShape::NotBoolean, "`quotes` must be a boolean")]
#[case::batch(
    UnservedShape::Batch,
    "it must name exactly one instrument (batches are refused), received 2"
)]
#[case::empty_time_range(
    UnservedShape::EmptyTimeRange,
    "its `time_range_generator` nests it in a time-range pipeline, which answers each child \
     window apart"
)]
#[case::time_range(
    UnservedShape::TimeRange,
    "its `time_range_generator` nests it in a time-range pipeline, which answers each child \
     window apart"
)]
#[case::continuous_future(
    UnservedShape::ContinuousFuture,
    "its continuous-future params nest it in a continuous-future pipeline, which answers bars \
     only"
)]
#[case::external_client(
    UnservedShape::ExternalClient,
    "its client SIM is external, so this engine answers nothing"
)]
fn test_quotes_request_of_an_unserved_shape_is_refused_before_any_dispatch(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] shape: UnservedShape,
    #[case] reason: &str,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let _catalog_dir = register_quoted_catalog(&fixture, "quotes-unserved", &COUNTEREXAMPLE);
    let mut params = quoted_params(&fixture, QuotedBars::Ticks, Some(true));
    let mut instrument_ids = vec![fixture.instrument_id];

    match shape {
        UnservedShape::NotBoolean => params["quotes"] = json!("true"),
        UnservedShape::Batch => instrument_ids.push(InstrumentId::from("GBP/USD.SIM")),
        UnservedShape::EmptyTimeRange => {
            params["time_range_generator"] = json!("");
            params["durations_seconds"] = json!([]);
        }
        UnservedShape::TimeRange => {
            params["time_range_generator"] = json!("");
            params["durations_seconds"] = json!([1]);
        }
        UnservedShape::ContinuousFuture => params["continuous_future_transitions"] = json!([]),
        UnservedShape::ExternalClient => {
            let engine = fixture.engine.borrow();
            engine
                .cache()
                .borrow_mut()
                .add_external_data_client(client_id);
        }
    }
    let request = RequestCommand::Trades(RequestTrades::new(
        instrument_ids,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_999).to_datetime_utc()),
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(serde_json::from_value(params).unwrap()),
    ));
    let tap = QuotedResponses::install(*request.request_id());

    let result = fixture.engine.borrow_mut().execute_request(request);

    let error = result
        .expect_err("a `quotes` request of an unserved shape must be refused")
        .to_string();
    assert_eq!(error, format!("{QUOTES_REFUSED}: {reason}"));
    assert_eq!(
        tap.take(),
        vec![quoted_end(QuotedPart::Nothing, Some(&error))],
        "one terminal and nothing else"
    );
    assert!(
        fixture.recorder.borrow().is_empty(),
        "a refused request reaches no client: {:?}",
        fixture.recorder.borrow()
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
    assert_eq!(fixture.engine.borrow().time_range_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_quotes_leg_of_a_request_join_is_refused_before_dispatch_naming_the_join(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let _catalog_dir = register_quoted_catalog(&fixture, "quotes-join-leg", &COUNTEREXAMPLE);
    let (leg_id, join_id) = (UUID4::new(), UUID4::new());
    let join = RequestJoin::new(
        vec![leg_id],
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_999).to_datetime_utc()),
        join_id,
        UnixNanos::default(),
        None,
        None,
    );
    let join_tap = QuotedResponses::install(join_id);
    fixture
        .engine
        .borrow_mut()
        .execute_request(RequestCommand::Join(join))
        .unwrap();
    let params = quoted_params(&fixture, QuotedBars::None, Some(true));
    let leg = RequestCommand::Trades(RequestTrades::new(
        vec![fixture.instrument_id],
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_999).to_datetime_utc()),
        None,
        Some(client_id),
        leg_id,
        UnixNanos::default(),
        Some(serde_json::from_value(params).unwrap()),
    ));

    let result = fixture.engine.borrow_mut().execute_request(leg);

    let error = result
        .expect_err("a `quotes` leg of a request join must be refused")
        .to_string();
    assert_eq!(
        error,
        format!(
            "{QUOTES_REFUSED}: it is a leg of the request join {join_id}, whose answer is \
             rebuilt as one data type"
        )
    );
    assert_eq!(
        join_tap.take(),
        vec![quoted_end(QuotedPart::Nothing, Some(&error))],
        "the join ends with the leg's reason, once"
    );
    assert!(
        fixture.recorder.borrow().is_empty(),
        "a refused leg reaches no client: {:?}",
        fixture.recorder.borrow()
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// How a request of the parameter-absent test is answered.
#[cfg(feature = "streaming")]
#[derive(Clone, Copy, Debug)]
enum UnquotedPath {
    Ordinary,
    Interleaved,
    Joined,
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::ordinary(UnquotedPath::Ordinary)]
#[case::interleaved(UnquotedPath::Interleaved)]
#[case::joined(UnquotedPath::Joined)]
fn test_trades_request_without_quotes_answers_as_before_from_a_catalog_holding_quotes(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] path: UnquotedPath,
    #[values(None, Some(false))] quotes: Option<bool>,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let hour = ONE_HOUR_NS;
    let (c1, c2) = (NY_2026_11_11_1800 + hour, NY_2026_11_11_1800 + 2 * hour);
    let _catalog_dir = register_quoted_catalog(
        &fixture,
        "unquoted",
        &[QuotedRun {
            interval: (NY_2026_11_11_1800, NY_2026_11_12_1800 - 1),
            quotes: &[(c1, "1.0000", "1.0002"), (c1 + 1, "1.0001", "1.0003")],
            trades: &[(c1, "c1"), (c2, "c2")],
        }],
    );

    let window = (NY_2026_11_11_1800, NY_2026_11_12_1800 - 1);
    let mut params = match path {
        UnquotedPath::Ordinary => quoted_params(&fixture, QuotedBars::None, quotes),
        UnquotedPath::Interleaved => quoted_params(&fixture, QuotedBars::InterleavedTicks, quotes),
        UnquotedPath::Joined => {
            let mut params = joined_params(
                fixture.bar_type,
                NY_2026_11_12_2000,
                &[NY_2026_11_11_1800, NY_2026_11_12_1800],
            );

            if let Some(quotes) = quotes {
                params["quotes"] = json!(quotes);
            }
            params
        }
    };
    let request = match path {
        UnquotedPath::Joined => {
            trades_request(vec![fixture.instrument_id], None, client_id, params.clone())
        }
        _ => windowed_trades_request(&fixture, window, params.clone()),
    };
    let request_id = *request.request_id();
    let tap = QuotedResponses::install(request_id);
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();

    if matches!(path, UnquotedPath::Joined) {
        let tail_id = assert_replay_tail(&fixture, NY_2026_11_12_1800);
        complete_empty_tail(&fixture, tail_id);
    }
    let envelopes = tap.take_envelopes();

    // Each part is an ordinary catalog leg's whole answer: its rows as written, the request's
    // window and params (plus the leg's `update_catalog`), and the clock's time as `ts_init`
    params["update_catalog"] = json!(false);
    let params: Params = serde_json::from_value(params).unwrap();
    let start = Some(UnixNanos::from(window.0));
    let end = match path {
        UnquotedPath::Joined => None,
        _ => Some(UnixNanos::from(window.1)),
    };
    let now = UnixNanos::from(NY_2026_11_12_2000);
    let trades = |rows: &[(u64, &str)]| {
        let data: Vec<_> = rows
            .iter()
            .map(|(ts, id)| unsided_trade(fixture.instrument_id, *ts, "1.0002", id))
            .collect();
        DataResponse::Trades(TradesResponse::new(
            request_id,
            client_id,
            fixture.instrument_id,
            data,
            start,
            end,
            now,
            Some(params.clone()),
        ))
    };
    let tick_bar = |ts: u64| {
        let price = Price::from("1.0002");
        let bar = Bar::new(
            fixture.bar_type,
            price,
            price,
            price,
            price,
            Quantity::from(1),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        );
        DataResponse::Bars(BarsResponse::new(
            request_id,
            client_id,
            fixture.bar_type,
            vec![bar],
            start,
            end,
            now,
            Some(params.clone()),
        ))
    };
    let envelope = |response: DataResponse, completed: bool| {
        serde_json::to_value(DataResponseEnvelope::new(response, completed, None)).unwrap()
    };
    let expected = match path {
        UnquotedPath::Ordinary | UnquotedPath::Joined => {
            vec![envelope(trades(&[(c1, "c1"), (c2, "c2")]), true)]
        }
        UnquotedPath::Interleaved => vec![
            envelope(tick_bar(c1), false),
            envelope(trades(&[(c1, "c1")]), false),
            envelope(tick_bar(c2), false),
            envelope(trades(&[(c2, "c2")]), true),
        ],
    };
    assert_eq!(envelopes, expected);
}

const INTERLEAVE_REFUSED: &str = "Cannot answer an `interleave` request in live order";

/// A shape of an `interleave` request whose bars cannot be given in live order.
#[cfg(feature = "streaming")]
#[derive(Clone, Copy, Debug)]
enum UninterleavedShape {
    /// Its `interleave` is not a boolean.
    NotBoolean,
    /// It is a quotes request.
    QuotesRequest,
    /// It is a bars request.
    BarsRequest,
    /// Its client is external.
    ExternalClient,
    /// It sets continuous-future params.
    ContinuousFuture,
    /// It sets a time-range pipeline with no child window.
    EmptyTimeRange,
    /// It sets a time-range pipeline with child windows.
    TimeRange,
    /// It names two instruments.
    Batch,
    /// A bar type is built from a trade-fed bar type.
    Composite,
    /// A bar type is built from a composite (two steps from the trades).
    CompositeTwoDeep,
    /// A time bar type is built from a trade-fed bar type.
    TimeComposite,
    /// A bar type is built from quotes.
    QuoteFed,
    /// A bar type is built from quotes, and the request carries `quotes`.
    QuoteFedWithQuotes,
    /// A bar type is of another instrument.
    OtherInstrument,
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::not_boolean(UninterleavedShape::NotBoolean, "`interleave` must be a boolean")]
#[case::quotes_request(
    UninterleavedShape::QuotesRequest,
    "only a trades request can set `interleave`"
)]
#[case::bars_request(
    UninterleavedShape::BarsRequest,
    "only a trades request can set `interleave`"
)]
#[case::external_client(
    UninterleavedShape::ExternalClient,
    "its client SIM is external, so this engine answers nothing"
)]
#[case::continuous_future(
    UninterleavedShape::ContinuousFuture,
    "its continuous-future params nest it in a continuous-future pipeline, which answers bars \
     only"
)]
#[case::empty_time_range(
    UninterleavedShape::EmptyTimeRange,
    "its `time_range_generator` nests it in a time-range pipeline, whose windows feed its bars \
     apart from the trades it is given"
)]
#[case::time_range(
    UninterleavedShape::TimeRange,
    "its `time_range_generator` nests it in a time-range pipeline, whose windows feed its bars \
     apart from the trades it is given"
)]
#[case::batch(
    UninterleavedShape::Batch,
    "it must name exactly one instrument (batches are refused), received 2"
)]
#[case::composite(
    UninterleavedShape::Composite,
    "its bar type AUD/USD.SIM-2-TICK-LAST-INTERNAL@10-RANGE_INCLUSIVE-INTERNAL is not built from \
     its trades: it is a composite, built from other bars"
)]
#[case::composite_two_deep(
    UninterleavedShape::CompositeTwoDeep,
    "its bar type AUD/USD.SIM-1-TICK-LAST-INTERNAL@2-TICK-INTERNAL is not built from its trades: \
     it is a composite, built from other bars"
)]
#[case::time_composite(
    UninterleavedShape::TimeComposite,
    "its bar type AUD/USD.SIM-1-SECOND-LAST-INTERNAL@10-RANGE_INCLUSIVE-INTERNAL is not built \
     from its trades: it is a composite, built from other bars"
)]
#[case::quote_fed(
    UninterleavedShape::QuoteFed,
    "its bar type AUD/USD.SIM-1-TICK-BID-INTERNAL is not built from its trades: it is built from \
     quotes"
)]
#[case::quote_fed_with_quotes(
    UninterleavedShape::QuoteFedWithQuotes,
    "its bar type AUD/USD.SIM-1-TICK-BID-INTERNAL is not built from its trades: it is built from \
     quotes"
)]
#[case::other_instrument(
    UninterleavedShape::OtherInstrument,
    "its bar type GBP/USD.SIM-1-TICK-LAST-INTERNAL is not built from its trades: it is of \
     another instrument"
)]
fn test_interleave_request_of_an_unserved_shape_is_refused_before_any_dispatch(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] shape: UninterleavedShape,
    #[case] reason: &str,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let _catalog_dir = register_quoted_catalog(&fixture, "interleave-unserved", &COUNTEREXAMPLE);
    let id = fixture.instrument_id;
    let tick = fixture.bar_type;
    let range_inclusive = BarType::from(format!("{id}-10-RANGE_INCLUSIVE-LAST-INTERNAL").as_str());
    let composite =
        BarType::from(format!("{id}-2-TICK-LAST-INTERNAL@10-RANGE_INCLUSIVE-INTERNAL").as_str());
    let bar_types = match shape {
        UninterleavedShape::Composite => vec![range_inclusive, composite],
        UninterleavedShape::CompositeTwoDeep => vec![
            range_inclusive,
            BarType::from(format!("{id}-1-TICK-LAST-INTERNAL@2-TICK-INTERNAL").as_str()),
            composite,
        ],
        UninterleavedShape::TimeComposite => vec![
            range_inclusive,
            BarType::from(
                format!("{id}-1-SECOND-LAST-INTERNAL@10-RANGE_INCLUSIVE-INTERNAL").as_str(),
            ),
        ],
        UninterleavedShape::QuoteFed | UninterleavedShape::QuoteFedWithQuotes => {
            vec![
                tick,
                BarType::from(format!("{id}-1-TICK-BID-INTERNAL").as_str()),
            ]
        }
        UninterleavedShape::OtherInstrument => {
            vec![tick, BarType::from("GBP/USD.SIM-1-TICK-LAST-INTERNAL")]
        }
        _ => vec![tick],
    };
    let mut params = json!({
        "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "update_subscriptions": false,
        "interleave": true,
    });
    let mut instrument_ids = vec![id];

    match shape {
        UninterleavedShape::NotBoolean => params["interleave"] = json!("true"),
        UninterleavedShape::ExternalClient => {
            let engine = fixture.engine.borrow();
            engine
                .cache()
                .borrow_mut()
                .add_external_data_client(client_id);
        }
        UninterleavedShape::ContinuousFuture => {
            params["continuous_future_transitions"] = json!([]);
        }
        UninterleavedShape::EmptyTimeRange => {
            params["time_range_generator"] = json!("");
            params["durations_seconds"] = json!([]);
        }
        UninterleavedShape::TimeRange => {
            params["time_range_generator"] = json!("");
            params["durations_seconds"] = json!([1]);
        }
        UninterleavedShape::Batch => instrument_ids.push(InstrumentId::from("GBP/USD.SIM")),
        UninterleavedShape::QuoteFedWithQuotes => params["quotes"] = json!(true),
        _ => {}
    }
    let (start, end) = (
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_999).to_datetime_utc()),
    );
    let params = Some(serde_json::from_value(params).unwrap());
    let request_id = UUID4::new();
    let request = match shape {
        UninterleavedShape::QuotesRequest => RequestCommand::Quotes(RequestQuotes::new(
            instrument_ids,
            start,
            end,
            None,
            Some(client_id),
            request_id,
            UnixNanos::default(),
            params,
        )),
        UninterleavedShape::BarsRequest => RequestCommand::Bars(RequestBars::new(
            tick,
            start,
            end,
            None,
            Some(client_id),
            request_id,
            UnixNanos::default(),
            params,
        )),
        _ => RequestCommand::Trades(RequestTrades::new(
            instrument_ids,
            start,
            end,
            None,
            Some(client_id),
            request_id,
            UnixNanos::default(),
            params,
        )),
    };
    let tap = TappedResponses::install();

    let result = fixture.engine.borrow_mut().execute_request(request);

    let error = result
        .expect_err("an `interleave` request of an unserved shape must be refused")
        .to_string();
    assert_eq!(error, format!("{INTERLEAVE_REFUSED}: {reason}"));
    assert_eq!(
        tap.take(request_id),
        vec![terminal(None, Some(&error))],
        "one terminal and nothing else"
    );
    assert!(
        fixture.recorder.borrow().is_empty(),
        "a refused request reaches no client: {:?}",
        fixture.recorder.borrow()
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
    assert_eq!(fixture.engine.borrow().time_range_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_interleave_leg_of_a_request_join_is_refused_before_dispatch_naming_the_join(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let _catalog_dir = register_quoted_catalog(&fixture, "interleave-join-leg", &COUNTEREXAMPLE);
    let (leg_id, join_id) = (UUID4::new(), UUID4::new());
    let join = RequestJoin::new(
        vec![leg_id],
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_999).to_datetime_utc()),
        join_id,
        UnixNanos::default(),
        None,
        None,
    );
    let join_tap = QuotedResponses::install(join_id);
    fixture
        .engine
        .borrow_mut()
        .execute_request(RequestCommand::Join(join))
        .unwrap();
    let params = quoted_params(&fixture, QuotedBars::InterleavedTicks, None);
    let leg = RequestCommand::Trades(RequestTrades::new(
        vec![fixture.instrument_id],
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_999).to_datetime_utc()),
        None,
        Some(client_id),
        leg_id,
        UnixNanos::default(),
        Some(serde_json::from_value(params).unwrap()),
    ));

    let result = fixture.engine.borrow_mut().execute_request(leg);

    let error = result
        .expect_err("an `interleave` leg of a request join must be refused")
        .to_string();
    assert_eq!(
        error,
        format!(
            "{INTERLEAVE_REFUSED}: it is a leg of the request join {join_id}, whose answer is \
             rebuilt as one data type"
        )
    );
    assert_eq!(
        join_tap.take(),
        vec![quoted_end(QuotedPart::Nothing, Some(&error))],
        "the join ends with the leg's reason, once"
    );
    assert!(
        fixture.recorder.borrow().is_empty(),
        "a refused leg reaches no client: {:?}",
        fixture.recorder.borrow()
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// A request that sets `interleave` and aggregates no bars.
#[derive(Clone, Copy, Debug)]
enum BarlessShape {
    /// A trades request whose `bar_types` is an empty list.
    EmptyBarTypes,
    /// A trades request without `bar_types` whose `interleave` is not a boolean.
    NotBoolean,
    /// A quotes request.
    QuotesRequest,
    /// A bars request.
    BarsRequest,
    /// A trades request for an external client.
    ExternalClient,
    /// A trades request nested in a time-range pipeline.
    TimeRange,
    /// A trades request for two instruments.
    Batch,
    /// A trades request a `RequestJoin` sent before it takes as a leg.
    JoinLeg,
}

/// What executing a request shows: its result, what its requester was given, the number of
/// commands its client received, and the request and time-range pipelines left open.
#[derive(Debug, PartialEq)]
struct Executed {
    result: Result<(), String>,
    delivered: Vec<Delivered>,
    dispatched: usize,
    pipelines: (usize, usize),
}

/// Executes the request `request_id` of `shape` on an engine of its own, with `interleave` set
/// to `interleave` or absent, and returns what it shows.
fn execute_barless(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
    shape: BarlessShape,
    interleave: Option<Value>,
    request_id: UUID4,
) -> Executed {
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let mut params = json!({ "update_subscriptions": false });
    let mut instrument_ids = vec![id];
    match shape {
        BarlessShape::EmptyBarTypes => params["bar_types"] = json!([]),
        BarlessShape::ExternalClient => {
            let engine = fixture.engine.borrow();
            engine
                .cache()
                .borrow_mut()
                .add_external_data_client(client_id);
        }
        BarlessShape::TimeRange => {
            params["time_range_generator"] = json!("");
            params["durations_seconds"] = json!([1]);
        }
        BarlessShape::Batch => instrument_ids.push(InstrumentId::from("GBP/USD.SIM")),
        BarlessShape::JoinLeg => {
            let join = RequestJoin::new(
                vec![request_id],
                Some(UnixNanos::from(1_000).to_datetime_utc()),
                Some(UnixNanos::from(2_999).to_datetime_utc()),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            );
            fixture
                .engine
                .borrow_mut()
                .execute_request(RequestCommand::Join(join))
                .unwrap();
        }
        BarlessShape::NotBoolean | BarlessShape::QuotesRequest | BarlessShape::BarsRequest => {}
    }
    if let Some(value) = interleave {
        params["interleave"] = value;
    }
    let (start, end) = (
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_999).to_datetime_utc()),
    );
    let params = Some(serde_json::from_value(params).unwrap());
    let request = match shape {
        BarlessShape::QuotesRequest => RequestCommand::Quotes(RequestQuotes::new(
            instrument_ids,
            start,
            end,
            None,
            Some(client_id),
            request_id,
            UnixNanos::default(),
            params,
        )),
        BarlessShape::BarsRequest => RequestCommand::Bars(RequestBars::new(
            fixture.bar_type,
            start,
            end,
            None,
            Some(client_id),
            request_id,
            UnixNanos::default(),
            params,
        )),
        _ => RequestCommand::Trades(RequestTrades::new(
            instrument_ids,
            start,
            end,
            None,
            Some(client_id),
            request_id,
            UnixNanos::default(),
            params,
        )),
    };
    let tap = TappedResponses::install();

    let result = fixture.engine.borrow_mut().execute_request(request);

    let engine = fixture.engine.borrow();
    Executed {
        result: result.map_err(|e| e.to_string()),
        delivered: tap.take(request_id),
        dispatched: fixture.recorder.borrow().len(),
        pipelines: (
            engine.request_pipeline_count(),
            engine.time_range_pipeline_count(),
        ),
    }
}

#[rstest]
#[case::empty_bar_types(BarlessShape::EmptyBarTypes, json!(true))]
#[case::not_boolean(BarlessShape::NotBoolean, json!("true"))]
#[case::quotes_request(BarlessShape::QuotesRequest, json!(true))]
#[case::bars_request(BarlessShape::BarsRequest, json!(true))]
#[case::external_client(BarlessShape::ExternalClient, json!(true))]
#[case::time_range(BarlessShape::TimeRange, json!(true))]
#[case::batch(BarlessShape::Batch, json!(true))]
#[case::join_leg(BarlessShape::JoinLeg, json!(true))]
fn test_interleave_request_without_bar_types_is_answered_as_without_the_parameter(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] shape: BarlessShape,
    #[case] interleave: Value,
) {
    let _ = stub_msgbus;
    let request_id = UUID4::new();

    let with = execute_barless(
        audusd_sim.clone(),
        client_id,
        venue,
        shape,
        Some(interleave),
        request_id,
    );
    let without = execute_barless(audusd_sim, client_id, venue, shape, None, request_id);

    assert!(
        with.result
            .as_ref()
            .err()
            .is_none_or(|error| !error.starts_with(INTERLEAVE_REFUSED)),
        "{with:?}"
    );
    assert_eq!(with, without);
}

#[rstest]
fn test_request_join_naming_a_dispatched_interleave_request_is_refused_and_leaves_it_its_answer(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let tick = fixture.bar_type;
    let tap = TappedResponses::install();
    // The request is admitted, its shared aggregator gathering, and a live subscription waits
    let leg_id = execute_interleaved(&fixture, id, &[tick], true);
    fixture
        .engine
        .borrow_mut()
        .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
            SubscribeBars::new(
                tick,
                Some(client_id),
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ),
        )));
    let live_bars = Rc::new(RefCell::new(Vec::new()));
    let recorded = Rc::clone(&live_bars);
    msgbus::subscribe_bars(
        switchboard::get_bars_topic(tick).into(),
        TypedHandler::from(move |bar: &Bar| recorded.borrow_mut().push(bar.ts_event)),
        None,
    );
    let join_id = UUID4::new();
    let join = RequestJoin::new(
        vec![leg_id],
        Some(UnixNanos::from(1).to_datetime_utc()),
        Some(UnixNanos::from(2).to_datetime_utc()),
        join_id,
        UnixNanos::default(),
        None,
        None,
    );

    let result = fixture
        .engine
        .borrow_mut()
        .execute_request(RequestCommand::Join(join));

    let error = result
        .expect_err("a join naming a dispatched `interleave` request must be refused")
        .to_string();
    assert_eq!(
        error,
        format!(
            "{INTERLEAVE_REFUSED}: the request join {join_id} would take {leg_id}, already \
             dispatched, as a leg and rebuild its answer as one data type"
        )
    );
    assert_eq!(
        tap.take(join_id),
        vec![terminal(None, Some(&error))],
        "the join ends with one terminal"
    );
    assert_eq!(tap.take(leg_id), Vec::new(), "the request is not ended");
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);

    let answer = answer_trades(&fixture, leg_id, id, &[(1, "t1"), (2, "t2")], true);
    fixture.engine.borrow_mut().response_envelope(answer);

    assert_eq!(
        tap.take(leg_id),
        vec![
            part(ResponsePart::Bars(leg_id, tick, at_ns(&[1]))),
            part(ResponsePart::Trades(leg_id, trade_ids(&["t1"]))),
            part(ResponsePart::Bars(leg_id, tick, at_ns(&[2]))),
            terminal(Some(ResponsePart::Trades(leg_id, trade_ids(&["t2"]))), None),
        ],
        "the request's own answer delivers its bars and its one terminal"
    );
    // Its entry and its aggregator are released: a live trade's bar is published, not gathered
    fixture
        .engine
        .borrow_mut()
        .process_data(Data::Trade(make_trade(id, "1.0000", 1, "live-1", 10)));
    assert_eq!(*live_bars.borrow(), at_ns(&[10]));
    assert_eq!(tap.take(leg_id), Vec::new());
    assert_eq!(tap.take(join_id), Vec::new());
}

#[rstest]
fn test_interleaved_failure_before_any_part_caches_each_held_trades_bar_just_before_its_part(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let tick = fixture.bar_type;
    let request_id = execute_interleaved(&fixture, id, &[tick], true);
    let cache = Rc::clone(fixture.engine.borrow().cache());
    let seen = Rc::new(RefCell::new(Vec::new()));
    let recorded = Rc::clone(&seen);
    // The failure hands the held trades to a new builder of the subscription, which publishes
    msgbus::subscribe_bars(
        switchboard::get_bars_topic(tick).into(),
        TypedHandler::from(move |bar: &Bar| {
            recorded
                .borrow_mut()
                .push((bar.ts_event, cache.borrow().bar_count(&tick)));
        }),
        None,
    );
    fixture
        .engine
        .borrow_mut()
        .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
            SubscribeBars::new(
                tick,
                Some(client_id),
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ),
        )));
    for (ts, trade_id) in [(10, "live-1"), (20, "live-2")] {
        fixture
            .engine
            .borrow_mut()
            .process_data(Data::Trade(make_trade(id, "1.0000", 1, trade_id, ts)));
    }

    fixture
        .engine
        .borrow_mut()
        .response_envelope(DataResponseEnvelope::terminal(
            request_id,
            Some("the source stopped".to_owned()),
        ));

    // The first bar's handler does not find the second in the cache
    assert_eq!(
        *seen.borrow(),
        vec![(UnixNanos::from(10), 1), (UnixNanos::from(20), 2)]
    );
}

/// Whether, between two trades of `delivered`, bars of two types come in another order than
/// they have in `bar_types`.
fn completes_two_types_out_of_order(delivered: &[OrderedDelivery], bar_types: &[BarType]) -> bool {
    delivered
        .split(|delivery| matches!(delivery, OrderedDelivery::Trade(..)))
        .any(|run| {
            let positions: Vec<usize> = run
                .iter()
                .filter_map(|delivery| match delivery {
                    OrderedDelivery::Bar(bar_type, ..) => bar_types
                        .iter()
                        .position(|candidate| candidate.standard() == *bar_type),
                    OrderedDelivery::Trade(..) => None,
                })
                .collect();
            positions.windows(2).any(|pair| pair[0] > pair[1])
        })
}

// Each pair is named out of live's order, and one trade of the order tape completes a bar of each:
// t3 the two `RANGE` bars and the `RANGE` and `VOLUME` bars, t5 the two `RANGE_INCLUSIVE` bars,
// and the 2-ms bar closes at 4 ms, just before t6 completes a `VOLUME` bar
#[rstest]
#[case::two_ranges(&["9-RANGE", "10-RANGE"])]
#[case::two_ranges_inclusive(&["9-RANGE_INCLUSIVE", "10-RANGE_INCLUSIVE"])]
#[case::volume_and_range(&["1500-VOLUME", "10-RANGE"])]
#[case::volume_and_time(&["1500-VOLUME", "2-MILLISECOND"])]
fn test_interleaved_bars_of_two_types_one_trade_completes_come_in_live_order_whatever_the_request_order(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] specs: &[&str],
) {
    let _ = stub_msgbus;
    let bar_types: Vec<BarType> = specs
        .iter()
        .map(|spec| BarType::from(format!("{}-{spec}-LAST-INTERNAL", audusd_sim.id).as_str()))
        .collect();

    let live = live_deliveries(audusd_sim.clone(), client_id, venue, &bar_types);

    assert!(
        completes_two_types_out_of_order(&live, &bar_types),
        "one trade completes bars of both types, which live gives in the other order: {live:?}"
    );
    for answer in history_answers() {
        let history =
            interleaved_deliveries(audusd_sim.clone(), client_id, venue, &bar_types, answer);
        assert_eq!(history, live, "{answer:?}");
    }
}

fn order_bar_types(instrument_id: InstrumentId, specs: &[&str]) -> Vec<BarType> {
    specs
        .iter()
        .map(|spec| BarType::from(format!("{instrument_id}-{spec}-LAST-INTERNAL").as_str()))
        .collect()
}

fn subscribe_order_bars(engine: &mut DataEngine, bar_types: &[BarType], client_id: ClientId) {
    for bar_type in bar_types {
        engine.execute(DataCommand::Subscribe(SubscribeCommand::Bars(
            SubscribeBars::new(
                *bar_type,
                Some(client_id),
                Some(bar_type.instrument_id().venue),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ),
        )));
    }
}

/// Subscribes `bar_types` live and processes `trades` live, the last bar type subscribed only
/// before the trade at `late` (with the others when `late` is zero), and returns the bars a
/// subscriber to every bar type is given, in order.
fn live_bars(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
    bar_types: &[BarType],
    late: usize,
) -> Vec<OrderedDelivery> {
    // A bus of its own: another engine's trade handlers would hold the same IDs
    let _msgbus = stub_msgbus();
    let instrument_id = audusd_sim.id;
    let (mut data_engine, cache) = order_engine(audusd_sim, client_id, venue);
    let delivered = Rc::new(RefCell::new(Vec::new()));
    for bar_type in bar_types {
        let recorded = Rc::clone(&delivered);
        let cache = Rc::clone(&cache);
        let watched = bar_types.to_vec();
        let handler = TypedHandler::from(move |bar: &Bar| {
            let cached = cached_bars(&cache, &watched);
            recorded
                .borrow_mut()
                .push(OrderedDelivery::Bar(bar.bar_type, bar.ts_event, cached));
        });
        msgbus::subscribe_bars(
            switchboard::get_bars_topic(bar_type.standard()).into(),
            handler,
            None,
        );
    }

    let (first, last) = bar_types.split_at(bar_types.len() - usize::from(late > 0));
    subscribe_order_bars(&mut data_engine, first, client_id);
    for (index, trade) in order_tape(instrument_id).into_iter().enumerate() {
        if index == late {
            subscribe_order_bars(&mut data_engine, last, client_id);
        }
        data_engine.process_data(Data::Trade(trade));
    }

    delivered.take()
}

/// How an interleaving request ends after its builders held live trades.
#[derive(Clone, Copy, Debug)]
enum HeldEnd {
    /// A payload-free failure.
    Failure,
    /// A completed answer holding this many of the order tape's first trades.
    Answer(usize),
}

/// Executes an interleaving `update_subscriptions` request for `bar_types`, subscribes them live
/// while its answer is in flight, the last only before the held trade at `late` (with the others
/// when `late` is zero), processes the order tape after the answered trades live (its builders
/// hold them), then ends the request as `end` says; returns the bars its requester is given,
/// and those published to the subscriptions (after a failure, the new builders' that took over
/// the held trades), in the order they were given.
fn released_bars(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
    bar_types: &[BarType],
    late: usize,
    end: HeldEnd,
) -> Vec<OrderedDelivery> {
    // A bus of its own: another engine's trade handlers would hold the same IDs
    let _msgbus = stub_msgbus();
    let instrument_id = audusd_sim.id;
    let (mut data_engine, cache) = order_engine(audusd_sim, client_id, venue);
    let params = json!({
        "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "update_subscriptions": true,
        "interleave": true,
    });
    let request = starting_at(
        trades_request(vec![instrument_id], None, client_id, params.clone()),
        0,
    );
    let request_id = *request.request_id();
    data_engine.execute_request(request).unwrap();
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let recorded = Rc::clone(&delivered);
    let watched = bar_types.to_vec();
    let answer_cache = Rc::clone(&cache);
    msgbus::register_response_handler(
        &request_id,
        TypedHandler::from(move |envelope: &DataResponseEnvelope| {
            if let Some(DataResponse::Bars(resp)) = envelope.response() {
                for bar in resp.data.iter() {
                    let cached = cached_bars(&answer_cache, &watched);
                    recorded.borrow_mut().push(OrderedDelivery::Bar(
                        bar.bar_type,
                        bar.ts_event,
                        cached,
                    ));
                }
            }
        }),
    );
    for bar_type in bar_types {
        let recorded = Rc::clone(&delivered);
        let cache = Rc::clone(&cache);
        let watched = bar_types.to_vec();
        let handler = TypedHandler::from(move |bar: &Bar| {
            let cached = cached_bars(&cache, &watched);
            recorded
                .borrow_mut()
                .push(OrderedDelivery::Bar(bar.bar_type, bar.ts_event, cached));
        });
        msgbus::subscribe_bars(
            switchboard::get_bars_topic(bar_type.standard()).into(),
            handler,
            None,
        );
    }

    let tape = order_tape(instrument_id);
    let answered = match end {
        HeldEnd::Failure => 0,
        HeldEnd::Answer(answered) => answered,
    };
    let (first, last) = bar_types.split_at(bar_types.len() - usize::from(late > 0));
    subscribe_order_bars(&mut data_engine, first, client_id);
    for (index, trade) in tape[answered..].iter().enumerate() {
        if index == late {
            subscribe_order_bars(&mut data_engine, last, client_id);
        }
        data_engine.process_data(Data::Trade(*trade));
    }

    let envelope = match end {
        HeldEnd::Failure => {
            DataResponseEnvelope::terminal(request_id, Some("the source stopped".to_owned()))
        }
        HeldEnd::Answer(answered) => DataResponseEnvelope::new(
            DataResponse::Trades(TradesResponse::new(
                request_id,
                client_id,
                instrument_id,
                tape[..answered].to_vec(),
                None,
                None,
                tape[answered - 1].ts_init,
                Some(serde_json::from_value(params).unwrap()),
            )),
            true,
            None,
        ),
    };
    data_engine.response_envelope(envelope);

    delivered.take()
}

// Over the order tape: 1-TICK and 2-TICK complete a bar at every trade and at every second one;
// the 10-RANGE bar closes before t3 and t6, the 10-RANGE_INCLUSIVE bar after t3 and t5; the
// 10-RANGE_INCLUSIVE bar type is named, and fed, before 3-TICK, whose bar closes before t3 and t6
#[rstest]
#[case::two_ticks(&["1-TICK", "2-TICK"])]
#[case::range_and_range_inclusive(&["10-RANGE", "10-RANGE_INCLUSIVE"])]
#[case::range_inclusive_fed_first(&["10-RANGE_INCLUSIVE", "3-TICK"])]
fn test_interleaved_failure_releases_held_trades_to_two_bar_types_trade_by_trade_as_live(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] specs: &[&str],
) {
    let _ = stub_msgbus;
    let bar_types = order_bar_types(audusd_sim.id, specs);

    let live = live_bars(audusd_sim.clone(), client_id, venue, &bar_types, 0);
    let released = released_bars(
        audusd_sim,
        client_id,
        venue,
        &bar_types,
        0,
        HeldEnd::Failure,
    );

    assert_eq!(released, live);
}

#[rstest]
#[case::two_ticks(&["1-TICK", "2-TICK"])]
#[case::range_and_range_inclusive(&["10-RANGE", "10-RANGE_INCLUSIVE"])]
fn test_interleaved_completion_releases_held_trades_to_two_bar_types_trade_by_trade_as_live(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] specs: &[&str],
) {
    let _ = stub_msgbus;
    let bar_types = order_bar_types(audusd_sim.id, specs);

    let live = live_bars(audusd_sim.clone(), client_id, venue, &bar_types, 0);
    let released = released_bars(
        audusd_sim,
        client_id,
        venue,
        &bar_types,
        0,
        HeldEnd::Answer(2),
    );

    assert_eq!(released, live);
}

#[rstest]
fn test_interleaved_release_feeds_a_bar_type_subscribed_later_only_the_trades_it_held(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let bar_types = order_bar_types(audusd_sim.id, &["1-TICK", "2-TICK"]);

    // 2-TICK is subscribed after t2: it holds t3 to t6, 1-TICK all six
    let live = live_bars(audusd_sim.clone(), client_id, venue, &bar_types, 2);
    let released = released_bars(
        audusd_sim,
        client_id,
        venue,
        &bar_types,
        2,
        HeldEnd::Failure,
    );

    assert_eq!(released, live);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_interleaved_joined_release_cuts_and_floors_each_bar_type_trade_by_trade(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let tick = fixture.bar_type;
    let volume = BarType::from(format!("{id}-1000-VOLUME-LAST-INTERNAL").as_str());
    let cache = Rc::clone(fixture.engine.borrow().cache());
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let recorded = Rc::clone(&delivered);

    run_joined_release_observed(
        &fixture,
        &[tick, volume],
        true,
        &[
            release_trade(id, 100 * MS_NS + 500_000, 210 * MS_NS, "0.65005", "same-ms"),
            release_trade(id, 101 * MS_NS, 199 * MS_NS, "0.65010", "h1"),
            release_trade(id, 102 * MS_NS, 400 * MS_NS, "0.65020", "h2"),
        ],
        vec![release_trade(id, 100 * MS_NS, 200 * MS_NS, "0.65000", "a1")],
        |request_id| {
            msgbus::register_response_handler(
                &request_id,
                TypedHandler::from(move |envelope: &DataResponseEnvelope| {
                    if let Some(DataResponse::Bars(resp)) = envelope.response() {
                        for bar in resp.data.iter() {
                            let cached = cached_bars(&cache, &[tick, volume]);
                            recorded.borrow_mut().push(OrderedDelivery::Bar(
                                bar.bar_type,
                                bar.ts_event,
                                cached,
                            ));
                        }
                    }
                }),
            );
        },
    );

    // Each builder drops the trade in the cut's millisecond and floors h1 at the answer's floor
    let expected = vec![
        at("0.65000", 200 * MS_NS),
        at("0.65010", 200 * MS_NS),
        at("0.65020", 400 * MS_NS),
    ];
    assert_eq!(built_bars(&fixture, tick), expected);
    assert_eq!(built_bars(&fixture, volume), expected);

    // The requester is given each held trade's bars of both types before the next trade's, as
    // live publishes them: a1's, then h1's, then h2's, each bar cached just before its part
    let bar = |bar_type, ts_init, cached: [usize; 2]| {
        OrderedDelivery::Bar(
            bar_type,
            UnixNanos::from(NY_2026_11_12_1800 + ts_init),
            cached.to_vec(),
        )
    };
    assert_eq!(
        *delivered.borrow(),
        vec![
            bar(tick, 200 * MS_NS, [1, 0]),
            bar(volume, 200 * MS_NS, [1, 1]),
            bar(tick, 200 * MS_NS, [2, 1]),
            bar(volume, 200 * MS_NS, [2, 2]),
            bar(tick, 400 * MS_NS, [3, 2]),
            bar(volume, 400 * MS_NS, [3, 3]),
        ]
    );
}

/// What the requester of an interleaving request with its own replay (no `update_subscriptions`)
/// is given, in order: each trade, and each bar with its volume, with the number of bars of
/// every watched bar type the cache holds at its callback; then the request's end, with its
/// failure reason.
#[derive(Clone, Debug, PartialEq)]
enum Replayed {
    Trade(TradeId, Vec<usize>),
    Bar(BarType, UnixNanos, Quantity, Vec<usize>),
    End(Option<String>),
}

fn traded(trade_id: &str, cached: &[usize]) -> Replayed {
    Replayed::Trade(TradeId::new(trade_id), cached.to_vec())
}

fn closed(bar_type: BarType, ts: u64, volume: u64, cached: &[usize]) -> Replayed {
    Replayed::Bar(
        bar_type.standard(),
        UnixNanos::from(ts),
        Quantity::from(volume),
        cached.to_vec(),
    )
}

/// The instant of a made-up civil time in New York, e.g. `2024-01-08T16:59:58`.
fn new_york(civil: &str) -> u64 {
    let zoned: jiff::Zoned = format!("{civil}[America/New_York]").parse().unwrap();
    u64::try_from(zoned.timestamp().as_nanosecond()).unwrap()
}

/// The instant of a made-up UTC time, e.g. `2024-01-08T10:00:00Z`.
fn utc_ns(value: &str) -> u64 {
    u64::try_from(value.parse::<jiff::Timestamp>().unwrap().as_nanosecond()).unwrap()
}

fn bar_type_of(instrument_id: InstrumentId, spec: &str) -> BarType {
    BarType::from(format!("{instrument_id}-{spec}-LAST-INTERNAL").as_str())
}

/// An interleaving trades request without `update_subscriptions`, as the engine clock reads
/// `dispatched` when it is dispatched.
#[derive(Clone, Copy, Default)]
struct ReplaySpec {
    dispatched: u64,
    start: Option<u64>,
    end: Option<u64>,
    limit: Option<usize>,
    time_zone: Option<&'static str>,
}

/// An engine configured by `config` with a mock client; its requester's deliveries.
struct ReplayFixture {
    engine: DataEngine,
    instrument_id: InstrumentId,
    client_id: ClientId,
    request_id: UUID4,
    delivered: Rc<RefCell<Vec<Replayed>>>,
}

impl ReplayFixture {
    /// Dispatches the request of `spec` for `bar_types` and records what its requester is
    /// given, watching the cache's bars of `bar_types`.
    fn execute(
        audusd_sim: CurrencyPair,
        client_id: ClientId,
        venue: Venue,
        config: DataEngineConfig,
        bar_types: &[BarType],
        spec: ReplaySpec,
    ) -> Self {
        let instrument_id = audusd_sim.id;
        let mut clock = VirtualClock::new();
        let _ = clock.advance_time(UnixNanos::from(spec.dispatched), true);
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(clock));
        let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
            .unwrap();
        let mut engine = DataEngine::new(clock, Rc::clone(&cache), Some(config));
        let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
        register_mock_client(
            Rc::new(RefCell::new(VirtualClock::new())),
            Rc::clone(&cache),
            client_id,
            venue,
            None,
            &recorder,
            &mut engine,
        );

        let mut params = json!({
            "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "interleave": true,
        });
        if let Some(time_zone) = spec.time_zone {
            params["time_zone"] = json!(time_zone);
        }
        let params: Params = serde_json::from_value(params).unwrap();
        let request_id = UUID4::new();

        let delivered = Rc::new(RefCell::new(Vec::new()));
        let recorded = Rc::clone(&delivered);
        let watched = bar_types.to_vec();
        let handler = TypedHandler::from(move |envelope: &DataResponseEnvelope| {
            match envelope.response() {
                Some(DataResponse::Trades(resp)) => {
                    for trade in resp.data.iter() {
                        let cached = cached_bars(&cache, &watched);
                        recorded
                            .borrow_mut()
                            .push(Replayed::Trade(trade.trade_id, cached));
                    }
                }
                Some(DataResponse::Bars(resp)) => {
                    for bar in resp.data.iter() {
                        let cached = cached_bars(&cache, &watched);
                        recorded.borrow_mut().push(Replayed::Bar(
                            bar.bar_type,
                            bar.ts_event,
                            bar.volume,
                            cached,
                        ));
                    }
                }
                _ => {}
            }

            if envelope.completed() {
                recorded
                    .borrow_mut()
                    .push(Replayed::End(envelope.failure_reason().map(str::to_owned)));
            }
        });
        msgbus::register_response_handler(&request_id, handler);

        let at = |ns: Option<u64>| ns.map(|ns| UnixNanos::from(ns).to_datetime_utc());
        engine
            .execute_request(RequestCommand::Trades(RequestTrades::new(
                instrument_id,
                at(spec.start),
                at(spec.end),
                spec.limit.map(|limit| NonZeroUsize::new(limit).unwrap()),
                Some(client_id),
                request_id,
                UnixNanos::from(spec.dispatched),
                Some(params),
            )))
            .unwrap();

        Self {
            engine,
            instrument_id,
            client_id,
            request_id,
            delivered,
        }
    }

    /// Answers the request in one envelope with trades (`ts`, size, trade ID) at one price.
    fn answer(&mut self, trades: &[(u64, u64, &str)], completed: bool, failure: Option<&str>) {
        let trades: Vec<_> = trades
            .iter()
            .map(|(ts, size, trade_id)| {
                make_trade(self.instrument_id, "1.00000", *size, trade_id, *ts)
            })
            .collect();
        let envelope = DataResponseEnvelope::new(
            DataResponse::Trades(TradesResponse::new(
                self.request_id,
                self.client_id,
                self.instrument_id,
                trades,
                None,
                None,
                UnixNanos::default(),
                None,
            )),
            completed,
            None,
        );
        let envelope = match failure {
            Some(reason) => envelope.with_failure(reason.to_owned()),
            None => envelope,
        };
        self.engine.response_envelope(envelope);
    }

    fn delivered(&self) -> Vec<Replayed> {
        self.delivered.borrow().clone()
    }
}

fn without_flat_bars() -> DataEngineConfig {
    DataEngineConfig {
        time_bars_build_with_no_updates: false,
        ..DataEngineConfig::default()
    }
}

#[rstest]
fn test_replayed_request_delivers_the_closes_after_its_last_trade_through_its_end(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, range) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "10-RANGE"));
    let spec = ReplaySpec {
        dispatched: new_york("2024-01-08T20:00:00"),
        start: Some(new_york("2024-01-08T16:50:00")),
        end: Some(new_york("2024-01-08T18:00:00") + 1),
        ..ReplaySpec::default()
    };
    let mut fixture = ReplayFixture::execute(
        audusd_sim,
        client_id,
        venue,
        without_flat_bars(),
        &[five, range],
        spec,
    );

    fixture.answer(
        &[
            (new_york("2024-01-08T16:52:00"), 1, "t1"),
            (new_york("2024-01-08T16:57:00"), 2, "t2"),
            (new_york("2024-01-08T16:59:58"), 3, "t3"),
        ],
        true,
        None,
    );

    // The 17:00 bar closes and the 18:00 boundary completes the range bar after the last trade
    assert_eq!(
        fixture.delivered(),
        vec![
            traded("t1", &[0, 0]),
            closed(five, new_york("2024-01-08T16:55:00"), 1, &[1, 0]),
            traded("t2", &[1, 0]),
            traded("t3", &[1, 0]),
            closed(five, new_york("2024-01-08T17:00:00"), 5, &[2, 0]),
            closed(range, new_york("2024-01-08T18:00:00"), 6, &[2, 1]),
            Replayed::End(None),
        ]
    );
}

#[rstest]
fn test_replayed_trade_at_a_close_is_in_the_closing_bar_and_at_the_boundary_completes_the_range_bar(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, range) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "10-RANGE"));
    let spec = ReplaySpec {
        dispatched: new_york("2024-01-08T20:00:00"),
        start: Some(new_york("2024-01-08T17:50:00")),
        end: Some(new_york("2024-01-08T18:02:00")),
        ..ReplaySpec::default()
    };
    let mut fixture = ReplayFixture::execute(
        audusd_sim,
        client_id,
        venue,
        without_flat_bars(),
        &[five, range],
        spec,
    );

    fixture.answer(
        &[
            (new_york("2024-01-08T17:52:00"), 1, "a"),
            (new_york("2024-01-08T17:55:00"), 2, "b"),
            (new_york("2024-01-08T18:00:00"), 4, "c"),
            (new_york("2024-01-08T18:01:00"), 8, "d"),
        ],
        true,
        None,
    );

    // b is in the bar closing at its instant, which comes after it; c's own update completes the
    // range bar at the boundary, before c, and c is in the 5-minute bar closing at 18:00
    assert_eq!(
        fixture.delivered(),
        vec![
            traded("a", &[0, 0]),
            traded("b", &[0, 0]),
            closed(five, new_york("2024-01-08T17:55:00"), 3, &[1, 0]),
            closed(range, new_york("2024-01-08T18:00:00"), 3, &[1, 1]),
            traded("c", &[1, 1]),
            closed(five, new_york("2024-01-08T18:00:00"), 4, &[2, 1]),
            traded("d", &[2, 1]),
            Replayed::End(None),
        ]
    );
}

#[rstest]
fn test_replayed_time_bars_whose_closes_interleave_come_in_close_order_ties_by_timer_name(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (two, three) = (bar_type_of(id, "2-MINUTE"), bar_type_of(id, "3-MINUTE"));
    let spec = ReplaySpec {
        dispatched: utc_ns("2024-01-08T12:00:00Z"),
        start: Some(utc_ns("2024-01-08T10:00:00Z")),
        end: Some(utc_ns("2024-01-08T10:07:59Z")),
        ..ReplaySpec::default()
    };
    let mut fixture = ReplayFixture::execute(
        audusd_sim,
        client_id,
        venue,
        DataEngineConfig::default(),
        &[three, two],
        spec,
    );

    fixture.answer(
        &[
            (utc_ns("2024-01-08T10:00:30Z"), 1, "t1"),
            (utc_ns("2024-01-08T10:07:30Z"), 2, "t2"),
        ],
        true,
        None,
    );

    // Named out of order: the bars come in close order, the 10:06 tie in timer name order
    assert_eq!(
        fixture.delivered(),
        vec![
            traded("t1", &[0, 0]),
            closed(two, utc_ns("2024-01-08T10:02:00Z"), 1, &[0, 1]),
            closed(three, utc_ns("2024-01-08T10:03:00Z"), 1, &[1, 1]),
            closed(two, utc_ns("2024-01-08T10:04:00Z"), 0, &[1, 2]),
            closed(two, utc_ns("2024-01-08T10:06:00Z"), 0, &[1, 3]),
            closed(three, utc_ns("2024-01-08T10:06:00Z"), 0, &[2, 3]),
            traded("t2", &[2, 3]),
            Replayed::End(None),
        ]
    );
}

#[rstest]
fn test_replayed_range_boundary_and_time_close_at_one_instant_come_range_then_time(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, range) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "10-RANGE"));
    let spec = ReplaySpec {
        dispatched: new_york("2024-01-08T20:00:00"),
        start: Some(new_york("2024-01-08T17:56:00")),
        end: Some(new_york("2024-01-08T18:04:00")),
        ..ReplaySpec::default()
    };
    let mut fixture = ReplayFixture::execute(
        audusd_sim,
        client_id,
        venue,
        without_flat_bars(),
        &[five, range],
        spec,
    );

    fixture.answer(
        &[
            (new_york("2024-01-08T17:58:00"), 1, "t1"),
            (new_york("2024-01-08T18:03:00"), 2, "t2"),
        ],
        true,
        None,
    );

    // No record at 18:00: the clock's order, `RANGE_BAR_` before `TIME_BAR_`
    assert_eq!(
        fixture.delivered(),
        vec![
            traded("t1", &[0, 0]),
            closed(range, new_york("2024-01-08T18:00:00"), 1, &[0, 1]),
            closed(five, new_york("2024-01-08T18:00:00"), 1, &[1, 1]),
            traded("t2", &[1, 1]),
            Replayed::End(None),
        ]
    );
}

#[rstest]
fn test_replayed_right_open_time_bar_takes_a_trade_at_its_close_as_a_subscription_does(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let config = DataEngineConfig {
        time_bars_interval_type: BarIntervalType::RightOpen,
        ..without_flat_bars()
    };
    let spec = ReplaySpec {
        dispatched: utc_ns("2024-01-08T12:00:00Z"),
        start: Some(utc_ns("2024-01-08T10:00:00Z")),
        end: Some(utc_ns("2024-01-08T10:10:00Z")),
        ..ReplaySpec::default()
    };
    let mut fixture = ReplayFixture::execute(audusd_sim, client_id, venue, config, &[five], spec);

    fixture.answer(
        &[
            (utc_ns("2024-01-08T10:01:00Z"), 1, "a"),
            (utc_ns("2024-01-08T10:05:00Z"), 2, "b"),
            (utc_ns("2024-01-08T10:06:00Z"), 4, "c"),
        ],
        true,
        None,
    );

    // The timer at b's instant fires after b, as a backtest runs it; a right-open bar is
    // stamped at its open
    assert_eq!(
        fixture.delivered(),
        vec![
            traded("a", &[0]),
            traded("b", &[0]),
            closed(five, utc_ns("2024-01-08T10:00:00Z"), 3, &[1]),
            traded("c", &[1]),
            closed(five, utc_ns("2024-01-08T10:05:00Z"), 4, &[2]),
            Replayed::End(None),
        ]
    );
}

#[rstest]
#[case::flat_bars(true)]
#[case::no_flat_bars(false)]
fn test_replayed_weekend_completes_the_range_bar_once_at_friday_18_00(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] build_with_no_updates: bool,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (hour, range) = (bar_type_of(id, "1-HOUR"), bar_type_of(id, "10-RANGE"));
    let config = DataEngineConfig {
        time_bars_build_with_no_updates: build_with_no_updates,
        ..DataEngineConfig::default()
    };
    let spec = ReplaySpec {
        dispatched: new_york("2024-01-15T09:00:00"),
        start: Some(new_york("2024-01-12T16:00:00")),
        end: Some(new_york("2024-01-14T18:00:00") + 1),
        ..ReplaySpec::default()
    };
    let mut fixture =
        ReplayFixture::execute(audusd_sim, client_id, venue, config, &[hour, range], spec);

    fixture.answer(
        &[
            (new_york("2024-01-12T16:30:00"), 1, "t1"),
            (new_york("2024-01-12T16:59:58"), 2, "t2"),
        ],
        true,
        None,
    );

    let friday_18_00 = new_york("2024-01-12T18:00:00");
    let mut expected = vec![
        traded("t1", &[0, 0]),
        traded("t2", &[0, 0]),
        closed(hour, new_york("2024-01-12T17:00:00"), 3, &[1, 0]),
        closed(range, friday_18_00, 3, &[1, 1]),
    ];
    if build_with_no_updates {
        // Every hour from Friday 18:00 to Sunday 18:00, after the range bar at a tie
        for (index, cached) in (0..=48).zip(2..) {
            let ts = friday_18_00 + index * 3_600 * NANOSECONDS_IN_SECOND;
            expected.push(closed(hour, ts, 0, &[cached, 1]));
        }
    }
    expected.push(Replayed::End(None));
    assert_eq!(fixture.delivered(), expected);
}

#[rstest]
fn test_replayed_two_day_grid_starts_at_the_request_start_not_at_the_first_trade(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let two_days = bar_type_of(audusd_sim.id, "2-DAY");
    let spec = ReplaySpec {
        dispatched: utc_ns("2024-01-11T00:00:00Z"),
        start: Some(utc_ns("2024-01-08T12:00:00Z")),
        end: Some(utc_ns("2024-01-10T02:00:00Z")),
        ..ReplaySpec::default()
    };
    let mut fixture = ReplayFixture::execute(
        audusd_sim,
        client_id,
        venue,
        DataEngineConfig::default(),
        &[two_days],
        spec,
    );

    fixture.answer(
        &[
            (utc_ns("2024-01-09T01:00:00Z"), 1, "t1"),
            (utc_ns("2024-01-10T01:00:00Z"), 2, "t2"),
        ],
        true,
        None,
    );

    // Anchored on Monday, the start: the bar closes Wednesday 00:00, not Thursday
    assert_eq!(
        fixture.delivered(),
        vec![
            traded("t1", &[0]),
            closed(two_days, utc_ns("2024-01-10T00:00:00Z"), 1, &[1]),
            traded("t2", &[1]),
            Replayed::End(None),
        ]
    );
}

#[rstest]
fn test_replayed_month_bar_closes_at_the_month_end_with_no_trade_near_it(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let month = bar_type_of(audusd_sim.id, "1-MONTH");
    let spec = ReplaySpec {
        dispatched: utc_ns("2024-03-05T00:00:00Z"),
        start: Some(utc_ns("2024-01-10T00:00:00Z")),
        end: Some(utc_ns("2024-02-21T00:00:00Z")),
        ..ReplaySpec::default()
    };
    let mut fixture = ReplayFixture::execute(
        audusd_sim,
        client_id,
        venue,
        DataEngineConfig::default(),
        &[month],
        spec,
    );

    fixture.answer(
        &[
            (utc_ns("2024-01-15T00:00:00Z"), 1, "t1"),
            (utc_ns("2024-02-20T00:00:00Z"), 2, "t2"),
        ],
        true,
        None,
    );

    assert_eq!(
        fixture.delivered(),
        vec![
            traded("t1", &[0]),
            closed(month, utc_ns("2024-02-01T00:00:00Z"), 1, &[1]),
            traded("t2", &[1]),
            Replayed::End(None),
        ]
    );
}

#[rstest]
fn test_replayed_day_bars_follow_new_york_midnight_across_a_daylight_saving_change(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let day = bar_type_of(audusd_sim.id, "1-DAY");
    let spec = ReplaySpec {
        dispatched: new_york("2024-03-12T00:00:00"),
        start: Some(new_york("2024-03-08T12:00:00")),
        end: Some(new_york("2024-03-11T10:00:00")),
        time_zone: Some("America/New_York"),
        ..ReplaySpec::default()
    };
    let mut fixture = ReplayFixture::execute(
        audusd_sim,
        client_id,
        venue,
        DataEngineConfig::default(),
        &[day],
        spec,
    );

    fixture.answer(
        &[
            (new_york("2024-03-08T13:00:00"), 1, "t1"),
            (new_york("2024-03-11T09:00:00"), 2, "t2"),
        ],
        true,
        None,
    );

    // Daylight saving starts 2024-03-10 02:00: that day lasts 23 hours
    assert_eq!(
        fixture.delivered(),
        vec![
            traded("t1", &[0]),
            closed(day, new_york("2024-03-09T00:00:00"), 1, &[1]),
            closed(day, new_york("2024-03-10T00:00:00"), 0, &[2]),
            closed(day, new_york("2024-03-11T00:00:00"), 0, &[3]),
            traded("t2", &[3]),
            Replayed::End(None),
        ]
    );
    assert_eq!(
        new_york("2024-03-11T00:00:00") - new_york("2024-03-10T00:00:00"),
        23 * 3_600 * NANOSECONDS_IN_SECOND
    );
}

#[rstest]
#[case::no_end(None)]
#[case::end_ahead_of_the_engine_clock(Some("2024-01-08T10:20:00Z"))]
fn test_replayed_closes_after_the_dispatch_time_do_not_fire(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] end: Option<&str>,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let spec = ReplaySpec {
        dispatched: utc_ns("2024-01-08T10:07:30Z"),
        start: Some(utc_ns("2024-01-08T10:00:00Z")),
        end: end.map(utc_ns),
        ..ReplaySpec::default()
    };
    let mut fixture = ReplayFixture::execute(
        audusd_sim,
        client_id,
        venue,
        DataEngineConfig::default(),
        &[five],
        spec,
    );
    // The engine clock moves on before the answer: the dispatch time bounds the coverage
    let _ = fixture
        .engine
        .clock()
        .borrow_mut()
        .as_any_mut()
        .downcast_mut::<VirtualClock>()
        .unwrap()
        .advance_time(UnixNanos::from(utc_ns("2024-01-08T10:30:00Z")), true);

    fixture.answer(
        &[
            (utc_ns("2024-01-08T10:01:00Z"), 1, "t1"),
            (utc_ns("2024-01-08T10:03:00Z"), 2, "t2"),
        ],
        true,
        None,
    );

    assert_eq!(
        fixture.delivered(),
        vec![
            traded("t1", &[0]),
            traded("t2", &[0]),
            closed(five, utc_ns("2024-01-08T10:05:00Z"), 3, &[1]),
            Replayed::End(None),
        ]
    );
}

/// How a replayed request ends without driving its replay through its end.
#[derive(Clone, Copy, Debug)]
enum ReplayStop {
    /// Its answer completes, but it named a `limit`.
    Limit,
    /// Its answer completes with a failure reason.
    FailedAnswer,
    /// A payload-free terminal with a failure reason follows a partial answer.
    FailedAfterParts,
}

#[rstest]
#[case::limit(ReplayStop::Limit)]
#[case::failed_answer(ReplayStop::FailedAnswer)]
#[case::failed_after_parts(ReplayStop::FailedAfterParts)]
fn test_replayed_request_with_a_limit_or_a_failure_fires_no_close_after_its_last_trade(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] stop: ReplayStop,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, range) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "10-RANGE"));
    let spec = ReplaySpec {
        dispatched: new_york("2024-01-08T20:00:00"),
        start: Some(new_york("2024-01-08T16:50:00")),
        end: Some(new_york("2024-01-08T18:00:00") + 1),
        limit: matches!(stop, ReplayStop::Limit).then_some(100),
        ..ReplaySpec::default()
    };
    let tap = TappedResponses::install();
    let mut fixture = ReplayFixture::execute(
        audusd_sim,
        client_id,
        venue,
        without_flat_bars(),
        &[five, range],
        spec,
    );
    let trades = [
        (new_york("2024-01-08T16:52:00"), 1, "t1"),
        (new_york("2024-01-08T16:57:00"), 2, "t2"),
        (new_york("2024-01-08T16:59:58"), 3, "t3"),
    ];
    let reason = "the source stopped";

    let failure = match stop {
        ReplayStop::Limit => {
            fixture.answer(&trades, true, None);
            None
        }
        ReplayStop::FailedAnswer => {
            fixture.answer(&trades, true, Some(reason));
            Some(reason.to_owned())
        }
        ReplayStop::FailedAfterParts => {
            fixture.answer(&trades, false, None);
            fixture
                .engine
                .response_envelope(DataResponseEnvelope::terminal(
                    fixture.request_id,
                    Some(reason.to_owned()),
                ));
            Some(reason.to_owned())
        }
    };

    assert_eq!(
        fixture.delivered(),
        vec![
            traded("t1", &[0, 0]),
            closed(five, new_york("2024-01-08T16:55:00"), 1, &[1, 0]),
            traded("t2", &[1, 0]),
            traded("t3", &[1, 0]),
            Replayed::End(failure),
        ],
        "{stop:?}"
    );
    let terminals = tap
        .take(fixture.request_id)
        .into_iter()
        .filter(|delivered| delivered.completed)
        .count();
    assert_eq!(terminals, 1, "{stop:?}");
}

/// Each watched bar type's bars in the cache, oldest first, as (close stamp, volume).
type CachedBars = Vec<Vec<(u64, u64)>>;

/// A request that builds bars without `interleave` or `update_subscriptions` (the plain path),
/// dispatched as the engine clock reads `dispatched`: the bars of the watched bar types that
/// reach the cache, and the failure reason of each terminal its requester is given.
struct PlainFixture {
    engine: DataEngine,
    cache: Rc<RefCell<Cache>>,
    recorder: Rc<RefCell<Vec<DataCommand>>>,
    client_id: ClientId,
    watched: Vec<BarType>,
    ends: Rc<RefCell<Vec<Option<String>>>>,
}

impl PlainFixture {
    fn new(
        instruments: Vec<InstrumentAny>,
        client_id: ClientId,
        venue: Venue,
        config: DataEngineConfig,
        dispatched: u64,
        watched: &[BarType],
    ) -> Self {
        let clock = data_engine_clock_at(dispatched);
        let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
        for instrument in instruments {
            cache.borrow_mut().add_instrument(instrument).unwrap();
        }
        let mut engine = DataEngine::new(clock, Rc::clone(&cache), Some(config));
        let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
        register_mock_client(
            Rc::new(RefCell::new(VirtualClock::new())),
            Rc::clone(&cache),
            client_id,
            venue,
            None,
            &recorder,
            &mut engine,
        );

        Self {
            engine,
            cache,
            recorder,
            client_id,
            watched: watched.iter().map(BarType::standard).collect(),
            ends: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Executes `request`, recording the failure reason of each terminal its requester is given.
    fn execute(&mut self, request: RequestCommand) {
        let ends = Rc::clone(&self.ends);
        let handler = TypedHandler::from(move |envelope: &DataResponseEnvelope| {
            if envelope.completed() {
                ends.borrow_mut()
                    .push(envelope.failure_reason().map(str::to_owned));
            }
        });
        msgbus::register_response_handler(request.request_id(), handler);
        self.engine.execute_request(request).unwrap();
    }

    /// Sends `response` in one envelope and returns the watched bars in the cache after it.
    fn answer(
        &mut self,
        response: DataResponse,
        completed: bool,
        failure: Option<&str>,
    ) -> CachedBars {
        let envelope = DataResponseEnvelope::new(response, completed, None);
        let envelope = match failure {
            Some(reason) => envelope.with_failure(reason.to_owned()),
            None => envelope,
        };
        self.engine.response_envelope(envelope);
        self.cached()
    }

    fn cached(&self) -> CachedBars {
        self.watched
            .iter()
            .map(|bar_type| {
                let mut bars: Vec<_> = self
                    .cache
                    .borrow()
                    .bars(bar_type)
                    .unwrap_or_default()
                    .iter()
                    .map(|bar| (bar.ts_event.as_u64(), bar.volume.as_f64() as u64))
                    .collect();
                bars.reverse();
                bars
            })
            .collect()
    }

    /// The trades requests the engine dispatched to the client, in order (a pipeline's
    /// children).
    fn trades_requests(&self) -> Vec<RequestTrades> {
        self.recorder
            .borrow()
            .iter()
            .filter_map(|command| match command {
                DataCommand::Request(RequestCommand::Trades(request)) => Some(request.clone()),
                _ => None,
            })
            .collect()
    }

    fn ends(&self) -> Vec<Option<String>> {
        self.ends.borrow().clone()
    }
}

fn plain_params(bar_types: &[BarType], extra: Value) -> Params {
    let mut params = json!({
        "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
    });
    if let Value::Object(extra) = extra {
        for (key, value) in extra {
            params[key] = value;
        }
    }
    serde_json::from_value(params).unwrap()
}

fn timestamp_of(ns: Option<u64>) -> Option<jiff::Timestamp> {
    ns.map(|ns| UnixNanos::from(ns).to_datetime_utc())
}

/// A trades request for `instrument_id` with `params`, in the window `start`..`end`.
fn plain_trades_request(
    instrument_id: InstrumentId,
    client_id: ClientId,
    request_id: UUID4,
    window: (Option<u64>, Option<u64>),
    limit: Option<usize>,
    params: Params,
) -> RequestCommand {
    RequestCommand::Trades(RequestTrades::new(
        instrument_id,
        timestamp_of(window.0),
        timestamp_of(window.1),
        limit.map(|limit| NonZeroUsize::new(limit).unwrap()),
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(params),
    ))
}

/// An answer of trades (`ts`, price, size) under `request_id`.
fn trades_answer(
    request_id: UUID4,
    client_id: ClientId,
    instrument_id: InstrumentId,
    trades: &[(u64, &str, u64)],
) -> DataResponse {
    let trades: Vec<_> = trades
        .iter()
        .map(|(ts, price, size)| make_trade(instrument_id, price, *size, &format!("t{ts}"), *ts))
        .collect();
    DataResponse::Trades(TradesResponse::new(
        request_id,
        client_id,
        instrument_id,
        trades,
        None,
        None,
        UnixNanos::default(),
        None,
    ))
}

/// The example: a plain trades request whose last trade is at 16:59:58 and whose end is just past
/// the 18:00 range boundary. Each trade comes in a partial envelope of its own, so the cache is
/// read after each record.
#[rstest]
fn test_plain_request_delivers_the_closes_after_its_last_trade_through_its_end(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, range) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "10-RANGE"));
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        new_york("2024-01-08T20:00:00"),
        &[five, range],
    );
    let request_id = UUID4::new();
    fixture.execute(plain_trades_request(
        id,
        client_id,
        request_id,
        (
            Some(new_york("2024-01-08T16:50:00")),
            Some(new_york("2024-01-08T18:00:00") + 1),
        ),
        None,
        plain_params(&[five, range], json!({})),
    ));

    let mut steps = Vec::new();
    for (ts, size) in [
        (new_york("2024-01-08T16:52:00"), 1),
        (new_york("2024-01-08T16:57:00"), 2),
        (new_york("2024-01-08T16:59:58"), 3),
    ] {
        let answer = trades_answer(request_id, client_id, id, &[(ts, "1.00000", size)]);
        steps.push(fixture.answer(answer, false, None));
    }
    let answer = trades_answer(request_id, client_id, id, &[]);
    steps.push(fixture.answer(answer, true, None));

    // The 16:55 close fires before the 16:57 trade; after the last trade the end of the answer
    // fires the 17:00 close and then the 18:00 boundary, whose range bar holds every trade
    let five_minute = |civil: &str, volume| (new_york(civil), volume);
    assert_eq!(
        steps,
        vec![
            vec![vec![], vec![]],
            vec![vec![five_minute("2024-01-08T16:55:00", 1)], vec![]],
            vec![vec![five_minute("2024-01-08T16:55:00", 1)], vec![]],
            vec![
                vec![
                    five_minute("2024-01-08T16:55:00", 1),
                    five_minute("2024-01-08T17:00:00", 5),
                ],
                vec![five_minute("2024-01-08T18:00:00", 6)],
            ],
        ]
    );
    assert_eq!(fixture.ends(), vec![None]);
}

/// A 15-MINUTE bar built from the request's own 5-MINUTE bar, listed after or before it.
#[rstest]
#[case::source_first(false)]
#[case::dependent_first(true)]
fn test_plain_request_bar_built_from_its_own_bar_holds_the_source_bar_closing_with_it(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] dependent_first: bool,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let fifteen = BarType::from(format!("{id}-15-MINUTE-LAST-INTERNAL@5-MINUTE-INTERNAL").as_str());
    let listed = if dependent_first {
        [fifteen, five]
    } else {
        [five, fifteen]
    };
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        utc_ns("2024-01-08T12:00:00Z"),
        &[five, fifteen],
    );
    let request_id = UUID4::new();
    fixture.execute(plain_trades_request(
        id,
        client_id,
        request_id,
        (
            Some(utc_ns("2024-01-08T10:00:00Z")),
            Some(utc_ns("2024-01-08T10:20:00Z")),
        ),
        None,
        plain_params(&listed, json!({})),
    ));

    let answer = trades_answer(
        request_id,
        client_id,
        id,
        &[
            (utc_ns("2024-01-08T10:01:00Z"), "1.00000", 1),
            (utc_ns("2024-01-08T10:06:00Z"), "1.00000", 2),
            (utc_ns("2024-01-08T10:11:00Z"), "1.00000", 4),
            (utc_ns("2024-01-08T10:16:00Z"), "1.00000", 8),
        ],
    );
    let cached = fixture.answer(answer, true, None);

    // At 10:15 the source closes first, so its bar (4) is in the 15-minute bar
    assert_eq!(
        cached,
        vec![
            vec![
                (utc_ns("2024-01-08T10:05:00Z"), 1),
                (utc_ns("2024-01-08T10:10:00Z"), 2),
                (utc_ns("2024-01-08T10:15:00Z"), 4),
                (utc_ns("2024-01-08T10:20:00Z"), 8),
            ],
            vec![(utc_ns("2024-01-08T10:15:00Z"), 7)],
        ]
    );
}

/// A bar type built from quotes (BID) on a plain quotes request.
#[rstest]
fn test_plain_quotes_request_closes_its_bid_bar_at_its_time_with_no_later_quote(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let bid = BarType::from(format!("{id}-5-MINUTE-BID-INTERNAL").as_str());
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        utc_ns("2024-01-08T12:00:00Z"),
        &[bid],
    );
    let request_id = UUID4::new();
    fixture.execute(RequestCommand::Quotes(RequestQuotes::new(
        id,
        timestamp_of(Some(utc_ns("2024-01-08T10:00:00Z"))),
        timestamp_of(Some(utc_ns("2024-01-08T10:05:00Z"))),
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(plain_params(&[bid], json!({}))),
    )));

    let quotes = [
        make_quote(id, "1.00000", "1.00010", utc_ns("2024-01-08T10:01:00Z")),
        make_quote(id, "1.00020", "1.00030", utc_ns("2024-01-08T10:03:00Z")),
    ];
    let answer = DataResponse::Quotes(QuotesResponse::new(
        request_id,
        client_id,
        id,
        quotes.to_vec(),
        None,
        None,
        UnixNanos::default(),
        None,
    ));
    let partial = fixture.answer(answer, false, None);
    let answer = DataResponse::Quotes(QuotesResponse::new(
        request_id,
        client_id,
        id,
        Vec::new(),
        None,
        None,
        UnixNanos::default(),
        None,
    ));
    let completed = fixture.answer(answer, true, None);

    assert_eq!(partial, vec![vec![]]);
    assert_eq!(completed, vec![vec![(utc_ns("2024-01-08T10:05:00Z"), 2)]]);
}

/// A plain bars request of external 1-MINUTE bars feeding a 5-MINUTE bar built from them.
#[rstest]
fn test_plain_bars_request_feeds_a_larger_time_bar_closing_at_its_time(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let minute = BarType::from(format!("{id}-1-MINUTE-LAST-EXTERNAL").as_str());
    let five = BarType::from(format!("{id}-5-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL").as_str());
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        utc_ns("2024-01-08T12:00:00Z"),
        &[five],
    );
    let request_id = UUID4::new();
    fixture.execute(RequestCommand::Bars(RequestBars::new(
        minute,
        timestamp_of(Some(utc_ns("2024-01-08T10:00:00Z"))),
        timestamp_of(Some(utc_ns("2024-01-08T10:10:00Z"))),
        None,
        Some(client_id),
        request_id,
        UnixNanos::default(),
        Some(plain_params(&[five], json!({}))),
    )));

    let bars = [
        ("2024-01-08T10:01:00Z", 1),
        ("2024-01-08T10:03:00Z", 2),
        ("2024-01-08T10:05:00Z", 4),
        ("2024-01-08T10:06:00Z", 8),
    ]
    .map(|(close, volume)| {
        make_bar(
            minute,
            "1.00000",
            "1.00000",
            "1.00000",
            "1.00000",
            volume,
            utc_ns(close),
        )
    });
    let mut steps = Vec::new();
    for bar in bars {
        let answer = DataResponse::Bars(BarsResponse::new(
            request_id,
            client_id,
            minute,
            vec![bar],
            None,
            None,
            UnixNanos::default(),
            None,
        ));
        steps.push(fixture.answer(answer, false, None));
    }
    let answer = DataResponse::Bars(BarsResponse::new(
        request_id,
        client_id,
        minute,
        Vec::new(),
        None,
        None,
        UnixNanos::default(),
        None,
    ));
    steps.push(fixture.answer(answer, true, None));

    // The 10:05 bar holds the 1-minute bar stamped 10:05 and closes before the 10:06 one
    let closed_at_five = (utc_ns("2024-01-08T10:05:00Z"), 7);
    assert_eq!(
        steps,
        vec![
            vec![vec![]],
            vec![vec![]],
            vec![vec![]],
            vec![vec![closed_at_five]],
            vec![vec![closed_at_five, (utc_ns("2024-01-08T10:10:00Z"), 8)]],
        ]
    );
}

/// A time-range pipeline over 10:00 to 10:06 in two 3-minute children: the first ends inside
/// the 5-minute interval that closes at 10:05.
fn time_range_fixture(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
    bar_type: BarType,
) -> (PlainFixture, UUID4) {
    let id = audusd_sim.id;
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        utc_ns("2024-01-08T12:00:00Z"),
        &[bar_type],
    );
    let request_id = UUID4::new();
    fixture.execute(plain_trades_request(
        id,
        client_id,
        request_id,
        (
            Some(utc_ns("2024-01-08T10:00:00Z")),
            Some(utc_ns("2024-01-08T10:06:00Z")),
        ),
        None,
        plain_params(
            &[bar_type],
            json!({ "time_range_generator": "", "durations_seconds": [180] }),
        ),
    ));
    (fixture, request_id)
}

#[rstest]
fn test_time_range_pipeline_runs_one_replay_across_its_children_and_ends_it_once(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let (mut fixture, _) = time_range_fixture(audusd_sim, client_id, venue, five);

    let first = fixture.trades_requests()[0].request_id;
    let answer = trades_answer(
        first,
        client_id,
        id,
        &[
            (utc_ns("2024-01-08T10:01:00Z"), "1.00000", 1),
            (utc_ns("2024-01-08T10:02:00Z"), "1.00000", 2),
        ],
    );
    let after_first = fixture.answer(answer, true, None);
    let children = fixture.trades_requests();
    let second = children[1].request_id;
    let answer = trades_answer(
        second,
        client_id,
        id,
        &[(utc_ns("2024-01-08T10:04:00Z"), "1.00000", 4)],
    );
    let after_second = fixture.answer(answer, true, None);

    // Nothing closes at the first child's end; the parent's end (10:06) fires the 10:05 close
    // once, after the last child, and its bar holds both children's trades
    assert_eq!(children.len(), 2);
    assert_eq!(
        (children[0].end, children[1].start),
        (
            timestamp_of(Some(utc_ns("2024-01-08T10:03:00Z"))),
            timestamp_of(Some(utc_ns("2024-01-08T10:03:00Z") + 1)),
        )
    );
    assert_eq!(after_first, vec![vec![]]);
    assert_eq!(
        after_second,
        vec![vec![(utc_ns("2024-01-08T10:05:00Z"), 7)]]
    );
    assert_eq!(fixture.trades_requests().len(), 2);
    assert_eq!(fixture.ends(), vec![None]);
}

#[rstest]
fn test_time_range_pipeline_child_failure_fires_no_end_of_coverage(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let (mut fixture, _) = time_range_fixture(audusd_sim, client_id, venue, five);

    let first = fixture.trades_requests()[0].request_id;
    let answer = trades_answer(
        first,
        client_id,
        id,
        &[(utc_ns("2024-01-08T10:01:00Z"), "1.00000", 1)],
    );
    let after_first = fixture.answer(answer, false, None);
    let answer = trades_answer(first, client_id, id, &[]);
    let failed = fixture.answer(answer, true, Some("the child failed"));

    assert_eq!(after_first, vec![vec![]]);
    assert_eq!(failed, vec![vec![]]);
    assert_eq!(fixture.trades_requests().len(), 1);
    assert_eq!(fixture.ends(), vec![Some("the child failed".to_owned())]);
}

/// A plain answer that completes with a failure reason fires no close after its last record.
#[rstest]
fn test_plain_request_failed_answer_fires_no_end_of_coverage(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        utc_ns("2024-01-08T12:00:00Z"),
        &[five],
    );
    let request_id = UUID4::new();
    fixture.execute(plain_trades_request(
        id,
        client_id,
        request_id,
        (
            Some(utc_ns("2024-01-08T10:00:00Z")),
            Some(utc_ns("2024-01-08T10:10:00Z")),
        ),
        None,
        plain_params(&[five], json!({})),
    ));

    let answer = trades_answer(
        request_id,
        client_id,
        id,
        &[
            (utc_ns("2024-01-08T10:01:00Z"), "1.00000", 1),
            (utc_ns("2024-01-08T10:06:00Z"), "1.00000", 2),
        ],
    );
    let cached = fixture.answer(answer, true, Some("the source stopped"));

    assert_eq!(cached, vec![vec![(utc_ns("2024-01-08T10:05:00Z"), 1)]]);
    assert_eq!(fixture.ends(), vec![Some("the source stopped".to_owned())]);
}

/// A continuous-future request across two segments (ESH24 to 10:03, then ESM24): one replay
/// clock runs across them. A segment change only sets the aggregator's price adjustment, which
/// applies to each record as it is fed, so the bar open at the change continues.
#[rstest]
fn test_continuous_future_request_runs_one_replay_across_its_segments(
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let target = BarType::from("ES.GLBX-5-MINUTE-LAST-INTERNAL");
    let mut fixture = PlainFixture::new(
        Vec::new(),
        client_id,
        Venue::from("GLBX"),
        DataEngineConfig::default(),
        utc_ns("2024-01-08T11:00:00Z"),
        &[target],
    );
    let esh = add_es_contract(&fixture.cache, "ESH24.GLBX", "ESH24");
    let esm = add_es_contract(&fixture.cache, "ESM24.GLBX", "ESM24");
    let params = params_from_json(json!({
        "continuous_future_adjustment_mode": "BACKWARD_SPREAD",
        "continuous_future_transitions": [{
            "transition_time_ns": utc_ns("2024-01-08T10:03:00Z"),
            "pre_instrument_id": esh.to_string(),
            "post_instrument_id": esm.to_string(),
            "pre_price": "100.00",
            "post_price": "95.00"
        }]
    }));
    fixture.execute(RequestCommand::Bars(RequestBars::new(
        target,
        timestamp_of(Some(utc_ns("2024-01-08T10:00:00Z"))),
        timestamp_of(Some(utc_ns("2024-01-08T10:10:00Z"))),
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(params),
    )));

    let first = fixture.trades_requests()[0].clone();
    let answer = trades_answer(
        first.request_id,
        fixture.client_id,
        esh,
        &[(utc_ns("2024-01-08T10:01:00Z"), "100.00", 1)],
    );
    let after_first = fixture.answer(answer, true, None);
    let second = fixture.trades_requests()[1].clone();
    let answer = trades_answer(
        second.request_id,
        fixture.client_id,
        esm,
        &[
            (utc_ns("2024-01-08T10:04:00Z"), "96.00", 2),
            (utc_ns("2024-01-08T10:06:00Z"), "97.00", 4),
        ],
    );
    let after_second = fixture.answer(answer, true, None);

    assert_eq!(
        (first.instrument_ids, second.instrument_ids),
        (vec![esh], vec![esm]),
        "one child per segment"
    );
    assert_eq!(after_first, vec![vec![]]);
    assert_eq!(
        after_second,
        vec![vec![
            (utc_ns("2024-01-08T10:05:00Z"), 3),
            (utc_ns("2024-01-08T10:10:00Z"), 4),
        ]]
    );
    // The 10:05 bar, closed before the 10:06 trade
    let bar = fixture.cache.borrow().bars(&target).unwrap()[1];
    assert_eq!(
        (bar.open, bar.high, bar.low, bar.close),
        (
            Price::from("95.00"),
            Price::from("96.00"),
            Price::from("95.00"),
            Price::from("96.00"),
        ),
        "ESH24 adjusted by -5.00, ESM24 as is"
    );
    assert_eq!(fixture.ends(), vec![None]);
}

/// A plain request with a `limit` fires no close after its last trade.
#[rstest]
fn test_plain_request_with_a_limit_fires_no_end_of_coverage(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        utc_ns("2024-01-08T12:00:00Z"),
        &[five],
    );
    let request_id = UUID4::new();
    fixture.execute(plain_trades_request(
        id,
        client_id,
        request_id,
        (
            Some(utc_ns("2024-01-08T10:00:00Z")),
            Some(utc_ns("2024-01-08T10:10:00Z")),
        ),
        Some(100),
        plain_params(&[five], json!({})),
    ));

    let answer = trades_answer(
        request_id,
        client_id,
        id,
        &[
            (utc_ns("2024-01-08T10:01:00Z"), "1.00000", 1),
            (utc_ns("2024-01-08T10:06:00Z"), "1.00000", 2),
        ],
    );
    let cached = fixture.answer(answer, true, None);

    assert_eq!(cached, vec![vec![(utc_ns("2024-01-08T10:05:00Z"), 1)]]);
    assert_eq!(fixture.ends(), vec![None]);
}

/// A plain request with no `end`: its answer covers up to the engine clock's time when it was
/// dispatched (10:07:30), and nothing after it fires.
#[rstest]
fn test_plain_request_without_an_end_fires_nothing_after_its_dispatch_time(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let one = bar_type_of(id, "1-MINUTE");
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        DataEngineConfig::default(),
        utc_ns("2024-01-08T10:07:30Z"),
        &[one],
    );
    let request_id = UUID4::new();
    fixture.execute(plain_trades_request(
        id,
        client_id,
        request_id,
        (Some(utc_ns("2024-01-08T10:00:00Z")), None),
        None,
        plain_params(&[one], json!({})),
    ));

    let answer = trades_answer(
        request_id,
        client_id,
        id,
        &[(utc_ns("2024-01-08T10:01:30Z"), "1.00000", 1)],
    );
    let cached = fixture.answer(answer, true, None);

    // Flat bars after the trade, the last at 10:07
    let flat = |civil: &str| (utc_ns(civil), 0);
    assert_eq!(
        cached,
        vec![vec![
            (utc_ns("2024-01-08T10:02:00Z"), 1),
            flat("2024-01-08T10:03:00Z"),
            flat("2024-01-08T10:04:00Z"),
            flat("2024-01-08T10:05:00Z"),
            flat("2024-01-08T10:06:00Z"),
            flat("2024-01-08T10:07:00Z"),
        ]]
    );
}

/// What the requester of an interleaving `quotes` request with its own replay is given, part by
/// part: a quotes part (its quotes' `ts_init`), a trades part (its trade IDs) or a bars part
/// (its bar, with its volume), each with what the cache holds at its callback (the bars of
/// every watched bar type, then the instrument's quotes); then the request's end, with its
/// failure reason.
#[cfg(feature = "streaming")]
#[derive(Clone, Debug, PartialEq)]
enum Merged {
    Quotes(Vec<UnixNanos>, Vec<usize>),
    Trades(Vec<TradeId>, Vec<usize>),
    Bar(BarType, UnixNanos, Quantity, Vec<usize>),
    End(Option<String>),
}

#[cfg(feature = "streaming")]
fn merged_quotes(ts: &[u64], cached: &[usize]) -> Merged {
    Merged::Quotes(at_ns(ts), cached.to_vec())
}

#[cfg(feature = "streaming")]
fn merged_trades(ids: &[&str], cached: &[usize]) -> Merged {
    Merged::Trades(trade_ids(ids), cached.to_vec())
}

#[cfg(feature = "streaming")]
fn merged_bar(bar_type: BarType, ts: u64, volume: u64, cached: &[usize]) -> Merged {
    Merged::Bar(
        bar_type.standard(),
        UnixNanos::from(ts),
        Quantity::from(volume),
        cached.to_vec(),
    )
}

/// One catalog run over `window` holding quotes at `quotes` (one bid and ask) and `trades`.
#[cfg(feature = "streaming")]
fn merged_run<'a>(
    window: (u64, u64),
    quotes: &'a [(u64, &'a str, &'a str)],
    trades: &'a [(u64, &'a str)],
) -> QuotedRun<'a> {
    QuotedRun {
        interval: window,
        quotes,
        trades,
    }
}

/// Executes an interleaving `quotes` trades request for `bar_types` over `window` (no
/// `update_subscriptions`), answered by a made-up catalog of `runs`, and returns what its
/// requester was given.
#[cfg(feature = "streaming")]
fn run_merged(
    fixture: &JoinFixture,
    label: &str,
    bar_types: &[BarType],
    window: (u64, u64),
    runs: &[QuotedRun],
) -> Vec<Merged> {
    let _catalog_dir = register_quoted_catalog(fixture, label, runs);
    let params = json!({
        "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "update_subscriptions": false,
        "interleave": true,
        "quotes": true,
    });
    let request = windowed_trades_request(fixture, window, params);
    let delivered = tap_merged(fixture, *request.request_id(), bar_types);

    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
    delivered.take()
}

/// Records, as [`Merged`], every part the requester of `request_id` is sent.
#[cfg(feature = "streaming")]
fn tap_merged(
    fixture: &JoinFixture,
    request_id: UUID4,
    bar_types: &[BarType],
) -> Rc<RefCell<Vec<Merged>>> {
    let cache = Rc::clone(fixture.engine.borrow().cache());
    let instrument_id = fixture.instrument_id;
    let watched = bar_types.to_vec();
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let recorded = Rc::clone(&delivered);
    let handler = TypedHandler::from(move |envelope: &DataResponseEnvelope| {
        let mut cached = cached_bars(&cache, &watched);
        cached.push(cache.borrow().quote_count(&instrument_id));

        match envelope.response() {
            Some(DataResponse::Quotes(resp)) => recorded.borrow_mut().push(Merged::Quotes(
                resp.data.iter().map(|quote| quote.ts_init).collect(),
                cached,
            )),
            Some(DataResponse::Trades(resp)) => recorded.borrow_mut().push(Merged::Trades(
                resp.data.iter().map(|trade| trade.trade_id).collect(),
                cached,
            )),
            Some(DataResponse::Bars(resp)) => {
                for bar in resp.data.iter() {
                    recorded.borrow_mut().push(Merged::Bar(
                        bar.bar_type,
                        bar.ts_event,
                        bar.volume,
                        cached.clone(),
                    ));
                }
            }
            _ => {}
        }

        if envelope.completed() {
            recorded
                .borrow_mut()
                .push(Merged::End(envelope.failure_reason().map(str::to_owned)));
        }
    });
    msgbus::register_response_handler(&request_id, handler);
    delivered
}

/// A quote at one bid and ask, stamped `ts`.
#[cfg(feature = "streaming")]
const fn quote_at(ts: u64) -> (u64, &'static str, &'static str) {
    (ts, "1.0000", "1.0002")
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_answer_sends_a_bar_before_the_first_quote_after_its_close(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let t = |time: &str| utc_ns(&format!("2024-01-08T{time}Z"));
    let window = (t("09:59:00"), t("10:01:00"));

    let delivered = run_merged(
        &fixture,
        "merged-bar-before-quote",
        &[minute],
        window,
        &[merged_run(
            window,
            &[
                quote_at(t("09:59:10")),
                quote_at(t("10:00:01")),
                quote_at(t("10:00:03")),
            ],
            &[(t("09:59:30"), "t1"), (t("10:00:06"), "t2")],
        )],
    );

    // A continuous run delivers the 10:00 bar, then the 10:00:01 quote, then t2; the quote run
    // has no close between its quotes, so it stays one part; each quote enters the cache with
    // its part
    assert_eq!(
        delivered,
        vec![
            merged_quotes(&[t("09:59:10")], &[0, 1]),
            merged_trades(&["t1"], &[0, 1]),
            merged_bar(minute, t("10:00:00"), 1, &[1, 1]),
            merged_quotes(&[t("10:00:01"), t("10:00:03")], &[1, 3]),
            merged_trades(&["t2"], &[1, 3]),
            merged_bar(minute, t("10:01:00"), 1, &[2, 3]),
            merged_trades(&[], &[2, 3]),
            Merged::End(None),
        ]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_answer_quote_at_a_close_comes_before_the_closing_bar(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let t = |time: &str| utc_ns(&format!("2024-01-08T{time}Z"));
    let window = (t("09:59:00"), t("10:00:30"));

    let delivered = run_merged(
        &fixture,
        "merged-quote-at-a-close",
        &[minute],
        window,
        &[merged_run(
            window,
            &[quote_at(t("10:00:00")), quote_at(t("10:00:02"))],
            &[(t("09:59:30"), "t1"), (t("10:00:06"), "t2")],
        )],
    );

    // The close at the quote's own stamp stays armed: the quote, then the bar, before the next
    // later record (the 10:00:02 quote), so the run of quotes is split there
    assert_eq!(
        delivered,
        vec![
            merged_trades(&["t1"], &[0, 0]),
            merged_quotes(&[t("10:00:00")], &[0, 1]),
            merged_bar(minute, t("10:00:00"), 1, &[1, 1]),
            merged_quotes(&[t("10:00:02")], &[1, 2]),
            merged_trades(&["t2"], &[1, 2]),
            Merged::End(None),
        ]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_answer_quote_at_the_range_boundary_comes_before_the_range_then_the_time_bar(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let (five, range) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "10-RANGE"));
    let t = |time: &str| new_york(&format!("2024-01-08T{time}"));
    let window = (t("17:50:00"), t("18:02:00"));

    let delivered = run_merged(
        &fixture,
        "merged-quote-at-the-boundary",
        &[five, range],
        window,
        &[merged_run(
            window,
            &[quote_at(t("18:00:00")), quote_at(t("18:01:00"))],
            &[
                (t("17:52:00"), "a"),
                (t("17:56:00"), "b"),
                (t("18:01:30"), "c"),
            ],
        )],
    );

    // No trade at 18:00: the quote stamped there, then the range bar its boundary completes, then
    // the 5-minute bar closing at the same instant (timer name order), then the next quote
    assert_eq!(
        delivered,
        vec![
            merged_trades(&["a"], &[0, 0, 0]),
            merged_bar(five, t("17:55:00"), 1, &[1, 0, 0]),
            merged_trades(&["b"], &[1, 0, 0]),
            merged_quotes(&[t("18:00:00")], &[1, 0, 1]),
            merged_bar(range, t("18:00:00"), 2, &[1, 1, 1]),
            merged_bar(five, t("18:00:00"), 1, &[2, 1, 1]),
            merged_quotes(&[t("18:01:00")], &[2, 1, 2]),
            merged_trades(&["c"], &[2, 1, 2]),
            Merged::End(None),
        ]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_answer_splits_a_run_of_quotes_at_each_close_of_two_time_bar_types(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let (two, three) = (bar_type_of(id, "2-MINUTE"), bar_type_of(id, "3-MINUTE"));
    let t = |time: &str| utc_ns(&format!("2024-01-08T{time}Z"));
    let window = (t("10:00:00"), t("10:07:30"));

    let delivered = run_merged(
        &fixture,
        "merged-run-across-closes",
        &[three, two],
        window,
        &[merged_run(
            window,
            &[
                quote_at(t("10:01:30")),
                quote_at(t("10:02:30")),
                quote_at(t("10:03:30")),
                quote_at(t("10:04:30")),
                quote_at(t("10:06:30")),
            ],
            &[(t("10:00:30"), "t1"), (t("10:07:00"), "t2")],
        )],
    );

    // One run of quotes, split at each close where it falls: 2-MINUTE 10:02, 3-MINUTE 10:03,
    // 2-MINUTE 10:04 (flat), and at 10:06 both, 2-MINUTE first (timer name order)
    assert_eq!(
        delivered,
        vec![
            merged_trades(&["t1"], &[0, 0, 0]),
            merged_quotes(&[t("10:01:30")], &[0, 0, 1]),
            merged_bar(two, t("10:02:00"), 1, &[0, 1, 1]),
            merged_quotes(&[t("10:02:30")], &[0, 1, 2]),
            merged_bar(three, t("10:03:00"), 1, &[1, 1, 2]),
            merged_quotes(&[t("10:03:30")], &[1, 1, 3]),
            merged_bar(two, t("10:04:00"), 0, &[1, 2, 3]),
            merged_quotes(&[t("10:04:30")], &[1, 2, 4]),
            merged_bar(two, t("10:06:00"), 0, &[1, 3, 4]),
            merged_bar(three, t("10:06:00"), 0, &[2, 3, 4]),
            merged_quotes(&[t("10:06:30")], &[2, 3, 5]),
            merged_trades(&["t2"], &[2, 3, 5]),
            Merged::End(None),
        ]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_answer_quotes_before_the_first_trade_start_no_bar_and_keep_the_start_grid(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let id = fixture.instrument_id;
    let (day, two_day) = (bar_type_of(id, "1-DAY"), bar_type_of(id, "2-DAY"));
    let t = |civil: &str| utc_ns(&format!("2024-01-{civil}Z"));
    let window = (t("08T00:00:00"), t("10T12:00:00"));

    let delivered = run_merged(
        &fixture,
        "merged-quotes-before-the-first-trade",
        &[day, two_day],
        window,
        &[merged_run(
            window,
            &[
                quote_at(t("08T10:00:00")),
                quote_at(t("09T10:00:00")),
                quote_at(t("10T00:00:01")),
            ],
            &[(t("09T12:00:00"), "t1"), (t("10T01:00:00"), "t2")],
        )],
    );

    // Monday's quotes and Tuesday's first quote cross the 1-DAY close of Tuesday 00:00 before
    // any trade: no bar. The 2-DAY grid starts at the request's start (Monday), so both bars
    // close Wednesday 00:00, before Wednesday's quote, 1-DAY first (timer name order)
    assert_eq!(
        delivered,
        vec![
            merged_quotes(&[t("08T10:00:00"), t("09T10:00:00")], &[0, 0, 2]),
            merged_trades(&["t1"], &[0, 0, 2]),
            merged_bar(day, t("10T00:00:00"), 1, &[1, 0, 2]),
            merged_bar(two_day, t("10T00:00:00"), 1, &[1, 1, 2]),
            merged_quotes(&[t("10T00:00:01")], &[1, 1, 3]),
            merged_trades(&["t2"], &[1, 1, 3]),
            Merged::End(None),
        ]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_answer_quotes_after_the_last_trade_take_their_bars_and_the_end_fires_the_rest(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let t = |time: &str| utc_ns(&format!("2024-01-08T{time}Z"));
    let window = (t("10:00:00"), t("10:04:00"));

    let delivered = run_merged(
        &fixture,
        "merged-quotes-after-the-last-trade",
        &[minute],
        window,
        &[merged_run(
            window,
            &[quote_at(t("10:01:30")), quote_at(t("10:02:30"))],
            &[(t("10:00:30"), "t1")],
        )],
    );

    // Each quote after t1 comes after the bars due before it; the end (10:04) fires the 10:03
    // and 10:04 closes after the last quote, before the empty last trades part
    assert_eq!(
        delivered,
        vec![
            merged_trades(&["t1"], &[0, 0]),
            merged_bar(minute, t("10:01:00"), 1, &[1, 0]),
            merged_quotes(&[t("10:01:30")], &[1, 1]),
            merged_bar(minute, t("10:02:00"), 0, &[2, 1]),
            merged_quotes(&[t("10:02:30")], &[2, 2]),
            merged_bar(minute, t("10:03:00"), 0, &[3, 2]),
            merged_bar(minute, t("10:04:00"), 0, &[4, 2]),
            merged_trades(&[], &[4, 2]),
            Merged::End(None),
        ]
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_answer_that_fails_fires_no_end_of_coverage_and_ends_once(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let t = |time: &str| utc_ns(&format!("2024-01-08T{time}Z"));
    let window = (t("10:00:00"), t("10:04:00"));

    // The trades cover the window; the quotes miss 10:02:00 to 10:02:59; no client resolves
    let _catalog_dir = register_quoted_catalog(
        &fixture,
        "merged-failed",
        &[
            merged_run(window, &[], &[(t("10:00:30"), "t1"), (t("10:03:30"), "t2")]),
            merged_run(
                (t("10:00:00"), t("10:02:00") - 1),
                &[quote_at(t("10:01:30"))],
                &[],
            ),
            merged_run(
                (t("10:03:00"), t("10:04:00")),
                &[quote_at(t("10:03:10"))],
                &[],
            ),
        ],
    );
    let params = json!({
        "bar_types": [minute.to_string()],
        "update_subscriptions": false,
        "interleave": true,
        "quotes": true,
    });
    let request = without_a_client(windowed_trades_request(&fixture, window, params));
    let delivered = tap_merged(&fixture, *request.request_id(), &[minute]);
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();

    // The records the catalog holds, the closes before the last of them, no bar of the end
    // (10:04), and one terminal naming the gap
    let reason = uncovered_part(t("10:02:00"), t("10:03:00") - 1, "quotes");
    assert_eq!(
        delivered.take(),
        vec![
            merged_trades(&["t1"], &[0, 0]),
            merged_bar(minute, t("10:01:00"), 1, &[1, 0]),
            merged_quotes(&[t("10:01:30")], &[1, 1]),
            merged_bar(minute, t("10:02:00"), 0, &[2, 1]),
            merged_bar(minute, t("10:03:00"), 0, &[3, 1]),
            merged_quotes(&[t("10:03:10")], &[3, 2]),
            merged_trades(&["t2"], &[3, 2]),
            Merged::End(Some(reason)),
        ]
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// 2024-01-08 (UTC): `time` that day, to the nanosecond.
#[cfg(feature = "streaming")]
fn edge_ns(time: &str) -> u64 {
    utc_ns(&format!("2024-01-08T{time}Z"))
}

/// The edge example's window, 09:59 to 10:01: its catalog holds trades t1 at 09:59:30 and t2 at
/// 10:00:06 and quotes at 09:59:00.010, 10:00:01 and 10:00:59.990.
#[cfg(feature = "streaming")]
fn edge_window() -> (u64, u64) {
    (edge_ns("09:59:00"), edge_ns("10:01:00"))
}

/// Registers the edge example's catalog: one trade file over `trade_file`, and the quote files
/// over `quote_files`, each holding the example's quotes it covers.
#[cfg(feature = "streaming")]
fn register_edge_catalog(
    fixture: &JoinFixture,
    label: &str,
    trade_file: (u64, u64),
    quote_files: &[(u64, u64)],
) -> CatalogTempDir {
    let trades = [(edge_ns("09:59:30"), "t1"), (edge_ns("10:00:06"), "t2")];
    let quotes = [
        quote_at(edge_ns("09:59:00.010")),
        quote_at(edge_ns("10:00:01")),
        quote_at(edge_ns("10:00:59.990")),
    ];
    let held: Vec<Vec<_>> = quote_files
        .iter()
        .map(|&(first, last)| {
            quotes
                .iter()
                .copied()
                .filter(|(ts, _, _)| (first..=last).contains(ts))
                .collect()
        })
        .collect();
    let mut runs = vec![merged_run(trade_file, &[], &trades)];
    runs.extend(
        quote_files
            .iter()
            .zip(&held)
            .map(|(&file, quotes)| merged_run(file, quotes, &[])),
    );
    register_quoted_catalog(fixture, label, &runs)
}

/// An interleaving `quotes` trades request for 1-MINUTE bars over the edge example's window.
#[cfg(feature = "streaming")]
fn edge_request(fixture: &JoinFixture, minute: BarType) -> RequestCommand {
    let params = json!({
        "bar_types": [minute.to_string()],
        "update_subscriptions": false,
        "interleave": true,
        "quotes": true,
    });
    windowed_trades_request(fixture, edge_window(), params)
}

/// What the edge example's requester is given when every record of its window is the catalog's
/// and nothing fails: the bar of 10:00 before the 10:00:01 quote, the end firing 10:01.
#[cfg(feature = "streaming")]
fn edge_answer(minute: BarType) -> Vec<Merged> {
    let t = edge_ns;
    vec![
        merged_quotes(&[t("09:59:00.010")], &[0, 1]),
        merged_trades(&["t1"], &[0, 1]),
        merged_bar(minute, t("10:00:00"), 1, &[1, 1]),
        merged_quotes(&[t("10:00:01")], &[1, 2]),
        merged_trades(&["t2"], &[1, 2]),
        merged_quotes(&[t("10:00:59.990")], &[1, 3]),
        merged_bar(minute, t("10:01:00"), 1, &[2, 3]),
        merged_trades(&[], &[2, 3]),
        Merged::End(None),
    ]
}

/// The legs a client received, in order: each one's kind, window and request ID.
#[cfg(feature = "streaming")]
fn client_legs(fixture: &JoinFixture) -> Vec<(&'static str, u64, u64, UUID4)> {
    let at = |ts: Option<jiff::Timestamp>| {
        u64::try_from(ts.expect("a bounded leg").as_nanosecond()).unwrap()
    };
    fixture
        .recorder
        .borrow()
        .iter()
        .map(|command| match command {
            DataCommand::Request(RequestCommand::Trades(cmd)) => {
                ("trades", at(cmd.start), at(cmd.end), cmd.request_id)
            }
            DataCommand::Request(RequestCommand::Quotes(cmd)) => {
                ("quotes", at(cmd.start), at(cmd.end), cmd.request_id)
            }
            command => panic!("a trades or quotes leg: {command:?}"),
        })
        .collect()
}

/// The kinds and windows of `legs`.
#[cfg(feature = "streaming")]
fn leg_windows(legs: &[(&'static str, u64, u64, UUID4)]) -> Vec<(&'static str, u64, u64)> {
    legs.iter()
        .map(|&(kind, start, end, _)| (kind, start, end))
        .collect()
}

/// Answers the client leg `request_id` in one completed envelope: `quotes` (stamps) on a quotes
/// leg, `trades` (stamp, trade ID) on a trades leg.
#[cfg(feature = "streaming")]
fn answer_leg(
    fixture: &JoinFixture,
    (kind, _, _, request_id): (&'static str, u64, u64, UUID4),
    quotes: &[u64],
    trades: &[(u64, &str)],
) {
    let instrument_id = fixture.instrument_id;
    let ts_init = UnixNanos::from(NY_2026_11_12_2000);
    let response = if kind == "quotes" {
        let quotes: Vec<_> = quotes
            .iter()
            .map(|&ts| {
                let (ts, bid, ask) = quote_at(ts);
                make_quote(instrument_id, bid, ask, ts)
            })
            .collect();
        DataResponse::Quotes(QuotesResponse::new(
            request_id,
            fixture.client_id,
            instrument_id,
            quotes,
            None,
            None,
            ts_init,
            None,
        ))
    } else {
        let trades: Vec<_> = trades
            .iter()
            .map(|&(ts, id)| unsided_trade(instrument_id, ts, "1.0002", id))
            .collect();
        DataResponse::Trades(TradesResponse::new(
            request_id,
            fixture.client_id,
            instrument_id,
            trades,
            None,
            None,
            ts_init,
            None,
        ))
    };
    fixture
        .engine
        .borrow_mut()
        .response_envelope(DataResponseEnvelope::new(response, true, None));
}

/// A merged request whose trade file starts 31 ms after its window's start and ends 13 ms before
/// its end, and whose quote file starts 5 ms after and ends 2 ms before: at each edge its client
/// is asked each feed over that feed's own gap; the answers, empty or not, are merged at their place, and once all
/// four have answered the end fires the 10:01 close.
#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_request_asks_its_client_both_feeds_over_each_part_its_files_miss(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[values(false, true)] client_rows: bool,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let t = edge_ns;
    let _catalog_dir = register_edge_catalog(
        &fixture,
        "merged-edges",
        (t("09:59:00.031"), t("10:00:59.987")),
        &[(t("09:59:00.005"), t("10:00:59.998"))],
    );
    let request = edge_request(&fixture, minute);
    let delivered = tap_merged(&fixture, *request.request_id(), &[minute]);
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();

    let legs = client_legs(&fixture);
    assert_eq!(
        leg_windows(&legs),
        vec![
            ("trades", t("09:59:00"), t("09:59:00.031") - 1),
            ("quotes", t("09:59:00"), t("09:59:00.005") - 1),
            ("trades", t("10:00:59.987") + 1, t("10:01:00")),
            ("quotes", t("10:00:59.998") + 1, t("10:01:00")),
        ]
    );
    // Nothing goes out before every leg has answered
    assert!(delivered.borrow().is_empty());

    if client_rows {
        answer_leg(
            &fixture,
            legs[1],
            &[t("09:59:00.002"), t("09:59:00.020")],
            &[],
        );
        answer_leg(&fixture, legs[0], &[], &[(t("09:59:00.025"), "c1")]);
    } else {
        answer_leg(&fixture, legs[1], &[], &[]);
        answer_leg(&fixture, legs[0], &[], &[]);
    }
    answer_leg(&fixture, legs[3], &[], &[]);
    assert!(delivered.borrow().is_empty());
    answer_leg(&fixture, legs[2], &[], &[]);

    let expected = if client_rows {
        // The client's quote before the quote file and its trade before the trade file at their
        // place, around the catalog's quote; its quote at 09:59:00.020, which the quote file
        // covers, is not kept
        vec![
            merged_quotes(&[t("09:59:00.002"), t("09:59:00.010")], &[0, 2]),
            merged_trades(&["c1", "t1"], &[0, 2]),
            merged_bar(minute, t("10:00:00"), 2, &[1, 2]),
            merged_quotes(&[t("10:00:01")], &[1, 3]),
            merged_trades(&["t2"], &[1, 3]),
            merged_quotes(&[t("10:00:59.990")], &[1, 4]),
            merged_bar(minute, t("10:01:00"), 1, &[2, 4]),
            merged_trades(&[], &[2, 4]),
            Merged::End(None),
        ]
    } else {
        edge_answer(minute)
    };
    assert_eq!(delivered.take(), expected);
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// A gap in the quote files alone goes to the client for quotes alone; the catalog's trade inside
/// it (t2, 10:00:06) is kept, and the client's quote (10:00:01) comes at its place.
#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_request_asks_its_client_quotes_alone_over_a_gap_in_its_quote_files_alone(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let t = edge_ns;
    let _catalog_dir = register_edge_catalog(
        &fixture,
        "merged-quote-gap",
        edge_window(),
        &[
            (t("09:59:00"), t("10:00:00") - 1),
            (t("10:00:30"), t("10:01:00")),
        ],
    );
    let request = edge_request(&fixture, minute);
    let delivered = tap_merged(&fixture, *request.request_id(), &[minute]);
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();

    let legs = client_legs(&fixture);
    assert_eq!(
        leg_windows(&legs),
        vec![("quotes", t("10:00:00"), t("10:00:30") - 1)]
    );
    answer_leg(&fixture, legs[0], &[t("10:00:01")], &[]);

    assert_eq!(delivered.take(), edge_answer(minute));
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// The review's example with catalog write-back (`update_catalog`): trades covered through 10:01,
/// quotes missing from 10:00 to 10:00:30, and the catalog's trade t2 at 10:00:06. The client is
/// asked quotes alone over that part (a client holding t2 would answer it to a trades ask), so
/// only its quote is written back: a second request over the window is answered by the catalog
/// alone, t2 once and its 10:01 bar of volume 1, not 2.
#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_request_with_update_catalog_writes_back_only_the_feed_its_catalog_lacks(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let t = edge_ns;
    let _catalog_dir = register_edge_catalog(
        &fixture,
        "merged-client-overlap",
        edge_window(),
        &[
            (t("09:59:00"), t("10:00:00") - 1),
            (t("10:00:30"), t("10:01:00")),
        ],
    );
    let request = || {
        let params = json!({
            "bar_types": [minute.to_string()],
            "update_subscriptions": false,
            "interleave": true,
            "quotes": true,
            "update_catalog": true,
        });
        windowed_trades_request(&fixture, edge_window(), params)
    };
    let without_counts = |delivered: Vec<Merged>| -> Vec<Merged> {
        delivered
            .into_iter()
            .map(|merged| match merged {
                Merged::Quotes(ts, _) => Merged::Quotes(ts, Vec::new()),
                Merged::Trades(ids, _) => Merged::Trades(ids, Vec::new()),
                Merged::Bar(bar_type, ts, volume, _) => {
                    Merged::Bar(bar_type, ts, volume, Vec::new())
                }
                Merged::End(reason) => Merged::End(reason),
            })
            .collect()
    };

    let first = request();
    let first_delivered = tap_merged(&fixture, *first.request_id(), &[minute]);
    fixture.engine.borrow_mut().execute_request(first).unwrap();
    let legs = client_legs(&fixture);
    for &leg in &legs {
        answer_leg(&fixture, leg, &[t("10:00:01")], &[(t("10:00:06"), "t2")]);
    }
    let second = request();
    let second_delivered = tap_merged(&fixture, *second.request_id(), &[minute]);
    fixture.engine.borrow_mut().execute_request(second).unwrap();

    assert_eq!(
        leg_windows(&legs),
        vec![("quotes", t("10:00:00"), t("10:00:30") - 1)]
    );
    assert_eq!(first_delivered.take(), edge_answer(minute));
    assert_eq!(client_legs(&fixture).len(), legs.len());
    assert_eq!(
        without_counts(second_delivered.take()),
        without_counts(edge_answer(minute))
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// A merged request whose catalog covers none of its window (its files hold another window) is
/// served by its client alone: a trades and a quotes leg over the whole window, their rows
/// merged by `ts_init`, a quote before a trade at one stamp (10:00:06), and once both have
/// answered the end fires the 10:01 close. With no client it fails naming the window and both
/// feeds.
#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_request_its_catalog_covers_none_of_is_served_by_its_client_alone(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[values(true, false)] with_client: bool,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let t = edge_ns;
    let _catalog_dir =
        register_quoted_catalog(&fixture, "merged-client-alone", &OUTSIDE_THE_WINDOW);
    let request = edge_request(&fixture, minute);
    let request = if with_client {
        request
    } else {
        without_a_client(request)
    };
    let delivered = tap_merged(&fixture, *request.request_id(), &[minute]);
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();

    let (start, end) = edge_window();
    if !with_client {
        assert!(fixture.recorder.borrow().is_empty());
        assert_eq!(
            delivered.take(),
            vec![Merged::End(Some(uncovered_part(
                start,
                end,
                "trades and quotes"
            )))]
        );
        assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
        return;
    }

    let legs = client_legs(&fixture);
    assert_eq!(
        leg_windows(&legs),
        vec![("trades", start, end), ("quotes", start, end)]
    );
    answer_leg(
        &fixture,
        legs[1],
        &[
            t("09:59:00.010"),
            t("10:00:01"),
            t("10:00:06"),
            t("10:00:59.990"),
        ],
        &[],
    );
    assert!(delivered.borrow().is_empty());
    answer_leg(
        &fixture,
        legs[0],
        &[],
        &[(t("09:59:30"), "t1"), (t("10:00:06"), "t2")],
    );

    assert_eq!(
        delivered.take(),
        vec![
            merged_quotes(&[t("09:59:00.010")], &[0, 1]),
            merged_trades(&["t1"], &[0, 1]),
            merged_bar(minute, t("10:00:00"), 1, &[1, 1]),
            merged_quotes(&[t("10:00:01"), t("10:00:06")], &[1, 3]),
            merged_trades(&["t2"], &[1, 3]),
            merged_quotes(&[t("10:00:59.990")], &[1, 4]),
            merged_bar(minute, t("10:01:00"), 1, &[2, 4]),
            merged_trades(&[], &[2, 4]),
            Merged::End(None),
        ]
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// A client that fails the quotes ask of a part fails the merged request with its reason: the
/// records go out, the closes before the last of them fire, the end (10:01) does not, and there
/// is one terminal.
#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_request_fails_when_its_client_fails_a_quotes_ask(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let t = edge_ns;
    let _catalog_dir = register_edge_catalog(
        &fixture,
        "merged-client-fails",
        (t("09:59:00.031"), t("10:00:59.987")),
        &[(t("09:59:00.005"), t("10:00:59.998"))],
    );
    let request = edge_request(&fixture, minute);
    let delivered = tap_merged(&fixture, *request.request_id(), &[minute]);
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();

    let legs = client_legs(&fixture);
    answer_leg(&fixture, legs[0], &[], &[]);
    fixture
        .engine
        .borrow_mut()
        .request_failed(DataRequestFailed {
            request_id: legs[1].3,
            reason: "the source stopped".to_owned(),
            response: None,
        });
    answer_leg(&fixture, legs[2], &[], &[]);
    answer_leg(&fixture, legs[3], &[], &[]);

    assert_eq!(
        delivered.take(),
        vec![
            merged_quotes(&[t("09:59:00.010")], &[0, 1]),
            merged_trades(&["t1"], &[0, 1]),
            merged_bar(minute, t("10:00:00"), 1, &[1, 1]),
            merged_quotes(&[t("10:00:01")], &[1, 2]),
            merged_trades(&["t2"], &[1, 2]),
            merged_quotes(&[t("10:00:59.990")], &[1, 3]),
            merged_trades(&[], &[1, 3]),
            Merged::End(Some("the source stopped".to_owned())),
        ]
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// With no client, a merged request fails naming the first part its files miss and the feeds
/// they lack there; the records go out, and the end (10:01) does not fire.
#[cfg(feature = "streaming")]
#[rstest]
#[case::trades_and_quotes("09:59:00.005", "trades and quotes")]
#[case::trades("09:59:00", "trades")]
fn test_merged_request_without_a_client_fails_naming_the_first_part_and_its_feeds(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] quote_file_start: &str,
    #[case] feeds: &str,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let t = edge_ns;
    let _catalog_dir = register_edge_catalog(
        &fixture,
        "merged-no-client",
        (t("09:59:00.031"), t("10:00:59.987")),
        &[(t(quote_file_start), t("10:00:59.998"))],
    );
    let request = without_a_client(edge_request(&fixture, minute));
    let delivered = tap_merged(&fixture, *request.request_id(), &[minute]);
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();

    let reason = uncovered_part(t("09:59:00"), t("09:59:00.031") - 1, feeds);
    assert_eq!(
        delivered.take(),
        vec![
            merged_quotes(&[t("09:59:00.010")], &[0, 1]),
            merged_trades(&["t1"], &[0, 1]),
            merged_bar(minute, t("10:00:00"), 1, &[1, 1]),
            merged_quotes(&[t("10:00:01")], &[1, 2]),
            merged_trades(&["t2"], &[1, 2]),
            merged_quotes(&[t("10:00:59.990")], &[1, 3]),
            merged_trades(&[], &[1, 3]),
            Merged::End(Some(reason)),
        ]
    );
    assert!(fixture.recorder.borrow().is_empty());
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// A window its files wholly cover is answered as before: no client leg, the end fires 10:01.
#[cfg(feature = "streaming")]
#[rstest]
fn test_merged_request_over_a_window_its_files_wholly_cover_asks_its_client_nothing(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, NY_2026_11_12_2000);
    let minute = bar_type_of(fixture.instrument_id, "1-MINUTE");
    let _catalog_dir =
        register_edge_catalog(&fixture, "merged-covered", edge_window(), &[edge_window()]);
    let request = edge_request(&fixture, minute);
    let delivered = tap_merged(&fixture, *request.request_id(), &[minute]);
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();

    assert_eq!(delivered.take(), edge_answer(minute));
    assert!(fixture.recorder.borrow().is_empty());
}

/// The gap example on 2024-01-08 (UTC): a request over 09:56 to 10:05 whose catalog file ends at
/// 09:59:59.999999999 and holds trades at 09:56:30, 09:57:30 and 09:58:30.
#[cfg(feature = "streaming")]
const GAP_WINDOW: (&str, &str) = ("09:56:00", "10:05:00");

#[cfg(feature = "streaming")]
fn gap_ns(time: &str) -> u64 {
    utc_ns(&format!("2024-01-08T{time}Z"))
}

/// Registers the gap example's catalog on `engine`, its file ending at `file_end`.
#[cfg(feature = "streaming")]
fn register_gap_catalog(
    engine: &mut DataEngine,
    instrument_id: InstrumentId,
    label: &str,
    file_end: u64,
) -> CatalogTempDir {
    let trades: Vec<_> = [("09:56:30", "t1"), ("09:57:30", "t2"), ("09:58:30", "t3")]
        .into_iter()
        .map(|(time, id)| split_trade(instrument_id, gap_ns(time), id))
        .collect();
    register_trade_catalog_with_trades(
        engine,
        label,
        &trades,
        Some((gap_ns(GAP_WINDOW.0), file_end)),
    )
}

/// A trades request over the gap example's window, routed to `client_id`.
#[cfg(feature = "streaming")]
fn gap_request(instrument_id: InstrumentId, client_id: ClientId, params: Value) -> RequestCommand {
    RequestCommand::Trades(RequestTrades::new(
        instrument_id,
        timestamp_of(Some(gap_ns(GAP_WINDOW.0))),
        timestamp_of(Some(gap_ns(GAP_WINDOW.1))),
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(serde_json::from_value(params).unwrap()),
    ))
}

/// The failure reason of the gap example: no source covers 10:00 to 10:05.
#[cfg(feature = "streaming")]
const GAP_REASON: &str = "No source covers 2024-01-08T10:00:00Z to 2024-01-08T10:05:00Z of the \
                          request's window: no catalog file holds it and no client resolves";

/// The gap example's engine, dispatched at 12:00, with a client the requests below may name.
#[cfg(feature = "streaming")]
fn gap_fixture(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
    watched: &[BarType],
) -> PlainFixture {
    PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        gap_ns("12:00:00"),
        watched,
    )
}

/// A plain request over a part of its window no catalog file holds and no client resolves:
/// its answer fails naming that part, delivers the catalog's records, fires the closes before
/// its last record and nothing after it, and ends once.
#[cfg(feature = "streaming")]
#[rstest]
fn test_plain_request_over_a_gap_no_client_resolves_fails_and_closes_nothing_after_it(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let minute = bar_type_of(id, "1-MINUTE");
    let mut fixture = gap_fixture(audusd_sim, client_id, venue, &[minute]);
    let _catalog_dir =
        register_gap_catalog(&mut fixture.engine, id, "gap-plain", gap_ns("10:00:00") - 1);

    let params = json!({ "bar_types": [minute.to_string()] });
    fixture.execute(gap_request(id, ClientId::new("ABSENT"), params));

    // Without the failure the end (10:05) would fire the 09:59 close holding t3
    assert_eq!(
        fixture.cached(),
        vec![vec![(gap_ns("09:57:00"), 1), (gap_ns("09:58:00"), 1)]]
    );
    assert_eq!(fixture.ends(), vec![Some(GAP_REASON.to_owned())]);
    assert_eq!(fixture.cache.borrow().trade_count(&id), 3);
    assert!(fixture.trades_requests().is_empty());
}

/// The same on the interleaved path: its trades and bars in live order, the closes before its
/// last record only, and one terminal, the last trades part, carrying the reason.
#[cfg(feature = "streaming")]
#[rstest]
fn test_interleaved_request_over_a_gap_no_client_resolves_fails_and_closes_nothing_after_it(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let fixture = join_fixture(audusd_sim, client_id, venue, gap_ns("12:00:00"));
    let id = fixture.instrument_id;
    let minute = bar_type_of(id, "1-MINUTE");
    let _catalog_dir = register_gap_catalog(
        &mut fixture.engine.borrow_mut(),
        id,
        "gap-interleaved",
        gap_ns("10:00:00") - 1,
    );

    let params = json!({
        "bar_types": [minute.to_string()],
        "update_subscriptions": false,
        "interleave": true,
    });
    let request = gap_request(id, ClientId::new("ABSENT"), params);
    let delivered = tap_merged(&fixture, *request.request_id(), &[minute]);
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();

    assert_eq!(
        delivered.take(),
        vec![
            merged_trades(&["t1"], &[0, 0]),
            merged_bar(minute, gap_ns("09:57:00"), 1, &[1, 0]),
            merged_trades(&["t2"], &[1, 0]),
            merged_bar(minute, gap_ns("09:58:00"), 1, &[2, 0]),
            merged_trades(&["t3"], &[2, 0]),
            Merged::End(Some(GAP_REASON.to_owned())),
        ]
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
}

/// With a client, the part no catalog file holds goes to it; its answer, here empty and bare,
/// is a source: nothing fails, and the end fires the closes through 10:05.
#[cfg(feature = "streaming")]
#[rstest]
fn test_plain_request_over_a_gap_its_client_answers_empty_fires_the_end_of_coverage(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let minute = bar_type_of(id, "1-MINUTE");
    let mut fixture = gap_fixture(audusd_sim, client_id, venue, &[minute]);
    let _catalog_dir = register_gap_catalog(
        &mut fixture.engine,
        id,
        "gap-client",
        gap_ns("10:00:00") - 1,
    );

    let params = json!({ "bar_types": [minute.to_string()] });
    fixture.execute(gap_request(id, client_id, params));
    let before = fixture.cached();
    let legs = fixture.trades_requests();
    fixture
        .engine
        .response(trades_answer(legs[0].request_id, client_id, id, &[]));

    assert_eq!(legs.len(), 1);
    assert_eq!(
        (legs[0].start, legs[0].end),
        (
            timestamp_of(Some(gap_ns("10:00:00"))),
            timestamp_of(Some(gap_ns("10:05:00")))
        )
    );
    // The catalog leg's records wait in the pipeline for the client leg
    assert_eq!(before, vec![vec![]]);
    assert_eq!(
        fixture.cached(),
        vec![vec![
            (gap_ns("09:57:00"), 1),
            (gap_ns("09:58:00"), 1),
            (gap_ns("09:59:00"), 1),
        ]]
    );
    assert_eq!(fixture.ends(), vec![None]);
}

/// A request for two instruments, AUD/USD over the gap example and GBP/USD wholly covered, with
/// no client: when it builds bars it fails naming the gap and closes nothing after its last
/// record, as one for a single instrument does; when it builds none it completes as before.
#[cfg(feature = "streaming")]
#[rstest]
fn test_request_for_two_instruments_over_a_gap_no_client_resolves_fails_when_it_builds_bars(
    audusd_sim: CurrencyPair,
    gbpusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[values(true, false)] builds_bars: bool,
) {
    let _ = stub_msgbus;
    let (aud, gbp) = (audusd_sim.id, gbpusd_sim.id);
    let minute = bar_type_of(aud, "1-MINUTE");
    let mut fixture = PlainFixture::new(
        vec![
            InstrumentAny::CurrencyPair(audusd_sim),
            InstrumentAny::CurrencyPair(gbpusd_sim),
        ],
        client_id,
        venue,
        without_flat_bars(),
        gap_ns("12:00:00"),
        &[minute],
    );
    // One catalog: AUD/USD's file ends at 09:59:59.999999999, GBP/USD's covers the window
    let catalog_dir = CatalogTempDir::new("batch-gap");
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);
    let aud_trades: Vec<_> = [("09:56:30", "t1"), ("09:57:30", "t2"), ("09:58:30", "t3")]
        .into_iter()
        .map(|(time, id)| split_trade(aud, gap_ns(time), id))
        .collect();
    let file = |start: u64, end: u64| (Some(UnixNanos::from(start)), Some(UnixNanos::from(end)));
    let (start, end) = file(gap_ns(GAP_WINDOW.0), gap_ns("10:00:00") - 1);
    catalog
        .write_to_parquet(&aud_trades, start, end, None)
        .unwrap();
    let (start, end) = file(gap_ns(GAP_WINDOW.0), gap_ns(GAP_WINDOW.1));
    catalog
        .write_to_parquet(
            &[split_trade(gbp, gap_ns("09:57:10"), "g1")],
            start,
            end,
            None,
        )
        .unwrap();
    fixture.engine.register_catalog(Box::new(catalog), None);

    let params = if builds_bars {
        json!({ "bar_types": [minute.to_string()] })
    } else {
        json!({})
    };
    let RequestCommand::Trades(mut request) = gap_request(aud, ClientId::new("ABSENT"), params)
    else {
        unreachable!("the gap request is a trades request");
    };
    request.instrument_ids = vec![aud, gbp];
    fixture.execute(RequestCommand::Trades(request));

    if builds_bars {
        // Without the failure the end (10:05) would fire the 09:59 close holding t3
        assert_eq!(
            fixture.cached(),
            vec![vec![(gap_ns("09:57:00"), 1), (gap_ns("09:58:00"), 1)]]
        );
        assert_eq!(fixture.ends(), vec![Some(GAP_REASON.to_owned())]);
    } else {
        assert_eq!(fixture.ends(), vec![None]);
    }
    assert_eq!(fixture.cache.borrow().trade_count(&aud), 3);
    assert_eq!(fixture.cache.borrow().trade_count(&gbp), 1);
    assert!(fixture.trades_requests().is_empty());
}

/// A request that builds no bars, over the same gap with no client, completes as before: the
/// catalog's records and no failure.
#[cfg(feature = "streaming")]
#[rstest]
fn test_request_without_bars_over_a_gap_no_client_resolves_completes_as_before(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let mut fixture = gap_fixture(audusd_sim, client_id, venue, &[]);
    let _catalog_dir = register_gap_catalog(
        &mut fixture.engine,
        id,
        "gap-no-bars",
        gap_ns("10:00:00") - 1,
    );

    fixture.execute(gap_request(id, ClientId::new("ABSENT"), json!({})));

    assert_eq!(fixture.ends(), vec![None]);
    assert_eq!(fixture.cache.borrow().trade_count(&id), 3);
}

/// A window its catalog file wholly holds, with no client, is answered as before: the end fires
/// the closes through 10:05.
#[cfg(feature = "streaming")]
#[rstest]
fn test_plain_request_over_a_wholly_covered_window_fires_the_end_of_coverage(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let minute = bar_type_of(id, "1-MINUTE");
    let mut fixture = gap_fixture(audusd_sim, client_id, venue, &[minute]);
    let _catalog_dir =
        register_gap_catalog(&mut fixture.engine, id, "gap-covered", gap_ns("10:05:00"));

    let params = json!({ "bar_types": [minute.to_string()] });
    fixture.execute(gap_request(id, ClientId::new("ABSENT"), params));

    assert_eq!(
        fixture.cached(),
        vec![vec![
            (gap_ns("09:57:00"), 1),
            (gap_ns("09:58:00"), 1),
            (gap_ns("09:59:00"), 1),
        ]]
    );
    assert_eq!(fixture.ends(), vec![None]);
}

/// A time-range pipeline over 10:00 to 10:09 in 3-minute children stops after an empty child
/// (its one duration runs out) at 10:06: the parent fails naming 10:06 to 10:09 and fires no
/// close after its last record.
#[rstest]
fn test_time_range_pipeline_ending_before_the_parent_end_fails_at_the_parent(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let minute = bar_type_of(id, "1-MINUTE");
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        utc_ns("2024-01-08T12:00:00Z"),
        &[minute],
    );
    fixture.execute(plain_trades_request(
        id,
        client_id,
        UUID4::new(),
        (
            Some(utc_ns("2024-01-08T10:00:00Z")),
            Some(utc_ns("2024-01-08T10:09:00Z")),
        ),
        None,
        plain_params(
            &[minute],
            json!({ "time_range_generator": "", "durations_seconds": [180] }),
        ),
    ));

    let first = fixture.trades_requests()[0].request_id;
    let answer = trades_answer(
        first,
        client_id,
        id,
        &[
            (utc_ns("2024-01-08T10:01:30Z"), "1.00000", 1),
            (utc_ns("2024-01-08T10:02:30Z"), "1.00000", 2),
        ],
    );
    let after_first = fixture.answer(answer, true, None);
    let second = fixture.trades_requests()[1].request_id;
    let answer = trades_answer(second, client_id, id, &[]);
    let after_second = fixture.answer(answer, true, None);

    // Without the failure the parent's end (10:09) would fire the 10:03 close holding the second
    // trade
    let closed = vec![vec![(utc_ns("2024-01-08T10:02:00Z"), 1)]];
    assert_eq!(after_first, closed);
    assert_eq!(after_second, closed);
    assert_eq!(fixture.trades_requests().len(), 2);
    assert_eq!(
        fixture.ends(),
        vec![Some(
            "No source covers 2024-01-08T10:06:00.000000001Z to 2024-01-08T10:09:00Z of the \
             request's window: its time-range pipeline ended before it, its durations having \
             run out"
                .to_owned()
        )]
    );
}

/// How a plain request's window is left not wholly known.
#[derive(Clone, Copy, Debug)]
enum NotWhole {
    /// It names a filter expression.
    Filter,
    /// Its client answers with a bare response.
    Bare,
}

/// A request that names a filter, or is answered by a bare response, completes with no failure,
/// fires the closes before its last record, and nothing after it.
#[rstest]
#[case::filter(NotWhole::Filter)]
#[case::bare_response(NotWhole::Bare)]
fn test_plain_request_whose_window_is_not_wholly_known_fires_no_end_of_coverage(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] not_whole: NotWhole,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, range) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "10-RANGE"));
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        new_york("2024-01-08T20:00:00"),
        &[five, range],
    );
    let extra = match not_whole {
        NotWhole::Filter => json!({ "filter_expr": "size > 0" }),
        NotWhole::Bare => json!({}),
    };
    let request_id = UUID4::new();
    fixture.execute(plain_trades_request(
        id,
        client_id,
        request_id,
        (
            Some(new_york("2024-01-08T16:50:00")),
            Some(new_york("2024-01-08T18:00:00") + 1),
        ),
        None,
        plain_params(&[five, range], extra),
    ));

    let answer = trades_answer(
        request_id,
        client_id,
        id,
        &[
            (new_york("2024-01-08T16:52:00"), "1.00000", 1),
            (new_york("2024-01-08T16:57:00"), "1.00000", 2),
            (new_york("2024-01-08T16:59:58"), "1.00000", 3),
        ],
    );
    let cached = match not_whole {
        NotWhole::Filter => fixture.answer(answer, true, None),
        NotWhole::Bare => {
            fixture.engine.response(answer);
            fixture.cached()
        }
    };

    // The 16:55 close fires before the 16:57 trade; neither 17:00 nor the 18:00 boundary fires
    assert_eq!(
        cached,
        vec![vec![(new_york("2024-01-08T16:55:00"), 1)], vec![]]
    );
    assert_eq!(fixture.ends(), vec![None]);
}

/// A time-range child over a part no catalog file holds, with no client, fails as a plain
/// request does, and so does its parent: the child's records delivered, no close after them.
#[cfg(feature = "streaming")]
#[rstest]
fn test_time_range_child_over_a_gap_no_client_resolves_fails_its_parent(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let minute = bar_type_of(id, "1-MINUTE");
    let mut fixture = gap_fixture(audusd_sim, client_id, venue, &[minute]);
    let trades = [
        split_trade(id, gap_ns("10:00:30"), "t1"),
        split_trade(id, gap_ns("10:01:30"), "t2"),
    ];
    let _catalog_dir = register_trade_catalog_with_trades(
        &mut fixture.engine,
        "gap-time-range",
        &trades,
        Some((gap_ns("10:00:00"), gap_ns("10:02:00") - 1)),
    );

    fixture.execute(plain_trades_request(
        id,
        ClientId::new("ABSENT"),
        UUID4::new(),
        (Some(gap_ns("10:00:00")), Some(gap_ns("10:06:00"))),
        None,
        plain_params(
            &[minute],
            json!({ "time_range_generator": "", "durations_seconds": [180] }),
        ),
    ));

    assert_eq!(fixture.cached(), vec![vec![(gap_ns("10:01:00"), 1)]]);
    assert_eq!(
        fixture.ends(),
        vec![Some(
            "No source covers 2024-01-08T10:02:00Z to 2024-01-08T10:03:00Z of the request's \
             window: no catalog file holds it and no client resolves"
                .to_owned()
        )]
    );
}

/// The review's example: a time-range parent over 16:50 to 18:00 whose one child its client
/// answers with a bare response, its last trade at 16:59:58. The child certifies no coverage, so
/// the parent's end fires neither the 17:00 close nor the 18:00 `RANGE` boundary.
#[rstest]
fn test_time_range_parent_whose_child_is_answered_bare_fires_no_end_of_coverage(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, range) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "10-RANGE"));
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        new_york("2024-01-08T20:00:00"),
        &[five, range],
    );
    fixture.execute(plain_trades_request(
        id,
        client_id,
        UUID4::new(),
        (
            Some(new_york("2024-01-08T16:50:00")),
            Some(new_york("2024-01-08T18:00:00") + 1),
        ),
        None,
        plain_params(
            &[five, range],
            json!({ "time_range_generator": "", "durations_seconds": [7200] }),
        ),
    ));

    let child = fixture.trades_requests()[0].request_id;
    fixture.engine.response(trades_answer(
        child,
        client_id,
        id,
        &[
            (new_york("2024-01-08T16:52:00"), "1.00000", 1),
            (new_york("2024-01-08T16:57:00"), "1.00000", 2),
            (new_york("2024-01-08T16:59:58"), "1.00000", 3),
        ],
    ));

    assert_eq!(fixture.trades_requests().len(), 1);
    // The 16:55 close fires before the 16:57 trade; neither 17:00 nor the 18:00 boundary fires
    assert_eq!(
        fixture.cached(),
        vec![vec![(new_york("2024-01-08T16:55:00"), 1)], vec![]]
    );
    assert_eq!(fixture.ends(), vec![None]);
}

/// A continuous-future request across two segments (ESH24 to 10:03, then ESM24) whose children
/// its client answers with bare responses: the 10:05 close fires before the 10:06 trade, and the
/// parent's end (10:10) fires nothing after it.
#[rstest]
fn test_continuous_future_parent_whose_children_are_answered_bare_fires_no_end_of_coverage(
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let target = BarType::from("ES.GLBX-5-MINUTE-LAST-INTERNAL");
    let mut fixture = PlainFixture::new(
        Vec::new(),
        client_id,
        Venue::from("GLBX"),
        DataEngineConfig::default(),
        utc_ns("2024-01-08T11:00:00Z"),
        &[target],
    );
    let esh = add_es_contract(&fixture.cache, "ESH24.GLBX", "ESH24");
    let esm = add_es_contract(&fixture.cache, "ESM24.GLBX", "ESM24");
    let params = params_from_json(json!({
        "continuous_future_adjustment_mode": "BACKWARD_SPREAD",
        "continuous_future_transitions": [{
            "transition_time_ns": utc_ns("2024-01-08T10:03:00Z"),
            "pre_instrument_id": esh.to_string(),
            "post_instrument_id": esm.to_string(),
            "pre_price": "100.00",
            "post_price": "95.00"
        }]
    }));
    fixture.execute(RequestCommand::Bars(RequestBars::new(
        target,
        timestamp_of(Some(utc_ns("2024-01-08T10:00:00Z"))),
        timestamp_of(Some(utc_ns("2024-01-08T10:10:00Z"))),
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(params),
    )));

    let first = fixture.trades_requests()[0].request_id;
    fixture.engine.response(trades_answer(
        first,
        fixture.client_id,
        esh,
        &[(utc_ns("2024-01-08T10:01:00Z"), "100.00", 1)],
    ));
    let second = fixture.trades_requests()[1].request_id;
    fixture.engine.response(trades_answer(
        second,
        fixture.client_id,
        esm,
        &[
            (utc_ns("2024-01-08T10:04:00Z"), "96.00", 2),
            (utc_ns("2024-01-08T10:06:00Z"), "97.00", 4),
        ],
    ));

    assert_eq!(
        fixture.cached(),
        vec![vec![(utc_ns("2024-01-08T10:05:00Z"), 3)]]
    );
    assert_eq!(fixture.ends(), vec![None]);
}

/// What a kept request's requester and the subscribers are given, in order, each with the
/// number of bars of every watched bar type the cache holds at its callback: the trades and
/// bars of the answer of the `n`th request and its end; a live trade on the trades topic; a
/// bar published to the subscribers of its bar type.
#[derive(Clone, Debug, PartialEq)]
enum Kept {
    Trade(usize, TradeId, Vec<usize>),
    Bar(usize, BarType, UnixNanos, Quantity, Vec<usize>),
    End(usize, Option<String>),
    Live(TradeId, Vec<usize>),
    Published(BarType, UnixNanos, Quantity, Vec<usize>),
}

fn kept_trade(request: usize, trade_id: &str, cached: &[usize]) -> Kept {
    Kept::Trade(request, TradeId::new(trade_id), cached.to_vec())
}

fn kept_bar(request: usize, bar_type: BarType, ts: u64, volume: u64, cached: &[usize]) -> Kept {
    Kept::Bar(
        request,
        bar_type.standard(),
        UnixNanos::from(ts),
        Quantity::from(volume),
        cached.to_vec(),
    )
}

fn live_trade(trade_id: &str, cached: &[usize]) -> Kept {
    Kept::Live(TradeId::new(trade_id), cached.to_vec())
}

fn published(bar_type: BarType, ts: u64, volume: u64, cached: &[usize]) -> Kept {
    Kept::Published(
        bar_type.standard(),
        UnixNanos::from(ts),
        Quantity::from(volume),
        cached.to_vec(),
    )
}

/// An interleaving trades request whose builders are kept by request (`update_subscriptions`);
/// with `plain`, one that does not interleave.
#[derive(Clone, Copy, Default)]
struct KeptSpec {
    start: Option<u64>,
    end: Option<u64>,
    limit: Option<usize>,
    time_zone: Option<&'static str>,
    filter_expr: Option<&'static str>,
    /// A time-range pipeline of 5-minute children that ask only the instant each starts at.
    point_data: bool,
    plain: bool,
    /// A quotes request, not a trades request.
    quotes: bool,
}

/// The window from `start` to just before `next`, the next request's start.
fn kept_window(start: u64, next: u64) -> KeptSpec {
    KeptSpec {
        start: Some(start),
        end: Some(next - 1),
        ..KeptSpec::default()
    }
}

/// An engine without flat bars whose clock reads `now`, with a mock client; what the requesters
/// of its kept requests and the subscribers of `watched` and of the instrument's trades are
/// given, in one order.
struct KeptFixture {
    engine: DataEngine,
    clock: Rc<RefCell<VirtualClock>>,
    recorder: Rc<RefCell<Vec<DataCommand>>>,
    instrument_id: InstrumentId,
    client_id: ClientId,
    venue: Venue,
    watched: Vec<BarType>,
    requests: Vec<UUID4>,
    cache: Rc<RefCell<Cache>>,
    delivered: Rc<RefCell<Vec<Kept>>>,
    /// The data commands real actors sent (see [`Self::actor`]), not executed yet.
    queued: Rc<RefCell<Vec<DataCommand>>>,
}

impl KeptFixture {
    fn new(
        audusd_sim: CurrencyPair,
        client_id: ClientId,
        venue: Venue,
        now: u64,
        watched: &[BarType],
    ) -> Self {
        let instrument_id = audusd_sim.id;
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let _ = clock.borrow_mut().advance_time(UnixNanos::from(now), true);
        let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
            .unwrap();
        let mut engine = DataEngine::new(
            Rc::clone(&clock) as Rc<RefCell<dyn Clock>>,
            Rc::clone(&cache),
            Some(without_flat_bars()),
        );
        let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
        register_mock_client(
            Rc::new(RefCell::new(VirtualClock::new())),
            Rc::clone(&cache),
            client_id,
            venue,
            None,
            &recorder,
            &mut engine,
        );

        let delivered = Rc::new(RefCell::new(Vec::new()));
        for bar_type in watched {
            let recorded = Rc::clone(&delivered);
            let cache = Rc::clone(&cache);
            let watched = watched.to_vec();
            let handler = TypedHandler::from(move |bar: &Bar| {
                let cached = cached_bars(&cache, &watched);
                recorded.borrow_mut().push(Kept::Published(
                    bar.bar_type,
                    bar.ts_event,
                    bar.volume,
                    cached,
                ));
            });
            msgbus::subscribe_bars(
                switchboard::get_bars_topic(bar_type.standard()).into(),
                handler,
                None,
            );
        }
        let recorded = Rc::clone(&delivered);
        let trades_cache = Rc::clone(&cache);
        let trades_watched = watched.to_vec();
        msgbus::subscribe_trades(
            switchboard::get_trades_topic(instrument_id).into(),
            TypedHandler::from(move |trade: &TradeTick| {
                let cached = cached_bars(&trades_cache, &trades_watched);
                recorded
                    .borrow_mut()
                    .push(Kept::Live(trade.trade_id, cached));
            }),
            None,
        );

        Self {
            engine,
            clock,
            recorder,
            instrument_id,
            client_id,
            venue,
            watched: watched.to_vec(),
            requests: Vec::new(),
            cache,
            delivered,
            queued: Rc::default(),
        }
    }

    /// Registers a real actor `name` on the fixture's clock and cache. Its data commands queue
    /// for the engine, as a runner queues what actors send to `DataEngine.queue_execute`, until
    /// [`Self::pump`] executes them.
    fn actor(&self, name: &str) -> KeptRequester {
        let queued = Rc::clone(&self.queued);
        msgbus::register_data_command_endpoint(
            MessagingSwitchboard::data_engine_queue_execute(),
            TypedIntoHandler::from(move |command: DataCommand| queued.borrow_mut().push(command)),
        );
        let mut actor = KeptRequester {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some(ActorId::from(name)),
                ..Default::default()
            }),
        };
        actor
            .register(
                TraderId::test_default(),
                Rc::clone(&self.clock) as Rc<RefCell<dyn Clock>>,
                Rc::clone(&self.cache),
            )
            .unwrap();
        actor
    }

    /// Executes the data commands real actors sent, in the order they sent them.
    fn pump(&mut self) {
        loop {
            let queued = std::mem::take(&mut *self.queued.borrow_mut());
            if queued.is_empty() {
                break;
            }
            for command in queued {
                self.engine.execute(command);
            }
        }
    }

    /// Sends `actor`'s trades request of `spec` for `bar_types` kept by request, executed at
    /// once; returns its index (see [`Self::answer`]).
    fn actor_request(
        &mut self,
        actor: &mut KeptRequester,
        bar_types: &[BarType],
        spec: KeptSpec,
    ) -> usize {
        let params = params_from_json(json!({
            "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "update_subscriptions": true,
        }));
        let at = |ns: Option<u64>| ns.map(|ns| UnixNanos::from(ns).to_datetime_utc());
        let request_id = actor
            .request_trades(
                self.instrument_id,
                at(spec.start),
                at(spec.end),
                None,
                Some(self.client_id),
                Some(params),
            )
            .unwrap();
        self.requests.push(request_id);
        self.pump();
        self.requests.len() - 1
    }

    /// Executes the kept request of `spec` for `bar_types`; returns its index, or the error
    /// that refused it (also its one terminal).
    fn request(&mut self, bar_types: &[BarType], spec: KeptSpec) -> Result<usize, String> {
        let index = self.requests.len();
        let request_id = UUID4::new();
        self.requests.push(request_id);

        let mut params = json!({
            "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "update_subscriptions": true,
            "interleave": !spec.plain,
        });
        if let Some(time_zone) = spec.time_zone {
            params["time_zone"] = json!(time_zone);
        }
        if let Some(filter_expr) = spec.filter_expr {
            params["filter_expr"] = json!(filter_expr);
        }
        if spec.point_data {
            params["time_range_generator"] = json!("");
            params["durations_seconds"] = json!([300]);
            params["point_data"] = json!(true);
        }

        let recorded = Rc::clone(&self.delivered);
        let cache = Rc::clone(&self.cache);
        let watched = self.watched.clone();
        let handler = TypedHandler::from(move |envelope: &DataResponseEnvelope| {
            match envelope.response() {
                Some(DataResponse::Trades(resp)) => {
                    for trade in resp.data.iter() {
                        let cached = cached_bars(&cache, &watched);
                        recorded
                            .borrow_mut()
                            .push(Kept::Trade(index, trade.trade_id, cached));
                    }
                }
                Some(DataResponse::Bars(resp)) => {
                    for bar in resp.data.iter() {
                        let cached = cached_bars(&cache, &watched);
                        recorded.borrow_mut().push(Kept::Bar(
                            index,
                            bar.bar_type,
                            bar.ts_event,
                            bar.volume,
                            cached,
                        ));
                    }
                }
                _ => {}
            }

            if envelope.completed() {
                let reason = envelope.failure_reason().map(str::to_owned);
                recorded.borrow_mut().push(Kept::End(index, reason));
            }
        });
        msgbus::register_response_handler(&request_id, handler);

        let at = |ns: Option<u64>| ns.map(|ns| UnixNanos::from(ns).to_datetime_utc());
        let now = self.clock.borrow().timestamp_ns();
        let (start, end) = (at(spec.start), at(spec.end));
        let limit = spec.limit.map(|limit| NonZeroUsize::new(limit).unwrap());
        let params = Some(serde_json::from_value(params).unwrap());
        let client_id = Some(self.client_id);
        let request = if spec.quotes {
            RequestCommand::Quotes(RequestQuotes::new(
                self.instrument_id,
                start,
                end,
                limit,
                client_id,
                request_id,
                now,
                params,
            ))
        } else {
            RequestCommand::Trades(RequestTrades::new(
                self.instrument_id,
                start,
                end,
                limit,
                client_id,
                request_id,
                now,
                params,
            ))
        };
        self.engine
            .execute_request(request)
            .map(|()| index)
            .map_err(|e| e.to_string())
    }

    /// Answers the `request`th request with trades (`ts`, size, trade ID) at one price in one
    /// completed envelope, with `failure` as its reason.
    fn answer(&mut self, request: usize, trades: &[(u64, u64, &str)], failure: Option<&str>) {
        let resp = self.trades_response(request, trades);
        let envelope = DataResponseEnvelope::new(DataResponse::Trades(resp), true, None);
        let envelope = match failure {
            Some(reason) => envelope.with_failure(reason.to_owned()),
            None => envelope,
        };
        self.engine.response_envelope(envelope);
    }

    /// Sends the `request`th request a part of its answer: trades (`ts`, size, trade ID) at one
    /// price in an envelope that does not complete it.
    fn answer_part(&mut self, request: usize, trades: &[(u64, u64, &str)]) {
        let resp = self.trades_response(request, trades);
        let envelope = DataResponseEnvelope::new(DataResponse::Trades(resp), false, None);
        self.engine.response_envelope(envelope);
    }

    /// Answers the `request`th request, a quotes request, with no quotes in one completed
    /// envelope.
    fn answer_quotes(&mut self, request: usize) {
        let resp = QuotesResponse::new(
            self.requests[request],
            self.client_id,
            self.instrument_id,
            Vec::<QuoteTick>::new(),
            None,
            None,
            self.clock.borrow().timestamp_ns(),
            None,
        );
        let envelope = DataResponseEnvelope::new(DataResponse::Quotes(resp), true, None);
        self.engine.response_envelope(envelope);
    }

    /// Releases the claim of the `request`th request on the builder of `bar_type`, as an actor
    /// does (see `ReleaseKeptBars`).
    fn release(&mut self, bar_type: BarType, request: usize) {
        let now = self.clock.borrow().timestamp_ns();
        self.engine
            .execute(DataCommand::ReleaseKeptBars(ReleaseKeptBars::new(
                bar_type,
                self.requests[request],
                UUID4::new(),
                now,
            )));
    }

    /// Ends the `request`th request with a payload-free terminal carrying no failure reason.
    fn end(&mut self, request: usize) {
        let terminal = DataResponseEnvelope::terminal(self.requests[request], None);
        self.engine.response_envelope(terminal);
    }

    /// Answers the `request`th request with trades (`ts`, size, trade ID) at one price in a bare
    /// response, as a client calling `DataEngine::response` does.
    fn answer_bare(&mut self, request: usize, trades: &[(u64, u64, &str)]) {
        let resp = self.trades_response(request, trades);
        self.engine.response(DataResponse::Trades(resp));
    }

    fn trades_response(&self, request: usize, trades: &[(u64, u64, &str)]) -> TradesResponse {
        let trades: Vec<TradeTick> = trades
            .iter()
            .map(|(ts, size, trade_id)| {
                make_trade(self.instrument_id, "1.00000", *size, trade_id, *ts)
            })
            .collect();
        TradesResponse::new(
            self.requests[request],
            self.client_id,
            self.instrument_id,
            trades,
            None,
            None,
            self.clock.borrow().timestamp_ns(),
            None,
        )
    }

    /// Subscribes `bar_type` as an actor does, with `time_zone` as its parameter.
    fn subscribe(&mut self, bar_type: BarType, time_zone: Option<&str>) {
        let params = time_zone
            .map(|zone| serde_json::from_value::<Params>(json!({ "time_zone": zone })).unwrap());
        let now = self.clock.borrow().timestamp_ns();
        self.engine
            .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
                SubscribeBars::new(
                    bar_type,
                    Some(self.client_id),
                    Some(self.venue),
                    UUID4::new(),
                    now,
                    None,
                    params,
                ),
            )));
    }

    /// Subscribes `bar_types` in one command, as an actor subscribing several bar types at once.
    fn subscribe_all(&mut self, bar_types: &[BarType]) {
        let now = self.clock.borrow().timestamp_ns();
        self.engine
            .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
                SubscribeBars::new(
                    bar_types.to_vec(),
                    Some(self.client_id),
                    Some(self.venue),
                    UUID4::new(),
                    now,
                    None,
                    None,
                ),
            )));
    }

    /// Processes a live quote (`ts`, bid size).
    fn live_quote(&mut self, ts: u64, bid_size: u64) {
        let quote = QuoteTick::new(
            self.instrument_id,
            Price::from("1.00000"),
            Price::from("1.00010"),
            Quantity::from(bid_size),
            Quantity::from(1),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        );
        self.engine.process_data(Data::Quote(quote));
    }

    /// Processes a live trade (`ts`, size, trade ID).
    fn live(&mut self, ts: u64, size: u64, trade_id: &str) {
        let trade = make_trade(self.instrument_id, "1.00000", size, trade_id, ts);
        self.engine.process_data(Data::Trade(trade));
    }

    /// Moves the engine clock to `to`, running each timer due by then in time order, one at a
    /// time, as a backtest's event loop runs them.
    fn advance(&self, to: u64) {
        loop {
            let handlers = {
                let mut clock = self.clock.borrow_mut();
                let Some(deadline) = clock
                    .timer_names()
                    .into_iter()
                    .filter_map(|name| clock.next_time_ns(name))
                    .filter(|deadline| deadline.as_u64() <= to)
                    .min()
                else {
                    break;
                };
                let events = clock.advance_time(deadline, true);
                clock.match_handlers(events)
            };
            for handler in handlers {
                handler.callback.call(handler.event);
            }
        }
        let _ = self
            .clock
            .borrow_mut()
            .advance_time(UnixNanos::from(to), true);
    }

    fn delivered(&self) -> Vec<Kept> {
        self.delivered.borrow().clone()
    }

    /// The commands the client received after the first `skip`.
    fn client_commands(&self, skip: usize) -> Vec<DataCommand> {
        self.recorder.borrow()[skip..].to_vec()
    }
}

/// The failure reason a kept request answered by a bare response ends with.
const KEPT_ANSWERED_BARE: &str = "A kept request answered without a plan: its answer came as a \
                                  bare response, which does not certify that it holds every \
                                  record of its window, so its builders are not kept";

/// 2024-01-08, a Monday: New York civil `time` that day.
fn monday(time: &str) -> u64 {
    new_york(&format!("2024-01-08T{time}"))
}

/// A kept 5-MINUTE request from 10:00 to 10:08 minus 1 ns, answered with a1 at 10:01 and a2 at
/// 10:06: the 10:05 bar closes before a2; a2's bar, closing at 10:10, stays open.
fn first_kept_request(fixture: &mut KeptFixture, five: BarType) -> usize {
    let request = fixture
        .request(&[five], kept_window(monday("10:00:00"), monday("10:08:00")))
        .unwrap();
    fixture.answer(
        request,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );
    request
}

fn first_kept_answer(five: BarType) -> Vec<Kept> {
    vec![
        kept_trade(0, "a1", &[0]),
        kept_bar(0, five, monday("10:05:00"), 1, &[1]),
        kept_trade(0, "a2", &[1]),
        Kept::End(0, None),
    ]
}

#[rstest]
fn test_kept_request_continues_its_replay_with_every_handler_on_the_new_answer(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, tick) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "3-TICK"));
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("12:00:00"),
        &[five, tick],
    );

    let first = fixture
        .request(
            &[five, tick],
            kept_window(monday("10:00:00"), monday("10:08:00")),
        )
        .unwrap();
    fixture.answer(
        first,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );
    let second = fixture
        .request(
            &[tick, five],
            kept_window(monday("10:08:00"), monday("10:15:00")),
        )
        .unwrap();
    fixture.answer(
        second,
        &[(monday("10:09:00"), 4, "b1"), (monday("10:12:00"), 8, "b2")],
        None,
    );

    // b1 completes the tick bar a1 and a2 opened; then the 10:10 close, a2's and b1's, fires
    // before b2 into the second answer, though no trade of it completes the 5-MINUTE bar
    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "a1", &[0, 0]),
            kept_bar(0, five, monday("10:05:00"), 1, &[1, 0]),
            kept_trade(0, "a2", &[1, 0]),
            Kept::End(0, None),
            kept_bar(1, tick, monday("10:09:00"), 7, &[1, 1]),
            kept_trade(1, "b1", &[1, 1]),
            kept_bar(1, five, monday("10:10:00"), 6, &[2, 1]),
            kept_trade(1, "b2", &[2, 1]),
            Kept::End(1, None),
        ]
    );
}

#[derive(Clone, Copy, Debug)]
enum Refusal {
    Gap,
    Overlap,
    Earlier,
    NoStart,
    Limit,
    SomeExist,
}

#[rstest]
#[case::gap(Refusal::Gap, "leaves a gap after the coverage through")]
#[case::overlap(Refusal::Overlap, "overlaps the coverage through")]
#[case::earlier(Refusal::Earlier, "is before the start of the replay that keeps")]
#[case::no_start(Refusal::NoStart, "it gives no `start`")]
#[case::limit(Refusal::Limit, "it names a `limit`")]
#[case::some_exist(Refusal::SomeExist, "has a kept builder and")]
fn test_kept_request_refused_by_name_leaves_its_replay_as_it_was(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] refusal: Refusal,
    #[case] reason: &str,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, tick) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "3-TICK"));
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    first_kept_request(&mut fixture, five);
    let next = kept_window(monday("10:08:00"), monday("10:15:00"));
    let (bar_types, spec) = match refusal {
        Refusal::Gap => (
            vec![five],
            kept_window(monday("10:08:00") + 1, monday("10:15:00")),
        ),
        Refusal::Overlap => (
            vec![five],
            kept_window(monday("10:07:00"), monday("10:15:00")),
        ),
        Refusal::Earlier => (
            vec![five],
            kept_window(monday("09:59:00"), monday("10:15:00")),
        ),
        Refusal::NoStart => (
            vec![five],
            KeptSpec {
                start: None,
                ..next
            },
        ),
        Refusal::Limit => (
            vec![five],
            KeptSpec {
                limit: Some(10),
                ..next
            },
        ),
        Refusal::SomeExist => (vec![five, tick], next),
    };

    let error = fixture.request(&bar_types, spec).unwrap_err();
    let continued = fixture.request(&[five], next).unwrap();
    fixture.answer(continued, &[(monday("10:12:00"), 4, "b1")], None);

    assert!(
        error.starts_with("Cannot keep the bar builders of an `update_subscriptions` request"),
        "{error}"
    );
    assert!(error.contains(reason), "{error}");
    // The refused request changed nothing: the next one continues a2's open bar
    let mut expected = first_kept_answer(five);
    expected.extend([
        Kept::End(1, Some(error)),
        kept_bar(2, five, monday("10:10:00"), 2, &[2]),
        kept_trade(2, "b1", &[2]),
        Kept::End(2, None),
    ]);
    assert_eq!(fixture.delivered(), expected);
}

#[derive(Clone, Copy, Debug)]
enum Occupied {
    TwoReplays,
    NotAllBuilders,
    Subscribed,
    InFlight,
}

#[rstest]
#[case::two_replays(Occupied::TwoReplays, "are kept by two replays")]
#[case::not_all_builders(Occupied::NotAllBuilders, "are not all the builders of the replay")]
#[case::subscribed(Occupied::Subscribed, "is already running for a subscription")]
#[case::in_flight(Occupied::InFlight, "is in use by the request")]
fn test_kept_request_refused_by_name_for_builders_it_cannot_continue(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] occupied: Occupied,
    #[case] reason: &str,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, tick) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "3-TICK"));
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let window = kept_window(monday("10:00:00"), monday("10:08:00"));
    let next = kept_window(monday("10:08:00"), monday("10:15:00"));
    let a1 = [(monday("10:01:00"), 1, "a1")];
    let asked = match occupied {
        Occupied::TwoReplays => {
            for bar_types in [[five], [tick]] {
                let request = fixture.request(&bar_types, window).unwrap();
                fixture.answer(request, &a1, None);
            }
            vec![five, tick]
        }
        Occupied::NotAllBuilders => {
            let request = fixture.request(&[five, tick], window).unwrap();
            fixture.answer(request, &a1, None);
            vec![five]
        }
        Occupied::Subscribed => {
            let request = fixture.request(&[five], window).unwrap();
            fixture.answer(request, &a1, None);
            fixture.subscribe(five, None);
            vec![five]
        }
        Occupied::InFlight => {
            fixture.request(&[five], window).unwrap();
            vec![five]
        }
    };
    let ends_before = fixture
        .delivered()
        .iter()
        .filter(|kept| matches!(kept, Kept::End(..)))
        .count();

    let error = fixture.request(&asked, next).unwrap_err();

    assert!(
        error.starts_with("Cannot keep the bar builders of an `update_subscriptions` request"),
        "{error}"
    );
    assert!(error.contains(reason), "{error}");
    let delivered = fixture.delivered();
    assert_eq!(
        delivered.last(),
        Some(&Kept::End(fixture.requests.len() - 1, Some(error)))
    );
    assert_eq!(
        delivered
            .iter()
            .filter(|kept| matches!(kept, Kept::End(..)))
            .count(),
        ends_before + 1
    );
}

/// Runs kept requests for a 3-TICK and a 2-DAY New York bar from Monday 00:00 over `windows`
/// (each next starts where the one before ended plus 1 ns) at Friday 00:00, answering each
/// with the trades of its window; returns what is delivered, without the requests' indices and
/// ends.
fn run_kept_windows(
    audusd_sim: CurrencyPair,
    client_id: ClientId,
    venue: Venue,
    windows: &[(u64, u64)],
) -> Vec<Kept> {
    let id = audusd_sim.id;
    let (tick, two_days) = (bar_type_of(id, "3-TICK"), bar_type_of(id, "2-DAY"));
    let now = new_york("2024-01-12T00:00:00");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, now, &[tick, two_days]);
    let trades = [
        (new_york("2024-01-08T10:00:00"), 1, "m1"),
        (new_york("2024-01-09T09:00:00"), 2, "t1"),
        (new_york("2024-01-09T15:00:00"), 4, "t2"),
        (new_york("2024-01-10T09:00:00"), 8, "w1"),
        (new_york("2024-01-11T09:00:00"), 16, "h1"),
    ];

    for &(start, next) in windows {
        let spec = KeptSpec {
            time_zone: Some("America/New_York"),
            ..kept_window(start, next)
        };
        let request = fixture.request(&[tick, two_days], spec).unwrap();
        let answered: Vec<_> = trades
            .iter()
            .copied()
            .filter(|(ts, ..)| (start..next).contains(ts))
            .collect();
        fixture.answer(request, &answered, None);
    }

    fixture
        .delivered()
        .into_iter()
        .filter_map(|kept| match kept {
            Kept::Trade(_, trade_id, cached) => Some(Kept::Trade(0, trade_id, cached)),
            Kept::Bar(_, bar_type, ts, volume, cached) => {
                Some(Kept::Bar(0, bar_type, ts, volume, cached))
            }
            Kept::End(..) => None,
            other => Some(other),
        })
        .collect()
}

#[rstest]
fn test_kept_requests_across_two_windows_build_the_tick_and_two_day_bars_of_one_request(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (tick, two_days) = (bar_type_of(id, "3-TICK"), bar_type_of(id, "2-DAY"));
    let (monday_open, split, end) = (
        new_york("2024-01-08T00:00:00"),
        new_york("2024-01-09T12:00:00"),
        new_york("2024-01-11T12:00:00"),
    );

    let one = run_kept_windows(audusd_sim.clone(), client_id, venue, &[(monday_open, end)]);
    let two = run_kept_windows(
        audusd_sim,
        client_id,
        venue,
        &[(monday_open, split), (split, end)],
    );

    // t2 completes the tick bar m1 and t1 opened in the first window; the 2-DAY bar keeps
    // Monday's grid and closes Wednesday 00:00 with the trades of both windows
    assert_eq!(two, one);
    assert_eq!(
        one,
        vec![
            kept_trade(0, "m1", &[0, 0]),
            kept_trade(0, "t1", &[0, 0]),
            kept_bar(0, tick, new_york("2024-01-09T15:00:00"), 7, &[1, 0]),
            kept_trade(0, "t2", &[1, 0]),
            kept_bar(0, two_days, new_york("2024-01-10T00:00:00"), 7, &[1, 1]),
            kept_trade(0, "w1", &[1, 1]),
            kept_trade(0, "h1", &[1, 1]),
        ]
    );
}

#[rstest]
fn test_failed_kept_request_removes_its_replay_and_the_next_starts_a_new_one(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let window = kept_window(monday("10:00:00"), monday("10:08:00"));
    let failed = fixture.request(&[five], window).unwrap();
    fixture.answer(
        failed,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        Some("the source stopped"),
    );

    // The same window again: admitted only because no builder of it is left
    let started = fixture
        .request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")))
        .unwrap();
    fixture.answer(
        started,
        &[(monday("10:07:00"), 4, "c1"), (monday("10:12:00"), 8, "c2")],
        None,
    );

    // A new builder: the 10:10 bar holds c1 alone, not the failed request's a2
    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "a1", &[0]),
            kept_bar(0, five, monday("10:05:00"), 1, &[1]),
            kept_trade(0, "a2", &[1]),
            Kept::End(0, Some("the source stopped".to_owned())),
            kept_trade(1, "c1", &[1]),
            kept_bar(1, five, monday("10:10:00"), 4, &[2]),
            kept_trade(1, "c2", &[2]),
            Kept::End(1, None),
        ]
    );
}

#[rstest]
fn test_releasing_a_kept_bar_type_never_subscribed_by_its_request_removes_its_builder_and_replay(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let request = first_kept_request(&mut fixture, five);
    let sent = fixture.client_commands(0).len();

    fixture.release(five, request);
    // The first window again: admitted only because no builder of it is left
    let started = fixture
        .request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")))
        .unwrap();
    fixture.answer(started, &[(monday("10:12:00"), 4, "c1")], None);

    // No subscription of it was released at the client
    assert!(
        !fixture
            .client_commands(sent)
            .iter()
            .any(|command| matches!(command, DataCommand::Unsubscribe(_))),
        "{:?}",
        fixture.client_commands(sent)
    );
    let mut expected = first_kept_answer(five);
    expected.extend([kept_trade(1, "c1", &[1]), Kept::End(1, None)]);
    assert_eq!(fixture.delivered(), expected);
}

#[rstest]
fn test_engine_stop_and_start_between_kept_requests_leave_the_pending_close_armed(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    first_kept_request(&mut fixture, five);

    fixture.engine.stop();
    fixture.engine.start();
    let continued = fixture
        .request(&[five], kept_window(monday("10:08:00"), monday("10:15:00")))
        .unwrap();
    fixture.answer(continued, &[(monday("10:12:00"), 4, "b1")], None);

    let mut expected = first_kept_answer(five);
    expected.extend([
        kept_bar(1, five, monday("10:10:00"), 2, &[2]),
        kept_trade(1, "b1", &[2]),
        Kept::End(1, None),
    ]);
    assert_eq!(fixture.delivered(), expected);
}

#[rstest]
fn test_subscription_after_a_kept_answer_publishes_the_range_bar_at_the_boundary_before_the_next_trade(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, range) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "10-RANGE"));
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("17:00:00"),
        &[five, range],
    );
    // The answer ends at now minus 1 ns; its range bar is open since 16:59:40
    let request = fixture
        .request(
            &[five, range],
            kept_window(monday("16:50:00"), monday("17:00:00")),
        )
        .unwrap();
    fixture.answer(request, &[(monday("16:59:40"), 1, "t1")], None);

    fixture.subscribe(five, None);
    fixture.subscribe(range, None);
    fixture.live(monday("17:00:00"), 2, "t2");
    fixture.advance(monday("17:00:00"));
    fixture.advance(monday("17:30:00"));
    fixture.live(monday("17:30:00"), 4, "t3");
    fixture.advance(monday("18:00:05"));
    fixture.live(monday("18:00:05"), 8, "t4");

    // The 17:00 close, at now, fires once on the engine clock, after t2 stamped then; the
    // range bar the answer opened completes once, at 18:00, before t4
    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "t1", &[0, 0]),
            Kept::End(0, None),
            live_trade("t2", &[0, 0]),
            published(five, monday("17:00:00"), 3, &[1, 0]),
            live_trade("t3", &[1, 0]),
            published(five, monday("17:35:00"), 4, &[2, 0]),
            published(range, monday("18:00:00"), 7, &[2, 1]),
            live_trade("t4", &[2, 1]),
        ]
    );
}

#[rstest]
fn test_subscription_at_the_range_boundary_builds_nothing_inside_the_call(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let range = bar_type_of(audusd_sim.id, "10-RANGE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("18:00:00"), &[range]);
    let request = fixture
        .request(
            &[range],
            kept_window(monday("17:50:00"), monday("18:00:00")),
        )
        .unwrap();
    fixture.answer(request, &[(monday("17:59:40"), 1, "t1")], None);

    fixture.subscribe(range, None);
    let at_the_call = fixture.delivered();
    fixture.advance(monday("18:00:00"));
    fixture.live(monday("18:00:05"), 2, "t2");

    assert_eq!(
        at_the_call,
        vec![kept_trade(0, "t1", &[0]), Kept::End(0, None)]
    );
    assert_eq!(
        fixture.delivered()[at_the_call.len()..],
        [
            published(range, monday("18:00:00"), 1, &[1]),
            live_trade("t2", &[1]),
        ]
    );
}

#[rstest]
fn test_subscription_adopting_a_two_day_bar_kept_across_two_requests_keeps_mondays_grid(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let two_days = bar_type_of(audusd_sim.id, "2-DAY");
    let new_york_zone = Some("America/New_York");
    let now = new_york("2024-01-11T18:00:00");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, now, &[two_days]);
    let windows = [
        (
            new_york("2024-01-08T00:00:00"),
            new_york("2024-01-09T12:00:00"),
        ),
        (
            new_york("2024-01-09T12:00:00"),
            new_york("2024-01-11T12:00:00"),
        ),
    ];
    let trades = [
        [(new_york("2024-01-08T10:00:00"), 1, "m1")],
        [(new_york("2024-01-09T15:00:00"), 2, "t1")],
    ];
    for ((start, next), answered) in windows.into_iter().zip(trades) {
        let spec = KeptSpec {
            time_zone: new_york_zone,
            ..kept_window(start, next)
        };
        let request = fixture.request(&[two_days], spec).unwrap();
        fixture.answer(request, &answered, None);
    }

    fixture.subscribe(two_days, new_york_zone);
    fixture.live(new_york("2024-01-11T20:00:00"), 4, "h1");
    fixture.advance(new_york("2024-01-13T00:00:00"));

    // Monday's grid: Wednesday 00:00 (m1, t1) at the second request's end of coverage, then
    // Friday 00:00 live (h1), not Saturday 00:00 as a grid from Thursday, when it is taken over
    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "m1", &[0]),
            Kept::End(0, None),
            kept_trade(1, "t1", &[0]),
            kept_bar(1, two_days, new_york("2024-01-10T00:00:00"), 3, &[1]),
            Kept::End(1, None),
            live_trade("h1", &[1]),
            published(two_days, new_york("2024-01-12T00:00:00"), 4, &[2]),
        ]
    );
}

#[rstest]
fn test_subscription_held_back_during_a_kept_request_gets_its_held_trades_bars_in_live_order(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (three, five) = (bar_type_of(id, "3-MINUTE"), bar_type_of(id, "5-MINUTE"));
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("10:08:00"),
        &[three, five],
    );
    let request = fixture
        .request(
            &[three, five],
            kept_window(monday("10:00:00"), monday("10:02:00")),
        )
        .unwrap();
    fixture.subscribe(three, None);
    fixture.subscribe(five, None);
    for (ts, size, trade_id) in [
        ("10:02:00", 2, "h1"),
        ("10:04:00", 4, "h2"),
        ("10:07:00", 8, "h3"),
    ] {
        fixture.live(monday(ts), size, trade_id);
    }
    fixture.answer(request, &[(monday("10:00:30"), 1, "a1")], None);
    fixture.advance(monday("10:09:30"));
    fixture.live(monday("10:09:30"), 16, "h4");
    fixture.advance(monday("10:10:00"));

    // The held trades drive the clock as the answer's trades: 10:03 (a1, h1), 10:05 (a1, h1,
    // h2) and 10:06 (h2) close between them, into the answer; then the subscriptions go live
    assert_eq!(
        fixture.delivered(),
        vec![
            live_trade("h1", &[0, 0]),
            live_trade("h2", &[0, 0]),
            live_trade("h3", &[0, 0]),
            kept_trade(0, "a1", &[0, 0]),
            kept_bar(0, three, monday("10:03:00"), 3, &[1, 0]),
            kept_bar(0, five, monday("10:05:00"), 7, &[1, 1]),
            kept_bar(0, three, monday("10:06:00"), 4, &[2, 1]),
            Kept::End(0, None),
            published(three, monday("10:09:00"), 8, &[3, 1]),
            live_trade("h4", &[3, 1]),
            published(five, monday("10:10:00"), 24, &[3, 2]),
        ]
    );
}

/// A kept request ending at 17:00:00.011, the engine clock's time, whose answer holds a trade
/// stamped exactly then: the same trade, held live for a subscription made while the answer was
/// in flight, is not fed again at the release, so the 17:05 bar counts it once.
#[rstest]
fn test_held_back_subscription_drops_a_held_trade_the_answer_already_holds(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let now = monday("17:00:00.011");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, now, &[five]);
    let spec = KeptSpec {
        start: Some(monday("16:55:00")),
        end: Some(now),
        ..KeptSpec::default()
    };
    let request = fixture.request(&[five], spec).unwrap();
    fixture.subscribe(five, None);
    fixture.live(now, 4, "x");
    fixture.answer(
        request,
        &[(monday("16:58:00"), 1, "a1"), (now, 4, "x")],
        None,
    );
    fixture.advance(monday("17:05:00"));

    assert_eq!(
        fixture.delivered(),
        vec![
            live_trade("x", &[0]),
            kept_trade(0, "a1", &[0]),
            kept_bar(0, five, monday("17:00:00"), 1, &[1]),
            kept_trade(0, "x", &[1]),
            Kept::End(0, None),
            published(five, monday("17:05:00"), 4, &[2]),
        ]
    );
}

/// A kept 5-MINUTE request through 16:59:59.999, given its 16:58 trade; while its answer is
/// unfinished, a subscription to the same standard bar type built from 1-MINUTE-EXTERNAL bars
/// is refused by name, before and after a plain 5-MINUTE subscription that waits on the request:
/// it would build the request's builder from source bars on top of its trades. Nothing of it
/// reaches the client, and a source bar stamped 17:00 (size 8) reaches no builder: the answer,
/// its terminal and the plain subscription's 17:00 bar (1 + 2) are as without it. Once the
/// request has ended, the same composite subscription is admitted.
#[rstest]
fn test_composite_subscription_sharing_an_unfinished_kept_builder_is_refused_by_name(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let composite = BarType::from(format!("{five}@1-MINUTE-EXTERNAL").as_str());
    assert_eq!(composite.standard(), five);
    let now = monday("17:00:30");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, now, &[five]);
    let spec = KeptSpec {
        start: Some(monday("16:55:00")),
        end: Some(monday("17:00:00") - 1_000_000),
        ..KeptSpec::default()
    };
    let request = fixture.request(&[five], spec).unwrap();
    fixture.answer_part(request, &[(monday("16:58:00"), 1, "a1")]);
    let sent = fixture.client_commands(0).len();

    let before_plain = subscribe_result(&mut fixture, composite).unwrap_err();
    assert!(fixture.client_commands(sent).is_empty());
    fixture.subscribe(five, None);
    let sent = fixture.client_commands(0).len();
    let after_plain = subscribe_result(&mut fixture, composite).unwrap_err();
    assert!(fixture.client_commands(sent).is_empty());
    for error in [before_plain, after_plain] {
        assert_eq!(
            error.to_string(),
            format!(
                "Cannot subscribe {composite}: a kept request is building {five} from another \
                 source; subscribe after it ends"
            ),
        );
    }

    let source_bar = Bar::new(
        composite.composite(),
        Price::from("1.00000"),
        Price::from("1.00000"),
        Price::from("1.00000"),
        Price::from("1.00000"),
        Quantity::from(8),
        UnixNanos::from(monday("17:00:00")),
        UnixNanos::from(monday("17:00:00")),
    );
    fixture.engine.process_data(Data::Bar(source_bar));
    fixture.answer(request, &[(monday("16:59:00"), 2, "a2")], None);
    fixture.advance(monday("17:05:00"));

    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "a1", &[0]),
            kept_trade(0, "a2", &[0]),
            Kept::End(0, None),
            published(five, monday("17:00:00"), 3, &[1]),
        ]
    );
    subscribe_result(&mut fixture, composite).unwrap();
}

/// A kept 5-MINUTE request is unfinished: a subscription to 3-MINUTE built from
/// 1-MINUTE-EXTERNAL bars shares none of its builders, so it is admitted, its source subscribed
/// at the client, as without the request.
#[rstest]
fn test_composite_subscription_sharing_no_kept_builder_is_admitted(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let composite =
        BarType::from(format!("{}@1-MINUTE-EXTERNAL", bar_type_of(id, "3-MINUTE")).as_str());
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("17:00:30"), &[five]);
    let spec = KeptSpec {
        start: Some(monday("16:55:00")),
        end: Some(monday("17:00:00") - 1_000_000),
        ..KeptSpec::default()
    };
    let request = fixture.request(&[five], spec).unwrap();
    fixture.answer_part(request, &[(monday("16:58:00"), 1, "a1")]);
    let sent = fixture.client_commands(0).len();

    subscribe_result(&mut fixture, composite).unwrap();

    assert!(
        fixture.client_commands(sent).iter().any(|command| matches!(
            command,
            DataCommand::Subscribe(SubscribeCommand::Bars(subscribe))
                if subscribe.bar_types.as_slice() == [composite.composite()]
        )),
        "{:?}",
        fixture.client_commands(sent)
    );
}

/// Executes an actor's subscription to `bar_type` at the fixture's clock; returns its result.
fn subscribe_result(fixture: &mut KeptFixture, bar_type: BarType) -> anyhow::Result<()> {
    let now = fixture.clock.borrow().timestamp_ns();
    fixture
        .engine
        .execute_subscribe(SubscribeCommand::Bars(SubscribeBars::new(
            bar_type,
            Some(fixture.client_id),
            Some(fixture.venue),
            UUID4::new(),
            now,
            None,
            None,
        )))
}

/// The command an actor sends to unsubscribe `bar_type` at the fixture's clock.
fn unsubscribe(fixture: &mut KeptFixture, bar_type: BarType) {
    let now = fixture.clock.borrow().timestamp_ns();
    fixture
        .engine
        .execute(DataCommand::Unsubscribe(UnsubscribeCommand::Bars(
            UnsubscribeBars::new(
                bar_type,
                Some(fixture.client_id),
                Some(fixture.venue),
                UUID4::new(),
                now,
                None,
                None,
            ),
        )));
}

/// A kept bar type released by its request while that request is in flight: the request runs to
/// its end, its answer whole, then its replay is removed, not kept, so the first window can be
/// asked again; no subscription is released at the client.
#[rstest]
fn test_releasing_a_kept_bar_type_while_its_request_runs_removes_the_replay_at_its_end(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let request = fixture
        .request(&[five], kept_window(monday("10:00:00"), monday("10:08:00")))
        .unwrap();
    let sent = fixture.client_commands(0).len();
    fixture.release(five, request);
    fixture.answer(
        request,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );

    // The first window again: admitted only because no builder of it is left
    let started = fixture
        .request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")))
        .unwrap();
    fixture.answer(started, &[(monday("10:12:00"), 4, "c1")], None);

    assert!(
        !fixture
            .client_commands(sent)
            .iter()
            .any(|command| matches!(command, DataCommand::Unsubscribe(_))),
        "{:?}",
        fixture.client_commands(sent)
    );
    let mut expected = first_kept_answer(five);
    expected.extend([kept_trade(1, "c1", &[1]), Kept::End(1, None)]);
    assert_eq!(fixture.delivered(), expected);
}

/// The same with a subscription held back on another builder of the request: a release removes
/// only the released bar type's builder. At the request's end the held-back 3-MINUTE builder is
/// handed over with a1 in its open bar and its held trades fed as the answer's; the released
/// 5-MINUTE bar type has no builder left.
#[rstest]
fn test_releasing_a_kept_bar_type_while_its_request_runs_removes_it_alone_and_hands_a_held_back_one_over(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (three, five) = (bar_type_of(id, "3-MINUTE"), bar_type_of(id, "5-MINUTE"));
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("10:08:00"),
        &[three, five],
    );
    let request = fixture
        .request(
            &[three, five],
            kept_window(monday("10:00:00"), monday("10:02:00")),
        )
        .unwrap();
    fixture.subscribe(three, None);
    for (ts, size, trade_id) in [
        ("10:02:00", 2, "h1"),
        ("10:04:00", 4, "h2"),
        ("10:07:00", 8, "h3"),
    ] {
        fixture.live(monday(ts), size, trade_id);
    }
    fixture.release(five, request);
    fixture.answer(request, &[(monday("10:00:30"), 1, "a1")], None);
    fixture.advance(monday("10:09:30"));
    fixture.live(monday("10:09:30"), 16, "h4");
    fixture.advance(monday("10:10:00"));

    // No 5-MINUTE builder is left to continue: a request from 10:00 starts a new one
    assert!(
        fixture
            .request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")))
            .is_ok()
    );
    assert_eq!(
        fixture.delivered(),
        vec![
            live_trade("h1", &[0, 0]),
            live_trade("h2", &[0, 0]),
            live_trade("h3", &[0, 0]),
            kept_trade(0, "a1", &[0, 0]),
            kept_bar(0, three, monday("10:03:00"), 3, &[1, 0]),
            kept_bar(0, three, monday("10:06:00"), 4, &[2, 0]),
            Kept::End(0, None),
            published(three, monday("10:09:00"), 8, &[3, 0]),
            live_trade("h4", &[3, 0]),
        ]
    );
}

/// A kept request naming a filter expression is refused by name before dispatch, as one naming a
/// `limit` is, and changes nothing: the next request continues the first one's open bar.
#[rstest]
fn test_kept_request_naming_a_filter_expression_is_refused_by_name_and_leaves_its_replay_as_it_was(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    first_kept_request(&mut fixture, five);
    let next = kept_window(monday("10:08:00"), monday("10:15:00"));
    let sent = fixture.client_commands(0).len();

    let error = fixture
        .request(
            &[five],
            KeptSpec {
                filter_expr: Some("size > 0"),
                ..next
            },
        )
        .unwrap_err();
    assert!(fixture.client_commands(sent).is_empty());
    let continued = fixture.request(&[five], next).unwrap();
    fixture.answer(continued, &[(monday("10:12:00"), 4, "b1")], None);

    assert!(
        error.starts_with("Cannot keep the bar builders of an `update_subscriptions` request"),
        "{error}"
    );
    assert!(error.contains("it names a filter expression"), "{error}");
    let mut expected = first_kept_answer(five);
    expected.extend([
        Kept::End(1, Some(error)),
        kept_bar(2, five, monday("10:10:00"), 2, &[2]),
        kept_trade(2, "b1", &[2]),
        Kept::End(2, None),
    ]);
    assert_eq!(fixture.delivered(), expected);
}

/// A kept request answered by a bare response, which certifies no coverage, ends as a failed one
/// does, with the reason naming it: its replay is removed, so the same window can be asked again
/// and starts a new builder (the 10:10 bar holds c1 alone, not a2).
#[rstest]
fn test_kept_request_answered_by_a_bare_response_removes_its_replay_and_the_next_starts_a_new_one(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let bare = fixture
        .request(&[five], kept_window(monday("10:00:00"), monday("10:08:00")))
        .unwrap();
    fixture.answer_bare(
        bare,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
    );

    let started = fixture
        .request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")))
        .unwrap();
    fixture.answer(
        started,
        &[(monday("10:07:00"), 4, "c1"), (monday("10:12:00"), 8, "c2")],
        None,
    );

    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "a1", &[0]),
            kept_bar(0, five, monday("10:05:00"), 1, &[1]),
            kept_trade(0, "a2", &[1]),
            Kept::End(0, Some(KEPT_ANSWERED_BARE.to_owned())),
            kept_trade(1, "c1", &[1]),
            kept_bar(1, five, monday("10:10:00"), 4, &[2]),
            kept_trade(1, "c2", &[2]),
            Kept::End(1, None),
        ]
    );
}

/// The same with a subscription held back on a builder of the request: nothing is handed over;
/// the subscription gets a new builder, started at the request's end and fed its held trades,
/// so its 10:09 bar holds h1, h2 and h3 (14) and not the answer's a1.
#[rstest]
fn test_kept_request_answered_by_a_bare_response_gives_a_held_back_subscription_a_new_builder(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (three, five) = (bar_type_of(id, "3-MINUTE"), bar_type_of(id, "5-MINUTE"));
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("10:08:00"),
        &[three, five],
    );
    let request = fixture
        .request(
            &[three, five],
            kept_window(monday("10:00:00"), monday("10:02:00")),
        )
        .unwrap();
    fixture.subscribe(three, None);
    for (ts, size, trade_id) in [
        ("10:02:00", 2, "h1"),
        ("10:04:00", 4, "h2"),
        ("10:07:00", 8, "h3"),
    ] {
        fixture.live(monday(ts), size, trade_id);
    }
    fixture.answer_bare(request, &[(monday("10:00:30"), 1, "a1")]);
    fixture.advance(monday("10:09:30"));
    fixture.live(monday("10:09:30"), 16, "h4");
    fixture.advance(monday("10:10:00"));

    // No 5-MINUTE builder is left to continue: a request from 10:00 starts a new one
    assert!(
        fixture
            .request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")))
            .is_ok()
    );
    assert_eq!(
        fixture.delivered(),
        vec![
            live_trade("h1", &[0, 0]),
            live_trade("h2", &[0, 0]),
            live_trade("h3", &[0, 0]),
            kept_trade(0, "a1", &[0, 0]),
            Kept::End(0, Some(KEPT_ANSWERED_BARE.to_owned())),
            published(three, monday("10:09:00"), 14, &[1, 0]),
            live_trade("h4", &[1, 0]),
        ]
    );
}

/// A kept request for 3-MINUTE and 5-MINUTE bars from 10:00, answered through 10:05 at 10:08,
/// both subscriptions held back while it ran: its last bar, 10:05, and its terminal go out
/// first; then the 10:06 bar its hand-over catches up is published.
#[rstest]
fn test_kept_request_sends_its_last_bars_and_its_end_before_its_hand_over_publishes(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (three, five) = (bar_type_of(id, "3-MINUTE"), bar_type_of(id, "5-MINUTE"));
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("10:08:00"),
        &[three, five],
    );
    let spec = KeptSpec {
        start: Some(monday("10:00:00")),
        end: Some(monday("10:05:00")),
        ..KeptSpec::default()
    };
    let request = fixture.request(&[three, five], spec).unwrap();
    fixture.subscribe(three, None);
    fixture.subscribe(five, None);
    fixture.answer(
        request,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:04:00"), 1, "a2")],
        None,
    );

    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "a1", &[0, 0]),
            kept_bar(0, three, monday("10:03:00"), 1, &[1, 0]),
            kept_trade(0, "a2", &[1, 0]),
            kept_bar(0, five, monday("10:05:00"), 2, &[1, 1]),
            Kept::End(0, None),
            published(three, monday("10:06:00"), 1, &[2, 1]),
        ]
    );
}

/// The same release on a kept request that does not interleave, answered at 10:10: its terminal
/// goes out first; then the bars of the live trades held for its subscriptions and the bars due
/// by 10:10 are published to them, in deadline order, not only cached.
#[rstest]
fn test_plain_kept_request_publishes_its_held_trades_bars_after_its_end(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (three, five) = (bar_type_of(id, "3-MINUTE"), bar_type_of(id, "5-MINUTE"));
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("10:10:00"),
        &[three, five],
    );
    let spec = KeptSpec {
        plain: true,
        ..kept_window(monday("10:00:00"), monday("10:02:00"))
    };
    let request = fixture.request(&[three, five], spec).unwrap();
    fixture.subscribe(three, None);
    fixture.subscribe(five, None);
    for (ts, size, trade_id) in [
        ("10:02:00", 2, "h1"),
        ("10:04:00", 4, "h2"),
        ("10:07:00", 8, "h3"),
    ] {
        fixture.live(monday(ts), size, trade_id);
    }
    fixture.answer(request, &[(monday("10:00:30"), 1, "a1")], None);
    fixture.advance(monday("10:10:00"));

    assert_eq!(
        fixture.delivered(),
        vec![
            live_trade("h1", &[0, 0]),
            live_trade("h2", &[0, 0]),
            live_trade("h3", &[0, 0]),
            kept_trade(0, "a1", &[0, 0]),
            Kept::End(0, None),
            published(three, monday("10:03:00"), 3, &[1, 0]),
            published(five, monday("10:05:00"), 7, &[1, 1]),
            published(three, monday("10:06:00"), 4, &[2, 1]),
            published(three, monday("10:09:00"), 8, &[3, 1]),
            published(five, monday("10:10:00"), 8, &[3, 2]),
        ]
    );
}

/// Populated 2-MINUTE and 3-MINUTE builders kept through 10:01, handed over together at 10:07
/// (both subscriptions held back while their plain request ran, which names 3-MINUTE first):
/// they catch up together, in deadline order, not in the order the request named them in.
#[rstest]
fn test_kept_builders_handed_over_in_one_turn_catch_up_together_in_deadline_order(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (two, three) = (bar_type_of(id, "2-MINUTE"), bar_type_of(id, "3-MINUTE"));
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("10:07:00"),
        &[two, three],
    );
    let request = fixture
        .request(
            &[three, two],
            KeptSpec {
                plain: true,
                ..kept_window(monday("10:00:00"), monday("10:01:00"))
            },
        )
        .unwrap();
    fixture.subscribe(three, None);
    fixture.subscribe(two, None);
    fixture.answer(request, &[(monday("10:00:30"), 1, "a1")], None);

    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "a1", &[0, 0]),
            Kept::End(0, None),
            published(two, monday("10:02:00"), 1, &[1, 0]),
            published(three, monday("10:03:00"), 1, &[1, 1]),
        ]
    );
}

/// Kept BID 2-MINUTE and 3-MINUTE builders of a quotes request covered through 10:00, both
/// subscribed while their request ran, each holding the live quotes of 1, 2 and 4 at 10:01, 10:04 and 10:07: the held
/// quotes are fed to both in arrival order, the clock readied before each, so each builder's
/// closes fire after the quotes before them.
#[rstest]
fn test_held_quotes_of_several_kept_builders_are_fed_in_arrival_order(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let two = BarType::from(format!("{id}-2-MINUTE-BID-INTERNAL").as_str());
    let three = BarType::from(format!("{id}-3-MINUTE-BID-INTERNAL").as_str());
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("10:08:00"),
        &[two, three],
    );
    let spec = KeptSpec {
        start: Some(monday("09:54:00")),
        end: Some(monday("10:00:00")),
        plain: true,
        quotes: true,
        ..KeptSpec::default()
    };
    let request = fixture.request(&[two, three], spec).unwrap();
    fixture.subscribe(two, None);
    fixture.subscribe(three, None);
    for (ts, bid_size) in [("10:01:00", 1), ("10:04:00", 2), ("10:07:00", 4)] {
        fixture.live_quote(monday(ts), bid_size);
    }
    fixture.answer_quotes(request);
    fixture.advance(monday("10:09:00"));

    assert_eq!(
        fixture.delivered(),
        vec![
            Kept::End(0, None),
            published(two, monday("10:02:00"), 1, &[1, 0]),
            published(three, monday("10:03:00"), 1, &[1, 1]),
            published(two, monday("10:04:00"), 2, &[2, 1]),
            published(three, monday("10:06:00"), 2, &[2, 2]),
            published(two, monday("10:08:00"), 4, &[3, 2]),
            published(three, monday("10:09:00"), 4, &[3, 3]),
        ]
    );
}

/// A UTC 5-MINUTE builder kept through 10:08 minus 1 ns, with a2's 2 open: the contiguous
/// request asking it in New York's time zone is refused by name before its replay is claimed,
/// and the next valid continuation still closes a2's bar at 10:10.
#[rstest]
fn test_kept_request_refused_for_its_time_zone_leaves_its_replay_as_it_was(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    first_kept_request(&mut fixture, five);
    let next = kept_window(monday("10:08:00"), monday("10:15:00"));

    let error = fixture
        .request(
            &[five],
            KeptSpec {
                time_zone: Some("America/New_York"),
                ..next
            },
        )
        .unwrap_err();
    let continued = fixture.request(&[five], next).unwrap();
    fixture.answer(continued, &[(monday("10:12:00"), 4, "b1")], None);

    assert!(
        error.starts_with(
            "Cannot keep the bar builders of an `update_subscriptions` request: its time zone"
        ),
        "{error}"
    );
    let mut expected = first_kept_answer(five);
    expected.extend([
        Kept::End(1, Some(error)),
        kept_bar(2, five, monday("10:10:00"), 2, &[2]),
        kept_trade(2, "b1", &[2]),
        Kept::End(2, None),
    ]);
    assert_eq!(fixture.delivered(), expected);
}

/// At 12:00 one requester keeps a 5-MINUTE builder through 10:08 minus 1 ns, with a2's 2 open;
/// another's overlapping kept request is refused, and its terminal carries the refusal, so that
/// requester has acquired nothing to unsubscribe (the actor drops the attempt): the first
/// requester's next 10:10 bar still holds a2's 2.
#[rstest]
fn test_refused_kept_request_ends_with_its_refusal_and_leaves_another_requesters_replay(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    first_kept_request(&mut fixture, five);

    let error = fixture
        .request(&[five], kept_window(monday("10:07:00"), monday("10:15:00")))
        .unwrap_err();
    let continued = fixture
        .request(&[five], kept_window(monday("10:08:00"), monday("10:15:00")))
        .unwrap();
    fixture.answer(continued, &[(monday("10:09:00"), 4, "b1")], None);

    assert!(error.contains("overlaps the coverage through"), "{error}");
    let mut expected = first_kept_answer(five);
    expected.extend([
        Kept::End(1, Some(error)),
        kept_trade(2, "b1", &[1]),
        kept_bar(2, five, monday("10:10:00"), 6, &[2]),
        Kept::End(2, None),
    ]);
    assert_eq!(fixture.delivered(), expected);
}

/// A kept 5-MINUTE request from 10:00 to 10:08 minus 1 ns, subscribed and then unsubscribed while
/// it runs (no other subscriber of its bars): its answer is delivered whole, the subscription's
/// trades are released at the client and nothing is published. The unsubscribe released only
/// the subscription: the request keeps its builder, so the first window again is refused as
/// overlapping its coverage, and the next window continues it, a2's 2 in its 10:10 bar.
#[rstest]
fn test_unsubscribing_a_held_back_kept_bar_type_leaves_its_builder_to_the_request(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[]);
    let request = fixture
        .request(&[five], kept_window(monday("10:00:00"), monday("10:08:00")))
        .unwrap();
    fixture.subscribe(five, None);
    let sent = fixture.client_commands(0).len();
    unsubscribe(&mut fixture, five);
    fixture.answer(
        request,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );
    let released = fixture.client_commands(sent);

    let spec = |start: &str| KeptSpec {
        start: Some(monday(start)),
        end: Some(monday("10:15:00")),
        ..KeptSpec::default()
    };
    let error = fixture.request(&[five], spec("10:00:00")).unwrap_err();
    let continued = fixture.request(&[five], spec("10:08:00")).unwrap();
    fixture.answer(continued, &[(monday("10:12:00"), 4, "c1")], None);

    assert!(
        matches!(
            released.as_slice(),
            [DataCommand::Unsubscribe(UnsubscribeCommand::Trades(_))]
        ),
        "{released:?}"
    );
    assert!(error.contains("overlaps the coverage through"), "{error}");
    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "a1", &[]),
            kept_bar(0, five, monday("10:05:00"), 1, &[]),
            kept_trade(0, "a2", &[]),
            Kept::End(0, None),
            Kept::End(1, Some(error)),
            kept_bar(2, five, monday("10:10:00"), 2, &[]),
            kept_trade(2, "c1", &[]),
            kept_bar(2, five, monday("10:15:00"), 4, &[]),
            Kept::End(2, None),
        ]
    );
    // Only the answers' bars entered the cache: none was published
    assert_eq!(fixture.cache.borrow().bar_count(&five), 3);
}

/// The review's example: a kept request over 16:55 to 18:05 for 5-MINUTE and 10-RANGE bars,
/// answered at 18:10 by a part holding t1 (3) at 16:59:58 and then a payload-free terminal with
/// no failure reason. That terminal is a success: the end of coverage fires the 17:00 close and
/// the 18:00 boundary, and the builders are kept, so a request over the same window is refused
/// and the next window continues them.
#[rstest]
#[case::interleaved(false)]
#[case::plain(true)]
fn test_kept_request_ended_by_a_terminal_without_a_reason_fires_its_end_and_keeps_its_builders(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] plain: bool,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (five, range) = (bar_type_of(id, "5-MINUTE"), bar_type_of(id, "10-RANGE"));
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("18:10:00"),
        &[five, range],
    );
    let window = KeptSpec {
        plain,
        ..kept_window(monday("16:55:00"), monday("18:05:00"))
    };
    let request = fixture.request(&[five, range], window).unwrap();
    fixture.answer_part(request, &[(monday("16:59:58"), 3, "t1")]);
    fixture.end(request);

    let again = fixture.request(&[five, range], window).unwrap_err();
    let next = KeptSpec {
        plain,
        ..kept_window(monday("18:05:00"), monday("18:10:00"))
    };
    assert!(fixture.request(&[five, range], next).is_ok());

    assert!(again.contains("overlaps the coverage through"), "{again}");
    let mut expected = vec![kept_trade(0, "t1", &[0, 0])];
    if !plain {
        expected.extend([
            kept_bar(0, five, monday("17:00:00"), 3, &[1, 0]),
            kept_bar(0, range, monday("18:00:00"), 3, &[1, 1]),
        ]);
    }
    expected.extend([Kept::End(0, None), Kept::End(1, Some(again))]);
    assert_eq!(fixture.delivered(), expected);
    assert_eq!(cached_bars(&fixture.cache, &[five, range]), vec![1, 1]);
}

/// A kept request naming `point_data` is refused by name before dispatch, as one naming a filter
/// expression is, and changes nothing: the next request continues the first one's open bar.
#[rstest]
fn test_kept_request_naming_point_data_is_refused_by_name_and_leaves_its_replay_as_it_was(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    first_kept_request(&mut fixture, five);
    let next = kept_window(monday("10:08:00"), monday("10:15:00"));
    let sent = fixture.client_commands(0).len();

    let error = fixture
        .request(
            &[five],
            KeptSpec {
                point_data: true,
                plain: true,
                ..next
            },
        )
        .unwrap_err();
    assert!(fixture.client_commands(sent).is_empty());
    let continued = fixture.request(&[five], next).unwrap();
    fixture.answer(continued, &[(monday("10:12:00"), 4, "b1")], None);

    assert!(
        error.starts_with("Cannot keep the bar builders of an `update_subscriptions` request"),
        "{error}"
    );
    assert!(error.contains("it names `point_data`"), "{error}");
    let mut expected = first_kept_answer(five);
    expected.extend([
        Kept::End(1, Some(error)),
        kept_bar(2, five, monday("10:10:00"), 2, &[2]),
        kept_trade(2, "b1", &[2]),
        Kept::End(2, None),
    ]);
    assert_eq!(fixture.delivered(), expected);
}

/// The review's example: a request over 10:00 to 10:10 building 5-MINUTE bars through a
/// time-range pipeline of 5-minute `point_data` children asks only 10:00, 10:05 and 10:10, so its
/// window is not wholly known: the 10:00 and 10:05 closes fire before the 10:05 and 10:10
/// records, and its end fires nothing after the last (no 10:10 bar over the unasked 10:05 to
/// 10:10).
#[rstest]
fn test_point_data_request_closes_nothing_after_its_last_record(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let at = |time: &str| utc_ns(&format!("2024-01-08T{time}Z"));
    let mut fixture = PlainFixture::new(
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
        client_id,
        venue,
        without_flat_bars(),
        at("12:00:00"),
        &[five],
    );
    fixture.execute(plain_trades_request(
        id,
        client_id,
        UUID4::new(),
        (Some(at("10:00:00")), Some(at("10:10:00"))),
        None,
        plain_params(
            &[five],
            json!({ "time_range_generator": "", "durations_seconds": [300], "point_data": true }),
        ),
    ));

    for (child, (time, size)) in [("10:00:00", 1), ("10:05:00", 2), ("10:10:00", 4)]
        .into_iter()
        .enumerate()
    {
        let request = &fixture.trades_requests()[child];
        assert_eq!(
            (request.start, request.end),
            (timestamp_of(Some(at(time))), timestamp_of(Some(at(time))))
        );
        let answer = trades_answer(
            request.request_id,
            client_id,
            id,
            &[(at(time), "1.00000", size)],
        );
        fixture.answer(answer, true, None);
    }

    assert_eq!(fixture.trades_requests().len(), 3);
    assert_eq!(
        fixture.cached(),
        vec![vec![(at("10:00:00"), 1), (at("10:05:00"), 2)]]
    );
    assert_eq!(fixture.ends(), vec![None]);
}

/// A kept 3-TICK request over 10:00 to 10:05, a subscription held back on it while it ran
/// holding live trades of 1, 2 and 4 at 10:06, 10:07 and 10:08, fails at 10:10: its answer and
/// its terminal go out first; then the subscription's new builder, fed the held trades,
/// publishes their 3-TICK bar (7).
#[rstest]
#[case::interleaved(false)]
#[case::plain(true)]
fn test_failed_kept_request_sends_its_end_before_its_held_back_subscription_publishes(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] plain: bool,
) {
    let _ = stub_msgbus;
    let tick = bar_type_of(audusd_sim.id, "3-TICK");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("10:10:00"), &[tick]);
    let spec = KeptSpec {
        start: Some(monday("10:00:00")),
        end: Some(monday("10:05:00")),
        plain,
        ..KeptSpec::default()
    };
    let request = fixture.request(&[tick], spec).unwrap();
    fixture.subscribe(tick, None);
    for (ts, size, trade_id) in [
        ("10:06:00", 1, "h1"),
        ("10:07:00", 2, "h2"),
        ("10:08:00", 4, "h3"),
    ] {
        fixture.live(monday(ts), size, trade_id);
    }
    fixture.answer(
        request,
        &[(monday("10:01:00"), 1, "a1")],
        Some("the client failed"),
    );

    assert_eq!(
        fixture.delivered(),
        vec![
            live_trade("h1", &[0]),
            live_trade("h2", &[0]),
            live_trade("h3", &[0]),
            kept_trade(0, "a1", &[0]),
            Kept::End(0, Some("the client failed".to_owned())),
            published(tick, monday("10:08:00"), 7, &[1]),
        ]
    );
}

/// A plain kept request naming 5-MINUTE bars and 15-MINUTE bars built from them, over 10:00 to
/// 10:08, is refused by name whole: no client is asked and no builder is made, so a kept 5-MINUTE
/// request over the same window then starts a replay of its own, as on a fresh engine.
#[rstest]
fn test_kept_request_naming_a_direct_and_a_composite_bar_type_is_refused_whole_and_changes_nothing(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let fifteen = BarType::from(format!("{id}-15-MINUTE-LAST-INTERNAL@5-MINUTE-INTERNAL").as_str());
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let window = kept_window(monday("10:00:00"), monday("10:08:00"));

    let plain = KeptSpec {
        plain: true,
        ..window
    };
    let error = fixture.request(&[five, fifteen], plain).unwrap_err();
    let refused_commands = fixture.client_commands(0).len();
    let request = fixture.request(&[five], window).unwrap();
    fixture.answer(
        request,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );

    assert!(
        error.starts_with("Cannot keep the bar builders of an `update_subscriptions` request"),
        "{error}"
    );
    assert!(
        error.contains(&format!("{fifteen} is built from another bar")),
        "{error}"
    );
    assert_eq!(refused_commands, 0);
    assert_eq!(fixture.client_commands(0).len(), 1);
    assert_eq!(
        fixture.delivered(),
        vec![
            Kept::End(0, Some(error)),
            kept_trade(1, "a1", &[0]),
            kept_bar(1, five, monday("10:05:00"), 1, &[1]),
            kept_trade(1, "a2", &[1]),
            Kept::End(1, None),
        ]
    );
}

/// A clock that is not a `VirtualClock`, as a live node's is not: it reads, and keeps its
/// timers on, a virtual clock inside it.
#[derive(Debug)]
struct WallClock(VirtualClock);

impl Clock for WallClock {
    fn timestamp_ns(&self) -> UnixNanos {
        self.0.timestamp_ns()
    }

    fn timestamp_us(&self) -> u64 {
        self.0.timestamp_us()
    }

    fn timestamp_ms(&self) -> u64 {
        self.0.timestamp_ms()
    }

    fn timestamp(&self) -> f64 {
        self.0.timestamp()
    }

    fn timer_names(&self) -> Vec<&str> {
        self.0.timer_names()
    }

    fn timer_count(&self) -> usize {
        self.0.timer_count()
    }

    fn timer_exists(&self, name: &Ustr) -> bool {
        self.0.timer_exists(name)
    }

    fn register_default_handler(&mut self, callback: nautilus_common::timer::TimeEventCallback) {
        self.0.register_default_handler(callback);
    }

    fn cancel_default_handler(&mut self) {
        self.0.cancel_default_handler();
    }

    fn cancel_callbacks(&mut self) {
        self.0.cancel_callbacks();
    }

    fn set_time_alert_ns(
        &mut self,
        name: &str,
        alert_time_ns: UnixNanos,
        callback: Option<nautilus_common::timer::TimeEventCallback>,
        allow_past: Option<bool>,
    ) -> anyhow::Result<()> {
        self.0
            .set_time_alert_ns(name, alert_time_ns, callback, allow_past)
    }

    fn set_timer_ns(
        &mut self,
        name: &str,
        interval_ns: DurationNanos,
        start_time_ns: Option<UnixNanos>,
        stop_time_ns: Option<UnixNanos>,
        callback: Option<nautilus_common::timer::TimeEventCallback>,
        allow_past: Option<bool>,
        fire_immediately: Option<bool>,
    ) -> anyhow::Result<()> {
        self.0.set_timer_ns(
            name,
            interval_ns,
            start_time_ns,
            stop_time_ns,
            callback,
            allow_past,
            fire_immediately,
        )
    }

    fn next_time_ns(&self, name: &str) -> Option<UnixNanos> {
        self.0.next_time_ns(name)
    }

    fn cancel_timer(&mut self, name: &str) {
        self.0.cancel_timer(name);
    }

    fn cancel_timers(&mut self) {
        self.0.cancel_timers();
    }

    fn reset(&mut self) {
        self.0.reset();
    }
}

#[derive(Clone, Copy, Debug)]
enum KeptRoute {
    Plain,
    Joined,
    Merged,
    ExternalClient,
    JoinWithoutADeadline,
    NotKept,
}

/// On an engine whose clock is not a `VirtualClock` (Tuesday 17:30 New York), a request keeping
/// its builders is refused by name on every route before any of it runs: no client is asked and
/// the requester gets the reason in one terminal. A joined request's own parameter refusal comes
/// first; a request that does not keep its builders is dispatched as before.
#[rstest]
#[case::plain(KeptRoute::Plain, Some("the engine's clock is not a `VirtualClock`"))]
#[case::joined(KeptRoute::Joined, Some("the engine's clock is not a `VirtualClock`"))]
#[case::merged(KeptRoute::Merged, Some("the engine's clock is not a `VirtualClock`"))]
#[case::external_client(
    KeptRoute::ExternalClient,
    Some("the engine's clock is not a `VirtualClock`")
)]
#[case::join_without_a_deadline(
    KeptRoute::JoinWithoutADeadline,
    Some("it must give a `deadline` in UNIX nanoseconds")
)]
#[case::not_kept(KeptRoute::NotKept, None)]
fn test_kept_request_on_a_clock_that_is_not_virtual_is_refused_by_name_before_any_route(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] route: KeptRoute,
    #[case] reason: Option<&str>,
) {
    let _ = stub_msgbus;
    let now = NY_2026_11_10_1730;
    let session_opens = [NY_2026_11_08_1800, NY_2026_11_09_1800];
    let mut wall_clock = WallClock(VirtualClock::new());
    let _ = wall_clock.0.advance_time(UnixNanos::from(now), true);
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let instrument_id = audusd_sim.id;
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();
    let mut engine = DataEngine::new(
        Rc::new(RefCell::new(wall_clock)) as Rc<RefCell<dyn Clock>>,
        Rc::clone(&cache),
        None,
    );
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut engine,
    );

    let bar_type = bar_type_of(instrument_id, "1-TICK");
    let kept = json!({
        "bar_types": [bar_type.to_string()],
        "update_subscriptions": true,
    });
    let params = match route {
        KeptRoute::Plain | KeptRoute::ExternalClient => kept,
        KeptRoute::Joined => joined_params(bar_type, now, &session_opens),
        KeptRoute::Merged => json!({
            "bar_types": [bar_type.to_string()],
            "update_subscriptions": true,
            "quotes": true,
        }),
        KeptRoute::JoinWithoutADeadline => {
            let mut params = joined_params(bar_type, now, &session_opens);
            params.as_object_mut().unwrap().remove("deadline");
            params
        }
        KeptRoute::NotKept => json!({ "bar_types": [bar_type.to_string()] }),
    };
    if matches!(route, KeptRoute::ExternalClient) {
        cache.borrow_mut().add_external_data_client(client_id);
    }
    let request = starting_at(
        trades_request(vec![instrument_id], None, client_id, params),
        session_opens[0],
    );

    let (result, failures) = execute_recording_failures(&mut engine, request);

    match reason {
        Some(reason) => {
            let error = result.unwrap_err().to_string();
            assert!(error.contains(reason), "{error}");
            assert_eq!(failures, vec![Some(error)]);
            assert!(recorder.borrow().is_empty(), "{:?}", recorder.borrow());
        }
        None => {
            result.unwrap();
            assert!(failures.is_empty(), "{failures:?}");
            assert_eq!(recorder.borrow().len(), 1);
        }
    }
}

/// A size-1 trade at 10:00:30 kept by a plain request for 3-MINUTE and 2-MINUTE bars through
/// 10:01, both idle at 10:07, when ONE subscribe command names them in that order: they are
/// adopted together and catch up in deadline order, the 10:02 bar before the 10:03 bar.
#[rstest]
fn test_one_subscribe_command_adopts_its_idle_kept_builders_together_in_deadline_order(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (two, three) = (bar_type_of(id, "2-MINUTE"), bar_type_of(id, "3-MINUTE"));
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("10:07:00"),
        &[two, three],
    );
    let request = fixture
        .request(
            &[three, two],
            KeptSpec {
                plain: true,
                ..kept_window(monday("10:00:00"), monday("10:01:00"))
            },
        )
        .unwrap();
    fixture.answer(request, &[(monday("10:00:30"), 1, "a1")], None);
    fixture.subscribe_all(&[three, two]);

    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "a1", &[0, 0]),
            Kept::End(0, None),
            published(two, monday("10:02:00"), 1, &[1, 0]),
            published(three, monday("10:03:00"), 1, &[1, 1]),
        ]
    );
}

/// An actor driven against the engine: it requests 5-MINUTE bars kept by request over 10:00 to
/// 10:08, answered with no failure reason, so it holds that builder. Disposed, it releases it:
/// the engine then admits a new kept request over the same window, which a kept builder would
/// refuse as overlapping its coverage.
#[rstest]
fn test_disposed_actor_releases_the_bar_types_it_kept_by_request(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    let _ = clock
        .borrow_mut()
        .advance_time(UnixNanos::from(monday("12:00:00")), true);
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(audusd_sim))
        .unwrap();
    let engine = Rc::new(RefCell::new(DataEngine::new(
        Rc::clone(&clock) as Rc<RefCell<dyn Clock>>,
        Rc::clone(&cache),
        Some(without_flat_bars()),
    )));
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        Rc::clone(&cache),
        client_id,
        venue,
        None,
        &recorder,
        &mut engine.borrow_mut(),
    );
    let target = Rc::clone(&engine);
    msgbus::register_data_command_endpoint(
        MessagingSwitchboard::data_engine_queue_execute(),
        TypedIntoHandler::from(move |command: DataCommand| target.borrow_mut().execute(command)),
    );
    let mut actor = KeptRequester {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some(ActorId::from("KEPT-REQUESTER")),
            ..Default::default()
        }),
    };
    actor
        .register(
            TraderId::test_default(),
            Rc::clone(&clock) as Rc<RefCell<dyn Clock>>,
            Rc::clone(&cache),
        )
        .unwrap();
    let params = || -> Params {
        params_from_json(json!({
            "bar_types": [five.to_string()],
            "update_subscriptions": true,
        }))
    };
    let at = |ns: u64| Some(UnixNanos::from(ns).to_datetime_utc());
    let (start, end) = (monday("10:00:00"), monday("10:08:00") - 1);

    let request_id = actor
        .request_trades(
            id,
            at(start),
            at(end),
            None,
            Some(client_id),
            Some(params()),
        )
        .unwrap();
    let answer = TradesResponse::new(
        request_id,
        client_id,
        id,
        vec![make_trade(id, "1.00000", 1, "a1", monday("10:01:00"))],
        None,
        None,
        monday("12:00:00").into(),
        None,
    );
    engine
        .borrow_mut()
        .response_envelope(DataResponseEnvelope::new(
            DataResponse::Trades(answer),
            true,
            None,
        ));
    actor.dispose().unwrap();

    let again = engine
        .borrow_mut()
        .execute_request(RequestCommand::Trades(RequestTrades::new(
            id,
            at(start),
            at(end),
            None,
            Some(client_id),
            UUID4::new(),
            monday("12:00:00").into(),
            Some(params()),
        )));
    assert!(again.is_ok(), "{again:?}");
}

/// An actor that only requests (see
/// `test_disposed_actor_releases_the_bar_types_it_kept_by_request`).
#[derive(Debug)]
struct KeptRequester {
    core: DataActorCore,
}

nautilus_actor!(KeptRequester);
impl DataActor for KeptRequester {}

/// How a real actor releases what it kept by request (see `KeptFixture::actor`).
#[derive(Clone, Copy, Debug)]
enum ActorRelease {
    Unsubscribe,
    Stop,
    Reset,
    Dispose,
}

impl ActorRelease {
    fn apply(self, actor: &mut KeptRequester, bar_type: BarType, client_id: ClientId) {
        match self {
            Self::Unsubscribe => actor.unsubscribe_bars(bar_type, Some(client_id), None),
            Self::Stop => {
                actor.start().unwrap();
                actor.stop().unwrap();
            }
            Self::Reset => actor.reset().unwrap(),
            Self::Dispose => actor.dispose().unwrap(),
        }
    }
}

/// The first read's sequence, by a real actor A: its R1 keeps 5-MINUTE bars over 10:00 to
/// 10:07:59.999999999, a2 (2) at 10:06 in the open bar; its R2 from 10:09 is refused for the gap,
/// so R1 still claims the replay (a request from 10:00 is refused as overlapping it). A releases
/// every kept request it sent, R1 with R2, at its unsubscribe, stop, reset or disposal: the
/// replay is gone, and another requester's fresh request from 10:00 is admitted.
#[rstest]
#[case::unsubscribe(ActorRelease::Unsubscribe)]
#[case::stop(ActorRelease::Stop)]
#[case::reset(ActorRelease::Reset)]
#[case::dispose(ActorRelease::Dispose)]
fn test_actor_releases_every_kept_request_it_sent_so_a_refused_one_hides_no_claim(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] release: ActorRelease,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let mut actor = fixture.actor("KEPT-A");
    let first = fixture.actor_request(
        &mut actor,
        &[five],
        kept_window(monday("10:00:00"), monday("10:08:00")),
    );
    fixture.answer(
        first,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );
    fixture.actor_request(
        &mut actor,
        &[five],
        kept_window(monday("10:09:00"), monday("10:15:00")),
    );
    let overlapping = fixture
        .request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")))
        .unwrap_err();

    release.apply(&mut actor, five, client_id);
    fixture.pump();
    let fresh = fixture.request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")));

    assert!(
        overlapping.contains("overlaps the coverage through"),
        "{overlapping}"
    );
    assert!(fresh.is_ok(), "{fresh:?}");
}

/// The second read's sequence: actor A's R1 keeps 5-MINUTE bars over 10:00 to 10:07:59.999999999;
/// A's continuation R2 is admitted and FAILS, so the engine removes the replay. Another requester
/// starts a fresh replay from 10:00 with b1 (4) at 10:11. A's later unsubscribe or disposal
/// releases R1 and R2, which claim nothing any more: the fresh replay is untouched, and its
/// continuation's 10:15 bar holds b1's 4.
#[rstest]
#[case::unsubscribe(ActorRelease::Unsubscribe)]
#[case::dispose(ActorRelease::Dispose)]
fn test_actor_release_after_its_failed_continuation_leaves_another_requesters_replay(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] release: ActorRelease,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let mut actor = fixture.actor("KEPT-A");
    let first = fixture.actor_request(
        &mut actor,
        &[five],
        kept_window(monday("10:00:00"), monday("10:08:00")),
    );
    fixture.answer(
        first,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );
    let second = fixture.actor_request(
        &mut actor,
        &[five],
        kept_window(monday("10:08:00"), monday("10:10:00")),
    );
    fixture.answer(
        second,
        &[(monday("10:09:00"), 1, "a3")],
        Some("the venue went away"),
    );
    let fresh = fixture
        .request(&[five], kept_window(monday("10:00:00"), monday("10:13:00")))
        .unwrap();
    fixture.answer(fresh, &[(monday("10:11:00"), 4, "b1")], None);

    release.apply(&mut actor, five, client_id);
    fixture.pump();
    let continued = fixture
        .request(&[five], kept_window(monday("10:13:00"), monday("10:16:00")))
        .unwrap();
    fixture.answer(continued, &[], None);

    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(fresh, "b1", &[1]),
            Kept::End(fresh, None),
            kept_bar(continued, five, monday("10:15:00"), 4, &[2]),
            Kept::End(continued, None),
        ]
    );
}

/// An actor disposed while its FIRST kept request is in flight releases it: the request then
/// succeeds, its replay is removed at its end, and a fresh request from 10:00 is admitted.
#[rstest]
fn test_actor_disposed_while_its_first_kept_request_runs_leaves_no_replay(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let mut actor = fixture.actor("KEPT-A");
    let first = fixture.actor_request(
        &mut actor,
        &[five],
        kept_window(monday("10:00:00"), monday("10:08:00")),
    );

    actor.dispose().unwrap();
    fixture.pump();
    fixture.answer(
        first,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );
    let fresh = fixture.request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")));

    assert!(fresh.is_ok(), "{fresh:?}");
}

/// The same with the actor's first kept request refused (from 10:09, a gap after another
/// requester's replay through 10:07:59.999999999): its release at disposal claims nothing, so
/// the other requester's continuation still closes a2's 2 at 10:10.
#[rstest]
fn test_actor_disposed_after_its_first_kept_request_was_refused_touches_no_other_replay(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    first_kept_request(&mut fixture, five);
    let mut actor = fixture.actor("KEPT-A");
    fixture.actor_request(
        &mut actor,
        &[five],
        kept_window(monday("10:09:00"), monday("10:15:00")),
    );

    actor.dispose().unwrap();
    fixture.pump();
    let continued = fixture
        .request(&[five], kept_window(monday("10:08:00"), monday("10:15:00")))
        .unwrap();
    fixture.answer(continued, &[(monday("10:12:00"), 4, "c1")], None);

    let mut expected = first_kept_answer(five);
    expected.extend([
        kept_bar(continued, five, monday("10:10:00"), 2, &[2]),
        kept_trade(continued, "c1", &[2]),
        Kept::End(continued, None),
    ]);
    assert_eq!(fixture.delivered(), expected);
}

/// The third read's sequence: A's 5-MINUTE builder kept idle through 10:07:59.999999999, a2 (2)
/// at 10:06 in its open bar. B subscribes 15-MINUTE@5-MINUTE and unsubscribes it, so the engine
/// unsubscribes the composite's 5-MINUTE source; or B unsubscribes 5-MINUTE itself. Neither
/// names a request: A's builder is left, and A's next continuation closes a2's 2 at 10:10.
#[rstest]
#[case::composite_source(true)]
#[case::direct(false)]
fn test_unsubscribe_naming_no_request_leaves_an_idle_kept_builder(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] composite: bool,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let fifteen = BarType::from(format!("{id}-15-MINUTE-LAST-INTERNAL@5-MINUTE-INTERNAL").as_str());
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    first_kept_request(&mut fixture, five);

    if composite {
        fixture.subscribe(fifteen, None);
        unsubscribe(&mut fixture, fifteen);
    } else {
        unsubscribe(&mut fixture, five);
    }
    let continued = fixture
        .request(&[five], kept_window(monday("10:08:00"), monday("10:15:00")))
        .unwrap();
    fixture.answer(continued, &[(monday("10:12:00"), 4, "c1")], None);

    let mut expected = first_kept_answer(five);
    expected.extend([
        kept_bar(continued, five, monday("10:10:00"), 2, &[2]),
        kept_trade(continued, "c1", &[2]),
        Kept::End(continued, None),
    ]);
    assert_eq!(fixture.delivered(), expected);
}

/// A release removes only the released bar type's builder: R0 keeps 3-MINUTE and 5-MINUTE over
/// 10:00 to 10:07:59.999999999, a2 (2) at 10:06; its release of 3-MINUTE leaves 5-MINUTE kept,
/// so a request continuing 5-MINUTE alone closes a2's 2 at 10:10. That request's release of
/// 5-MINUTE, the last builder, drops the replay: a request for both from 10:00 is admitted.
#[rstest]
fn test_release_removes_only_its_bar_types_builder_and_the_replay_goes_with_its_last(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (three, five) = (bar_type_of(id, "3-MINUTE"), bar_type_of(id, "5-MINUTE"));
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let first = fixture
        .request(
            &[three, five],
            kept_window(monday("10:00:00"), monday("10:08:00")),
        )
        .unwrap();
    fixture.answer(
        first,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );
    let answered = fixture.delivered().len();

    fixture.release(three, first);
    let continued = fixture
        .request(&[five], kept_window(monday("10:08:00"), monday("10:15:00")))
        .unwrap();
    fixture.answer(continued, &[(monday("10:12:00"), 4, "c1")], None);
    fixture.release(five, continued);
    let fresh = fixture.request(
        &[three, five],
        kept_window(monday("10:00:00"), monday("10:15:00")),
    );

    assert_eq!(
        fixture.delivered()[answered..],
        [
            kept_bar(continued, five, monday("10:10:00"), 2, &[2]),
            kept_trade(continued, "c1", &[2]),
            Kept::End(continued, None),
        ]
    );
    assert!(fresh.is_ok(), "{fresh:?}");
}

/// A release naming a stale request is ignored: R0 keeps 5-MINUTE through 10:07:59.999999999, a2
/// (2) at 10:06 open, and R1 continues it, claiming it. R0's release while R1 runs, and again
/// once R1 has ended, touches nothing: R1 closes a2's 2 at 10:10, and the next continuation
/// closes c1's 4 with c2's 8 at 10:15.
#[rstest]
fn test_release_naming_a_stale_request_is_ignored(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let first = first_kept_request(&mut fixture, five);
    let continued = fixture
        .request(&[five], kept_window(monday("10:08:00"), monday("10:13:00")))
        .unwrap();

    fixture.release(five, first);
    fixture.answer(continued, &[(monday("10:12:00"), 4, "c1")], None);
    fixture.release(five, first);
    let next = fixture
        .request(&[five], kept_window(monday("10:13:00"), monday("10:20:00")))
        .unwrap();
    fixture.answer(next, &[(monday("10:14:00"), 8, "c2")], None);

    let mut expected = first_kept_answer(five);
    expected.extend([
        kept_bar(continued, five, monday("10:10:00"), 2, &[2]),
        kept_trade(continued, "c1", &[2]),
        Kept::End(continued, None),
        kept_trade(next, "c2", &[2]),
        kept_bar(next, five, monday("10:15:00"), 12, &[3]),
        Kept::End(next, None),
    ]);
    assert_eq!(fixture.delivered(), expected);
}

/// After adoption a release by request does nothing to the subscribed builder: R0 keeps 5-MINUTE
/// through 10:07:59.999999999, a2 (2) at 10:06 open; a subscription at 12:00 adopts it, its
/// catch-up publishing a2's 10:10 bar. R0's release then touches nothing: a live trade at 12:01
/// is published in the 12:05 bar.
#[rstest]
fn test_release_after_adoption_leaves_the_subscribed_builder(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let first = first_kept_request(&mut fixture, five);

    fixture.subscribe(five, None);
    fixture.release(five, first);
    fixture.advance(monday("12:01:00"));
    fixture.live(monday("12:01:00"), 3, "l1");
    fixture.advance(monday("12:05:00"));

    let mut expected = first_kept_answer(five);
    expected.extend([
        published(five, monday("10:10:00"), 2, &[2]),
        live_trade("l1", &[2]),
        published(five, monday("12:05:00"), 3, &[3]),
    ]);
    assert_eq!(fixture.delivered(), expected);
}

/// A kept request names only bar types its own records feed: a BID or ASK bar on a trades
/// request, a LAST bar on a quotes request and a bar of another instrument are refused by name
/// before dispatch; a BID bar on a quotes request is dispatched.
#[rstest]
#[case::bid_on_trades("AUD/USD.SIM-5-MINUTE-BID-INTERNAL", false, Some("built from quotes"))]
#[case::ask_on_trades("AUD/USD.SIM-5-MINUTE-ASK-INTERNAL", false, Some("built from quotes"))]
#[case::last_on_quotes("AUD/USD.SIM-5-MINUTE-LAST-INTERNAL", true, Some("built from trades"))]
#[case::other_instrument(
    "GBP/USD.SIM-5-MINUTE-LAST-INTERNAL",
    false,
    Some("another instrument")
)]
#[case::bid_on_quotes("AUD/USD.SIM-5-MINUTE-BID-INTERNAL", true, None)]
fn test_kept_request_naming_a_bar_type_its_records_do_not_feed_is_refused_by_name(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] bar_type: &str,
    #[case] quotes: bool,
    #[case] refusal: Option<&str>,
) {
    let _ = stub_msgbus;
    let bar_type = BarType::from(bar_type);
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[]);
    let spec = KeptSpec {
        plain: true,
        quotes,
        ..kept_window(monday("10:00:00"), monday("10:08:00"))
    };

    let result = fixture.request(&[bar_type], spec);

    match refusal {
        Some(refusal) => {
            let error = result.unwrap_err();
            assert!(
                error.contains("its own records do not feed") && error.contains(refusal),
                "{error}"
            );
            assert!(fixture.client_commands(0).is_empty());
        }
        None => {
            assert!(result.is_ok(), "{result:?}");
            assert!(matches!(
                fixture.client_commands(0).as_slice(),
                [DataCommand::Request(RequestCommand::Quotes(_))]
            ));
        }
    }
}

/// A bars request feeds only bars built from other bars, which a kept request may not name: a
/// kept bars request is refused by name before dispatch.
#[rstest]
fn test_kept_bars_request_is_refused_by_name(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let one = BarType::from(format!("{id}-1-MINUTE-LAST-EXTERNAL").as_str());
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[]);
    let at = |ns: u64| Some(UnixNanos::from(ns).to_datetime_utc());

    let result = fixture
        .engine
        .execute_request(RequestCommand::Bars(RequestBars::new(
            one,
            at(monday("10:00:00")),
            at(monday("10:08:00") - 1),
            None,
            Some(client_id),
            UUID4::new(),
            monday("12:00:00").into(),
            Some(params_from_json(json!({
                "bar_types": [five.to_string()],
                "update_subscriptions": true,
            }))),
        )));

    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("its own records do not feed") && error.contains("a bars request"),
        "{error}"
    );
    assert!(fixture.client_commands(0).is_empty());
}

/// Executes at 12:00:00 a trades request (or, with `quotes`, a quotes request) for AUD/USD.SIM
/// and GBP/USD.SIM, added to the cache, over 10:00 to 10:07:59.999999999, asking 5-MINUTE bars
/// of both (`BID` bars on a quotes request), kept by request or not, that does not interleave.
fn execute_batch_request(
    fixture: &mut KeptFixture,
    quotes: bool,
    kept: bool,
) -> anyhow::Result<()> {
    let gbpusd = gbpusd_sim();
    let (aud, gbp) = (fixture.instrument_id, gbpusd.id);
    fixture
        .cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(gbpusd))
        .unwrap();
    let price_type = if quotes { "BID" } else { "LAST" };
    let bar_types: Vec<String> = [aud, gbp]
        .iter()
        .map(|id| format!("{id}-5-MINUTE-{price_type}-INTERNAL"))
        .collect();
    let params = Some(params_from_json(json!({
        "bar_types": bar_types,
        "update_subscriptions": kept,
    })));
    let at = |ns: u64| Some(UnixNanos::from(ns).to_datetime_utc());
    let (start, end) = (at(monday("10:00:00")), at(monday("10:08:00") - 1));
    let (client_id, now) = (Some(fixture.client_id), UnixNanos::from(monday("12:00:00")));
    let request = if quotes {
        RequestCommand::Quotes(RequestQuotes::new(
            vec![aud, gbp],
            start,
            end,
            None,
            client_id,
            UUID4::new(),
            now,
            params,
        ))
    } else {
        RequestCommand::Trades(RequestTrades::new(
            vec![aud, gbp],
            start,
            end,
            None,
            client_id,
            UUID4::new(),
            now,
            params,
        ))
    };
    fixture.engine.execute_request(request)
}

/// The review's example: at 12:00:00 a plain kept trades request for AUD/USD.SIM and
/// GBP/USD.SIM over 10:00 to 10:08 asking the 5-MINUTE-LAST bars of both, whose builders would
/// share one replay and one hand-over, is refused by name before dispatch; so is a kept quotes
/// batch asking their BID bars. Nothing of it is made: a kept AUD/USD.SIM request over the same
/// window is then admitted and dispatched.
#[rstest]
#[case::trades(false)]
#[case::quotes(true)]
fn test_kept_batch_request_is_refused_by_name_and_makes_nothing(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] quotes: bool,
) {
    let _ = stub_msgbus;
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[]);

    let error = execute_batch_request(&mut fixture, quotes, true)
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("it must name exactly one instrument (batches are refused")
            && error.contains("received 2"),
        "{error}"
    );
    assert!(fixture.client_commands(0).is_empty());

    let spec = KeptSpec {
        plain: true,
        quotes,
        ..kept_window(monday("10:00:00"), monday("10:08:00"))
    };
    let price_type = if quotes { "BID" } else { "LAST" };
    let five =
        BarType::from(format!("{}-5-MINUTE-{price_type}-INTERNAL", fixture.instrument_id).as_str());
    let result = fixture.request(&[five], spec);

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(fixture.client_commands(0).len(), 1);
}

/// A batch request with bar types that does not keep its builders is untouched by the kept
/// refusal: it is dispatched to its client as one request naming both instruments.
#[rstest]
#[case::trades(false)]
#[case::quotes(true)]
fn test_batch_request_not_kept_is_dispatched_naming_both_instruments(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] quotes: bool,
) {
    let _ = stub_msgbus;
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[]);
    let both = vec![fixture.instrument_id, gbpusd_sim().id];

    let result = execute_batch_request(&mut fixture, quotes, false);

    assert!(result.is_ok(), "{result:?}");
    match fixture.client_commands(0).as_slice() {
        [DataCommand::Request(RequestCommand::Trades(request))] if !quotes => {
            assert_eq!(request.instrument_ids, both);
        }
        [DataCommand::Request(RequestCommand::Quotes(request))] if quotes => {
            assert_eq!(request.instrument_ids, both);
        }
        commands => panic!("{commands:?}"),
    }
}

/// The third read's sequence, by a real actor A: at 12:00:00 its R1 asks 5-MINUTE bars kept by
/// request over 10:00 to 10:07:59.999999999; at 12:00:01 A subscribes 5-MINUTE, held back while
/// R1 runs; at 12:00:02 A, its only subscriber, unsubscribes it or is disposed. A still holds R1,
/// so it releases it; R1 succeeds at 12:00:03 and its builder is removed at its end: at 12:00:04
/// a fresh request from 10:00 is admitted.
#[rstest]
#[case::unsubscribe(ActorRelease::Unsubscribe)]
#[case::dispose(ActorRelease::Dispose)]
fn test_actor_subscribing_forgets_no_kept_request_so_its_release_before_the_hand_over_removes_the_builder(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] release: ActorRelease,
) {
    let _ = stub_msgbus;
    let five = bar_type_of(audusd_sim.id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[]);
    let mut actor = fixture.actor("KEPT-A");
    let first = fixture.actor_request(
        &mut actor,
        &[five],
        kept_window(monday("10:00:00"), monday("10:08:00")),
    );

    fixture.advance(monday("12:00:01"));
    actor.subscribe_bars(five, Some(client_id), None);
    fixture.pump();
    fixture.advance(monday("12:00:02"));
    release.apply(&mut actor, five, client_id);
    fixture.pump();
    fixture.advance(monday("12:00:03"));
    fixture.answer(
        first,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );
    fixture.advance(monday("12:00:04"));
    let fresh = fixture.request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")));

    assert!(fresh.is_ok(), "{fresh:?}");
}

/// The third read's mixed command: 2-MINUTE and 3-MINUTE kept through 10:00:59.999999999, one
/// size-1 trade at 10:00:30. At 10:07 one command subscribes [2-MINUTE, 6-MINUTE@2-MINUTE,
/// 3-MINUTE]: the kept builders are adopted and caught up (10:02 and 10:03) before the composite
/// is prepared, so its interval from 10:06 holds no catch-up bar and it publishes nothing at
/// 10:12.
#[rstest]
fn test_composite_in_a_command_adopting_its_kept_source_takes_none_of_the_catch_up_bars(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let (two, three) = (bar_type_of(id, "2-MINUTE"), bar_type_of(id, "3-MINUTE"));
    let six = BarType::from(format!("{id}-6-MINUTE-LAST-INTERNAL@2-MINUTE-INTERNAL").as_str());
    let mut fixture = KeptFixture::new(
        audusd_sim,
        client_id,
        venue,
        monday("10:07:00"),
        &[two, six, three],
    );
    let request = fixture
        .request(
            &[two, three],
            KeptSpec {
                plain: true,
                ..kept_window(monday("10:00:00"), monday("10:01:00"))
            },
        )
        .unwrap();
    fixture.answer(request, &[(monday("10:00:30"), 1, "a1")], None);

    fixture.subscribe_all(&[two, six, three]);
    fixture.advance(monday("10:13:00"));

    assert_eq!(
        fixture.delivered(),
        vec![
            kept_trade(0, "a1", &[0, 0, 0]),
            Kept::End(0, None),
            published(two, monday("10:02:00"), 1, &[1, 0, 0]),
            published(three, monday("10:03:00"), 1, &[1, 0, 1]),
        ]
    );
}

/// A quote feeds only `BID`, `ASK` and `MID` bars and a trade only `LAST` bars: a kept quotes or
/// trades request naming a `MARK` bar is refused by name before dispatch.
#[rstest]
#[case::quotes(true)]
#[case::trades(false)]
fn test_kept_request_naming_a_mark_bar_is_refused_by_name(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] quotes: bool,
) {
    let _ = stub_msgbus;
    let mark = BarType::from("AUD/USD.SIM-5-MINUTE-MARK-INTERNAL");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[]);
    let spec = KeptSpec {
        plain: true,
        quotes,
        ..kept_window(monday("10:00:00"), monday("10:08:00"))
    };

    let error = fixture.request(&[mark], spec).unwrap_err();

    assert!(
        error.contains("its own records do not feed")
            && error.contains("neither trades nor quotes feed its price type"),
        "{error}"
    );
    assert!(fixture.client_commands(0).is_empty());
}

/// Only a trades request and a quotes request feed bar builders: a book deltas request naming
/// 5-MINUTE bars kept by request over 10:00 to 10:07:59.999999999 is refused by name before
/// dispatch.
#[rstest]
fn test_kept_book_deltas_request_is_refused_by_name(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[]);
    let at = |ns: u64| Some(UnixNanos::from(ns).to_datetime_utc());

    let result =
        fixture
            .engine
            .execute_request(RequestCommand::BookDeltas(RequestBookDeltas::new(
                id,
                at(monday("10:00:00")),
                at(monday("10:08:00") - 1),
                None,
                Some(client_id),
                UUID4::new(),
                monday("12:00:00").into(),
                Some(params_from_json(json!({
                    "bar_types": [five.to_string()],
                    "update_subscriptions": true,
                }))),
            )));

    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("its own records do not feed")
            && error.contains("only a trades request or a quotes request feeds bar builders"),
        "{error}"
    );
    assert!(fixture.client_commands(0).is_empty());
}

/// An actor whose stop handler fails (see
/// `test_actor_whose_stop_handler_fails_still_releases_its_kept_requests`).
#[derive(Debug)]
struct FailingStopRequester {
    core: DataActorCore,
}

nautilus_actor!(FailingStopRequester);
impl DataActor for FailingStopRequester {
    fn on_stop(&mut self) -> anyhow::Result<()> {
        anyhow::bail!("the stop handler failed")
    }
}

/// An actor's release at its stop does not depend on its stop handler: A's R1 keeps 5-MINUTE
/// bars over 10:00 to 10:07:59.999999999; A's stop handler fails, and A still releases R1, so a
/// fresh request from 10:00 is admitted.
#[rstest]
fn test_actor_whose_stop_handler_fails_still_releases_its_kept_requests(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let id = audusd_sim.id;
    let five = bar_type_of(id, "5-MINUTE");
    let mut fixture = KeptFixture::new(audusd_sim, client_id, venue, monday("12:00:00"), &[five]);
    let queued = Rc::clone(&fixture.queued);
    msgbus::register_data_command_endpoint(
        MessagingSwitchboard::data_engine_queue_execute(),
        TypedIntoHandler::from(move |command: DataCommand| queued.borrow_mut().push(command)),
    );
    let mut actor = FailingStopRequester {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some(ActorId::from("KEPT-A")),
            ..Default::default()
        }),
    };
    actor
        .register(
            TraderId::test_default(),
            Rc::clone(&fixture.clock) as Rc<RefCell<dyn Clock>>,
            Rc::clone(&fixture.cache),
        )
        .unwrap();
    let at = |ns: u64| Some(UnixNanos::from(ns).to_datetime_utc());
    let request_id = actor
        .request_trades(
            id,
            at(monday("10:00:00")),
            at(monday("10:08:00") - 1),
            None,
            Some(client_id),
            Some(params_from_json(json!({
                "bar_types": [five.to_string()],
                "update_subscriptions": true,
            }))),
        )
        .unwrap();
    fixture.requests.push(request_id);
    fixture.pump();
    fixture.answer(
        0,
        &[(monday("10:01:00"), 1, "a1"), (monday("10:06:00"), 2, "a2")],
        None,
    );

    actor.start().unwrap();
    let stopped = actor.stop();
    fixture.pump();
    let fresh = fixture.request(&[five], kept_window(monday("10:00:00"), monday("10:15:00")));

    assert!(stopped.is_err(), "{stopped:?}");
    assert!(fresh.is_ok(), "{fresh:?}");
}
