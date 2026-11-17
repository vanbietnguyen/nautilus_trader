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

    data_engine.response(DataResponse::Trades(TradesResponse::new(
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
    )));

    assert_eq!(
        cache.borrow().bar(&bar_type).map(|bar| bar.ts_event),
        Some(UnixNanos::from(1_000_000_000)),
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
        None,
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
            None,
            None,
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
    data_engine.response(DataResponse::Quotes(QuotesResponse::new(
        request_id,
        client_id,
        instrument_id,
        history,
        None,
        None,
        UnixNanos::from(t0 + 30 * SEC),
        Some(params),
    )));

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
            None,
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

#[rstest]
#[case::composite_first(true)]
#[case::source_first(false)]
fn test_update_subscriptions_request_chain_replays_live_quotes_before_dependents_go_live(
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

    let request_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({
        "bar_types": bar_types.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "update_subscriptions": true,
    }))
    .unwrap();

    data_engine.execute(DataCommand::Request(RequestCommand::Quotes(
        RequestQuotes::new(
            instrument_id,
            None,
            None,
            None,
            Some(client_id),
            request_id,
            UnixNanos::from(boundary + 10 * SEC),
            Some(params.clone()),
        ),
    )));

    for bar_type in bar_types {
        data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Bars(
            SubscribeBars::new(
                bar_type,
                Some(client_id),
                Some(venue),
                UUID4::new(),
                UnixNanos::from(boundary + 10 * SEC),
                None,
                None,
            ),
        )));
    }

    let make_quote = |ts: u64| {
        QuoteTick::new(
            instrument_id,
            Price::from("0.65000"),
            Price::from("0.65010"),
            Quantity::from("1000"),
            Quantity::from("1000"),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    };

    // Two live quotes straddling the 5-minute boundary arrive while the response is in flight
    data_engine.process_data(Data::Quote(make_quote(boundary - 30 * SEC)));
    data_engine.process_data(Data::Quote(make_quote(boundary + 5 * SEC)));
    assert!(cache.borrow().bar(&composite.standard()).is_none());

    data_engine.response(DataResponse::Quotes(QuotesResponse::new(
        request_id,
        client_id,
        instrument_id,
        Vec::new(),
        None,
        None,
        UnixNanos::from(boundary + 10 * SEC),
        Some(params),
    )));

    // The held quotes replay through the source before the composite goes live
    let cache_ref = cache.borrow();
    let source_bar = cache_ref.bar(&source.standard()).unwrap();
    assert_eq!(source_bar.ts_event, UnixNanos::from(boundary));
    let composite_bar = cache_ref.bar(&composite.standard()).unwrap();
    assert_eq!(composite_bar.ts_event, UnixNanos::from(boundary));
    assert_eq!(composite_bar.close, Price::from("0.65005"));
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

fn trades_request(
    instrument_ids: Vec<InstrumentId>,
    end: Option<u64>,
    client_id: ClientId,
    params: Value,
) -> RequestCommand {
    RequestCommand::Trades(RequestTrades::new(
        instrument_ids,
        None,
        end.map(|end| UnixNanos::from(end).to_datetime_utc()),
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::default(),
        Some(serde_json::from_value(params).unwrap()),
    ))
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
#[cfg(feature = "streaming")]
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
/// every answer the requester received.
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
        start.map(|start| UnixNanos::from(start).to_datetime_utc()),
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
    let mut params = joined_params(fixture.bar_type, NY_2026_11_12_2000, &[NY_2026_11_12_1800]);
    params["bar_types"] = json!(
        bar_types
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    params["interleave"] = json!(interleave);
    let request = trades_request(vec![fixture.instrument_id], None, fixture.client_id, params);
    fixture
        .engine
        .borrow_mut()
        .execute_request(request)
        .unwrap();
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
    assert_eq!(
        bars,
        vec![
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
        ]
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
    let request = trades_request(vec![id], None, client_id, params.clone());
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
    // The bar a2 completed is pending until the clean-up flushes it
    assert_eq!(
        tap.take(first),
        vec![
            part(ResponsePart::Trades(first, trade_ids(&["a1", "a2"]))),
            part(ResponsePart::Bars(
                first,
                seconds,
                at_ns(&[SECOND_NS + SECOND_NS / 5])
            )),
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

    let request_id = execute_interleaved(&fixture, id, &[seconds], false);
    let answer = answer_trades(
        &fixture,
        request_id,
        id,
        &[(SECOND_NS / 5, "a1"), (SECOND_NS + SECOND_NS / 5, "a2")],
        true,
    );
    fixture.engine.borrow_mut().response_envelope(answer);

    // The bar a2 completed is pending until the clean-up flushes it
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
fn test_interleaved_failure_before_any_part_delivers_held_trades_bars_through_the_dispatched_client(
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

    assert_eq!(
        tap.take(request_id),
        vec![
            part(ResponsePart::Bars(request_id, tick, at_ns(&[10, 20]))),
            terminal(None, Some("the source stopped")),
        ]
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

    let first = execute_interleaved(&fixture, id, &[seconds], false);
    let answer = answer_trades(
        &fixture,
        first,
        id,
        &[(SECOND_NS / 5, "a1"), (SECOND_NS + SECOND_NS / 5, "a2")],
        true,
    );
    fixture.engine.borrow_mut().response_envelope(answer);
    // The same trades again: each request's aggregators are its own
    let second = execute_interleaved(&fixture, id, &[seconds], false);
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
    let second = execute_interleaved(&fixture, gbp, &[gbp_seconds], false);
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

    // A completed time bar is pending until the next one completes or the clean-up flushes it
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
    assert_eq!(
        tap.take(first),
        vec![
            part(ResponsePart::Trades(first, trade_ids(&["a1", "a2"]))),
            part(ResponsePart::Bars(first, aud_seconds, at_ns(&[a2]))),
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

    let request_id = execute_interleaved(&fixture, id, &[seconds], false);
    for (index, trade_id) in ["a1", "a2", "a3"].into_iter().enumerate() {
        let ts = index as u64 * SECOND_NS + SECOND_NS / 5;
        let answer = answer_trades(&fixture, request_id, id, &[(ts, trade_id)], false);
        fixture.engine.borrow_mut().response_envelope(answer);
    }
    let answer = answer_trades(&fixture, request_id, id, &[], true);
    fixture.engine.borrow_mut().response_envelope(answer);

    // A completed time bar is pending until the next one completes or the clean-up flushes it
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
fn test_joined_request_expiry_releases_the_held_live_trades_through_the_answered_cut_and_floor(
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
        // In the answered cut's millisecond: the answer holds it already
        release_trade(
            id,
            100 * MS_NS + MS_NS / 2,
            205 * MS_NS,
            "0.65010",
            "overlap",
        ),
        // Past the cut, received below the answer's last `ts_init`
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

    run_timers(move_join_clock(&fixture, JOIN_DEADLINE));
    let later = release_trade(id, 102 * MS_NS, 210 * MS_NS, "0.65030", "later");
    fixture.engine.borrow_mut().process_data(Data::Trade(later));

    assert_eq!(
        built_bars(&fixture, bar_type),
        vec![
            at("0.65000", 200 * MS_NS),
            at("0.65020", 200 * MS_NS),
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
    let request = windowed_trades_request(&fixture, (1_000, 2_999), params);

    assert_eq!(
        run_quoted(&fixture, request),
        vec![quoted_end(
            QuotedPart::Nothing,
            Some("Catalog catalog_0 does not cover the quotes of AUD/USD.SIM from 2000 to 2099"),
        )]
    );
    assert_eq!(fixture.engine.borrow().request_pipeline_count(), 0);
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
    /// The catalog holds nothing in the window: one client leg answers it.
    CatalogOutsideTheWindow,
    /// It sets `skip_catalog_data`: one client leg answers it.
    SkipCatalogData,
    /// It sets a `schema`: its client answers it whole.
    Schema,
    /// The catalog covers the window's first run only: a client leg fills the gap.
    ClientGap,
}

/// A catalog run outside the window `[1 000, 2 999]` of the refusal test.
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
#[case::catalog_outside_the_window(
    Unquotable::CatalogOutsideTheWindow,
    "its plan has no catalog leg; its client leg from 1000 to 2999 carries no quotes"
)]
#[case::skip_catalog_data(
    Unquotable::SkipCatalogData,
    "its plan has no catalog leg; its client leg from 1000 to 2999 carries no quotes"
)]
#[case::schema(
    Unquotable::Schema,
    "its `schema` sends it to its client, so its plan has no catalog leg"
)]
#[case::client_gap(
    Unquotable::ClientGap,
    "its client leg from 2000 to 2999 carries no quotes"
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
        Unquotable::CatalogOutsideTheWindow => &OUTSIDE_THE_WINDOW,
        Unquotable::SkipCatalogData | Unquotable::Schema => &COUNTEREXAMPLE,
        Unquotable::ClientGap => &COUNTEREXAMPLE[..1],
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
