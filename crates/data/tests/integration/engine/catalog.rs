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

#[cfg(feature = "streaming")]
use super::*;

#[cfg(feature = "streaming")]
#[rstest]
fn test_continuous_future_request_serves_segments_from_catalog(
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    // With every segment covered by the catalog, the client is never asked and the parent
    // response completes from catalog legs alone.
    let _ = stub_msgbus;
    let minute = |value: u64| value * 60_000_000_000;
    let clock = data_engine_clock_at(minute(3));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let esh = add_es_contract(&cache, "ESH24.GLBX", "ESH24");
    let esm = add_es_contract(&cache, "ESM24.GLBX", "ESM24");

    let venue = Venue::from("GLBX");
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

    let catalog_dir = CatalogTempDir::new("continuous-future-segments");
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);
    let esh_bar_type = BarType::from("ESH24.GLBX-1-MINUTE-LAST-EXTERNAL");
    let esm_bar_type = BarType::from("ESM24.GLBX-1-MINUTE-LAST-EXTERNAL");

    for bar in [
        make_bar(
            esh_bar_type,
            "100.00",
            "101.00",
            "99.00",
            "100.50",
            1,
            minute(1),
        ),
        make_bar(
            esm_bar_type,
            "96.00",
            "97.00",
            "95.50",
            "96.50",
            2,
            minute(2),
        ),
    ] {
        catalog
            .write_to_parquet(
                &[bar],
                Some(UnixNanos::default()),
                Some(UnixNanos::from(minute(10))),
                None,
            )
            .unwrap();
    }

    data_engine.register_catalog(Box::new(catalog), None);

    let target_bar_type = BarType::from("ES.GLBX-1-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL");
    let parent_id = UUID4::new();
    let params = params_from_json(json!({
        "continuous_future_adjustment_mode": "BACKWARD_SPREAD",
        "continuous_future_transitions": [
            {
                "transition_time_ns": minute(2),
                "pre_instrument_id": esh.to_string(),
                "post_instrument_id": esm.to_string(),
                "pre_price": "100.00",
                "post_price": "95.00"
            }
        ]
    }));
    let (response_handler, response_saver) =
        get_response_saving_handler::<BarsResponse>(Some(Ustr::from("continuous-catalog-bars")));
    msgbus::register_response_handler(&parent_id, response_handler);

    let request = RequestBars::new(
        target_bar_type,
        Some(UnixNanos::from(minute(1)).to_datetime_utc()),
        Some(UnixNanos::from(minute(3)).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        Some(params),
    );
    data_engine
        .execute_request(RequestCommand::Bars(request))
        .unwrap();

    let cached_bar = cache
        .borrow()
        .bar(&target_bar_type.standard())
        .copied()
        .unwrap();
    let responses = response_saver.get_messages();
    assert!(recorder.borrow().is_empty());
    assert_eq!(responses.len(), 1);
    assert_eq!(response_data_count(&responses[0]), Some(2));
    assert_eq!(cached_bar.open, Price::from("96.00"));
    assert_eq!(cached_bar.close, Price::from("96.50"));
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_quotes_from_catalog(
    audusd_sim: CurrencyPair,
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);
    let _catalog_dir = register_quote_catalog(&mut data_engine, audusd_sim.id, 1_000);
    let correlation_id = UUID4::new();

    let sub = SubscribeQuotes::new(
        audusd_sim.id,
        Some(client_id),
        Some(venue),
        UUID4::new(),
        UnixNanos::default(),
        Some(correlation_id),
        None,
    );
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Quotes(sub)));

    let SubscribeCommand::Quotes(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, correlation_id)
    else {
        panic!("expected quotes subscribe");
    };

    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get_u64("start_ns")),
        Some(1_001)
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_quotes_preserves_existing_start_ns(
    audusd_sim: CurrencyPair,
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);
    let _catalog_dir = register_quote_catalog(&mut data_engine, audusd_sim.id, 1_000);
    let params: Params = serde_json::from_value(json!({"start_ns": 42})).unwrap();
    let correlation_id = UUID4::new();

    let sub = SubscribeQuotes::new(
        audusd_sim.id,
        Some(client_id),
        Some(venue),
        UUID4::new(),
        UnixNanos::default(),
        Some(correlation_id),
        Some(params),
    );
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Quotes(sub)));

    let SubscribeCommand::Quotes(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, correlation_id)
    else {
        panic!("expected quotes subscribe");
    };

    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get_u64("start_ns")),
        Some(42)
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_quotes_sets_null_without_catalog_hit(
    audusd_sim: CurrencyPair,
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);
    let _catalog_dir = register_empty_catalog(&mut data_engine, "empty-quotes");
    let correlation_id = UUID4::new();

    let sub = SubscribeQuotes::new(
        audusd_sim.id,
        Some(client_id),
        Some(venue),
        UUID4::new(),
        UnixNanos::default(),
        Some(correlation_id),
        None,
    );
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Quotes(sub)));

    let SubscribeCommand::Quotes(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, correlation_id)
    else {
        panic!("expected quotes subscribe");
    };

    let null_value = json!(null);
    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get("start_ns")),
        Some(&null_value)
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_trades_from_catalog(
    #[values(false, true)] batch: bool,
    #[values(None, Some(79))] start_ns: Option<u64>,
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let audusd_sim = audusd_sim();
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);
    let _catalog_dir = register_trade_catalog(&mut data_engine, audusd_sim.id, 2_000);
    let correlation_id = UUID4::new();

    let ids = if batch {
        vec![audusd_sim.id, InstrumentId::from("ETH/USD.SIM")]
    } else {
        vec![audusd_sim.id]
    };

    let params = start_ns
        .map(|value| Params::new([("start_ns".to_string(), serde_json::json!(value))].into()));

    let sub = SubscribeTrades::new(
        ids,
        Some(client_id),
        Some(venue),
        UUID4::new(),
        UnixNanos::default(),
        Some(correlation_id),
        params,
    );
    let mut expected = serde_json::to_value(&sub).unwrap();
    if !batch && start_ns.is_none() {
        expected["params"] = serde_json::json!({"start_ns": 2_001});
    }

    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Trades(sub)));

    let SubscribeCommand::Trades(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, correlation_id)
    else {
        panic!("expected trades subscribe");
    };

    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get_u64("start_ns")),
        start_ns.or((!batch).then_some(2_001))
    );
    assert_eq!(serde_json::to_value(recorded).unwrap(), expected);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_external_bars_from_catalog(
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);
    let bar_type = BarType::from("AUD/USD.SIM-1-MINUTE-LAST-EXTERNAL");
    let _catalog_dir = register_bar_catalog(&mut data_engine, bar_type, 3_000);
    let correlation_id = UUID4::new();

    let sub = SubscribeBars::new(
        bar_type,
        Some(client_id),
        Some(venue),
        UUID4::new(),
        UnixNanos::default(),
        Some(correlation_id),
        None,
    );
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Bars(sub)));

    let SubscribeCommand::Bars(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, correlation_id)
    else {
        panic!("expected bars subscribe");
    };

    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get_u64("start_ns")),
        Some(3_001)
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_skips_internal_bars(
    audusd_sim: CurrencyPair,
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);

    let inst_any = InstrumentAny::CurrencyPair(audusd_sim);
    data_engine.process(&inst_any as &dyn Any);

    let bar_type = BarType::from("AUD/USD.SIM-1-MINUTE-LAST-INTERNAL");
    let _catalog_dir = register_bar_catalog(&mut data_engine, bar_type, 4_000);
    let command_id = UUID4::new();

    let sub = SubscribeBars::new(
        bar_type,
        Some(client_id),
        Some(venue),
        command_id,
        UnixNanos::default(),
        None,
        None,
    );
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Bars(sub)));

    let SubscribeCommand::Trades(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, command_id)
    else {
        panic!("expected source trades subscribe");
    };

    assert_eq!(
        recorded.single_instrument_id().unwrap(),
        bar_type.instrument_id()
    );
    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get("start_ns")),
        Some(&json!(null))
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_custom_data_from_catalog(
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);
    let data_type = DataType::new("CustomFeed", None, Some("SIM//AUDUSD".to_string()));
    let _catalog_dir = register_custom_catalog(&mut data_engine, &data_type, 5_000);
    let correlation_id = UUID4::new();

    let sub = SubscribeCustomData::new(
        Some(client_id),
        Some(venue),
        data_type,
        UUID4::new(),
        UnixNanos::default(),
        Some(correlation_id),
        None,
    );
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Data(sub)));

    let SubscribeCommand::Data(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, correlation_id)
    else {
        panic!("expected custom data subscribe");
    };

    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get_u64("start_ns")),
        Some(5_001)
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_custom_data_sets_null_without_catalog_hit(
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);
    let data_type = DataType::new("CustomFeed", None, Some("SIM//MISSING".to_string()));
    let _catalog_dir = register_empty_catalog(&mut data_engine, "empty-custom");
    let correlation_id = UUID4::new();

    let sub = SubscribeCustomData::new(
        Some(client_id),
        Some(venue),
        data_type,
        UUID4::new(),
        UnixNanos::default(),
        Some(correlation_id),
        None,
    );
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Data(sub)));

    let SubscribeCommand::Data(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, correlation_id)
    else {
        panic!("expected custom data subscribe");
    };

    let null_value = json!(null);
    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get("start_ns")),
        Some(&null_value)
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_custom_data_without_identifier_merges_catalog_intervals(
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);
    let type_name = "CustomFeed";
    let catalog_dir = CatalogTempDir::new("custom-no-identifier");
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);
    write_custom_catalog_file(
        &catalog_dir,
        &catalog,
        type_name,
        Some("SIM//AUDUSD"),
        1_000,
        10_000,
    );
    write_custom_catalog_file(
        &catalog_dir,
        &catalog,
        type_name,
        Some("SIM//EURUSD"),
        5_000,
        6_000,
    );
    data_engine.register_catalog(Box::new(catalog), None);
    let data_type = DataType::new(type_name, None, None);
    let correlation_id = UUID4::new();

    let sub = SubscribeCustomData::new(
        Some(client_id),
        Some(venue),
        data_type,
        UUID4::new(),
        UnixNanos::default(),
        Some(correlation_id),
        None,
    );
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Data(sub)));

    let SubscribeCommand::Data(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, correlation_id)
    else {
        panic!("expected custom data subscribe");
    };

    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get_u64("start_ns")),
        Some(10_001)
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_custom_data_preserves_existing_start_ns(
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);
    let data_type = DataType::new("CustomFeed", None, Some("SIM//AUDUSD".to_string()));
    let _catalog_dir = register_custom_catalog(&mut data_engine, &data_type, 6_000);
    let params: Params = serde_json::from_value(json!({"start_ns": 42})).unwrap();
    let correlation_id = UUID4::new();

    let sub = SubscribeCustomData::new(
        Some(client_id),
        Some(venue),
        data_type,
        UUID4::new(),
        UnixNanos::default(),
        Some(correlation_id),
        Some(params),
    );
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Data(sub)));

    let SubscribeCommand::Data(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, correlation_id)
    else {
        panic!("expected custom data subscribe");
    };

    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get_u64("start_ns")),
        Some(42)
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_catalog_start_ns_prefill_custom_data_preserves_command_metadata(
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let mut data_engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut data_engine, clock, cache, client_id, venue);
    let metadata = serde_json::from_value(json!({
        "instrument_id": "IGNORED.SIM",
        "source": "metadata",
    }))
    .unwrap();

    let data_type = DataType::new(
        "CustomMetadataFeed",
        Some(metadata),
        Some("SIM//METADATA".to_string()),
    );
    let _catalog_dir = register_custom_catalog(&mut data_engine, &data_type, 7_000);
    let command_id = UUID4::new();
    let ts_init = UnixNanos::from(123);
    let correlation_id = UUID4::new();
    let params: Params = serde_json::from_value(json!({"source": "params"})).unwrap();

    let sub = SubscribeCustomData::new(
        Some(client_id),
        Some(venue),
        data_type.clone(),
        command_id,
        ts_init,
        Some(correlation_id),
        Some(params),
    );
    data_engine.execute(DataCommand::Subscribe(SubscribeCommand::Data(sub)));

    let SubscribeCommand::Data(recorded) =
        recorded_subscribe_command_with_correlation(&recorder, correlation_id)
    else {
        panic!("expected custom data subscribe");
    };

    assert_eq!(recorded.client_id, Some(client_id));
    assert_eq!(recorded.venue, Some(venue));
    assert_eq!(recorded.data_type.type_name(), data_type.type_name());
    assert_eq!(recorded.data_type.metadata(), data_type.metadata());
    assert_eq!(recorded.data_type.identifier(), data_type.identifier());
    assert_eq!(recorded.command_id, command_id);
    assert_eq!(recorded.ts_init, ts_init);
    assert_eq!(recorded.correlation_id, Some(correlation_id));
    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get_u64("start_ns")),
        Some(7_001)
    );
    assert_eq!(
        recorded
            .params
            .as_ref()
            .and_then(|params| params.get_str("source")),
        Some("params")
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_time_range_pipeline_child_uses_catalog_client_fanin(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_test_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_quote_catalog_with_quotes(
        &mut data_engine,
        "time-range-split-quotes",
        &[split_quote(instrument_id, 1_500_000_000)],
        Some((1_000_000_000, 1_500_000_000)),
    );
    let recorder = register_time_range_recorder(&mut data_engine, clock, cache, client_id, venue);

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<QuotesResponse>(Some(Ustr::from("time-range-split-parent")));
    msgbus::register_response_handler(&parent_id, handler);

    let params: Params = serde_json::from_value(json!({
        "time_range_generator": "",
        "durations_seconds": [2],
    }))
    .unwrap();
    let req = RequestCommand::Quotes(RequestQuotes::new(
        instrument_id,
        Some(UnixNanos::from(1_000_000_000).to_datetime_utc()),
        Some(UnixNanos::from(5_000_000_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        Some(params),
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_quotes(&recorder);
    assert_eq!(
        recorded.len(),
        1,
        "first time-range child should split into one client leg"
    );
    assert_eq!(data_engine.request_pipeline_count(), 1);
    assert_eq!(data_engine.time_range_pipeline_count(), 1);

    data_engine.response(time_range_quote_response(
        &recorded[0],
        instrument_id,
        client_id,
        1,
        vec![split_quote(instrument_id, 2_500_000_000)],
    ));

    let recorded = recorded_request_quotes(&recorder);
    assert_eq!(
        recorded.len(),
        2,
        "next time-range child should be issued after catalog/client fan-in"
    );
    assert_eq!(
        recorded[1].start.map(|dt| dt.as_nanosecond()),
        Some(3_000_000_001)
    );
    assert_eq!(
        recorded[1].end.map(|dt| dt.as_nanosecond()),
        Some(5_000_000_000)
    );

    data_engine.response(time_range_quote_response(
        &recorded[1],
        instrument_id,
        client_id,
        0,
        Vec::new(),
    ));

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert!(received[0].data.is_empty());
    assert_eq!(
        received[0]
            .params
            .as_ref()
            .and_then(|params| params.get("data_count"))
            .and_then(Value::as_u64),
        Some(2)
    );
    assert_eq!(data_engine.request_pipeline_count(), 0);
    assert_eq!(data_engine.time_range_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::explicit(false)]
#[case::venue(true)]
fn test_request_quotes_catalog_only_serves_from_disk(
    #[case] venue_routed: bool,
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;

    let client_id = if venue_routed {
        ClientId::from("VENUE_SOURCE")
    } else {
        client_id
    };

    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, Rc::clone(&cache), None);

    if venue_routed {
        register_recording_client(
            &mut data_engine,
            Rc::new(RefCell::new(VirtualClock::new())),
            cache,
            client_id,
            instrument_id.venue,
        );
        data_engine
            .register_venue_routing(client_id, instrument_id.venue)
            .unwrap();
    }

    let _catalog_dir = register_quote_catalog_with_quotes(
        &mut data_engine,
        "catalog-only",
        &[
            split_quote(instrument_id, 1_000),
            split_quote(instrument_id, 2_000),
        ],
        Some((1_000, 2_000)),
    );

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<QuotesResponse>(Some(Ustr::from("catalog-only")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Quotes(RequestQuotes::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_000).to_datetime_utc()),
        None,
        (!venue_routed).then_some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert_eq!(received[0].client_id, client_id);
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|q| q.ts_init.as_u64())
        .collect();
    assert_eq!(ts_inits, vec![1_000, 2_000]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_quotes_client_only_when_catalog_has_no_data(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_empty_catalog(&mut data_engine, "empty-quotes-only");

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let req = RequestCommand::Quotes(RequestQuotes::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_quotes(&recorder);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].start.map(|d| d.as_nanosecond()), Some(1_000));
    assert_eq!(recorded[0].end.map(|d| d.as_nanosecond()), Some(3_000));
    assert_eq!(data_engine.request_pipeline_count(), 1);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_quotes_catalog_plus_client_split(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_quote_catalog_with_quotes(
        &mut data_engine,
        "split-quotes",
        &[split_quote(instrument_id, 1_500)],
        Some((1_000, 1_500)),
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<QuotesResponse>(Some(Ustr::from("split-quotes-parent")));
    msgbus::register_response_handler(&parent_id, handler);

    let parent_limit = NonZeroUsize::new(50).unwrap();
    let sentinel_params: Params = serde_json::from_value(json!({"feed_tag": "alpha"})).unwrap();
    let req = RequestCommand::Quotes(RequestQuotes::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        Some(parent_limit),
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        Some(sentinel_params),
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_quotes(&recorder);
    assert_eq!(
        recorded.len(),
        1,
        "expected one client leg for the missing interval"
    );
    let client_start = recorded[0].start.map_or(0, |d| d.as_nanosecond());
    let client_end = recorded[0].end.map_or(0, |d| d.as_nanosecond());
    assert!(
        client_start > 1_500,
        "client leg should start after the catalog coverage ends (was {client_start})"
    );
    assert_eq!(client_end, 3_000);
    assert_eq!(
        recorded[0].limit,
        Some(parent_limit),
        "with_dates_for_pipeline must carry the parent limit to each leg"
    );
    assert_eq!(
        recorded[0]
            .params
            .as_ref()
            .and_then(|p| p.get("feed_tag"))
            .and_then(Value::as_str),
        Some("alpha"),
        "with_dates_for_pipeline must carry parent params to each leg"
    );

    let leg_request_id = recorded[0].request_id;
    data_engine.response(leg_quotes_response(
        leg_request_id,
        instrument_id,
        client_id,
        vec![split_quote(instrument_id, 2_500)],
        recorded[0]
            .start
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
        recorded[0]
            .end
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
    ));

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|q| q.ts_init.as_u64())
        .collect();
    assert_eq!(ts_inits, vec![1_500, 2_500]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_quotes_skip_catalog_data_param_honored(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_quote_catalog_with_quotes(
        &mut data_engine,
        "skip-catalog",
        &[split_quote(instrument_id, 1_500)],
        Some((1_000, 1_500)),
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let params: Params = serde_json::from_value(json!({"skip_catalog_data": true})).unwrap();
    let parent_id = UUID4::new();
    let req = RequestCommand::Quotes(RequestQuotes::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        Some(params),
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_quotes(&recorder);
    assert_eq!(recorded.len(), 1, "skip flag should bypass catalog leg");
    assert_eq!(
        recorded[0].start.map(|d| d.as_nanosecond()),
        Some(1_000),
        "client leg should cover the full parent window when catalog is skipped"
    );
    assert_eq!(recorded[0].end.map(|d| d.as_nanosecond()), Some(3_000));
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_quotes_no_client_and_no_catalog_data_emits_empty(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_empty_catalog(&mut data_engine, "empty-no-client");

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<QuotesResponse>(Some(Ustr::from("empty-no-client")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Quotes(RequestQuotes::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert!(received[0].data.is_empty());
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_bars_catalog_lookup_uses_bar_type_identifier(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let bar_type = BarType::from(format!("{}-1-MINUTE-LAST-EXTERNAL", audusd_sim.id).as_str());
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_bar_catalog_with_bars(
        &mut data_engine,
        "bars-by-bar-type",
        &[split_bar(bar_type, 2_000)],
        Some((1_000, 3_000)),
    );

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<BarsResponse>(Some(Ustr::from("bars-by-bar-type")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Bars(RequestBars::new(
        bar_type,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert_eq!(received[0].bar_type, bar_type);
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|b| b.ts_init.as_u64())
        .collect();
    assert_eq!(ts_inits, vec![2_000]);
    assert_eq!(received[0].data[0].bar_type, bar_type);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_trades_catalog_plus_client_split(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_trade_catalog_with_trades(
        &mut data_engine,
        "split-trades",
        &[split_trade(instrument_id, 1_500, "T-1")],
        Some((1_000, 1_500)),
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<TradesResponse>(Some(Ustr::from("split-trades-parent")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Trades(RequestTrades::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_trades(&recorder);
    assert_eq!(
        recorded.len(),
        1,
        "expected one client leg for trades split"
    );
    let leg_request_id = recorded[0].request_id;

    data_engine.response(DataResponse::Trades(TradesResponse::new(
        leg_request_id,
        client_id,
        instrument_id,
        vec![split_trade(instrument_id, 2_500, "T-2")],
        recorded[0]
            .start
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
        recorded[0]
            .end
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
        UnixNanos::default(),
        None,
    )));

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|t| t.ts_init.as_u64())
        .collect();
    assert_eq!(ts_inits, vec![1_500, 2_500]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_quotes_dispatches_straight_to_client_with_no_catalog_registered(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let original_start = UnixNanos::from(1_000).to_datetime_utc();
    let original_end = UnixNanos::from(3_000).to_datetime_utc();
    let req = RequestCommand::Quotes(RequestQuotes::new(
        instrument_id,
        Some(original_start),
        Some(original_end),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_quotes(&recorder);
    assert_eq!(
        recorded.len(),
        1,
        "no-catalog path must dispatch a single direct client request"
    );
    assert_eq!(
        recorded[0].request_id, parent_id,
        "no-catalog path must preserve the parent request id (no pipeline rebinding)"
    );
    assert_eq!(recorded[0].start, Some(original_start));
    assert_eq!(recorded[0].end, Some(original_end));
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_pipeline_count_resets_after_catalog_split_fanin(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_quote_catalog_with_quotes(
        &mut data_engine,
        "pipeline-reset",
        &[split_quote(instrument_id, 1_500)],
        Some((1_000, 1_500)),
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let req = RequestCommand::Quotes(RequestQuotes::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_quotes(&recorder);
    assert_eq!(recorded.len(), 1);

    data_engine.response(leg_quotes_response(
        recorded[0].request_id,
        instrument_id,
        client_id,
        vec![split_quote(instrument_id, 2_500)],
        recorded[0]
            .start
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
        recorded[0]
            .end
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
    ));

    assert_eq!(data_engine.request_pipeline_count(), 0);
    assert_eq!(data_engine.pending_join_request_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_bars_catalog_plus_client_split(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let bar_type = BarType::from(format!("{}-1-MINUTE-LAST-EXTERNAL", audusd_sim.id).as_str());
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_bar_catalog_with_bars(
        &mut data_engine,
        "bars-split",
        &[split_bar(bar_type, 1_500)],
        Some((1_000, 1_500)),
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<BarsResponse>(Some(Ustr::from("bars-split-parent")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Bars(RequestBars::new(
        bar_type,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded: Vec<RequestBars> = recorder
        .borrow()
        .iter()
        .filter_map(|cmd| match cmd {
            DataCommand::Request(RequestCommand::Bars(req)) => Some(req.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].single_bar_type().unwrap(),
        bar_type,
        "client leg must preserve the parent bar_type"
    );

    data_engine.response(DataResponse::Bars(BarsResponse::new(
        recorded[0].request_id,
        client_id,
        bar_type,
        vec![split_bar(bar_type, 2_500)],
        recorded[0]
            .start
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
        recorded[0]
            .end
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
        UnixNanos::default(),
        None,
    )));

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].bar_type, bar_type);
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|b| b.ts_init.as_u64())
        .collect();
    assert_eq!(ts_inits, vec![1_500, 2_500]);
    assert_eq!(received[0].data[0].bar_type, bar_type);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_funding_rates_catalog_only_serves_from_disk(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_funding_catalog_with_rates(
        &mut data_engine,
        "funding-catalog-only",
        &[
            split_funding_rate(instrument_id, 1_000, "0.0001"),
            split_funding_rate(instrument_id, 2_000, "0.0002"),
        ],
        Some((1_000, 2_000)),
    );

    let parent_id = UUID4::new();
    let (handler, saver) = get_response_saving_handler::<FundingRatesResponse>(Some(Ustr::from(
        "funding-catalog-only",
    )));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::FundingRates(RequestFundingRates::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|rate| rate.ts_init.as_u64())
        .collect();

    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert_eq!(ts_inits, vec![1_000, 2_000]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_funding_rates_catalog_plus_client_split(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_funding_catalog_with_rates(
        &mut data_engine,
        "funding-split",
        &[split_funding_rate(instrument_id, 1_500, "0.0001")],
        Some((1_000, 1_500)),
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let (handler, saver) = get_response_saving_handler::<FundingRatesResponse>(Some(Ustr::from(
        "funding-split-parent",
    )));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::FundingRates(RequestFundingRates::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_funding_rates(&recorder);
    assert_eq!(
        recorded.len(),
        1,
        "expected one client leg for the missing interval"
    );

    data_engine.response(DataResponse::FundingRates(FundingRatesResponse::new(
        recorded[0].request_id,
        client_id,
        instrument_id,
        vec![split_funding_rate(instrument_id, 2_500, "0.0002")],
        recorded[0].start.map(datetime_to_unix_nanos_for_test),
        recorded[0].end.map(datetime_to_unix_nanos_for_test),
        UnixNanos::default(),
        None,
    )));

    let received = saver.get_messages();
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|rate| rate.ts_init.as_u64())
        .collect();

    assert_eq!(received.len(), 1);
    assert_eq!(ts_inits, vec![1_500, 2_500]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_funding_rates_no_client_no_catalog_emits_empty(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_empty_catalog(&mut data_engine, "funding-empty");

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<FundingRatesResponse>(Some(Ustr::from("funding-empty")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::FundingRates(RequestFundingRates::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert!(received[0].data.is_empty());
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_funding_rates_dispatches_straight_to_client_with_no_catalog_registered(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let req = RequestCommand::FundingRates(RequestFundingRates::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_funding_rates(&recorder);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].request_id, parent_id);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_custom_data_catalog_only_serves_from_disk(
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let instrument_id = InstrumentId::from("RUST.TEST");
    let data_type = rust_test_custom_data_type("RUST.TEST");
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_custom_catalog_with_data(
        &mut data_engine,
        "custom-catalog-only",
        vec![
            split_custom(data_type.clone(), instrument_id, 1_000, 1.0),
            split_custom(data_type.clone(), instrument_id, 2_000, 2.0),
        ],
        Some((1_000, 2_000)),
    );

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<CustomDataResponse>(Some(Ustr::from("custom-catalog-only")));
    msgbus::register_response_handler(&parent_id, handler);
    let params: Params = serde_json::from_value(json!({"source": "params"})).unwrap();

    let req = RequestCommand::Data(RequestCustomData::new(
        client_id,
        data_type.clone(),
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_000).to_datetime_utc()),
        None,
        parent_id,
        UnixNanos::default(),
        Some(params),
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    let data = custom_response_payload(&received[0]);

    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert_eq!(received[0].data_type, data_type);
    assert_eq!(
        received[0]
            .params
            .as_ref()
            .and_then(|params| params.get_bool("update_catalog")),
        Some(false)
    );
    assert_eq!(custom_values(&data), vec![1.0, 2.0]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_custom_data_without_identifier_catalog_only_serves_from_disk(
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let instrument_id = InstrumentId::from("RUST.TEST");

    let data_type = DataType::new(
        "RustTestCustomData",
        Some(serde_json::from_value(json!({"source": "catalog-test"})).unwrap()),
        None,
    );
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_custom_catalog_with_data(
        &mut data_engine,
        "custom-catalog-no-identifier",
        vec![
            split_custom(data_type.clone(), instrument_id, 1_000, 1.0),
            split_custom(data_type.clone(), instrument_id, 2_000, 2.0),
        ],
        Some((1_000, 2_000)),
    );

    let parent_id = UUID4::new();
    let (handler, saver) = get_response_saving_handler::<CustomDataResponse>(Some(Ustr::from(
        "custom-catalog-no-identifier",
    )));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Data(RequestCustomData::new(
        client_id,
        data_type.clone(),
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_000).to_datetime_utc()),
        None,
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    let data = custom_response_payload(&received[0]);

    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert_eq!(received[0].data_type, data_type);
    assert_eq!(custom_values(&data), vec![1.0, 2.0]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::raw(false)]
#[case::writeback(true)]
fn test_request_custom_data_catalog_plus_client_split(
    #[case] writeback: bool,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = InstrumentId::from("RUST.TEST");
    let data_type = rust_test_custom_data_type("RUST.TEST");
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_custom_catalog_with_data(
        &mut data_engine,
        "custom-split",
        vec![split_custom(data_type.clone(), instrument_id, 1_500, 1.5)],
        Some((1_000, 1_500)),
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<CustomDataResponse>(Some(Ustr::from("custom-split-parent")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Data(RequestCustomData::new(
        client_id,
        data_type.clone(),
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        parent_id,
        UnixNanos::default(),
        writeback.then(|| params_from_json(json!({"update_catalog": true}))),
    ));
    let mut retry = req.clone();
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_data(&recorder);
    assert_eq!(
        recorded.len(),
        1,
        "expected one client leg for the missing interval"
    );

    let response = DataResponse::Data(CustomDataResponse::new(
        recorded[0].request_id,
        client_id,
        Some(venue),
        data_type.clone(),
        split_custom(data_type, instrument_id, 2_500, 2.5),
        recorded[0].start.map(datetime_to_unix_nanos_for_test),
        recorded[0].end.map(datetime_to_unix_nanos_for_test),
        UnixNanos::default(),
        None,
    ));

    if writeback {
        data_engine.response_envelope(DataResponseEnvelope::new(response, true, None));
    } else {
        data_engine.response(response);
    }

    let received = saver.get_messages();
    let data = custom_response_payload(&received[0]);

    assert_eq!(received.len(), 1);
    assert_eq!(custom_values(&data), vec![1.5, 2.5]);
    assert_eq!(data_engine.request_pipeline_count(), 0);

    if writeback {
        let RequestCommand::Data(request) = &mut retry else {
            unreachable!();
        };

        request.request_id = UUID4::new();
        let (handler, saver) = get_response_saving_handler::<CustomDataResponse>(None);
        msgbus::register_response_handler(&request.request_id, handler);
        data_engine.execute_request(retry).unwrap();
        let received = saver.get_messages();
        assert_eq!(received.len(), 1);
        assert_eq!(
            custom_values(&custom_response_payload(&received[0])),
            vec![1.5, 2.5]
        );
        assert_eq!(recorded_request_data(&recorder).len(), 1);
    }
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_custom_data_no_client_no_catalog_emits_empty(
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let data_type = rust_test_custom_data_type("RUST.TEST");
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_empty_catalog(&mut data_engine, "custom-empty");

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<CustomDataResponse>(Some(Ustr::from("custom-empty")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Data(RequestCustomData::new(
        client_id,
        data_type,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    let data = custom_response_payload(&received[0]);
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert!(data.is_empty());
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_custom_data_dispatches_straight_to_client_with_no_catalog_registered(
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let data_type = rust_test_custom_data_type("RUST.TEST");
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let req = RequestCommand::Data(RequestCustomData::new(
        client_id,
        data_type,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_data(&recorder);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].request_id, parent_id);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_instruments_no_client_no_catalog_emits_empty(
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_empty_catalog(&mut data_engine, "instruments-empty");

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<InstrumentsResponse>(Some(Ustr::from("instruments-empty")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Instruments(RequestInstruments::new(
        None,
        None,
        Some(client_id),
        Some(venue),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert!(received[0].data.is_empty());
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_instrument_catalog_uses_latest_record(
    mut audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let mut earlier = audusd_sim.clone();
    earlier.ts_init = UnixNanos::from(1_000);
    audusd_sim.ts_init = UnixNanos::from(2_000);
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_instrument_catalog_with_instruments(
        &mut data_engine,
        "instrument-latest",
        vec![
            InstrumentAny::CurrencyPair(earlier),
            InstrumentAny::CurrencyPair(audusd_sim),
        ],
    );

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<InstrumentResponse>(Some(Ustr::from("instrument-latest")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Instrument(RequestInstrument::new(
        instrument_id,
        None,
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert_eq!(received[0].instrument_id, instrument_id);
    assert_eq!(received[0].data.ts_init(), UnixNanos::from(2_000));
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_instruments_catalog_applies_only_last(
    mut audusd_sim: CurrencyPair,
    mut gbpusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let mut audusd_earlier = audusd_sim.clone();
    audusd_earlier.ts_init = UnixNanos::from(1_000);
    audusd_sim.ts_init = UnixNanos::from(2_000);
    gbpusd_sim.ts_init = UnixNanos::from(3_000);
    let audusd_id = audusd_sim.id;
    let gbpusd_id = gbpusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_instrument_catalog_with_instruments(
        &mut data_engine,
        "instruments-only-last",
        vec![
            InstrumentAny::CurrencyPair(audusd_earlier),
            InstrumentAny::CurrencyPair(audusd_sim),
            InstrumentAny::CurrencyPair(gbpusd_sim),
        ],
    );

    let parent_id = UUID4::new();
    let (handler, saver) = get_response_saving_handler::<InstrumentsResponse>(Some(Ustr::from(
        "instruments-only-last",
    )));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Instruments(RequestInstruments::new(
        None,
        None,
        Some(client_id),
        Some(venue),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    let mut ids_and_timestamps: Vec<(InstrumentId, u64)> = received[0]
        .data
        .iter()
        .map(|instrument| (instrument.id(), instrument.ts_init().as_u64()))
        .collect();
    ids_and_timestamps.sort_by_key(|(id, _)| id.to_string());

    assert_eq!(received.len(), 1);
    assert_eq!(
        ids_and_timestamps,
        vec![(audusd_id, 2_000), (gbpusd_id, 3_000)]
    );
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::later_id_after_clock(3_000, 20_000_000_000)]
#[case::earlier_id_after_clock(20_000_000_000, 3_000)]
fn test_request_instruments_catalog_result_independent_of_id_order(
    mut audusd_sim: CurrencyPair,
    mut gbpusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
    #[case] audusd_ts_init: u64,
    #[case] gbpusd_ts_init: u64,
) {
    // The query has no end bound, so a definition stamped after the clock is still returned
    let _ = stub_msgbus;
    audusd_sim.ts_init = UnixNanos::from(audusd_ts_init);
    gbpusd_sim.ts_init = UnixNanos::from(gbpusd_ts_init);
    let audusd_id = audusd_sim.id;
    let gbpusd_id = gbpusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_instrument_catalog_with_instruments(
        &mut data_engine,
        "instruments-id-order",
        vec![
            InstrumentAny::CurrencyPair(audusd_sim),
            InstrumentAny::CurrencyPair(gbpusd_sim),
        ],
    );

    let parent_id = UUID4::new();
    let (handler, saver) = get_response_saving_handler::<InstrumentsResponse>(Some(Ustr::from(
        "instruments-id-order",
    )));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::Instruments(RequestInstruments::new(
        None,
        None,
        Some(client_id),
        Some(venue),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    let ids: Vec<InstrumentId> = received[0]
        .data
        .iter()
        .map(|instrument| instrument.id())
        .collect();

    assert_eq!(received.len(), 1);
    assert_eq!(ids, vec![audusd_id, gbpusd_id]);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_instrument_dispatches_straight_to_client_with_no_catalog_registered(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let req = RequestCommand::Instrument(RequestInstrument::new(
        instrument_id,
        None,
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_instrument(&recorder);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].request_id, parent_id);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_instruments_dispatches_straight_to_client_with_no_catalog_registered(
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let req = RequestCommand::Instruments(RequestInstruments::new(
        None,
        None,
        Some(client_id),
        Some(venue),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_instruments(&recorder);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].request_id, parent_id);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_instrument_force_update_dispatches_to_client_with_catalog_registered(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_instrument_catalog_with_instruments(
        &mut data_engine,
        "instrument-force-update",
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let params: Params = serde_json::from_value(json!({"force_instrument_update": true})).unwrap();
    let parent_id = UUID4::new();
    let req = RequestCommand::Instrument(RequestInstrument::new(
        instrument_id,
        None,
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        Some(params),
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_instrument(&recorder);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].request_id, parent_id);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_instruments_update_catalog_dispatches_to_client_with_catalog_registered(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_instrument_catalog_with_instruments(
        &mut data_engine,
        "instruments-update-catalog",
        vec![InstrumentAny::CurrencyPair(audusd_sim)],
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let params: Params = serde_json::from_value(json!({"update_catalog": true})).unwrap();
    let parent_id = UUID4::new();
    let req = RequestCommand::Instruments(RequestInstruments::new(
        None,
        None,
        Some(client_id),
        Some(venue),
        parent_id,
        UnixNanos::default(),
        Some(params),
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_instruments(&recorder);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].request_id, parent_id);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_subscription_name_param_disables_now_clamping(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    // Clock at 1_000; the request asks for data up to 5_000. Without the
    // subscription_name bypass, bound_request_dates clamps end to 1_000.
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 1_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_empty_catalog(&mut data_engine, "subscription-name");

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let params: Params = serde_json::from_value(json!({"subscription_name": "feed-a"})).unwrap();
    let parent_id = UUID4::new();
    let req = RequestCommand::Quotes(RequestQuotes::new(
        instrument_id,
        Some(UnixNanos::from(2_000).to_datetime_utc()),
        Some(UnixNanos::from(5_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        Some(params),
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_quotes(&recorder);
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].start.map(|d| d.as_nanosecond()),
        Some(2_000),
        "subscription_name must bypass start clamping"
    );
    assert_eq!(
        recorded[0].end.map(|d| d.as_nanosecond()),
        Some(5_000),
        "subscription_name must bypass end clamping"
    );
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_book_deltas_catalog_only_serves_from_disk(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_deltas_catalog_with_deltas(
        &mut data_engine,
        "deltas-catalog-only",
        &[
            split_delta(instrument_id, 1_000),
            split_delta(instrument_id, 2_000),
        ],
        Some((1_000, 2_000)),
    );

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<BookDeltasResponse>(Some(Ustr::from("deltas-catalog-only")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::BookDeltas(RequestBookDeltas::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|d| d.ts_init.as_u64())
        .collect();
    assert_eq!(ts_inits, vec![1_000, 2_000]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_book_deltas_catalog_plus_client_split(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_deltas_catalog_with_deltas(
        &mut data_engine,
        "deltas-split",
        &[split_delta(instrument_id, 1_500)],
        Some((1_000, 1_500)),
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<BookDeltasResponse>(Some(Ustr::from("deltas-split-parent")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::BookDeltas(RequestBookDeltas::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_book_deltas(&recorder);
    assert_eq!(
        recorded.len(),
        1,
        "expected one client leg for the missing interval"
    );
    assert_eq!(recorded[0].single_instrument_id().unwrap(), instrument_id);

    let leg_request_id = recorded[0].request_id;
    data_engine.response(DataResponse::BookDeltas(BookDeltasResponse::new(
        leg_request_id,
        client_id,
        instrument_id,
        vec![split_delta(instrument_id, 2_500)],
        recorded[0]
            .start
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
        recorded[0]
            .end
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
        UnixNanos::default(),
        None,
    )));

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|d| d.ts_init.as_u64())
        .collect();
    assert_eq!(ts_inits, vec![1_500, 2_500]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_book_deltas_no_client_no_catalog_emits_empty(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_empty_catalog(&mut data_engine, "deltas-empty");

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<BookDeltasResponse>(Some(Ustr::from("deltas-empty")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::BookDeltas(RequestBookDeltas::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert!(received[0].data.is_empty());
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_book_depth_catalog_only_serves_from_disk(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_depth_catalog_with_depths(
        &mut data_engine,
        "depth-catalog-only",
        &[
            book_depth_at(instrument_id, 1_000),
            book_depth_at(instrument_id, 2_000),
        ],
        Some((1_000, 2_000)),
    );

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<BookDepthResponse>(Some(Ustr::from("depth-catalog-only")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::BookDepth(RequestBookDepth::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_000).to_datetime_utc()),
        None,
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|d| d.ts_init.as_u64())
        .collect();
    assert_eq!(ts_inits, vec![1_000, 2_000]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_book_depth_catalog_plus_client_split(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);

    let _catalog_dir = register_depth_catalog_with_depths(
        &mut data_engine,
        "depth-split",
        &[book_depth_at(instrument_id, 1_500)],
        Some((1_000, 1_500)),
    );

    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    let mock_client = MockDataClient::new_with_recorder(
        clock,
        cache,
        client_id,
        Some(venue),
        Some(Rc::clone(&recorder)),
    );
    let adapter = DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(mock_client));
    data_engine.register_client(adapter, None);

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<BookDepthResponse>(Some(Ustr::from("depth-split-parent")));
    msgbus::register_response_handler(&parent_id, handler);

    let parent_depth = NonZeroUsize::new(10).unwrap();
    let req = RequestCommand::BookDepth(RequestBookDepth::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        Some(parent_depth),
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let recorded = recorded_request_book_depth(&recorder);
    assert_eq!(
        recorded.len(),
        1,
        "expected one client leg for the missing interval"
    );
    assert_eq!(recorded[0].single_instrument_id().unwrap(), instrument_id);
    assert_eq!(
        recorded[0].depth,
        Some(parent_depth),
        "with_dates_for_pipeline must carry the parent depth to each leg"
    );

    let leg_request_id = recorded[0].request_id;
    data_engine.response(DataResponse::BookDepth(BookDepthResponse::new(
        leg_request_id,
        client_id,
        instrument_id,
        vec![book_depth_at(instrument_id, 2_500)],
        recorded[0]
            .start
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
        recorded[0]
            .end
            .map(|d| UnixNanos::from(u64::try_from(d.as_nanosecond().max(0)).unwrap_or(0))),
        UnixNanos::default(),
        None,
    )));

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    let ts_inits: Vec<u64> = received[0]
        .data
        .iter()
        .map(|d| d.ts_init.as_u64())
        .collect();
    assert_eq!(ts_inits, vec![1_500, 2_500]);
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_request_book_depth_no_client_no_catalog_emits_empty(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let instrument_id = audusd_sim.id;
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
    advance_clock_to(&clock, 10_000_000_000);
    let mut data_engine = DataEngine::new(clock, cache, None);

    let _catalog_dir = register_empty_catalog(&mut data_engine, "depth-empty");

    let parent_id = UUID4::new();
    let (handler, saver) =
        get_response_saving_handler::<BookDepthResponse>(Some(Ustr::from("depth-empty")));
    msgbus::register_response_handler(&parent_id, handler);

    let req = RequestCommand::BookDepth(RequestBookDepth::new(
        instrument_id,
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(3_000).to_datetime_utc()),
        None,
        None,
        Some(client_id),
        parent_id,
        UnixNanos::default(),
        None,
    ));
    data_engine.execute_request(req).unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].correlation_id, parent_id);
    assert!(received[0].data.is_empty());
    assert_eq!(data_engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
fn register_empty_catalog(data_engine: &mut DataEngine, label: &str) -> CatalogTempDir {
    let catalog_dir = CatalogTempDir::new(label);
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);
    data_engine.register_catalog(Box::new(catalog), None);
    catalog_dir
}

#[cfg(feature = "streaming")]
fn register_quote_catalog(
    data_engine: &mut DataEngine,
    instrument_id: InstrumentId,
    last_timestamp: u64,
) -> CatalogTempDir {
    let catalog_dir = CatalogTempDir::new("quotes");
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);
    catalog
        .write_to_parquet(
            &[QuoteTick::new(
                instrument_id,
                Price::from("1.0000"),
                Price::from("1.0001"),
                Quantity::from(1),
                Quantity::from(1),
                UnixNanos::from(last_timestamp),
                UnixNanos::from(last_timestamp),
            )],
            None,
            None,
            None,
        )
        .unwrap();
    data_engine.register_catalog(Box::new(catalog), None);
    catalog_dir
}

#[cfg(feature = "streaming")]
fn register_trade_catalog(
    data_engine: &mut DataEngine,
    instrument_id: InstrumentId,
    last_timestamp: u64,
) -> CatalogTempDir {
    let catalog_dir = CatalogTempDir::new("trades");
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);
    catalog
        .write_to_parquet(
            &[TradeTick::new(
                instrument_id,
                Price::from("1.0000"),
                Quantity::from(1),
                AggressorSide::Buy,
                TradeId::new("T-1"),
                UnixNanos::from(last_timestamp),
                UnixNanos::from(last_timestamp),
            )],
            None,
            None,
            None,
        )
        .unwrap();
    data_engine.register_catalog(Box::new(catalog), None);
    catalog_dir
}

#[cfg(feature = "streaming")]
fn register_bar_catalog(
    data_engine: &mut DataEngine,
    bar_type: BarType,
    last_timestamp: u64,
) -> CatalogTempDir {
    let catalog_dir = CatalogTempDir::new("bars");
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);
    catalog
        .write_to_parquet(
            &[Bar::new(
                bar_type,
                Price::from("1.0000"),
                Price::from("1.0001"),
                Price::from("0.9999"),
                Price::from("1.0000"),
                Quantity::from(1),
                UnixNanos::from(last_timestamp),
                UnixNanos::from(last_timestamp),
            )],
            None,
            None,
            None,
        )
        .unwrap();
    data_engine.register_catalog(Box::new(catalog), None);
    catalog_dir
}

#[cfg(feature = "streaming")]
fn register_custom_catalog(
    data_engine: &mut DataEngine,
    data_type: &DataType,
    last_timestamp: u64,
) -> CatalogTempDir {
    let catalog_dir = CatalogTempDir::new("custom");
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);
    write_custom_catalog_file(
        &catalog_dir,
        &catalog,
        data_type.type_name(),
        data_type.identifier(),
        last_timestamp,
        last_timestamp,
    );

    data_engine.register_catalog(Box::new(catalog), None);
    catalog_dir
}

#[cfg(feature = "streaming")]
fn register_recording_client(
    data_engine: &mut DataEngine,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) -> Rc<RefCell<Vec<DataCommand>>> {
    let recorder: Rc<RefCell<Vec<DataCommand>>> = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(clock, cache, client_id, venue, None, &recorder, data_engine);
    recorder
}

#[cfg(feature = "streaming")]
fn recorded_subscribe_command_with_correlation(
    recorder: &Rc<RefCell<Vec<DataCommand>>>,
    correlation_id: UUID4,
) -> SubscribeCommand {
    let command = recorded_subscribe_command(recorder);
    assert_eq!(command.correlation_id(), Some(correlation_id));
    command
}

#[cfg(feature = "streaming")]
fn register_bar_catalog_with_bars(
    data_engine: &mut DataEngine,
    label: &str,
    bars: &[Bar],
    interval: Option<(u64, u64)>,
) -> CatalogTempDir {
    let catalog_dir = CatalogTempDir::new(label);
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);

    let (start, end) = match interval {
        Some((s, e)) => (Some(UnixNanos::from(s)), Some(UnixNanos::from(e))),
        None => (None, None),
    };

    catalog.write_to_parquet(bars, start, end, None).unwrap();
    data_engine.register_catalog(Box::new(catalog), None);
    catalog_dir
}

#[cfg(feature = "streaming")]
fn split_bar(bar_type: BarType, ts: u64) -> Bar {
    make_bar(bar_type, "1.0000", "1.0001", "0.9999", "1.0000", 1, ts)
}

#[cfg(feature = "streaming")]
fn recorded_request_quotes(recorder: &Rc<RefCell<Vec<DataCommand>>>) -> Vec<RequestQuotes> {
    recorder
        .borrow()
        .iter()
        .filter_map(|cmd| match cmd {
            DataCommand::Request(RequestCommand::Quotes(req)) => Some(req.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(feature = "streaming")]
fn recorded_request_trades(recorder: &Rc<RefCell<Vec<DataCommand>>>) -> Vec<RequestTrades> {
    recorder
        .borrow()
        .iter()
        .filter_map(|cmd| match cmd {
            DataCommand::Request(RequestCommand::Trades(req)) => Some(req.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(feature = "streaming")]
fn recorded_request_funding_rates(
    recorder: &Rc<RefCell<Vec<DataCommand>>>,
) -> Vec<RequestFundingRates> {
    recorder
        .borrow()
        .iter()
        .filter_map(|cmd| match cmd {
            DataCommand::Request(RequestCommand::FundingRates(req)) => Some(req.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(feature = "streaming")]
fn recorded_request_data(recorder: &Rc<RefCell<Vec<DataCommand>>>) -> Vec<RequestCustomData> {
    recorder
        .borrow()
        .iter()
        .filter_map(|cmd| match cmd {
            DataCommand::Request(RequestCommand::Data(req)) => Some(req.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(feature = "streaming")]
fn recorded_request_instrument(recorder: &Rc<RefCell<Vec<DataCommand>>>) -> Vec<RequestInstrument> {
    recorder
        .borrow()
        .iter()
        .filter_map(|cmd| match cmd {
            DataCommand::Request(RequestCommand::Instrument(req)) => Some(req.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(feature = "streaming")]
fn recorded_request_instruments(
    recorder: &Rc<RefCell<Vec<DataCommand>>>,
) -> Vec<RequestInstruments> {
    recorder
        .borrow()
        .iter()
        .filter_map(|cmd| match cmd {
            DataCommand::Request(RequestCommand::Instruments(req)) => Some(req.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(feature = "streaming")]
fn split_funding_rate(instrument_id: InstrumentId, ts: u64, rate: &str) -> FundingRateUpdate {
    FundingRateUpdate::new(
        instrument_id,
        rate.parse().unwrap(),
        None,
        None,
        UnixNanos::from(ts),
        UnixNanos::from(ts),
    )
}

#[cfg(feature = "streaming")]
fn register_funding_catalog_with_rates(
    data_engine: &mut DataEngine,
    label: &str,
    rates: &[FundingRateUpdate],
    interval: Option<(u64, u64)>,
) -> CatalogTempDir {
    let catalog_dir = CatalogTempDir::new(label);
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);

    let (start, end) = match interval {
        Some((s, e)) => (Some(UnixNanos::from(s)), Some(UnixNanos::from(e))),
        None => (None, None),
    };

    catalog.write_to_parquet(rates, start, end, None).unwrap();
    data_engine.register_catalog(Box::new(catalog), None);
    catalog_dir
}

#[cfg(feature = "streaming")]
fn rust_test_custom_data_type(identifier: &str) -> DataType {
    DataType::new(
        "RustTestCustomData",
        Some(serde_json::from_value(json!({"source": "catalog-test"})).unwrap()),
        Some(identifier.to_string()),
    )
}

#[cfg(feature = "streaming")]
fn split_custom(
    data_type: DataType,
    instrument_id: InstrumentId,
    ts: u64,
    value: f64,
) -> CustomData {
    CustomData::new(
        std::sync::Arc::new(RustTestCustomData {
            instrument_id,
            value,
            flag: value > 1.0,
            ts_event: UnixNanos::from(ts),
            ts_init: UnixNanos::from(ts),
        }),
        data_type,
    )
}

#[cfg(feature = "streaming")]
fn register_custom_catalog_with_data(
    data_engine: &mut DataEngine,
    label: &str,
    data: Vec<CustomData>,
    interval: Option<(u64, u64)>,
) -> CatalogTempDir {
    ensure_engine_custom_data_registered();
    let catalog_dir = CatalogTempDir::new(label);
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);

    let (start, end) = match interval {
        Some((s, e)) => (Some(UnixNanos::from(s)), Some(UnixNanos::from(e))),
        None => (None, None),
    };

    catalog
        .write_custom_data_batch(data, start, end, None)
        .unwrap();
    data_engine.register_catalog(Box::new(catalog), None);
    catalog_dir
}

#[cfg(feature = "streaming")]
fn custom_response_payload(resp: &CustomDataResponse) -> Vec<CustomData> {
    resp.data
        .as_ref()
        .downcast_ref::<Vec<CustomData>>()
        .expect("custom response payload should be Vec<CustomData>")
        .clone()
}

#[cfg(feature = "streaming")]
fn custom_values(data: &[CustomData]) -> Vec<f64> {
    data.iter()
        .map(|custom| {
            custom
                .data
                .as_any()
                .downcast_ref::<RustTestCustomData>()
                .expect("custom payload should be RustTestCustomData")
                .value
        })
        .collect()
}

#[cfg(feature = "streaming")]
fn register_instrument_catalog_with_instruments(
    data_engine: &mut DataEngine,
    label: &str,
    instruments: Vec<InstrumentAny>,
) -> CatalogTempDir {
    let catalog_dir = CatalogTempDir::new(label);
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);
    catalog.write_instruments(instruments).unwrap();
    data_engine.register_catalog(Box::new(catalog), None);
    catalog_dir
}

#[cfg(feature = "streaming")]
fn recorded_request_book_deltas(
    recorder: &Rc<RefCell<Vec<DataCommand>>>,
) -> Vec<RequestBookDeltas> {
    recorder
        .borrow()
        .iter()
        .filter_map(|cmd| match cmd {
            DataCommand::Request(RequestCommand::BookDeltas(req)) => Some(req.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(feature = "streaming")]
fn register_depth_catalog_with_depths(
    data_engine: &mut DataEngine,
    label: &str,
    depths: &[OrderBookDepth],
    interval: Option<(u64, u64)>,
) -> CatalogTempDir {
    let catalog_dir = CatalogTempDir::new(label);
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);

    let (start, end) = match interval {
        Some((s, e)) => (Some(UnixNanos::from(s)), Some(UnixNanos::from(e))),
        None => (None, None),
    };

    catalog.write_to_parquet(depths, start, end, None).unwrap();
    data_engine.register_catalog(Box::new(catalog), None);
    catalog_dir
}

#[cfg(feature = "streaming")]
fn recorded_request_book_depth(recorder: &Rc<RefCell<Vec<DataCommand>>>) -> Vec<RequestBookDepth> {
    recorder
        .borrow()
        .iter()
        .filter_map(|cmd| match cmd {
            DataCommand::Request(RequestCommand::BookDepth(req)) => Some(req.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_bar_list_prefills_each_catalog_start_independently(
    data_engine: Rc<RefCell<DataEngine>>,
    clock: Rc<RefCell<VirtualClock>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
    #[values(None, Some(79))] start_ns: Option<u64>,
    #[values(false, true)] equal: bool,
) {
    let mut engine = data_engine.borrow_mut();
    let recorder = register_recording_client(&mut engine, clock, cache, client_id, venue);
    let first = BarType::from("AUD/USD.SIM-1-MINUTE-LAST-EXTERNAL");
    let second = BarType::from("AUD/USD.SIM-5-MINUTE-LAST-EXTERNAL");
    let catalog_dir = CatalogTempDir::new("bar-list-starts");
    let catalog = ParquetDataCatalog::new(catalog_dir.path(), None, None, None, None);

    for (bar_type, timestamp) in [(first, 3_000), (second, if equal { 3_000 } else { 7_000 })] {
        let bar = make_bar(
            bar_type, "1.0000", "1.0001", "0.9999", "1.0000", 1, timestamp,
        );
        catalog.write_to_parquet(&[bar], None, None, None).unwrap();
    }

    engine.register_catalog(Box::new(catalog), None);
    let params = start_ns.map(|value| params_from_json(json!({"start_ns": value})));

    let command = SubscribeBars::new(
        [second, first],
        Some(client_id),
        Some(venue),
        UUID4::new(),
        UnixNanos::from(53),
        Some(UUID4::new()),
        params,
    );
    engine
        .execute_subscribe(SubscribeCommand::Bars(command.clone()))
        .unwrap();
    let grouped = equal || start_ns.is_some();
    let mut expected = Vec::new();

    for (types, timestamp) in if grouped {
        vec![(vec![second, first], 3_001)]
    } else {
        vec![(vec![second], 7_001), (vec![first], 3_001)]
    } {
        let mut dispatch = command.with_bar_types(types);
        dispatch.params = Some(params_from_json(
            json!({"start_ns": start_ns.unwrap_or(timestamp)}),
        ));
        expected.push(serde_json::to_value(SubscribeCommand::Bars(dispatch)).unwrap());
    }

    let release =
        SubscribeCommand::Bars(command).into_unsubscribe(UUID4::new(), UnixNanos::from(73), None);
    engine.execute_unsubscribe(&release).unwrap();
    expected.push(serde_json::to_value(release).unwrap());

    let actual: Vec<_> = recorder
        .borrow()
        .iter()
        .map(|command| match command {
            DataCommand::Subscribe(command) => serde_json::to_value(command).unwrap(),
            DataCommand::Unsubscribe(command) => serde_json::to_value(command).unwrap(),
            _ => unreachable!(),
        })
        .collect();

    assert_eq!(actual, expected);
}

#[cfg(feature = "streaming")]
mod retained_intent {
    use nautilus_model::data::{DataBatch, NautilusDataType, NautilusRecordType};
    use nautilus_persistence::catalog::{
        session::DataBatchQueryResult,
        traits::{CatalogReader, CatalogWriter, RecordBatch},
        types::{CatalogInstrumentQuery, CatalogQuery},
    };

    use super::*;

    #[derive(Debug)]
    struct UnavailableCatalog;

    impl CatalogReader for UnavailableCatalog {
        fn reset_session(&mut self) {}

        fn instruments(
            &mut self,
            _: &CatalogInstrumentQuery,
        ) -> anyhow::Result<Vec<InstrumentAny>> {
            unreachable!()
        }

        fn query_batch(&mut self, _: &CatalogQuery) -> anyhow::Result<DataBatch> {
            unreachable!()
        }

        fn query_batch_session(
            &mut self,
            _: &CatalogQuery,
            _: Option<usize>,
        ) -> anyhow::Result<DataBatchQueryResult> {
            unreachable!()
        }

        fn query_last_timestamp(
            &mut self,
            _: NautilusDataType,
            _: Option<&str>,
        ) -> anyhow::Result<Option<u64>> {
            anyhow::bail!("catalog start unavailable")
        }
    }
    impl CatalogWriter for UnavailableCatalog {
        fn write_instruments(&mut self, _: &[InstrumentAny]) -> anyhow::Result<()> {
            unreachable!()
        }

        fn write_data(
            &mut self,
            _: &[Data],
            _: Option<UnixNanos>,
            _: Option<UnixNanos>,
            _: Option<Params>,
        ) -> anyhow::Result<()> {
            unreachable!()
        }

        fn write_records(
            &mut self,
            _: NautilusRecordType,
            _: &[RecordBatch],
            _: Option<Params>,
        ) -> anyhow::Result<()> {
            unreachable!()
        }

        fn record_empty_coverage(
            &mut self,
            _: NautilusDataType,
            _: Option<&str>,
            _: UnixNanos,
            _: UnixNanos,
        ) -> anyhow::Result<()> {
            unreachable!()
        }
    }

    #[rstest]
    #[case("Quotes")]
    #[case("Trades")]
    fn test_scalar_catalog_error_retains_intent_without_releasing_peer(
        #[case] family: &str,
        data_engine: Rc<RefCell<DataEngine>>,
        clock: Rc<RefCell<VirtualClock>>,
        cache: Rc<RefCell<Cache>>,
        client_id: ClientId,
        venue: Venue,
    ) {
        let mut engine = data_engine.borrow_mut();
        let recorder = register_recording_client(&mut engine, clock, cache, client_id, venue);
        let mut fields = json!({"instrument_ids": ["AUD/USD.SIM"], "client_id": client_id, "venue": venue,
            "command_id": UUID4::new(), "ts_init": 17, "correlation_id": UUID4::new(),
            "params": {"route": 23, "start_ns": 29}});
        let original: SubscribeCommand = serde_json::from_value(json!({family: fields})).unwrap();
        engine.execute_subscribe(original.clone()).unwrap();
        engine.register_catalog(Box::new(UnavailableCatalog), None);
        fields["command_id"] = json!(UUID4::new());
        fields["params"] = json!({"route": 31});
        let failed: SubscribeCommand = serde_json::from_value(json!({family: fields})).unwrap();
        let error = engine.execute_subscribe(failed.clone()).unwrap_err();
        assert_eq!(error.to_string(), "catalog start unavailable");
        engine
            .execute_unsubscribe(&failed.into_unsubscribe(UUID4::new(), UnixNanos::from(37), None))
            .unwrap();
        assert_eq!(recorder.borrow().len(), 1);

        let active = if family == "Quotes" {
            engine.subscribed_quotes()
        } else {
            engine.subscribed_trades()
        };

        assert_eq!(active, vec![InstrumentId::from("AUD/USD.SIM")]);
        let release = original
            .clone()
            .into_unsubscribe(UUID4::new(), UnixNanos::from(41), None);
        engine.execute_unsubscribe(&release).unwrap();

        let actual: Vec<_> = recorder
            .borrow()
            .iter()
            .map(|command| match command {
                DataCommand::Subscribe(command) => serde_json::to_value(command).unwrap(),
                DataCommand::Unsubscribe(command) => serde_json::to_value(command).unwrap(),
                _ => unreachable!(),
            })
            .collect();

        assert_eq!(
            actual,
            vec![
                serde_json::to_value(original).unwrap(),
                serde_json::to_value(release).unwrap()
            ]
        );
    }
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::complete(false, true)]
#[case::partial_failure(true, true)]
#[case::raw_reply(false, false)]
fn test_batch_writeback_records_only_successful_source_coverage(
    #[case] failed: bool,
    #[case] explicit_completion: bool,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
) {
    use nautilus_common::messages::data::{DataRequestFailed, DataResponseEnvelope};
    use nautilus_model::data::{DataBatch, NautilusDataType};
    use nautilus_persistence::catalog::traits::{CatalogQuery, CatalogReader};
    let _ = stub_msgbus;
    let first = InstrumentId::from("AUD/USD.SIM");
    let second = InstrumentId::from("GBP/USD.SIM");
    let mut engine = DataEngine::new(data_engine_clock_at(3_000), Rc::clone(&cache), None);
    let recorder = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache,
        client_id,
        first.venue,
        None,
        &recorder,
        &mut engine,
    );
    let directory = CatalogTempDir::new("batch-source-writeback");
    let catalog = ParquetDataCatalog::new(directory.path(), None, None, None, None);
    let seed = quote_tick(second, "2.00000", "2.00010", 500);
    catalog
        .write_to_parquet(
            &[seed],
            Some(UnixNanos::from(500)),
            Some(UnixNanos::from(999)),
            None,
        )
        .unwrap();
    engine.register_catalog(Box::new(catalog), Some("history"));
    let root = UUID4::new();
    let (handler, saver) = get_typed_message_saving_handler::<DataResponseEnvelope>(None);
    msgbus::register_response_handler(&root, handler);
    let params = params_from_json(json!({"update_catalog": true, "catalog_name": "history"}));

    let request = RequestQuotes::new(
        [first, second],
        Some(UnixNanos::from(1_000).to_datetime_utc()),
        Some(UnixNanos::from(2_000).to_datetime_utc()),
        None,
        Some(client_id),
        root,
        UnixNanos::from(3_000),
        Some(params),
    );
    engine
        .execute_request(RequestCommand::Quotes(request))
        .unwrap();
    let recorded = recorded_request_quotes(&recorder);
    assert_eq!(recorded.len(), 1);
    let child = &recorded[0];
    assert_eq!(child.instrument_ids, vec![first, second]);
    assert_eq!(child.start, Some(UnixNanos::from(1_000).to_datetime_utc()));
    assert_eq!(child.end, Some(UnixNanos::from(2_000).to_datetime_utc()));
    let quote = quote_tick(first, "1.00000", "1.00010", 1_500);
    let response = DataResponse::Quotes(QuotesResponse::new(
        child.request_id,
        client_id,
        child.instrument_ids.clone(),
        vec![quote],
        Some(UnixNanos::from(1_000)),
        Some(UnixNanos::from(2_000)),
        UnixNanos::from(3_100),
        child.params.clone(),
    ));

    if failed {
        engine.request_failed(DataRequestFailed {
            request_id: child.request_id,
            reason: "source interrupted".to_owned(),
            response: Some(response),
        });
    } else if explicit_completion {
        engine.response_envelope(DataResponseEnvelope::new(response, true, None));
    } else {
        engine.response(response);
    }

    let results = saver.get_messages();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].failure_reason(),
        failed.then_some("source interrupted")
    );

    let Some(DataResponse::Quotes(response)) = results[0].response() else {
        panic!("expected valid quote rows");
    };

    assert_eq!(response.data.as_ref(), &vec![quote]);
    let mut catalog = ParquetDataCatalog::new(directory.path(), None, None, None, None);

    for id in [first, second] {
        let missing = CatalogReader::get_missing_intervals_for_request(
            &mut catalog,
            UnixNanos::from(1_000),
            UnixNanos::from(2_000),
            NautilusDataType::QuoteTick,
            Some(&id.to_string()),
        )
        .unwrap();
        assert_eq!(
            missing,
            if explicit_completion && !failed {
                Vec::new()
            } else {
                vec![(1_000, 2_000)]
            }
        );
    }

    let query = CatalogQuery::new(NautilusDataType::QuoteTick)
        .with_identifiers(Some(vec![first.to_string()]))
        .with_range(Some(UnixNanos::from(1_000)), Some(UnixNanos::from(2_000)));

    let DataBatch::Quote(rows) = CatalogReader::query_batch(&mut catalog, &query).unwrap() else {
        panic!("expected quote batch");
    };

    assert_eq!(
        &rows[..],
        if explicit_completion && !failed {
            std::slice::from_ref(&quote)
        } else {
            &[]
        }
    );
    assert_eq!(engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::single(false, false, false)]
#[case::empty(false, false, true)]
#[case::intermediate(true, false, false)]
#[case::write_error(false, true, false)]
fn test_single_request_writeback_coverage_boundaries(
    #[case] intermediate: bool,
    #[case] write_error: bool,
    #[case] empty: bool,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
) {
    use std::sync::Arc;

    use nautilus_model::data::{BatchView, DataBatch, NautilusDataType};
    use nautilus_persistence::catalog::traits::{CatalogQuery, CatalogReader};

    let _ = stub_msgbus;
    let id = InstrumentId::from("AUD/USD.SIM");
    let mut engine = DataEngine::new(data_engine_clock_at(3000), Rc::clone(&cache), None);
    let recorder = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache,
        client_id,
        id.venue,
        None,
        &recorder,
        &mut engine,
    );
    let directory = CatalogTempDir::new("single-writeback");
    let catalog = ParquetDataCatalog::new(directory.path(), None, None, None, None);

    let (seed_start, seed_end, seed_ts) = if write_error {
        (1500, 2500, 1500)
    } else {
        (500, 999, 500)
    };

    let seed = quote_tick(id, "9.00000", "9.00010", seed_ts);
    catalog
        .write_to_parquet(
            &[seed],
            Some(UnixNanos::from(seed_start)),
            Some(UnixNanos::from(seed_end)),
            None,
        )
        .unwrap();
    engine.register_catalog(Box::new(catalog), Some("history"));
    let root = UUID4::new();
    let (handler, saver) = get_typed_message_saving_handler::<DataResponseEnvelope>(None);
    msgbus::register_response_handler(&root, handler);
    let params = Some(params_from_json(
        json!({"update_catalog": true, "catalog_name": "history", "skip_catalog_data": true}),
    ));
    engine
        .execute_request(RequestCommand::Quotes(RequestQuotes::new(
            id,
            Some(UnixNanos::from(1000).to_datetime_utc()),
            Some(UnixNanos::from(2000).to_datetime_utc()),
            None,
            Some(client_id),
            root,
            UnixNanos::from(3000),
            params,
        )))
        .unwrap();
    let child = recorded_request_quotes(&recorder)[0].clone();
    let quote = quote_tick(id, "1.00000", "1.00010", 1500);
    let visible_len = usize::from(!empty);
    let response = DataResponse::Quotes(QuotesResponse::new(
        child.request_id,
        client_id,
        id,
        BatchView::new(
            Arc::new(vec![
                quote_tick(id, "3.00000", "3.00010", 999),
                quote,
                quote_tick(id, "4.00000", "4.00010", 2001),
            ]),
            1..1 + visible_len,
        ),
        Some(UnixNanos::from(1000)),
        Some(UnixNanos::from(2000)),
        UnixNanos::from(3001),
        child.params.clone(),
    ));

    if intermediate {
        let earlier = DataResponse::Quotes(QuotesResponse::new(
            child.request_id,
            client_id,
            id,
            vec![quote_tick(id, "2.00000", "2.00010", 1250)],
            None,
            None,
            UnixNanos::from(3001),
            child.params,
        ));
        engine.response_envelope(DataResponseEnvelope::new(earlier, false, None));
    }

    engine.response_envelope(DataResponseEnvelope::new(response, true, None));

    let replies = saver.get_messages();
    assert_eq!(replies.len(), 1);
    let final_reply = replies.last().unwrap();
    assert_eq!(final_reply.correlation_id(), &root);
    assert!(final_reply.completed());

    if write_error {
        let reason = final_reply.failure_reason().unwrap();
        assert!(
            reason.starts_with("Catalog write-back failed for Some(\"AUD/USD.SIM\"): "),
            "{reason}"
        );
    } else {
        assert_eq!(final_reply.failure_reason(), None);
    }

    let DataResponse::Quotes(returned) = final_reply.response().unwrap() else {
        panic!("Expected quotes");
    };

    let mut expected = std::slice::from_ref(&quote)[..visible_len].to_vec();
    if intermediate {
        expected.insert(0, quote_tick(id, "2.00000", "2.00010", 1250));
    }

    assert_eq!(returned.data.as_ref(), expected.as_slice());
    assert_eq!(engine.request_pipeline_count(), 0);
    let mut catalog = ParquetDataCatalog::new(directory.path(), None, None, None, None);
    let missing = CatalogReader::get_missing_intervals_for_request(
        &mut catalog,
        UnixNanos::from(1000),
        UnixNanos::from(2000),
        NautilusDataType::QuoteTick,
        Some(&id.to_string()),
    )
    .unwrap();

    let expected_missing = if intermediate {
        vec![(1000, 2000)]
    } else if write_error {
        vec![(1000, 1499)]
    } else {
        Vec::new()
    };

    assert_eq!(missing, expected_missing);
    let query = CatalogQuery::new(NautilusDataType::QuoteTick)
        .with_identifiers(Some(vec![id.to_string()]))
        .with_range(Some(UnixNanos::from(1000)), Some(UnixNanos::from(2000)));

    let DataBatch::Quote(rows) = CatalogReader::query_batch(&mut catalog, &query).unwrap() else {
        panic!("Expected quote batch");
    };

    let (expected_rows, expected_all) = match (write_error, intermediate || empty) {
        (true, _) => (vec![seed], vec![seed]),
        (false, true) => (Vec::new(), vec![seed]),
        (false, false) => (vec![quote], vec![seed, quote]),
    };

    assert_eq!(&rows[..], expected_rows.as_slice());
    let query =
        CatalogQuery::new(NautilusDataType::QuoteTick).with_identifiers(Some(vec![id.to_string()]));

    let DataBatch::Quote(all_rows) = CatalogReader::query_batch(&mut catalog, &query).unwrap()
    else {
        panic!("Expected quote batch");
    };

    assert_eq!(all_rows.as_ref(), expected_all.as_slice());
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::no_catalog(false, None, None, "Catalog write-back requires a registered catalog")]
#[case::limit(
    true,
    Some(2),
    None,
    "Catalog write-back is unsupported for requests with a record limit"
)]
#[case::schema(
    true,
    None,
    Some("mbp-1"),
    "Catalog write-back is unsupported for requests with a source schema"
)]
fn test_catalog_update_rejects_request_before_fetching(
    #[case] catalog: bool,
    #[case] limit: Option<usize>,
    #[case] schema: Option<&str>,
    #[case] reason: &str,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let id = InstrumentId::from("AUD/USD.SIM");
    let mut engine = DataEngine::new(data_engine_clock_at(3000), Rc::clone(&cache), None);
    let recorder = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache,
        client_id,
        id.venue,
        None,
        &recorder,
        &mut engine,
    );
    let _directory = catalog.then(|| register_empty_catalog(&mut engine, "rejected-write-back"));
    let root = UUID4::new();
    let (handler, saver) = get_typed_message_saving_handler::<DataResponseEnvelope>(None);
    msgbus::register_response_handler(&root, handler);
    let mut params = json!({"update_catalog": true});

    if let Some(schema) = schema {
        params["schema"] = json!(schema);
    }

    let error = engine
        .execute_request(RequestCommand::Quotes(RequestQuotes::new(
            id,
            Some(UnixNanos::from(1000).to_datetime_utc()),
            Some(UnixNanos::from(2000).to_datetime_utc()),
            limit.map(|value| NonZeroUsize::new(value).unwrap()),
            Some(client_id),
            root,
            UnixNanos::from(3000),
            Some(params_from_json(params)),
        )))
        .unwrap_err();
    let replies = saver.get_messages();

    assert_eq!(error.to_string(), reason);
    assert!(recorder.borrow().is_empty());
    assert_eq!(replies.len(), 1);
    assert!(replies[0].completed());
    assert_eq!(replies[0].failure_reason(), Some(reason));
    assert!(replies[0].response().is_none());
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::default(None)]
#[case::explicit_default(Some("mbp-1"))]
#[case::sampled(Some("bbo-1s"))]
#[case::trade_triggered(Some("tbbo"))]
fn test_quote_schema_request_bypasses_canonical_catalog(
    #[case] schema: Option<&str>,
    #[values(false, true)] multiple: bool,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
) {
    let _ = stub_msgbus;
    let mut ids = vec![
        InstrumentId::from("AUD/USD.SIM"),
        InstrumentId::from("GBP/USD.SIM"),
    ];

    if !multiple {
        ids.truncate(1);
    }

    let mut engine = DataEngine::new(data_engine_clock_at(3000), Rc::clone(&cache), None);
    let recorder = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        Rc::clone(&cache),
        client_id,
        ids[0].venue,
        None,
        &recorder,
        &mut engine,
    );
    let directory = CatalogTempDir::new("quote-schema-read");
    let catalog = ParquetDataCatalog::new(directory.path(), None, None, None, None);
    let canonical = ids
        .iter()
        .map(|id| quote_tick(*id, "9.00000", "9.00010", 1500))
        .collect::<Vec<_>>();

    for row in &canonical {
        catalog
            .write_to_parquet(
                std::slice::from_ref(row),
                Some(UnixNanos::from(1000)),
                Some(UnixNanos::from(2000)),
                None,
            )
            .unwrap();
    }

    engine.register_catalog(Box::new(catalog), Some("history"));
    let root = UUID4::new();
    let (handler, saver) = get_typed_message_saving_handler::<DataResponseEnvelope>(None);
    msgbus::register_response_handler(&root, handler);
    let params = schema.map(|schema| params_from_json(json!({"schema": schema})));

    let request = RequestQuotes::new(
        ids.clone(),
        Some(UnixNanos::from(1000).to_datetime_utc()),
        Some(UnixNanos::from(2000).to_datetime_utc()),
        None,
        Some(client_id),
        root,
        UnixNanos::from(3000),
        params.clone(),
    );
    engine
        .execute_request(RequestCommand::Quotes(request))
        .unwrap();
    let recorded = recorded_request_quotes(&recorder);

    let expected = if schema.is_some() {
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].instrument_ids, ids);
        assert_eq!(recorded[0].params, params);
        let rows = ids
            .iter()
            .map(|id| quote_tick(*id, "1.00000", "1.00010", 1500))
            .collect::<Vec<_>>();

        let response = QuotesResponse::new(
            recorded[0].request_id,
            client_id,
            ids.clone(),
            rows.clone(),
            Some(UnixNanos::from(1000)),
            Some(UnixNanos::from(2000)),
            UnixNanos::from(3001),
            recorded[0].params.clone(),
        );
        engine.response_envelope(DataResponseEnvelope::new(
            DataResponse::Quotes(response),
            true,
            None,
        ));
        rows
    } else {
        assert_eq!(recorded.len(), 0);
        canonical
    };

    let replies = saver.get_messages();

    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].correlation_id(), &root);
    assert!(replies[0].completed());
    assert_eq!(replies[0].failure_reason(), None);

    let Some(DataResponse::Quotes(response)) = replies[0].response() else {
        panic!("expected quotes");
    };

    assert_eq!(response.instrument_ids, ids);
    assert_eq!(response.data.as_ref(), expected.as_slice());
    assert_eq!(response.start, Some(UnixNanos::from(1000)));
    assert_eq!(response.end, Some(UnixNanos::from(2000)));
    assert_eq!(engine.request_pipeline_count(), 0);

    for (id, row) in ids.iter().zip(expected) {
        assert_eq!(cache.borrow().quotes(id), Some(vec![row]));
    }
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::requested_header(false)]
#[case::concrete_header(true)]
fn test_remapped_response_does_not_certify_concrete_contract_coverage(
    #[case] concrete_header: bool,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
) {
    use nautilus_model::data::{DataBatch, NautilusDataType};
    use nautilus_persistence::catalog::traits::{CatalogQuery, CatalogReader};

    let _ = stub_msgbus;
    let parent = InstrumentId::from("ES.c.0.GLBX");
    let first = InstrumentId::from("ESM4.GLBX");
    let second = InstrumentId::from("ESU4.GLBX");
    let mut engine = DataEngine::new(data_engine_clock_at(4000), Rc::clone(&cache), None);
    let recorder = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache,
        client_id,
        parent.venue,
        None,
        &recorder,
        &mut engine,
    );
    let directory = CatalogTempDir::new("remapped-source-coverage");
    let catalog = ParquetDataCatalog::new(directory.path(), None, None, None, None);
    let seeds = [
        quote_tick(first, "3.00000", "3.00010", 500),
        quote_tick(second, "4.00000", "4.00010", 500),
    ];

    for seed in seeds {
        catalog
            .write_to_parquet(
                &[seed],
                Some(UnixNanos::from(500)),
                Some(UnixNanos::from(999)),
                None,
            )
            .unwrap();
    }

    engine.register_catalog(Box::new(catalog), Some("history"));
    let root = UUID4::new();
    let (handler, saver) = get_typed_message_saving_handler::<DataResponseEnvelope>(None);
    msgbus::register_response_handler(&root, handler);
    let params = params_from_json(json!({"update_catalog": true, "catalog_name": "history"}));

    let request = RequestQuotes::new(
        parent,
        Some(UnixNanos::from(1000).to_datetime_utc()),
        Some(UnixNanos::from(3000).to_datetime_utc()),
        None,
        Some(client_id),
        root,
        UnixNanos::from(4000),
        Some(params),
    );
    engine
        .execute_request(RequestCommand::Quotes(request))
        .unwrap();
    let recorded = recorded_request_quotes(&recorder);
    let child = &recorded[0];
    let rows = vec![
        quote_tick(first, "1.00000", "1.00010", 1500),
        quote_tick(second, "2.00000", "2.00010", 2500),
    ];

    let header = if concrete_header {
        vec![first, second]
    } else {
        vec![parent]
    };

    let response = QuotesResponse::new(
        child.request_id,
        client_id,
        header,
        rows.clone(),
        Some(UnixNanos::from(1000)),
        Some(UnixNanos::from(3000)),
        UnixNanos::from(4100),
        child.params.clone(),
    );
    engine.response_envelope(DataResponseEnvelope::new(
        DataResponse::Quotes(response),
        true,
        None,
    ));
    let replies = saver.get_messages();
    let reply = &replies[0];

    assert_eq!(recorded.len(), 1);
    assert_eq!(replies.len(), 1);
    assert_eq!(reply.correlation_id(), &root);
    assert!(reply.completed());
    assert_eq!(
        reply.failure_reason(),
        Some(
            "Catalog write-back failed: identifier coverage is unverified, \
             identifier=Some(\"ESM4.GLBX\"); \
             Catalog write-back failed: identifier coverage is unverified, \
             identifier=Some(\"ESU4.GLBX\")"
        )
    );

    let DataResponse::Quotes(response) = reply.response().unwrap() else {
        panic!("expected quotes");
    };

    assert_eq!(response.data.as_ref(), rows.as_slice());
    assert_eq!(response.instrument_ids, vec![parent]);
    let mut catalog = ParquetDataCatalog::new(directory.path(), None, None, None, None);

    for (id, seed) in [first, second].into_iter().zip(seeds) {
        let missing = CatalogReader::get_missing_intervals_for_request(
            &mut catalog,
            UnixNanos::from(1000),
            UnixNanos::from(3000),
            NautilusDataType::QuoteTick,
            Some(&id.to_string()),
        )
        .unwrap();
        let query = CatalogQuery::new(NautilusDataType::QuoteTick)
            .with_identifiers(Some(vec![id.to_string()]));

        let DataBatch::Quote(stored) = CatalogReader::query_batch(&mut catalog, &query).unwrap()
        else {
            panic!("expected quote batch");
        };

        assert_eq!(missing, vec![(1000, 3000)]);
        assert_eq!(stored.as_ref(), &[seed]);
    }
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_instrument_writeback_serves_next_request_from_catalog(
    audusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    let instrument = InstrumentAny::CurrencyPair(audusd_sim);
    let id = instrument.id();
    let mut engine = DataEngine::new(data_engine_clock_at(3000), Rc::clone(&cache), None);
    let recorder = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache,
        client_id,
        venue,
        None,
        &recorder,
        &mut engine,
    );
    let _directory = register_empty_catalog(&mut engine, "instrument-writeback");

    let mut request = RequestInstrument::new(
        id,
        None,
        None,
        Some(client_id),
        UUID4::new(),
        UnixNanos::from(3000),
        Some(params_from_json(
            json!({"update_catalog": true, "force_instrument_update": true}),
        )),
    );
    engine
        .execute_request(RequestCommand::Instrument(request.clone()))
        .unwrap();

    let response = InstrumentResponse::new(
        request.request_id,
        client_id,
        id,
        instrument.clone(),
        None,
        None,
        UnixNanos::from(3001),
        None,
    );
    engine.response_envelope(DataResponseEnvelope::new(
        DataResponse::Instrument(Box::new(response)),
        true,
        None,
    ));
    request.request_id = UUID4::new();
    request.params = None;
    let (handler, saver) = get_response_saving_handler::<InstrumentResponse>(None);
    msgbus::register_response_handler(&request.request_id, handler);

    engine
        .execute_request(RequestCommand::Instrument(request))
        .unwrap();

    let received = saver.get_messages();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].data, instrument);
    assert_eq!(recorded_request_instrument(&recorder).len(), 1);
    assert_eq!(engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
fn test_instrument_list_catalog_merge_preserves_current_client_definition(
    mut audusd_sim: CurrencyPair,
    mut gbpusd_sim: CurrencyPair,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    client_id: ClientId,
    venue: Venue,
) {
    let _ = stub_msgbus;
    audusd_sim.ts_init = UnixNanos::from(1000);
    gbpusd_sim.ts_init = UnixNanos::from(3000);
    let cached = InstrumentAny::CurrencyPair(audusd_sim);
    let fetched = InstrumentAny::CurrencyPair(gbpusd_sim);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    advance_clock_to(&clock, 4000);
    let mut engine = DataEngine::new(Rc::clone(&clock), Rc::clone(&cache), None);
    let recorder =
        register_time_range_recorder(&mut engine, clock, Rc::clone(&cache), client_id, venue);
    let _directory = register_instrument_catalog_with_instruments(
        &mut engine,
        "definition-list-window",
        vec![cached.clone()],
    );
    let root = UUID4::new();
    let (handler, saver) = get_typed_message_saving_handler::<DataResponseEnvelope>(None);
    msgbus::register_response_handler(&root, handler);
    engine
        .execute_request(RequestCommand::Instrument(RequestInstrument::new(
            [cached.id(), fetched.id()],
            Some(UnixNanos::from(500).to_datetime_utc()),
            Some(UnixNanos::from(2000).to_datetime_utc()),
            Some(client_id),
            root,
            UnixNanos::from(4000),
            None,
        )))
        .unwrap();
    let requests = recorded_request_instrument(&recorder);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].instrument_ids, vec![fetched.id()]);
    engine.response(DataResponse::Instrument(Box::new(InstrumentResponse::new(
        requests[0].request_id,
        client_id,
        fetched.id(),
        fetched.clone(),
        Some(UnixNanos::from(500)),
        Some(UnixNanos::from(2000)),
        UnixNanos::from(4001),
        None,
    ))));
    let responses = saver.get_messages();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].correlation_id(), &root);
    assert!(responses[0].completed());
    assert_eq!(responses[0].failure_reason(), None);

    let Some(DataResponse::Instruments(response)) = responses[0].response() else {
        panic!("Expected instrument definitions");
    };

    assert_eq!(response.data, vec![cached.clone(), fetched.clone()]);
    assert_eq!(response.start, Some(UnixNanos::from(500)));
    assert_eq!(response.end, Some(UnixNanos::from(2000)));
    assert_eq!(cache.borrow().instrument(&cached.id()), Some(&cached));
    assert_eq!(cache.borrow().instrument(&fetched.id()), Some(&fetched));
    assert_eq!(engine.request_pipeline_count(), 0);
}

#[cfg(feature = "streaming")]
#[rstest]
#[case::default(None)]
#[case::explicit_default(Some("trades"))]
#[case::top_of_book(Some("mbp-1"))]
#[case::trade_triggered(Some("tbbo"))]
#[case::consolidated(Some("cmbp-1"))]
#[case::consolidated_trade_triggered(Some("tcbbo"))]
fn test_trade_schema_preserves_source_rows_and_catalog_isolation(
    #[case] schema: Option<&str>,
    #[values(false, true)] multiple: bool,
    stub_msgbus: Rc<RefCell<MessageBus>>,
    cache: Rc<RefCell<Cache>>,
    client_id: ClientId,
) {
    use nautilus_model::data::{DataBatch, NautilusDataType};
    use nautilus_persistence::catalog::traits::{CatalogQuery, CatalogReader, CatalogWriter};

    let _ = stub_msgbus;
    let mut ids = vec![
        InstrumentId::from("AUD/USD.SIM"),
        InstrumentId::from("GBP/USD.SIM"),
    ];

    if !multiple {
        ids.truncate(1);
    }

    let mut engine = DataEngine::new(data_engine_clock_at(4000), Rc::clone(&cache), None);
    let recorder = Rc::new(RefCell::new(Vec::new()));
    register_mock_client(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache,
        client_id,
        ids[0].venue,
        None,
        &recorder,
        &mut engine,
    );
    let directory = CatalogTempDir::new("trade-schema-isolation");
    let mut catalog = ParquetDataCatalog::new(directory.path(), None, None, None, None);
    let canonical = ids
        .iter()
        .map(|id| make_trade(*id, "9.00000", 3, &format!("sequence-{id}"), 1500))
        .collect::<Vec<_>>();
    let seed = canonical
        .iter()
        .copied()
        .map(Data::Trade)
        .collect::<Vec<_>>();
    CatalogWriter::write_data(
        &mut catalog,
        &seed,
        Some(UnixNanos::from(1000)),
        Some(UnixNanos::from(2000)),
        None,
    )
    .unwrap();
    engine.register_catalog(Box::new(catalog), Some("history"));
    let root = UUID4::new();
    let (handler, saver) = get_typed_message_saving_handler::<DataResponseEnvelope>(None);
    msgbus::register_response_handler(&root, handler);
    // Write-back is rejected for a source schema, so only the canonical request asks for it
    let mut params = json!({"catalog_name": "history"});
    match schema {
        Some(schema) => params["schema"] = json!(schema),
        None => params["update_catalog"] = json!(true),
    }

    let params = params_from_json(params);

    let request = RequestTrades::new(
        ids.clone(),
        Some(UnixNanos::from(1000).to_datetime_utc()),
        Some(UnixNanos::from(3000).to_datetime_utc()),
        None,
        Some(client_id),
        root,
        UnixNanos::from(4000),
        Some(params.clone()),
    );
    engine
        .execute_request(RequestCommand::Trades(request))
        .unwrap();
    let recorded = recorded_request_trades(&recorder);
    assert_eq!(recorded.len(), 1);
    let child = &recorded[0];
    let actual_start = child.start.unwrap().as_nanosecond() as u64;
    let source_rows = [1500, 2500]
        .into_iter()
        .filter(|ts| *ts >= actual_start)
        .flat_map(|ts| {
            ids.iter()
                .map(move |id| make_trade(*id, "1.00000", 7, &format!("hash-{id}-{ts}"), ts))
        })
        .collect::<Vec<_>>();

    let response = TradesResponse::new(
        child.request_id,
        client_id,
        ids.clone(),
        source_rows,
        Some(UnixNanos::from(actual_start)),
        Some(UnixNanos::from(3000)),
        UnixNanos::from(4001),
        child.params.clone(),
    );
    engine.response_envelope(DataResponseEnvelope::new(
        DataResponse::Trades(response),
        true,
        None,
    ));
    let canonical_ref = &canonical;

    let expected_rows = [1500, 2500]
        .into_iter()
        .flat_map(|ts| {
            ids.iter().enumerate().map(move |(index, id)| {
                if schema.is_none() && ts == 1500 {
                    canonical_ref[index]
                } else {
                    make_trade(*id, "1.00000", 7, &format!("hash-{id}-{ts}"), ts)
                }
            })
        })
        .collect::<Vec<_>>();

    let replies = saver.get_messages();
    assert_eq!(actual_start, if schema.is_some() { 1000 } else { 2001 });
    assert_eq!(child.end, Some(UnixNanos::from(3000).to_datetime_utc()));
    assert_eq!(child.instrument_ids, ids);
    assert_eq!(child.params, Some(params));
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].correlation_id(), &root);
    assert!(replies[0].completed());
    assert_eq!(replies[0].failure_reason(), None);

    let Some(DataResponse::Trades(response)) = replies[0].response() else {
        panic!("Expected trades");
    };

    assert_eq!(response.instrument_ids, ids);
    assert_eq!(response.data.as_ref(), expected_rows.as_slice());
    assert_eq!(response.start, Some(UnixNanos::from(1000)));
    assert_eq!(response.end, Some(UnixNanos::from(3000)));
    assert_eq!(engine.request_pipeline_count(), 0);
    let mut catalog = ParquetDataCatalog::new(directory.path(), None, None, None, None);

    for (id, seed) in ids.iter().zip(canonical) {
        let query = CatalogQuery::new(NautilusDataType::TradeTick)
            .with_identifiers(Some(vec![id.to_string()]));

        let DataBatch::Trade(stored) = CatalogReader::query_batch(&mut catalog, &query).unwrap()
        else {
            panic!("Expected stored trades");
        };

        let missing = CatalogReader::get_missing_intervals_for_request(
            &mut catalog,
            UnixNanos::from(1000),
            UnixNanos::from(3000),
            NautilusDataType::TradeTick,
            Some(&id.to_string()),
        )
        .unwrap();

        let expected_stored = if schema.is_some() {
            vec![seed]
        } else {
            vec![
                seed,
                make_trade(*id, "1.00000", 7, &format!("hash-{id}-2500"), 2500),
            ]
        };

        assert_eq!(stored.as_ref(), expected_stored.as_slice());
        assert_eq!(
            missing,
            if schema.is_some() {
                vec![(2001, 3000)]
            } else {
                Vec::new()
            }
        );
    }
}

/// A trades request carrying `quotes` read through one counting catalog: its catalog leg opens
/// two sessions when the request is sent and the merge reads them as it sends the answer.
#[cfg(feature = "streaming")]
mod quotes_sessions {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Context;
    use nautilus_model::data::{DataBatch, NautilusDataType, NautilusRecordType};
    use nautilus_persistence::{
        backend::parquet::catalog::QUERY_OPTIMIZE_FILE_LOADING,
        catalog::{
            session::{DataBatchQuery, DataBatchQueryResult},
            traits::{CatalogReader, CatalogWriter, RecordBatch},
            types::{CatalogInstrumentQuery, CatalogQuery},
        },
    };

    use super::*;

    /// 2024-01-08T10:00:00Z.
    const T0: u64 = 1_704_708_000_000_000_000;
    const NOW: u64 = T0 + 3_600 * NANOSECONDS_IN_SECOND;
    /// Every request's window: ninety seconds from `T0`.
    const WINDOW: (u64, u64) = (T0, T0 + 90 * NANOSECONDS_IN_SECOND);
    /// The rows of one page the counting catalog hands out.
    const PAGE_ROWS: usize = 1_000;
    /// The most rows one part of a merged answer carries.
    const RUN_CAP: usize = 10_000;
    /// The trades of `WINDOW` the catalog lacks in the tests that wait on a client leg.
    const CLIENT_PART: (u64, u64) = (T0 + 80 * NANOSECONDS_IN_SECOND, WINDOW.1);

    /// `T0` plus `seconds`.
    const fn at_second(seconds: u64) -> u64 {
        T0 + seconds * NANOSECONDS_IN_SECOND
    }

    /// `rows` stamps, one every `step` nanoseconds from `first`.
    fn every(first: u64, step: u64, rows: u64) -> Vec<u64> {
        (0..rows).map(|row| first + row * step).collect()
    }

    /// The counting catalog's reads and its requesters' deliveries, shared with its sessions.
    #[derive(Clone, Debug, Default)]
    struct Counts {
        /// Rows its sessions handed out, page by page.
        yielded: Arc<AtomicUsize>,
        /// Quotes and trades the requesters' handlers were given, part by part.
        delivered: Arc<AtomicUsize>,
        /// The most rows yielded and not yet delivered when a page was handed out.
        peak: Arc<AtomicUsize>,
        /// Sessions dropped.
        dropped: Arc<AtomicUsize>,
    }

    impl Counts {
        fn yield_page(&self, rows: usize) {
            let yielded = self.yielded.fetch_add(rows, Ordering::Relaxed) + rows;
            let held = yielded.saturating_sub(self.delivered.load(Ordering::Relaxed));
            self.peak.fetch_max(held, Ordering::Relaxed);
        }

        fn deliver(&self, rows: usize) {
            self.delivered.fetch_add(rows, Ordering::Relaxed);
        }

        fn peak(&self) -> usize {
            self.peak.load(Ordering::Relaxed)
        }

        fn dropped(&self) -> usize {
            self.dropped.load(Ordering::Relaxed)
        }
    }

    /// One feed of the counting catalog.
    #[derive(Clone, Debug, Default)]
    struct Feed {
        /// Its rows' stamps, in order.
        stamps: Vec<u64>,
        /// The intervals of a request's window it does not cover.
        missing: Vec<(u64, u64)>,
        /// The rows its session hands out before the page after them fails.
        fails_after: Option<usize>,
    }

    fn feed(stamps: Vec<u64>) -> Feed {
        Feed {
            stamps,
            ..Feed::default()
        }
    }

    /// A catalog that answers trades and quotes only through per-file sessions, each handing out
    /// made-up rows of the instrument it is asked in pages of `PAGE_ROWS`, counted.
    #[derive(Debug)]
    struct CountingCatalog {
        quotes: Feed,
        trades: Feed,
        counts: Counts,
    }

    impl CountingCatalog {
        fn feed(&self, data_type: &NautilusDataType) -> anyhow::Result<(&'static str, &Feed)> {
            match data_type {
                NautilusDataType::QuoteTick => Ok(("quotes", &self.quotes)),
                NautilusDataType::TradeTick => Ok(("trades", &self.trades)),
                other => anyhow::bail!("the counting catalog holds no {other}"),
            }
        }
    }

    impl CatalogReader for CountingCatalog {
        fn reset_session(&mut self) {}

        fn instruments(
            &mut self,
            _: &CatalogInstrumentQuery,
        ) -> anyhow::Result<Vec<InstrumentAny>> {
            Ok(Vec::new())
        }

        fn query_batch(&mut self, query: &CatalogQuery) -> anyhow::Result<DataBatch> {
            anyhow::bail!(
                "the counting catalog answers {} only per file",
                query.data_type
            )
        }

        fn query_batch_session(
            &mut self,
            query: &CatalogQuery,
            _: Option<usize>,
        ) -> anyhow::Result<DataBatchQueryResult> {
            let per_file = query
                .params
                .as_ref()
                .and_then(|params| params.get_bool(QUERY_OPTIMIZE_FILE_LOADING));
            anyhow::ensure!(
                per_file == Some(false),
                "a catalog leg's session must list its files when it is opened, received \
                 {per_file:?}"
            );
            let identifier = query
                .identifiers
                .as_deref()
                .and_then(<[String]>::first)
                .context("a catalog leg names its instrument")?;
            let (kind, feed) = self.feed(&query.data_type)?;

            Ok(Box::new(CountingSession {
                instrument_id: InstrumentId::from(identifier.as_str()),
                kind,
                stamps: feed.stamps.clone(),
                fails_after: feed.fails_after,
                next: 0,
                counts: self.counts.clone(),
            }))
        }

        fn get_missing_intervals_for_request(
            &mut self,
            _: UnixNanos,
            _: UnixNanos,
            data_type: NautilusDataType,
            _: Option<&str>,
        ) -> anyhow::Result<Vec<(u64, u64)>> {
            Ok(self.feed(&data_type)?.1.missing.clone())
        }
    }

    impl CatalogWriter for CountingCatalog {
        fn write_instruments(&mut self, _: &[InstrumentAny]) -> anyhow::Result<()> {
            anyhow::bail!("the counting catalog is read only")
        }

        fn write_data(
            &mut self,
            _: &[Data],
            _: Option<UnixNanos>,
            _: Option<UnixNanos>,
            _: Option<Params>,
        ) -> anyhow::Result<()> {
            anyhow::bail!("the counting catalog is read only")
        }

        fn write_records(
            &mut self,
            _: NautilusRecordType,
            _: &[RecordBatch],
            _: Option<Params>,
        ) -> anyhow::Result<()> {
            anyhow::bail!("the counting catalog is read only")
        }

        fn record_empty_coverage(
            &mut self,
            _: NautilusDataType,
            _: Option<&str>,
            _: UnixNanos,
            _: UnixNanos,
        ) -> anyhow::Result<()> {
            anyhow::bail!("the counting catalog is read only")
        }
    }

    /// One session of the counting catalog: its feed's rows, made as each page is handed out.
    struct CountingSession {
        instrument_id: InstrumentId,
        kind: &'static str,
        stamps: Vec<u64>,
        fails_after: Option<usize>,
        next: usize,
        counts: Counts,
    }

    impl DataBatchQuery for CountingSession {
        fn next_batch(&mut self) -> anyhow::Result<Option<DataBatch>> {
            if let Some(rows) = self.fails_after.filter(|rows| self.next >= *rows) {
                anyhow::bail!(
                    "the made-up {} page after row {rows} is unreadable",
                    self.kind
                );
            }
            let end = (self.next + PAGE_ROWS)
                .min(self.stamps.len())
                .min(self.fails_after.unwrap_or(usize::MAX));
            let rows = self.next..end;

            if rows.is_empty() {
                return Ok(None);
            }
            self.next = end;
            self.counts.yield_page(rows.len());

            let instrument_id = self.instrument_id;
            let page = rows.map(|row| (row, self.stamps[row]));
            Ok(Some(if self.kind == "quotes" {
                DataBatch::Quote(
                    page.map(|(_, ts)| make_quote(instrument_id, "1.0000", "1.0002", ts))
                        .collect::<Vec<_>>()
                        .into(),
                )
            } else {
                DataBatch::Trade(
                    page.map(|(row, ts)| {
                        make_trade(instrument_id, "1.0001", 1, &format!("t{row}"), ts)
                    })
                    .collect::<Vec<_>>()
                    .into(),
                )
            }))
        }
    }

    impl Drop for CountingSession {
        fn drop(&mut self) {
            self.counts.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One envelope a requester was sent: its quotes or trades by `ts_init`, its bars by
    /// `ts_event`, and, when it completed, its end with its reason.
    #[derive(Clone, Debug, PartialEq)]
    enum Sent {
        Quotes(Vec<u64>),
        Trades(Vec<u64>),
        Bars(Vec<u64>),
        End(Option<String>),
    }

    /// The identity a quotes or trades part carries.
    #[derive(Clone, Debug, PartialEq)]
    struct Identity {
        correlation_id: UUID4,
        client_id: ClientId,
        instrument_ids: Vec<InstrumentId>,
        window: (Option<UnixNanos>, Option<UnixNanos>),
        ts_init: UnixNanos,
        params: Option<Params>,
    }

    /// What a requester was sent, and each identity its parts carried, in order, a repeat of
    /// the one before left out.
    #[derive(Debug, Default)]
    struct Tapped {
        sent: Vec<Sent>,
        identities: Vec<Identity>,
    }

    impl Tapped {
        fn record(&mut self, sent: Sent, identity: Option<Identity>) {
            self.sent.push(sent);

            if let Some(identity) = identity
                && self.identities.last() != Some(&identity)
            {
                self.identities.push(identity);
            }
        }
    }

    /// Records what the requester of `request_id` is sent, counting each part's quotes and trades
    /// as delivered.
    fn tap(request_id: UUID4, counts: &Counts) -> Rc<RefCell<Tapped>> {
        let tapped = Rc::new(RefCell::new(Tapped::default()));
        let recorded = Rc::clone(&tapped);
        let counts = counts.clone();
        let handler = TypedHandler::from(move |envelope: &DataResponseEnvelope| {
            let mut tapped = recorded.borrow_mut();
            match envelope.response() {
                Some(DataResponse::Quotes(r)) => {
                    counts.deliver(r.data.len());
                    let identity = Identity {
                        correlation_id: r.correlation_id,
                        client_id: r.client_id,
                        instrument_ids: r.instrument_ids.clone(),
                        window: (r.start, r.end),
                        ts_init: r.ts_init,
                        params: r.params.clone(),
                    };
                    let stamps = r.data.iter().map(|quote| quote.ts_init.as_u64()).collect();
                    tapped.record(Sent::Quotes(stamps), Some(identity));
                }
                Some(DataResponse::Trades(r)) => {
                    counts.deliver(r.data.len());
                    let identity = Identity {
                        correlation_id: r.correlation_id,
                        client_id: r.client_id,
                        instrument_ids: r.instrument_ids.clone(),
                        window: (r.start, r.end),
                        ts_init: r.ts_init,
                        params: r.params.clone(),
                    };
                    let stamps = r.data.iter().map(|trade| trade.ts_init.as_u64()).collect();
                    tapped.record(Sent::Trades(stamps), Some(identity));
                }
                Some(DataResponse::Bars(r)) => {
                    let stamps = r.data.iter().map(|bar| bar.ts_event.as_u64()).collect();
                    tapped.record(Sent::Bars(stamps), None);
                }
                _ => {}
            }

            if envelope.completed() {
                let reason = envelope.failure_reason().map(str::to_owned);
                tapped.record(Sent::End(reason), None);
            }
        });
        msgbus::register_response_handler(&request_id, handler);
        tapped
    }

    /// A quotes part holding the quotes at `stamps`.
    fn quotes_of(stamps: &[u64]) -> Sent {
        Sent::Quotes(stamps.to_vec())
    }

    /// A trades part holding the trades at `stamps`.
    fn trades_of(stamps: &[u64]) -> Sent {
        Sent::Trades(stamps.to_vec())
    }

    /// An engine at `NOW` holding `instruments`, with a recording mock client and a counting
    /// catalog of `quotes` and `trades`.
    struct Fixture {
        engine: DataEngine,
        recorder: Rc<RefCell<Vec<DataCommand>>>,
        client_id: ClientId,
        venue: Venue,
        counts: Counts,
    }

    impl Fixture {
        fn new(
            instruments: Vec<CurrencyPair>,
            client_id: ClientId,
            venue: Venue,
            quotes: Feed,
            trades: Feed,
        ) -> Self {
            let clock = Rc::new(RefCell::new(VirtualClock::new()));
            let _ = clock.borrow_mut().advance_time(UnixNanos::from(NOW), true);
            let cache: Rc<RefCell<Cache>> = Rc::new(RefCell::new(Cache::default()));
            for instrument in instruments {
                cache
                    .borrow_mut()
                    .add_instrument(InstrumentAny::CurrencyPair(instrument))
                    .unwrap();
            }
            let mut engine = DataEngine::new(
                Rc::clone(&clock) as Rc<RefCell<dyn Clock>>,
                Rc::clone(&cache),
                None,
            );
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
            let counts = Counts::default();
            let catalog = CountingCatalog {
                quotes,
                trades,
                counts: counts.clone(),
            };
            engine.register_catalog(Box::new(catalog), None);

            Self {
                engine,
                recorder,
                client_id,
                venue,
                counts,
            }
        }

        /// Sends a trades request carrying `quotes` for `instrument_id` over `WINDOW` with
        /// `params`, its requester tapped first; returns its ID and its tap.
        fn request(
            &mut self,
            instrument_id: InstrumentId,
            mut params: Value,
        ) -> (UUID4, Rc<RefCell<Tapped>>) {
            params["quotes"] = json!(true);
            let at = |ns: u64| Some(UnixNanos::from(ns).to_datetime_utc());
            let request = RequestTrades::new(
                vec![instrument_id],
                at(WINDOW.0),
                at(WINDOW.1),
                None,
                Some(self.client_id),
                UUID4::new(),
                UnixNanos::default(),
                Some(params_from_json(params)),
            );
            let request_id = request.request_id;
            let tapped = tap(request_id, &self.counts);
            self.engine
                .execute_request(RequestCommand::Trades(request))
                .unwrap();
            (request_id, tapped)
        }

        /// The legs the client was asked, in order: each one's request ID and instrument, and
        /// whether it asks quotes.
        fn client_legs(&self) -> Vec<(UUID4, InstrumentId, bool)> {
            self.recorder
                .borrow()
                .iter()
                .filter_map(|command| match command {
                    DataCommand::Request(RequestCommand::Trades(cmd)) => {
                        Some((cmd.request_id, cmd.instrument_ids[0], false))
                    }
                    DataCommand::Request(RequestCommand::Quotes(cmd)) => {
                        Some((cmd.request_id, cmd.instrument_ids[0], true))
                    }
                    _ => None,
                })
                .collect()
        }

        /// Answers the client leg `leg` in one completed envelope with rows at `stamps`.
        fn answer_leg(
            &mut self,
            (leg_id, instrument_id, quotes): (UUID4, InstrumentId, bool),
            stamps: &[u64],
        ) {
            let ts_init = UnixNanos::from(NOW);
            let response = if quotes {
                let quotes: Vec<_> = stamps
                    .iter()
                    .map(|&ts| make_quote(instrument_id, "1.0000", "1.0002", ts))
                    .collect();
                DataResponse::Quotes(QuotesResponse::new(
                    leg_id,
                    self.client_id,
                    instrument_id,
                    quotes,
                    None,
                    None,
                    ts_init,
                    None,
                ))
            } else {
                let trades: Vec<_> = stamps
                    .iter()
                    .map(|&ts| make_trade(instrument_id, "1.0001", 1, "c1", ts))
                    .collect();
                DataResponse::Trades(TradesResponse::new(
                    leg_id,
                    self.client_id,
                    instrument_id,
                    trades,
                    None,
                    None,
                    ts_init,
                    None,
                ))
            };
            self.engine
                .response_envelope(DataResponseEnvelope::new(response, true, None));
        }
    }

    /// Two hundred thousand quotes and as many trades, one after the other: the requester gets
    /// each row as its own part, in the generated order, under one identity, and one terminal.
    /// Rows yielded and not yet delivered never exceed the run cap plus a page of each feed
    /// whenever a page is handed out, where a session held whole would hold 400,000: two pages,
    /// and the row of each the cutter has not yet sent, is all it reaches.
    #[rstest]
    fn test_quotes_request_reads_its_catalog_a_page_at_a_time_and_never_holds_a_session(
        audusd_sim: CurrencyPair,
        stub_msgbus: Rc<RefCell<MessageBus>>,
        client_id: ClientId,
        venue: Venue,
    ) {
        let _ = stub_msgbus;
        let instrument_id = audusd_sim.id;
        let rows = 200_000;
        let mut fixture = Fixture::new(
            vec![audusd_sim],
            client_id,
            venue,
            feed(every(T0, 2, rows)),
            feed(every(T0 + 1, 2, rows)),
        );

        let (request_id, tapped) = fixture.request(instrument_id, json!({}));

        let mut expected = Vec::new();
        for row in 0..rows {
            expected.push(quotes_of(&[T0 + 2 * row]));
            expected.push(trades_of(&[T0 + 2 * row + 1]));
        }
        expected.push(Sent::End(None));
        let tapped = tapped.borrow();
        let first_difference = tapped
            .sent
            .iter()
            .zip(&expected)
            .position(|(sent, expected)| sent != expected);
        assert_eq!(
            (first_difference, tapped.sent.len()),
            (None, expected.len())
        );
        assert_eq!(
            tapped.identities,
            vec![Identity {
                correlation_id: request_id,
                client_id,
                instrument_ids: vec![instrument_id],
                window: (
                    Some(UnixNanos::from(WINDOW.0)),
                    Some(UnixNanos::from(WINDOW.1))
                ),
                ts_init: UnixNanos::from(NOW),
                params: Some(params_from_json(
                    json!({"quotes": true, "update_catalog": false})
                )),
            }]
        );
        assert!(fixture.counts.peak() <= RUN_CAP + 2 * PAGE_ROWS);
        assert_eq!(fixture.counts.peak(), 2 * PAGE_ROWS + 1);
        assert_eq!(fixture.counts.dropped(), 2);
        assert_eq!(fixture.engine.request_pipeline_count(), 0);
    }

    /// A quote page that ends at stamp 9 and a next one starting at 10, the trades holding a
    /// trade at 10: the quote at 10 goes out before the trade at 10.
    #[rstest]
    fn test_quotes_request_sends_a_quote_before_a_trade_at_the_stamp_its_next_catalog_page_starts(
        audusd_sim: CurrencyPair,
        stub_msgbus: Rc<RefCell<MessageBus>>,
        client_id: ClientId,
        venue: Venue,
    ) {
        let _ = stub_msgbus;
        let instrument_id = audusd_sim.id;
        let mut first_page = vec![T0 + 8; PAGE_ROWS - 1];
        first_page.push(T0 + 9);
        let mut quotes = first_page.clone();
        quotes.extend([T0 + 10, T0 + 11]);
        let mut fixture = Fixture::new(
            vec![audusd_sim],
            client_id,
            venue,
            feed(quotes),
            feed(vec![T0 + 10]),
        );

        let (_, tapped) = fixture.request(instrument_id, json!({}));

        let mut first_run = first_page;
        first_run.push(T0 + 10);
        assert_eq!(
            tapped.borrow().sent,
            vec![
                Sent::Quotes(first_run),
                trades_of(&[T0 + 10]),
                quotes_of(&[T0 + 11]),
                trades_of(&[]),
                Sent::End(None),
            ]
        );
    }

    /// Two requests on two instruments, each waiting on its client leg with its catalog sessions
    /// open, answered in reverse order: each answer is whole and in order, and each request's
    /// sessions are dropped when it completes, the other's left open until its own completion.
    #[rstest]
    fn test_two_quotes_requests_keep_their_catalog_sessions_apart_and_each_answers_whole(
        audusd_sim: CurrencyPair,
        stub_msgbus: Rc<RefCell<MessageBus>>,
        client_id: ClientId,
        venue: Venue,
    ) {
        let _ = stub_msgbus;
        let (first, second) = (audusd_sim.id, gbpusd_sim().id);
        let trades = Feed {
            missing: vec![CLIENT_PART],
            ..feed(vec![at_second(2), at_second(4)])
        };
        let mut fixture = Fixture::new(
            vec![audusd_sim, gbpusd_sim()],
            client_id,
            venue,
            feed(vec![at_second(1), at_second(3), at_second(5)]),
            trades,
        );

        let (_, first_tapped) = fixture.request(first, json!({}));
        let (_, second_tapped) = fixture.request(second, json!({}));
        let legs = fixture.client_legs();
        assert_eq!(
            legs.iter()
                .map(|&(_, id, quotes)| (id, quotes))
                .collect::<Vec<_>>(),
            vec![(first, false), (second, false)]
        );
        assert_eq!(fixture.counts.dropped(), 0);

        fixture.answer_leg(legs[1], &[at_second(85)]);
        assert_eq!(fixture.counts.dropped(), 2);
        assert!(first_tapped.borrow().sent.is_empty());
        fixture.answer_leg(legs[0], &[at_second(85)]);

        let answer = vec![
            quotes_of(&[at_second(1)]),
            trades_of(&[at_second(2)]),
            quotes_of(&[at_second(3)]),
            trades_of(&[at_second(4)]),
            quotes_of(&[at_second(5)]),
            trades_of(&[at_second(85)]),
            Sent::End(None),
        ];
        for (tapped, instrument_id) in [(first_tapped, first), (second_tapped, second)] {
            let tapped = tapped.borrow();
            assert_eq!(tapped.sent, answer);
            let subjects: Vec<_> = tapped
                .identities
                .iter()
                .map(|identity| identity.instrument_ids.clone())
                .collect();
            assert_eq!(subjects, vec![vec![instrument_id]]);
        }
        assert_eq!(fixture.counts.dropped(), 4);
        assert_eq!(fixture.engine.request_pipeline_count(), 0);
    }

    /// A kept, interleaved request whose quotes session fails after 5,000 rows, while a 3-TICK
    /// subscription is held back on its builder: the parts before the failure go out, then one
    /// failed terminal naming it; no bar of its end; its sessions dropped; and its builders are
    /// removed, not handed over: the subscription's new builder takes only the held trades, so
    /// its first bar closes at the third of them (its kept builder held two of the answer's).
    #[rstest]
    fn test_quotes_request_whose_catalog_read_fails_after_parts_were_sent_ends_once_and_hands_over_no_builder(
        audusd_sim: CurrencyPair,
        stub_msgbus: Rc<RefCell<MessageBus>>,
        client_id: ClientId,
        venue: Venue,
    ) {
        let _ = stub_msgbus;
        let instrument_id = audusd_sim.id;
        let millisecond = 1_000_000;
        let quote_stamps = every(T0, millisecond, 10_000);
        let trade_stamps = [999, 2_999, 7_999].map(|ms| T0 + ms * millisecond + millisecond / 2);
        let quotes = Feed {
            fails_after: Some(5_000),
            ..feed(quote_stamps.clone())
        };
        let trades = Feed {
            missing: vec![CLIENT_PART],
            ..feed(trade_stamps.to_vec())
        };
        let mut fixture = Fixture::new(vec![audusd_sim], client_id, venue, quotes, trades);
        let three = BarType::from(format!("{instrument_id}-3-TICK-LAST-INTERNAL").as_str());
        let minute = BarType::from(format!("{instrument_id}-1-MINUTE-LAST-INTERNAL").as_str());
        let published = Rc::new(RefCell::new(Vec::new()));
        let recorded = Rc::clone(&published);
        msgbus::subscribe_bars(
            switchboard::get_bars_topic(three.standard()).into(),
            TypedHandler::from(move |bar: &Bar| {
                recorded
                    .borrow_mut()
                    .push((bar.ts_event.as_u64(), bar.volume));
            }),
            None,
        );

        let params = json!({
            "bar_types": [three.to_string(), minute.to_string()],
            "update_subscriptions": true,
            "interleave": true,
        });
        let (_, tapped) = fixture.request(instrument_id, params);
        let legs = fixture.client_legs();
        fixture
            .engine
            .execute(DataCommand::Subscribe(SubscribeCommand::Bars(
                SubscribeBars::new(
                    three,
                    Some(fixture.client_id),
                    Some(fixture.venue),
                    UUID4::new(),
                    UnixNanos::from(NOW),
                    None,
                    None,
                ),
            )));
        let live = |trade_id: &str, seconds: u64| {
            Data::Trade(make_trade(
                instrument_id,
                "1.0001",
                1,
                trade_id,
                at_second(seconds),
            ))
        };
        fixture.engine.process_data(live("h1", 100));
        fixture.engine.process_data(live("h2", 101));
        fixture.answer_leg(legs[0], &[]);

        let reason = "Cannot answer the quotes with the trades: the made-up quotes page after row \
                      5000 is unreadable";
        assert_eq!(
            tapped.borrow().sent,
            vec![
                quotes_of(&quote_stamps[..1_000]),
                trades_of(&trade_stamps[..1]),
                quotes_of(&quote_stamps[1_000..3_000]),
                trades_of(&trade_stamps[1..2]),
                Sent::End(Some(reason.to_owned())),
            ]
        );
        assert_eq!(fixture.counts.dropped(), 2);
        assert_eq!(fixture.engine.request_pipeline_count(), 0);
        assert!(published.borrow().is_empty());

        fixture.engine.process_data(live("h3", 102));
        assert_eq!(published.take(), vec![(at_second(102), Quantity::from(3))]);
    }

    /// How a request whose quotes session fails on its first page is answered.
    #[derive(Clone, Copy, Debug)]
    enum FirstPageFails {
        /// Its catalog covers the window: no client leg.
        CatalogAlone,
        /// Its catalog lacks quotes over a part its client is asked, and its client answers a
        /// quote there after the catalog leg failed: with no trades identity to rebuild, the
        /// request ends with the failure and the client's quote is not delivered.
        ClientAnswersAfterTheCatalogFailed,
    }

    /// A quotes session whose first page fails fails the catalog leg when the request is sent:
    /// no part, one failed terminal, both sessions dropped.
    #[rstest]
    #[case::catalog_alone(FirstPageFails::CatalogAlone)]
    #[case::client_answers_after_the_catalog_failed(
        FirstPageFails::ClientAnswersAfterTheCatalogFailed
    )]
    fn test_quotes_request_whose_catalog_cannot_be_read_when_sent_fails_before_any_part(
        audusd_sim: CurrencyPair,
        stub_msgbus: Rc<RefCell<MessageBus>>,
        client_id: ClientId,
        venue: Venue,
        #[case] shape: FirstPageFails,
    ) {
        let _ = stub_msgbus;
        let instrument_id = audusd_sim.id;
        let missing = match shape {
            FirstPageFails::CatalogAlone => Vec::new(),
            FirstPageFails::ClientAnswersAfterTheCatalogFailed => vec![CLIENT_PART],
        };
        let quotes = Feed {
            stamps: vec![at_second(1), at_second(3)],
            missing,
            fails_after: Some(0),
        };
        let mut fixture = Fixture::new(
            vec![audusd_sim],
            client_id,
            venue,
            quotes,
            feed(vec![at_second(2)]),
        );

        let (_, tapped) = fixture.request(instrument_id, json!({}));
        assert_eq!(fixture.counts.dropped(), 2);

        match shape {
            FirstPageFails::CatalogAlone => assert!(fixture.client_legs().is_empty()),
            FirstPageFails::ClientAnswersAfterTheCatalogFailed => {
                let legs = fixture.client_legs();
                assert_eq!(
                    legs.iter()
                        .map(|&(_, id, quotes)| (id, quotes))
                        .collect::<Vec<_>>(),
                    vec![(instrument_id, true)]
                );
                assert!(tapped.borrow().sent.is_empty());
                fixture.answer_leg(legs[0], &[at_second(85)]);
            }
        }

        let reason = "the made-up quotes page after row 0 is unreadable";
        assert_eq!(
            tapped.borrow().sent,
            vec![Sent::End(Some(reason.to_owned()))]
        );
        assert_eq!(fixture.engine.request_pipeline_count(), 0);
    }

    /// A session of 25,000 quotes and no trade from the catalog: parts of 10,000, 10,000 and
    /// 5,000 quotes, then the empty last trades part; the rows yielded and not yet delivered
    /// stay within the run cap plus a page of each feed.
    #[rstest]
    fn test_quotes_request_over_quotes_alone_from_its_catalog_sends_parts_of_at_most_ten_thousand(
        audusd_sim: CurrencyPair,
        stub_msgbus: Rc<RefCell<MessageBus>>,
        client_id: ClientId,
        venue: Venue,
    ) {
        let _ = stub_msgbus;
        let instrument_id = audusd_sim.id;
        let stamps = every(T0, 1_000_000, 25_000);
        let mut fixture = Fixture::new(
            vec![audusd_sim],
            client_id,
            venue,
            feed(stamps.clone()),
            feed(Vec::new()),
        );

        let (_, tapped) = fixture.request(instrument_id, json!({}));

        assert_eq!(
            tapped.borrow().sent,
            vec![
                quotes_of(&stamps[..10_000]),
                quotes_of(&stamps[10_000..20_000]),
                quotes_of(&stamps[20_000..]),
                trades_of(&[]),
                Sent::End(None),
            ]
        );
        assert!(fixture.counts.peak() <= RUN_CAP + 2 * PAGE_ROWS);
        assert_eq!(fixture.counts.dropped(), 2);
    }

    /// What the engine is told while a request waits on its client leg.
    #[derive(Clone, Copy, Debug)]
    enum Interruption {
        Reset,
        Dispose,
        Stop,
    }

    /// A request waiting on its client leg with its catalog sessions open: a `reset` or a
    /// `dispose` drops both; after a `stop` they stay, and the client's answer completes the
    /// request whole.
    #[rstest]
    #[case::reset(Interruption::Reset)]
    #[case::dispose(Interruption::Dispose)]
    #[case::stop(Interruption::Stop)]
    fn test_quotes_request_catalog_sessions_are_dropped_by_reset_and_dispose_and_kept_by_stop(
        audusd_sim: CurrencyPair,
        stub_msgbus: Rc<RefCell<MessageBus>>,
        client_id: ClientId,
        venue: Venue,
        #[case] interruption: Interruption,
    ) {
        let _ = stub_msgbus;
        let instrument_id = audusd_sim.id;
        let trades = Feed {
            missing: vec![CLIENT_PART],
            ..feed(vec![at_second(2)])
        };
        let mut fixture = Fixture::new(
            vec![audusd_sim],
            client_id,
            venue,
            feed(vec![at_second(1), at_second(3)]),
            trades,
        );
        let (_, tapped) = fixture.request(instrument_id, json!({}));
        let legs = fixture.client_legs();
        assert_eq!(fixture.counts.dropped(), 0);
        assert_eq!(fixture.engine.request_pipeline_count(), 1);

        match interruption {
            Interruption::Reset => fixture.engine.reset(),
            Interruption::Dispose => fixture.engine.dispose(),
            Interruption::Stop => {
                fixture.engine.stop();
                assert_eq!(fixture.counts.dropped(), 0);
                fixture.answer_leg(legs[0], &[at_second(85)]);
            }
        }

        let sent = match interruption {
            Interruption::Reset | Interruption::Dispose => Vec::new(),
            Interruption::Stop => vec![
                quotes_of(&[at_second(1)]),
                trades_of(&[at_second(2)]),
                quotes_of(&[at_second(3)]),
                trades_of(&[at_second(85)]),
                Sent::End(None),
            ],
        };
        assert_eq!(tapped.borrow().sent, sent);
        assert_eq!(fixture.counts.dropped(), 2);
        assert_eq!(fixture.engine.request_pipeline_count(), 0);
    }
}
