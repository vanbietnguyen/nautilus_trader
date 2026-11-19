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
    clients::{DataClient, ExecutionReportTask},
    component::component_state,
    enums::ComponentState,
    live::dst::time,
    messages::{
        data::UnsubscribeCommand,
        execution::{CancelOrder, QueryOrder},
    },
    msgbus::{stubs::get_any_saving_handler, switchboard::get_quotes_topic},
};
use nautilus_data::DataClientAdapter;
use nautilus_model::data::Data;
#[cfg(feature = "python")]
use pyo3::{prelude::*, types::PyDict};

use super::*;
#[cfg(feature = "python")]
use crate::python::node::PyLiveNode;

#[tokio::test]
async fn test_final_drain_blocks_subscriptions_and_preserves_data_and_cleanup() {
    let client_id = ClientId::from("EXTERNAL");

    let config = LiveNodeConfig {
        data_engine: crate::config::LiveDataEngineConfig {
            external_clients: Some(vec![client_id]),
            ..Default::default()
        },
        ..Default::default()
    };

    let mut node = LiveNode::build("FinalDrainNode".to_string(), Some(config)).unwrap();
    let topic = format!("commands.data.{client_id}");
    let (handler, subscribes) = get_any_saving_handler::<SubscribeCommand>(None);
    msgbus::subscribe_any(topic.as_str().into(), handler, None);

    let subscribe = |instrument_id| {
        SubscribeCommand::Quotes(SubscribeQuotes::new(
            instrument_id,
            Some(client_id),
            Some(Venue::from("SIM")),
            UUID4::new(),
            UnixNanos::from(40),
            None,
            None,
        ))
    };

    let active = subscribe(InstrumentId::from("AUD/USD.SIM"));
    node.kernel
        .data_engine
        .borrow_mut()
        .execute(DataCommand::Subscribe(active));
    assert_eq!(subscribes.get_messages().len(), 1);

    node.finalize_stop().await.unwrap();
    let blocked = subscribe(InstrumentId::from("EUR/USD.SIM"));
    let sender = nautilus_common::runner::get_data_cmd_sender();
    sender.execute(DataCommand::Subscribe(blocked.clone()));
    get_data_event_sender().send(stub_data_event()).unwrap();
    node.drain_runner_pending();

    assert_eq!(subscribes.get_messages().len(), 1);
    assert_eq!(node.kernel.data_engine().command_count(), 2);
    assert_eq!(node.kernel.data_engine().data_count(), 1);

    let (handler, unsubscribes) = get_any_saving_handler::<UnsubscribeCommand>(None);
    msgbus::subscribe_any(topic.as_str().into(), handler, None);
    let unsubscribe = blocked.into_unsubscribe(UUID4::new(), UnixNanos::from(41), None);
    sender.execute(DataCommand::Unsubscribe(unsubscribe.clone()));
    node.drain_runner_pending();
    assert_eq!(
        serde_json::to_value(unsubscribes.get_messages()).unwrap(),
        serde_json::to_value([unsubscribe]).unwrap()
    );
    node.dispose();
}

#[cfg(not(madsim))]
#[rstest]
#[case::failed_first(false)]
#[case::pending_first(true)]
#[tokio::test(flavor = "current_thread")]
async fn test_report_shutdown_timeout_preserves_observed_worker_failure(
    #[case] pending_first: bool,
) {
    let mut node = LiveNode::build("ReportShutdownNode".to_string(), None).unwrap();
    let command =
        GenerateOrderStatusReports::new(UUID4::new(), 0.into(), true, None, None, None, None, None);
    let (failed, failed_started, failed_completed) =
        shutdown_report_client(Duration::from_millis(30), true, Duration::ZERO, None);
    let (pending, pending_started, _) =
        shutdown_report_client(Duration::from_millis(200), false, Duration::ZERO, None);
    let mut failed_collection = Box::pin(failed.generate_order_status_reports(&command));
    tokio::select! {
        biased;
        started = failed_started => { started.unwrap(); },
        result = &mut failed_collection => panic!("worker completed before observation: {result:?}"),
    }
    drop(failed_collection);
    failed_completed.await.unwrap();
    let mut pending_collection = Box::pin(pending.generate_order_status_reports(&command));
    tokio::select! {
        biased;
        started = pending_started => { started.unwrap(); },
        result = &mut pending_collection => panic!("worker completed before observation: {result:?}"),
    }
    drop(pending_collection);
    node.exec_clients = vec![failed, pending];
    if pending_first {
        node.exec_clients.reverse();
    }

    let mut errors = Vec::new();
    let timed_out = tokio::time::timeout(
        Duration::from_millis(50),
        LiveNode::finish_report_tasks(&node.exec_clients, &mut errors),
    )
    .await;
    let observed = errors.clone();
    tokio::time::timeout(
        Duration::from_secs(5),
        LiveNode::finish_report_tasks(&node.exec_clients, &mut errors),
    )
    .await
    .unwrap();
    assert!(timed_out.is_err());
    assert_eq!(observed.len(), 1);
    assert!(observed[0].contains("SHUTDOWN report collection:"));
    assert!(observed[0].contains("report worker failed"));
    assert_eq!(errors, observed);
}

#[rstest]
#[case::execution_start(None, false)]
#[case::execution_owned(Some(NodeRunMode::Owned), false)]
#[case::execution_hosted(Some(NodeRunMode::Hosted), false)]
#[case::data_start(None, true)]
#[case::data_owned(Some(NodeRunMode::Owned), true)]
#[case::data_hosted(Some(NodeRunMode::Hosted), true)]
#[tokio::test]
async fn test_client_connect_failure_aborts_startup(
    #[case] mode: Option<NodeRunMode>,
    #[case] data: bool,
    #[values(false, true)] pending: bool,
) {
    let config = LiveNodeConfig {
        exec_engine: crate::config::LiveExecutionEngineConfig {
            reconciliation: false,
            ..Default::default()
        },
        timeout_connection: if pending {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(30)
        },
        timeout_disconnection: Duration::from_millis(100),
        delay_post_stop: Duration::ZERO,
        timeout_shutdown: Duration::from_millis(100),
        ..Default::default()
    };

    let mut node = LiveNode::build("FailedConnectNode".to_string(), Some(config)).unwrap();
    let (_, control) = TestCacheDatabaseControl::create();
    node.add_strategy(StateStrategy::new(
        StrategyId::from("CONNECT-001"),
        control.clone(),
        IndexMap::new(),
    ))
    .unwrap();
    let connect_error = "password=SYN-password token=SYN-token";

    let connect_delay = if pending {
        Duration::from_secs(30)
    } else {
        Duration::ZERO
    };

    if data {
        node.kernel.data_engine.borrow_mut().register_client(
            DataClientAdapter::new(
                ClientId::from("DATA"),
                None,
                true,
                true,
                Box::new(DataConnectClient {
                    connected: false,
                    connect_error: Some(connect_error),
                    connect_delay,
                    stop_handle: None,
                }),
            ),
            None,
        );
    } else {
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(ShutdownClient {
                connected: false,
                connect_error: Some(connect_error),
                connect_count: Rc::default(),
                connect_delay,
                retain: false,
                disconnect_event: Rc::default(),
                submitted: Rc::default(),
                queries: Rc::default(),
                report_task: RefCell::default(),
                disconnect_delay: Duration::ZERO,
            }))
            .unwrap();
    }

    let e = tokio::time::timeout(Duration::from_secs(2), async {
        match mode {
            None => node.start().await,
            Some(mode) => node.run_with_mode(mode).await,
        }
    })
    .await
    .expect("failed connection waited for startup readiness")
    .unwrap_err();

    assert_eq!(
        e.to_string(),
        match (data, pending) {
            (true, true) => "data-connect timeout",
            (true, false) => "Failed to connect data clients: DATA",
            (false, true) => "exec-connect timeout",
            (false, false) => "Failed to connect execution clients: SHUTDOWN",
        }
    );
    assert_eq!(e.chain().count(), 1);
    assert_eq!(node.state(), NodeState::Stopped);
    assert_eq!(control.events(), Vec::<String>::new());

    if data {
        assert!(
            node.kernel
                .data_engine
                .borrow_mut()
                .get_client(Some(&ClientId::from("DATA")), None)
                .unwrap()
                .is_disconnected()
        );
    } else {
        assert!(
            !node
                .kernel
                .exec_engine
                .borrow()
                .get_client(&ClientId::from("SHUTDOWN"))
                .unwrap()
                .is_connected()
        );
    }
}

#[rstest]
#[case::connection("timeout_connection", |config: &mut LiveNodeConfig, duration| config.timeout_connection = duration)]
#[case::disconnection("timeout_disconnection", |config: &mut LiveNodeConfig, duration| config.timeout_disconnection = duration)]
#[case::post_stop("delay_post_stop", |config: &mut LiveNodeConfig, duration| config.delay_post_stop = duration)]
fn test_node_build_rejects_overflowing_lifecycle_deadline(
    #[case] field: &str,
    #[case] configure: fn(&mut LiveNodeConfig, Duration),
    #[values(Duration::MAX, Duration::from_millis(u64::MAX))] duration: Duration,
) {
    let mut config = LiveNodeConfig::default();
    configure(&mut config, duration);
    let expected =
        format!("invalid LiveNodeConfig.{field}: duration exceeds the monotonic deadline range");

    let direct = LiveNode::build("InvalidDeadlineNode".to_string(), Some(config.clone()));
    assert_eq!(direct.unwrap_err().to_string(), expected);

    let builder = LiveNodeBuilder::from_config(config).unwrap();
    assert_eq!(builder.build().unwrap_err().to_string(), expected);
}

mod dst {
    use super::*;

    #[rstest]
    #[case::manual(None)]
    #[case::hosted(Some(NodeRunMode::Hosted))]
    #[cfg_attr(not(all(feature = "simulation", madsim)), tokio::test)]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_stop_during_data_connect_skips_execution_connect(
        #[case] mode: Option<NodeRunMode>,
    ) {
        let config = LiveNodeConfig {
            timeout_connection: Duration::ZERO,
            timeout_disconnection: Duration::ZERO,
            delay_post_stop: Duration::ZERO,
            ..Default::default()
        };

        let mut node = LiveNode::build("CancelledStartupNode".to_string(), Some(config)).unwrap();
        let handle = node.handle();
        let connect_count = Rc::new(Cell::new(0));

        node.kernel.data_engine.borrow_mut().register_client(
            DataClientAdapter::new(
                ClientId::from("DATA"),
                None,
                true,
                true,
                Box::new(DataConnectClient {
                    connected: false,
                    connect_error: None,
                    connect_delay: Duration::ZERO,
                    stop_handle: Some(handle.clone()),
                }),
            ),
            None,
        );

        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(ShutdownClient {
                connected: false,
                connect_error: None,
                connect_count: Rc::clone(&connect_count),
                connect_delay: Duration::ZERO,
                retain: false,
                disconnect_event: Rc::default(),
                submitted: Rc::default(),
                queries: Rc::default(),
                report_task: RefCell::default(),
                disconnect_delay: Duration::ZERO,
            }))
            .unwrap();

        time::timeout(Duration::from_secs(2), async {
            match mode {
                None => node.start().await,
                Some(mode) => node.run_with_mode(mode).await,
            }
        })
        .await
        .expect("startup did not abort")
        .unwrap();

        assert_eq!(connect_count.get(), 0);
        assert_eq!(node.state(), NodeState::Stopped);
        assert!(handle.should_stop());
    }

    #[rstest]
    #[cfg_attr(not(all(feature = "simulation", madsim)), tokio::test)]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_hosted_startup_aborts_when_connection_deadline_overflows() {
        let mut node = LiveNode::build("InvalidDeadlineNode".to_string(), None).unwrap();
        node.config.timeout_connection = Duration::MAX;

        let error = node.run_with_mode(NodeRunMode::Hosted).await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid LiveNodeConfig.timeout_connection: duration exceeds the monotonic deadline range"
        );
        assert_eq!(node.state(), NodeState::Stopped);
    }
}

#[cfg(target_os = "linux")]
#[rstest]
fn test_live_deadline_leaves_room_for_timer_rounding() {
    let now = time::Instant::now();
    let mut duration = Duration::ZERO;
    let mut step = Duration::MAX / 2;

    // Find a duration within one millisecond of the platform's last representable deadline
    while !step.is_zero() {
        let candidate = duration + step;
        if now.checked_add(candidate).is_some() {
            duration = candidate;
        }

        step /= 2;
    }

    let deadline = now.checked_add(duration).unwrap();
    let rounding = Duration::from_millis(1);
    let near_limit = deadline.checked_sub(rounding).unwrap();
    assert!(deadline.checked_add(rounding).is_none());
    assert_eq!(
        live_deadline(near_limit, rounding, "delay_post_stop")
            .unwrap_err()
            .to_string(),
        "invalid LiveNodeConfig.delay_post_stop: duration exceeds the monotonic deadline range"
    );
    assert_eq!(
        live_deadline(near_limit, Duration::ZERO, "delay_post_stop").unwrap(),
        near_limit
    );
}

#[derive(Debug)]
struct DataConnectClient {
    connected: bool,
    connect_error: Option<&'static str>,
    connect_delay: Duration,
    stop_handle: Option<LiveNodeHandle>,
}

#[async_trait::async_trait(?Send)]
impl DataClient for DataConnectClient {
    fn client_id(&self) -> ClientId {
        ClientId::from("DATA")
    }

    fn venue(&self) -> Option<Venue> {
        None
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.connected = true;
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn is_disconnected(&self) -> bool {
        !self.connected
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if !self.connect_delay.is_zero() {
            tokio::time::sleep(self.connect_delay).await;
        }

        if let Some(message) = self.connect_error {
            anyhow::bail!("{message}");
        }

        self.connected = true;

        if let Some(handle) = &self.stop_handle {
            handle.stop();
        }

        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }
}

#[cfg(not(madsim))]
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_report_shutdown_timeout_preserves_disconnect_budget() {
    let config = LiveNodeConfig {
        timeout_disconnection: Duration::from_millis(50),
        ..Default::default()
    };

    let mut node =
        LiveNode::build("ReportShutdownDisconnectNode".to_string(), Some(config)).unwrap();

    let (release, blocked) = std::sync::mpsc::channel();
    let (client, started, _) = shutdown_report_client(
        Duration::ZERO,
        false,
        Duration::from_millis(15),
        Some(blocked),
    );
    node.kernel
        .exec_engine
        .borrow_mut()
        .register_client(Box::new(client.clone()))
        .unwrap();
    let command =
        GenerateOrderStatusReports::new(UUID4::new(), 0.into(), true, None, None, None, None, None);
    let mut collection = Box::pin(client.generate_order_status_reports(&command));
    tokio::select! {
        biased;
        started = started => { started.unwrap(); },
        result = &mut collection => panic!("worker completed before observation: {result:?}"),
    }
    drop(collection);
    node.exec_clients = vec![client.clone()];
    let failure = node.finalize_stop().await.unwrap_err();
    let disconnected = node.kernel.check_engines_disconnected();

    // The report worker runs on a separate runtime, so bound its cleanup in real time
    tokio::time::resume();
    release.send(()).unwrap();

    let mut errors = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        LiveNode::finish_report_tasks(&node.exec_clients, &mut errors),
    )
    .await
    .unwrap();

    assert!(
        failure
            .to_string()
            .contains("report collection shutdown timeout")
    );
    assert!(!failure.to_string().contains("disconnect timeout"));
    assert!(disconnected);
    assert!(!client.is_connected());
    assert_eq!(node.state(), NodeState::Stopped);
    assert!(errors.is_empty());
}

#[cfg(not(madsim))]
fn shutdown_report_client(
    duration: Duration,
    fail: bool,
    disconnect_delay: Duration,
    release: Option<std::sync::mpsc::Receiver<()>>,
) -> (
    LiveExecutionClient,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let (started_sender, started) = tokio::sync::oneshot::channel();
    let (completed_sender, completed) = tokio::sync::oneshot::channel();

    let task = ExecutionReportTask::new(
        async move {
            started_sender.send(()).unwrap();

            if let Some(release) = release {
                release.recv().unwrap();
            } else {
                let deadline = std::time::Instant::now() + duration;
                while std::time::Instant::now() < deadline {
                    std::hint::spin_loop();
                }
            }

            let outcome = std::panic::catch_unwind(|| assert!(!fail, "report worker failed"));
            let _ = completed_sender.send(());

            if let Err(payload) = outcome {
                std::panic::resume_unwind(payload);
            }

            Ok(Vec::new())
        },
        Ok,
    );

    let client = LiveExecutionClient::new(Box::new(ShutdownClient {
        connected: true,
        connect_error: None,
        connect_count: Rc::default(),
        connect_delay: Duration::ZERO,
        retain: false,
        disconnect_event: Rc::default(),
        submitted: Rc::default(),
        queries: Rc::default(),
        report_task: RefCell::new(Some(task)),
        disconnect_delay,
    }));

    (client, started, completed)
}

#[derive(Debug)]
struct ShutdownClient {
    connected: bool,
    connect_error: Option<&'static str>,
    connect_count: Rc<Cell<usize>>,
    connect_delay: Duration,
    retain: bool,
    disconnect_event: Rc<RefCell<Option<OrderEventAny>>>,
    submitted: Rc<RefCell<Vec<SubmitOrder>>>,
    queries: Rc<Cell<usize>>,
    report_task: RefCell<Option<ExecutionReportTask<Vec<OrderStatusReport>>>>,
    disconnect_delay: Duration,
}

#[async_trait::async_trait(?Send)]
impl ExecutionClient for ShutdownClient {
    fn is_connected(&self) -> bool {
        self.connected
    }

    fn client_id(&self) -> ClientId {
        ClientId::from("SHUTDOWN")
    }

    fn account_id(&self) -> AccountId {
        AccountId::from("BINANCE-001")
    }

    fn venue(&self) -> Venue {
        crypto_perpetual_ethusdt().id().venue
    }

    fn oms_type(&self) -> OmsType {
        OmsType::Netting
    }

    fn get_account(&self) -> Option<AccountAny> {
        None
    }

    fn generate_order_status_reports_task(
        &self,
        _command: &GenerateOrderStatusReports,
    ) -> Option<ExecutionReportTask<Vec<OrderStatusReport>>> {
        self.report_task.borrow_mut().take()
    }

    fn retain_unresolved_submissions(&self) -> bool {
        self.retain
    }

    fn generate_account_state(
        &self,
        _balances: Vec<AccountBalance>,
        _margins: Vec<MarginBalance>,
        _reported: bool,
        _ts_event: UnixNanos,
        _info: Option<Params>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.connected = true;
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.connect_count.set(self.connect_count.get() + 1);

        if !self.connect_delay.is_zero() {
            tokio::time::sleep(self.connect_delay).await;
        }

        if let Some(message) = self.connect_error {
            anyhow::bail!("{message}");
        }

        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if !self.disconnect_delay.is_zero() {
            tokio::time::sleep(self.disconnect_delay).await;
        }

        self.connected = false;

        if let Some(event) = self.disconnect_event.borrow_mut().take() {
            get_exec_event_sender().send(ExecutionEvent::Order(event))?;
        }

        Ok(())
    }

    fn submit_order(&self, command: SubmitOrder) -> anyhow::Result<()> {
        let mut order = OrderAny::try_from(command.order_init.clone())?;
        let submitted = TestOrderEventStubs::submitted(&order, self.account_id());
        order.apply(submitted.clone())?;
        order.apply(TestOrderEventStubs::accepted(
            &order,
            self.account_id(),
            VenueOrderId::from(format!("V-{}", order.client_order_id()).as_str()),
        ))?;
        let fill = OrderFilledTestBuilder::new(
            &order,
            &InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()),
        )
        .position_id(command.position_id.expect("managed exit position"))
        .last_px(Price::from("100.00"))
        .commission(Money::zero(Currency::USDT()))
        .build();
        self.submitted.borrow_mut().push(command);
        get_exec_event_sender().send(ExecutionEvent::Order(submitted))?;
        get_exec_event_sender().send(ExecutionEvent::Order(fill))?;
        Ok(())
    }

    fn query_order(&self, _command: QueryOrder) -> anyhow::Result<()> {
        self.queries.set(self.queries.get() + 1);
        Ok(())
    }

    fn cancel_order(&self, _command: CancelOrder) -> anyhow::Result<()> {
        // An unacknowledged submission has no cancel outcome to report.
        Ok(())
    }
}

fn shutdown_node(
    policy: SubmissionRecoveryPolicy,
    client: ShutdownClient,
    manage_stop: bool,
) -> (LiveNode, OrderAny) {
    let config = LiveNodeConfig {
        exec_engine: crate::config::LiveExecutionEngineConfig {
            reconciliation: false,
            inflight_check_threshold_ms: 0,
            inflight_check_retries: 1,
            submission_recovery_policy: policy,
            ..Default::default()
        },
        timeout_connection: Duration::ZERO,
        timeout_reconciliation: Duration::ZERO,
        timeout_portfolio: Duration::ZERO,
        timeout_disconnection: Duration::from_millis(10),
        delay_post_stop: Duration::from_millis(if manage_stop { 250 } else { 50 }),
        timeout_shutdown: Duration::ZERO,
        ..Default::default()
    };

    let mut node = LiveNode::build("SubmissionShutdown".to_string(), Some(config)).unwrap();
    let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
    let account_id = client.account_id();
    let client_id = client.client_id();
    let strategy_id = StrategyId::from("SHUTDOWN-001");
    node.exec_manager.register_submission_retention(&client);
    node.kernel
        .exec_engine
        .borrow_mut()
        .register_client(Box::new(client))
        .unwrap();
    node.kernel
        .exec_engine
        .borrow_mut()
        .register_venue_routing(client_id, instrument.id().venue)
        .unwrap();
    node.kernel
        .cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    node.kernel
        .cache
        .borrow_mut()
        .add_quote(QuoteTick::new(
            instrument.id(),
            Price::from("100.00"),
            Price::from("100.01"),
            Quantity::from("10.000"),
            Quantity::from("10.000"),
            UnixNanos::default(),
            UnixNanos::default(),
        ))
        .unwrap();
    node.kernel
        .cache
        .borrow_mut()
        .add_account(AccountAny::Margin(MarginAccount::new(
            AccountState::new(
                account_id,
                AccountType::Margin,
                vec![AccountBalance::new(
                    Money::from("1000000 USDT"),
                    Money::zero(Currency::USDT()),
                    Money::from("1000000 USDT"),
                )],
                Vec::new(),
                true,
                UUID4::new(),
                UnixNanos::default(),
                UnixNanos::default(),
                Some(Currency::USDT()),
            ),
            true,
        )))
        .unwrap();
    node.add_strategy(TestStrategy::new(StrategyConfig {
        strategy_id: Some(strategy_id),
        oms_type: Some(OmsType::Netting),
        manage_stop,
        market_exit_interval_ms: 5,
        market_exit_max_attempts: 100,
        ..Default::default()
    }))
    .unwrap();

    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(node.trader_id())
        .strategy_id(strategy_id)
        .client_order_id(ClientOrderId::from("O-SHUTDOWN"))
        .instrument_id(instrument.id())
        .quantity(Quantity::from("1.000"))
        .price(Price::from("100.00"))
        .build();
    node.observe_exec_command_before_dispatch(&TradingCommand::SubmitOrder(
        SubmitOrder::from_order(
            &order,
            node.trader_id(),
            Some(client_id),
            None,
            UUID4::new(),
            UnixNanos::default(),
        ),
    ));
    node.kernel
        .cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(client_id), false)
        .unwrap();
    node.process_exec_event(ExecutionEvent::Order(TestOrderEventStubs::submitted(
        &order, account_id,
    )));
    (node, order)
}

#[rstest]
#[case::default(SubmissionRecoveryPolicy::ResolveLocally, false, false, false)]
#[case::active(SubmissionRecoveryPolicy::RetainUnresolved, false, false, false)]
#[case::exhausted(SubmissionRecoveryPolicy::RetainUnresolved, false, true, false)]
#[case::client_required(SubmissionRecoveryPolicy::ResolveLocally, true, true, false)]
#[case::after_deadline(SubmissionRecoveryPolicy::RetainUnresolved, false, true, true)]
#[tokio::test(start_paused = true)]
async fn test_submission_shutdown_boundary(
    #[values(false, true)] hosted: bool,
    #[case] policy: SubmissionRecoveryPolicy,
    #[case] client_retains: bool,
    #[case] exhaust: bool,
    #[case] evidence_on_disconnect: bool,
) {
    let queries = Rc::new(Cell::new(0));
    let disconnect_event = Rc::default();

    let (mut node, order) = shutdown_node(
        policy,
        ShutdownClient {
            connected: false,
            connect_error: None,
            connect_count: Rc::default(),
            connect_delay: Duration::ZERO,
            retain: client_retains,
            disconnect_event: Rc::clone(&disconnect_event),
            submitted: Rc::default(),
            queries: queries.clone(),
            report_task: RefCell::default(),
            disconnect_delay: Duration::ZERO,
        },
        false,
    );

    if evidence_on_disconnect {
        *disconnect_event.borrow_mut() = Some(TestOrderEventStubs::accepted(
            &order,
            AccountId::from("BINANCE-001"),
            VenueOrderId::from("V-SHUTDOWN"),
        ));
    }

    if exhaust {
        tokio::time::advance(Duration::from_millis(1)).await;
        let result = node.exec_manager.check_inflight_orders();
        assert!(result.queries.is_empty());
        assert!(result.events.is_empty());
        assert_eq!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .len(),
            1
        );
    }

    let start = tokio::time::Instant::now();

    let result = if hosted {
        let handle = node.handle();
        let stop = tokio::spawn(async move {
            while !handle.is_running() {
                tokio::task::yield_now().await;
            }

            handle.stop();
        });

        let result = node.run_with_mode(NodeRunMode::Hosted).await;
        stop.await.unwrap();
        result
    } else {
        node.start().await.unwrap();
        node.stop().await
    };

    if policy == SubmissionRecoveryPolicy::RetainUnresolved || client_retains {
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("Submission recovery incomplete at shutdown"),
            "{error}"
        );
        assert!(error.contains(order.client_order_id().as_str()), "{error}");
    } else {
        result.unwrap();
    }

    assert!(start.elapsed() < Duration::from_millis(200));
    assert_eq!(node.state(), NodeState::Stopped);
    assert!(node.kernel.check_engines_disconnected());
    assert_eq!(queries.get(), 0);
    assert_eq!(
        node.kernel
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        if evidence_on_disconnect {
            OrderStatus::Accepted
        } else {
            OrderStatus::Submitted
        },
    );
    node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_submission_shutdown_late_fill_completes_managed_exit(
    #[values(false, true)] hosted: bool,
) {
    let submitted = Rc::new(RefCell::new(Vec::new()));

    let (mut node, order) = shutdown_node(
        SubmissionRecoveryPolicy::RetainUnresolved,
        ShutdownClient {
            connected: false,
            connect_error: None,
            connect_count: Rc::default(),
            connect_delay: Duration::ZERO,
            retain: false,
            disconnect_event: Rc::default(),
            submitted: submitted.clone(),
            queries: Rc::default(),
            report_task: RefCell::default(),
            disconnect_delay: Duration::ZERO,
        },
        true,
    );

    tokio::time::sleep(Duration::from_millis(1)).await;
    let exhausted = node.exec_manager.check_inflight_orders();
    assert!(exhausted.events.is_empty());
    assert!(exhausted.queries.is_empty());
    assert_eq!(
        node.exec_manager
            .take_submission_recovery_exhaustions()
            .len(),
        1
    );
    let fill = OrderFilledTestBuilder::new(
        &order,
        &InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()),
    )
    .account_id(AccountId::from("BINANCE-001"))
    .without_position_id()
    .last_px(Price::from("100.00"))
    .commission(Money::zero(Currency::USDT()))
    .build();
    let handle = node.handle();
    if !hosted {
        node.start().await.unwrap();
    }

    let sender = get_exec_event_sender();

    let late_fill = tokio::spawn(async move {
        if hosted {
            while !handle.is_running() {
                tokio::task::yield_now().await;
            }

            handle.stop();
        }

        while handle.state() != NodeState::ShuttingDown {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(handle.state(), NodeState::ShuttingDown);
        sender.send(ExecutionEvent::Order(fill)).unwrap();
    });

    if hosted {
        node.run_with_mode(NodeRunMode::Hosted).await.unwrap();
    } else {
        node.stop().await.unwrap();
    }

    late_fill.await.unwrap();
    let commands = submitted.borrow();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].order_init.order_side, OrderSide::Sell);
    assert!(commands[0].order_init.reduce_only);
    let cache = node.kernel.cache.borrow();
    assert_eq!(
        cache.order(&order.client_order_id()).unwrap().status(),
        OrderStatus::Filled
    );
    assert_eq!(
        cache.order(&commands[0].client_order_id).unwrap().status(),
        OrderStatus::Filled
    );
    assert_eq!(cache.positions_open_count(None, None, None, None, None), 0);
    assert_eq!(
        cache.positions_closed_count(None, None, None, None, None),
        1
    );
    assert_eq!(cache.orders_inflight_count(None, None, None, None, None), 0);
    assert!(node.exec_manager.unresolved_submission_ids().is_empty());
    assert_eq!(
        component_state(&order.strategy_id().inner()).unwrap(),
        ComponentState::Stopped,
    );
    drop(cache);
    assert_eq!(node.state(), NodeState::Stopped);
    node.dispose();
}

#[cfg(feature = "python")]
#[derive(Debug)]
struct FailingStopActor {
    core: DataActorCore,
}

#[cfg(feature = "python")]
nautilus_actor!(FailingStopActor);

#[cfg(feature = "python")]
impl DataActor for FailingStopActor {
    fn on_stop(&mut self) -> anyhow::Result<()> {
        let callback = TimeEventCallback::RustLocal(Rc::new(move |_| {
            crate::dispatch::tests::latch_callback_failure();
        }));

        nautilus_common::runner::get_time_event_sender().send(TimeEventMessage::new(
            TimeEvent::new(
                "shutdown-callback-failure".into(),
                UUID4::new(),
                UnixNanos::default(),
                UnixNanos::default(),
            ),
            callback,
        ));
        Ok(())
    }
}

#[cfg(feature = "python")]
#[rstest]
fn test_submission_shutdown_python_result(
    #[values("stop", "cancel", "timeout")] mode: &str,
    #[values(false, true)] retain: bool,
    #[values(false, true)] callback_failure: bool,
) {
    Python::initialize();

    let (mut node, order) = shutdown_node(
        if retain {
            SubmissionRecoveryPolicy::RetainUnresolved
        } else {
            SubmissionRecoveryPolicy::ResolveLocally
        },
        ShutdownClient {
            connected: false,
            connect_error: None,
            connect_count: Rc::default(),
            connect_delay: Duration::ZERO,
            retain: false,
            disconnect_event: Rc::default(),
            submitted: Rc::default(),
            queries: Rc::default(),
            report_task: RefCell::default(),
            disconnect_delay: Duration::ZERO,
        },
        false,
    );

    if callback_failure {
        node.add_actor(FailingStopActor {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some(ActorId::from("SHUTDOWN-CALLBACK-FAILURE")),
                ..Default::default()
            }),
        })
        .unwrap();
    }

    let handle = node.handle();
    let cache = node.kernel.cache.clone();

    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals.set_item("mode", mode).unwrap();
        locals.set_item("retain", retain).unwrap();
        locals
            .set_item("callback_failure", callback_failure)
            .unwrap();
        locals
            .set_item("node", Py::new(py, PyLiveNode::new(node)).unwrap())
            .unwrap();
        py.run(
            pyo3::ffi::c_str!(
                r#"
import asyncio

async def exercise():
    handle = node.handle()
    task = asyncio.create_task(node.run_async())

    while not handle.is_running:

        if task.done():
            await task
        await asyncio.sleep(0)

    try:

        if mode == "stop":
            handle.stop()
            await task
        elif mode == "cancel":
            task.cancel()
            await task
        else:
            async with asyncio.timeout(0):
                await task
    except BaseException as error:
        expected = {"stop": RuntimeError, "cancel": asyncio.CancelledError, "timeout": TimeoutError}
        assert isinstance(error, expected[mode]), repr(error)
        causes = []
        while error is not None:
            causes.append(error)
            error = error.__cause__
        incomplete = [e for e in causes if "Submission recovery incomplete at shutdown" in str(e)]
        assert bool(incomplete) == retain, repr(causes)
        callbacks = [e for e in causes if "Callback delivery unwound" in str(e)]
        assert bool(callbacks) == callback_failure, repr(causes)
        if retain:
            assert "O-SHUTDOWN" in str(incomplete[0])
    else:
        assert mode == "stop" and not retain and not callback_failure

    assert task.done()
    assert task.cancelled() == (mode != "stop")
    assert not handle.is_running

asyncio.run(exercise())
"#
            ),
            Some(&locals),
            None,
        )
        .unwrap();

        assert_eq!(handle.state(), NodeState::Stopped);
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Submitted,
        );
        locals
            .get_item("node")
            .unwrap()
            .unwrap()
            .call_method0("dispose")
            .unwrap();
    });
}

/// The steps of a stop as the node's way out and its client see them, in order.
type StopSteps = Arc<Mutex<Vec<String>>>;

/// The topic the client publishes on while it disconnects.
const DISCONNECT_TOPIC: &str = "data.quotes.DISCONNECT";

/// The topic the client publishes on when the kernel's finalize stops it.
const FINALIZE_TOPIC: &str = "data.quotes.FINALIZE";

/// A node whose way out records its steps, with an inbound key listed when `inbound` is set, and
/// one data client that publishes while it disconnects, failing its disconnect when
/// `fail_disconnect` is set.
fn stop_recording_node(inbound: bool, fail_disconnect: bool) -> (LiveNode, StopSteps) {
    sending_stop_recording_node(inbound, fail_disconnect, false)
}

/// The node of [`stop_recording_node`], whose client also sends one quote through the node's data
/// event sender while it disconnects when `send_on_disconnect` is set.
fn sending_stop_recording_node(
    inbound: bool,
    fail_disconnect: bool,
    send_on_disconnect: bool,
) -> (LiveNode, StopSteps) {
    let steps = StopSteps::default();
    let factory = StopRecordingBackingFactory {
        steps: steps.clone(),
        close_gate: Mutex::default(),
        receiver: Mutex::default(),
    };
    let node = recording_node(
        inbound,
        fail_disconnect,
        send_on_disconnect,
        factory,
        LoggerConfig::default(),
    );
    (node, steps)
}

/// The node of [`sending_stop_recording_node`], its way out made by `factory`, logging as
/// `logging` says.
fn recording_node(
    inbound: bool,
    fail_disconnect: bool,
    send_on_disconnect: bool,
    factory: StopRecordingBackingFactory,
    logging: LoggerConfig,
) -> LiveNode {
    let steps = factory.steps.clone();
    let config = LiveNodeConfig {
        logging,
        msgbus: inbound.then(|| MessageBusConfig {
            external_streams: Some(vec!["operator".to_string()]),
            ..Default::default()
        }),
        exec_engine: crate::config::LiveExecutionEngineConfig {
            reconciliation: false,
            ..Default::default()
        },
        delay_post_stop: Duration::ZERO,
        timeout_disconnection: Duration::from_millis(500),
        ..Default::default()
    };
    let node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_external_msgbus_factory(Box::new(factory))
        .build()
        .unwrap();
    let client = StopRecordingClient {
        steps,
        connected: true,
        fail_disconnect,
        send_on_disconnect,
    };
    node.kernel.data_engine.borrow_mut().register_client(
        DataClientAdapter::new(client.client_id(), None, false, false, Box::new(client)),
        None,
    );
    node
}

#[tokio::test(flavor = "current_thread")]
async fn test_stop_with_inbound_key_writes_record_published_during_disconnect() {
    let (mut node, steps) = stop_recording_node(true, false);

    node.finalize_stop().await.unwrap();
    let seen = steps.lock().clone();
    node.dispose();

    assert!(
        seen.contains(&format!("published {DISCONNECT_TOPIC}")),
        "{seen:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_stop_without_inbound_key_closes_way_out() {
    let (mut node, steps) = stop_recording_node(false, false);
    node.start().await.unwrap();

    node.stop().await.unwrap();
    let seen = steps.lock().clone();
    node.dispose();

    assert_eq!(
        seen,
        vec![
            "client connected".to_string(),
            "client disconnected".to_string(),
            format!("published {DISCONNECT_TOPIC}"),
            format!("published {FINALIZE_TOPIC}"),
            "closed".to_string(),
        ]
    );
}

/// The steps of a started node's stop with an inbound key listed: the client connected, then
/// the read stopped, the client disconnected, its record and the kernel's written, then the way
/// out closed.
fn stop_steps() -> Vec<String> {
    vec![
        "client connected".to_string(),
        "read stopped".to_string(),
        "client disconnected".to_string(),
        format!("published {DISCONNECT_TOPIC}"),
        format!("published {FINALIZE_TOPIC}"),
        "closed".to_string(),
    ]
}

#[tokio::test(flavor = "current_thread")]
async fn test_stop_stops_read_then_disconnects_then_closes_way_out() {
    let (mut node, steps) = stop_recording_node(true, false);
    node.start().await.unwrap();

    node.stop().await.unwrap();
    let seen = steps.lock().clone();
    node.dispose();

    assert_eq!(seen, stop_steps());
}

#[tokio::test(flavor = "current_thread")]
async fn test_stop_closes_way_out_when_disconnect_fails() {
    let (mut node, steps) = stop_recording_node(true, true);
    node.start().await.unwrap();

    let error = node.stop().await.unwrap_err();
    let seen = steps.lock().clone();
    node.dispose();

    assert!(
        error
            .to_string()
            .contains("Failed to disconnect data clients: STOP-RECORDING"),
        "{error}"
    );
    assert_eq!(seen, stop_steps());
}

#[tokio::test(flavor = "current_thread")]
async fn test_dispose_after_stop_closes_way_out_once() {
    let (mut node, steps) = stop_recording_node(true, false);
    node.start().await.unwrap();

    node.stop().await.unwrap();
    node.dispose();
    let seen = steps.lock().clone();

    assert_eq!(seen, stop_steps());
}

/// The instrument of the quote the client sends through the node's data event sender.
const SENT_INSTRUMENT_ID: &str = "SENT.SIM";

/// The quote the client sends through the node's data event sender while it disconnects.
fn sent_quote() -> QuoteTick {
    QuoteTick {
        instrument_id: InstrumentId::from(SENT_INSTRUMENT_ID),
        ..QuoteTick::default()
    }
}

/// The steps of a started node's stop with no inbound key, whose client sent a quote while it
/// disconnected: its record and the kernel's written, the sent quote written by the final drain,
/// then the way out closed, once.
fn sent_stop_steps() -> Vec<String> {
    vec![
        "client connected".to_string(),
        "client disconnected".to_string(),
        format!("published {DISCONNECT_TOPIC}"),
        format!("published {FINALIZE_TOPIC}"),
        format!("published {}", get_quotes_topic(sent_quote().instrument_id)),
        "closed".to_string(),
    ]
}

fn assert_stop_result(result: anyhow::Result<()>, fail_disconnect: bool) {
    if fail_disconnect {
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("Failed to disconnect data clients: STOP-RECORDING"),
            "{error}"
        );
    } else {
        result.unwrap();
    }
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn test_run_writes_record_sent_during_disconnect_before_closing_way_out(
    #[values(false, true)] fail_disconnect: bool,
) {
    let (mut node, steps) = sending_stop_recording_node(false, fail_disconnect, true);
    let handle = node.handle();
    let stop = tokio::spawn(async move {
        while !handle.is_running() {
            tokio::task::yield_now().await;
        }
        handle.stop();
    });

    let result = node.run().await;
    stop.await.unwrap();
    node.dispose();
    let seen = steps.lock().clone();

    assert_stop_result(result, fail_disconnect);
    assert_eq!(seen, sent_stop_steps());
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn test_stop_writes_record_sent_during_disconnect_before_closing_way_out(
    #[values(false, true)] fail_disconnect: bool,
) {
    let (mut node, steps) = sending_stop_recording_node(false, fail_disconnect, true);
    node.start().await.unwrap();

    let result = node.stop().await;
    node.dispose();
    let seen = steps.lock().clone();

    assert_stop_result(result, fail_disconnect);
    assert_eq!(seen, sent_stop_steps());
}

#[tokio::test(flavor = "current_thread")]
async fn test_start_aborted_after_client_connected_closes_way_out_once() {
    let (mut node, steps) = stop_recording_node(false, false);
    node.handle().stop();

    node.start().await.unwrap();
    node.dispose();
    let seen = steps.lock().clone();

    assert_eq!(
        seen,
        vec![
            "client connected".to_string(),
            "client disconnected".to_string(),
            format!("published {DISCONNECT_TOPIC}"),
            format!("published {FINALIZE_TOPIC}"),
            "closed".to_string(),
        ]
    );
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn test_start_failed_at_cache_preparation_closes_way_out_once(
    #[values(false, true)] through_run: bool,
) {
    let (mut node, steps) = stop_recording_node(false, false);
    node.cache_database_factory = Some(Box::new(FailingCacheDatabaseFactory));

    let error = if through_run {
        node.run().await.unwrap_err()
    } else {
        node.start().await.unwrap_err()
    };
    let seen = steps.lock().clone();
    node.dispose();
    let seen_after_dispose = steps.lock().clone();

    assert_eq!(
        format!("{error:#}"),
        "failed to create cache database backing: cache database unavailable"
    );
    assert_eq!(seen, vec!["closed".to_string()]);
    assert_eq!(seen_after_dispose, seen);
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn test_run_aborted_start_writes_record_sent_during_disconnect_before_closing_way_out(
    #[values(false, true)] fail_disconnect: bool,
) {
    let (mut node, steps) = sending_stop_recording_node(false, fail_disconnect, true);
    node.handle().stop();

    let result = node.run().await;
    let seen = steps.lock().clone();
    node.dispose();
    let seen_after_dispose = steps.lock().clone();

    assert_stop_result(result, fail_disconnect);
    assert_eq!(seen, sent_stop_steps());
    assert_eq!(seen_after_dispose, seen);
}

/// The topic published on after an entry point refused to act.
const AFTER_REFUSAL_TOPIC: &str = "data.quotes.AFTER-REFUSAL";

#[tokio::test(flavor = "current_thread")]
async fn test_start_refused_while_running_keeps_way_out_open() {
    let (mut node, steps) = stop_recording_node(false, false);
    node.start().await.unwrap();

    let error = node.start().await.unwrap_err();
    msgbus::publish_quote(AFTER_REFUSAL_TOPIC.into(), &QuoteTick::default());
    let seen = steps.lock().clone();
    node.stop().await.unwrap();
    node.dispose();

    assert_eq!(error.to_string(), "Already running");
    assert_eq!(
        seen,
        vec![
            "client connected".to_string(),
            format!("published {AFTER_REFUSAL_TOPIC}"),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_stop_refused_before_start_keeps_way_out_open() {
    let (mut node, steps) = stop_recording_node(false, false);

    let error = node.stop().await.unwrap_err();
    let seen_after_refusal = steps.lock().clone();
    node.start().await.unwrap();
    msgbus::publish_quote(AFTER_REFUSAL_TOPIC.into(), &QuoteTick::default());
    let seen = steps.lock().clone();
    node.stop().await.unwrap();
    node.dispose();

    assert_eq!(error.to_string(), "Not running");
    assert!(seen_after_refusal.is_empty(), "{seen_after_refusal:?}");
    assert_eq!(
        seen,
        vec![
            "client connected".to_string(),
            format!("published {AFTER_REFUSAL_TOPIC}"),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_start_that_succeeds_keeps_way_out_open() {
    let (mut node, steps) = stop_recording_node(false, false);

    node.start().await.unwrap();
    let seen = steps.lock().clone();
    node.stop().await.unwrap();
    node.dispose();

    assert_eq!(seen, vec!["client connected".to_string()]);
}

/// What ends a start after its client connected.
#[derive(Clone, Copy, Debug)]
enum StartFailure {
    StopRequested,
    TraderStart,
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn test_failed_start_writes_record_sent_during_disconnect_before_closing_way_out(
    #[values(StartFailure::StopRequested, StartFailure::TraderStart)] failure: StartFailure,
) {
    let (mut node, steps) = sending_stop_recording_node(false, false, true);
    match failure {
        StartFailure::StopRequested => node.handle().stop(),
        StartFailure::TraderStart => {
            node.add_actor(StartFailingActor::new()).unwrap();
        }
    }

    let result = node.start().await;
    let seen = steps.lock().clone();
    node.dispose();
    let seen_after_dispose = steps.lock().clone();

    match failure {
        StartFailure::StopRequested => result.unwrap(),
        StartFailure::TraderStart => {
            let error = result.unwrap_err().to_string();
            assert!(error.contains(START_REFUSED), "{error}");
        }
    }
    assert_eq!(seen, sent_stop_steps());
    assert_eq!(seen_after_dispose, seen);
}

#[tokio::test(flavor = "current_thread")]
async fn test_start_after_failed_start_closed_way_out_is_refused() {
    let (mut node, steps) = stop_recording_node(false, false);
    node.cache_database_factory = Some(Box::new(FailingOnceCacheDatabaseFactory::default()));

    let failure = node.start().await.unwrap_err();
    let refusal = node.start().await.unwrap_err();
    let state = node.state();
    let cache_pending = node.has_pending_cache_database();
    let seen = steps.lock().clone();
    node.dispose();

    assert_eq!(
        format!("{failure:#}"),
        "failed to create cache database backing: cache database unavailable"
    );
    assert_eq!(refusal.to_string(), EXTERNAL_EGRESS_CLOSED);
    assert_eq!(state, NodeState::Idle);
    assert!(cache_pending);
    assert_eq!(seen, vec!["closed".to_string()]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_start_and_run_after_stop_closed_way_out_are_refused() {
    let (mut node, steps) = stop_recording_node(false, false);
    node.start().await.unwrap();
    node.stop().await.unwrap();
    let seen_after_stop = steps.lock().clone();

    let start_refusal = node.start().await.unwrap_err();
    // A stop is asked first, so a run that is not refused ends instead of running on
    node.handle().stop();
    let run_refusal = node.run().await.unwrap_err();
    let state = node.state();
    let seen = steps.lock().clone();
    node.dispose();

    assert_eq!(start_refusal.to_string(), EXTERNAL_EGRESS_CLOSED);
    assert_eq!(run_refusal.to_string(), EXTERNAL_EGRESS_CLOSED);
    assert_eq!(state, NodeState::Stopped);
    assert_eq!(seen, seen_after_stop);
}

#[tokio::test(flavor = "current_thread")]
async fn test_start_without_way_out_keeps_upstream_retry_and_restart() {
    let config = LiveNodeConfig {
        exec_engine: crate::config::LiveExecutionEngineConfig {
            reconciliation: false,
            ..Default::default()
        },
        delay_post_stop: Duration::ZERO,
        ..Default::default()
    };
    let mut node = LiveNode::build("RetryNode".to_string(), Some(config)).unwrap();
    node.cache_database_factory = Some(Box::new(FailingOnceCacheDatabaseFactory::default()));

    let failure = node.start().await.unwrap_err();
    let cache_pending_after_failure = node.has_pending_cache_database();
    node.start().await.unwrap();
    let state_after_retry = node.state();
    let cache_pending_after_retry = node.has_pending_cache_database();
    node.stop().await.unwrap();
    // Upstream lets a stopped node start again, and its trader then refuses the start
    let restart = node.start().await.unwrap_err();
    node.dispose();

    assert_eq!(
        format!("{failure:#}"),
        "failed to create cache database backing: cache database unavailable"
    );
    assert!(cache_pending_after_failure);
    assert_eq!(state_after_retry, NodeState::Running);
    assert!(!cache_pending_after_retry);
    assert_eq!(
        restart.to_string(),
        "Failed to start trader: Invalid state trigger STOPPED -> START"
    );
}

/// The entry point that leaves the node not running.
#[derive(Clone, Copy, Debug)]
enum NodeExit {
    Stop,
    Run,
    FailedStart,
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn test_entry_point_returns_after_its_awaited_close_without_blocking_its_thread(
    #[values(NodeExit::Stop, NodeExit::Run, NodeExit::FailedStart)] exit: NodeExit,
) {
    let (begun_tx, begun_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let steps = StopSteps::default();
    let factory = StopRecordingBackingFactory {
        steps: steps.clone(),
        close_gate: Mutex::new(Some(CloseGate {
            begun: begun_tx,
            release: release_rx,
        })),
        receiver: Mutex::default(),
    };
    let mut node = recording_node(false, false, true, factory, LoggerConfig::default());
    // Another task on the node's one thread: it runs only while the node's thread is free
    let released = Arc::new(AtomicBool::new(false));
    tokio::spawn({
        let released = released.clone();
        async move {
            if begun_rx.await.is_ok() {
                released.store(true, Ordering::SeqCst);
                release_tx.send(()).unwrap();
            }
        }
    });

    let result = match exit {
        NodeExit::Stop => {
            node.start().await.unwrap();
            node.stop().await
        }
        NodeExit::Run => {
            let handle = node.handle();
            let stop = tokio::spawn(async move {
                while !handle.is_running() {
                    tokio::task::yield_now().await;
                }
                handle.stop();
            });
            let result = node.run().await;
            stop.await.unwrap();
            result
        }
        NodeExit::FailedStart => {
            node.cache_database_factory = Some(Box::new(FailingCacheDatabaseFactory));
            node.start().await
        }
    };
    let released_before_return = released.load(Ordering::SeqCst);
    let seen = steps.lock().clone();
    node.dispose();
    let seen_after_dispose = steps.lock().clone();

    match exit {
        NodeExit::Stop | NodeExit::Run => {
            result.unwrap();
            assert_eq!(seen, sent_stop_steps());
        }
        NodeExit::FailedStart => {
            assert_eq!(
                format!("{:#}", result.unwrap_err()),
                "failed to create cache database backing: cache database unavailable"
            );
            assert_eq!(seen, vec!["closed".to_string()]);
        }
    }
    assert!(released_before_return);
    assert_eq!(seen_after_dispose, seen);
}

/// The topic published on after the inbound read ended.
const AFTER_READ_END_TOPIC: &str = "data.quotes.AFTER-READ-END";

/// A node with an inbound key listed whose read delivers what the returned sender sends and ends
/// when it is dropped, logging to a file in the returned folder.
fn reading_node() -> (
    LiveNode,
    StopSteps,
    tokio::sync::mpsc::Sender<BusMessage>,
    std::path::PathBuf,
) {
    let directory = std::env::temp_dir().join(format!("nautilus-inbound-read-{}", UUID4::new()));
    let logging = LoggerConfig {
        fileout_level: LevelFilter::Info,
        file_config: Some(FileWriterConfig {
            directory: Some(directory.to_str().unwrap().to_string()),
            file_name: Some("inbound-read".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let steps = StopSteps::default();
    let factory = StopRecordingBackingFactory {
        steps: steps.clone(),
        close_gate: Mutex::default(),
        receiver: Mutex::new(Some(receiver)),
    };
    let node = recording_node(true, false, false, factory, logging);
    (node, steps, sender, directory)
}

/// The node's error lines in the log of [`reading_node`] in `directory`, which it then removes.
fn node_errors(directory: &std::path::Path) -> Vec<String> {
    let errors = node_lines(directory, "[ERROR]");
    std::fs::remove_dir_all(directory).unwrap();
    errors
}

/// The node's lines at `level` in the log of [`reading_node`] in `directory`.
fn node_lines(directory: &std::path::Path, level: &str) -> Vec<String> {
    logging_sync_to_disk().unwrap();
    let output = std::fs::read_to_string(directory.join("inbound-read.log")).unwrap();
    output
        .lines()
        .filter(|line| line.contains(level))
        .filter_map(|line| {
            line.split_once(".nautilus_live::node: ")
                .map(|(_, text)| text.to_string())
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn test_run_logs_one_error_when_its_inbound_read_ends_unasked() {
    let (mut node, steps, sender, directory) = reading_node();
    let handle = node.handle();
    let control = tokio::spawn({
        let steps = steps.clone();
        async move {
            while !handle.is_running() {
                tokio::task::yield_now().await;
            }
            drop(sender);
            while !steps.lock().contains(&"read stopped".to_string()) {
                tokio::task::yield_now().await;
            }
            msgbus::publish_quote(AFTER_READ_END_TOPIC.into(), &QuoteTick::default());
            let running = handle.is_running();
            let seen = steps.lock().clone();
            handle.stop();
            (running, seen)
        }
    });

    let result = node.run().await;
    let (running_after_end, seen_after_end) = control.await.unwrap();
    node.dispose();
    let errors = node_errors(&directory);

    result.unwrap();
    assert!(running_after_end);
    assert_eq!(
        seen_after_end,
        vec![
            "client connected".to_string(),
            "read stopped".to_string(),
            format!("published {AFTER_READ_END_TOPIC}"),
        ]
    );
    assert_eq!(
        errors,
        vec![
            "External message bus ingress read ended unexpectedly: inbound messages are no longer received"
                .to_string()
        ]
    );
}

// `run` drops its receiver before its stop stops the read, so this stop never reaches the end of
// the read: `test_inbound_read_end_after_the_node_stopped_the_read_logs_info` holds that end.
#[tokio::test(flavor = "current_thread")]
async fn test_run_stopped_by_its_handle_stops_its_read_and_logs_no_error() {
    let (mut node, steps, sender, directory) = reading_node();
    let handle = node.handle();
    let stop = tokio::spawn(async move {
        while !handle.is_running() {
            tokio::task::yield_now().await;
        }
        handle.stop();
    });

    let result = node.run().await;
    stop.await.unwrap();
    drop(sender);
    let seen = steps.lock().clone();
    node.dispose();
    let errors = node_errors(&directory);

    result.unwrap();
    assert!(seen.contains(&"read stopped".to_string()), "{seen:?}");
    assert_eq!(errors, Vec::<String>::new());
}

// The node's own order at a stop: it stops the read, then the receiver ends. No entry point
// reaches the end in that order today (`run` drops its receiver first), so the steps are driven
// here as the run loop's end arm meets them.
#[tokio::test(flavor = "current_thread")]
async fn test_inbound_read_end_after_the_node_stopped_the_read_logs_info() {
    let (mut node, steps, sender, directory) = reading_node();
    let mut receiver = node.take_external_ingress_receiver().unwrap().unwrap();

    node.close_external_ingress();
    drop(sender);
    let end = receiver.recv().await;
    node.end_external_ingress();
    let seen = steps.lock().clone();
    let infos = node_lines(&directory, "[INFO]");
    node.dispose();
    let errors = node_errors(&directory);

    assert!(end.is_none());
    assert_eq!(seen, vec!["read stopped".to_string()]);
    assert_eq!(
        infos
            .iter()
            .filter(|line| *line == "External message bus ingress closed")
            .count(),
        1,
        "{infos:?}"
    );
    assert_eq!(errors, Vec::<String>::new());
}

/// The error of [`StartFailingActor`]'s start.
const START_REFUSED: &str = "actor refused to start";

/// An actor whose start fails, so the trader's start fails.
#[derive(Debug)]
struct StartFailingActor {
    core: DataActorCore,
}

impl StartFailingActor {
    fn new() -> Self {
        Self {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some(ActorId::from("START-FAILING-ACTOR")),
                ..Default::default()
            }),
        }
    }
}

impl DataActor for StartFailingActor {
    fn on_start(&mut self) -> anyhow::Result<()> {
        anyhow::bail!(START_REFUSED)
    }
}

nautilus_actor!(StartFailingActor);

/// A cache database factory whose database does not answer the first time, and answers after.
#[derive(Debug, Default)]
struct FailingOnceCacheDatabaseFactory {
    failed: AtomicBool,
}

#[async_trait::async_trait]
impl CacheDatabaseFactory for FailingOnceCacheDatabaseFactory {
    async fn create(
        &self,
        _trader_id: TraderId,
        _instance_id: UUID4,
        _config: nautilus_common::cache::CacheConfig,
    ) -> anyhow::Result<Box<dyn CacheDatabaseAdapter>> {
        if !self.failed.swap(true, Ordering::SeqCst) {
            anyhow::bail!("cache database unavailable");
        }
        let (database, _control) = nautilus_testkit::cache::TestCacheDatabaseControl::create();
        Ok(Box::new(database))
    }
}

/// A cache database factory whose database does not answer.
#[derive(Debug)]
struct FailingCacheDatabaseFactory;

#[async_trait::async_trait]
impl CacheDatabaseFactory for FailingCacheDatabaseFactory {
    async fn create(
        &self,
        _trader_id: TraderId,
        _instance_id: UUID4,
        _config: nautilus_common::cache::CacheConfig,
    ) -> anyhow::Result<Box<dyn CacheDatabaseAdapter>> {
        anyhow::bail!("cache database unavailable")
    }
}

struct StopRecordingBackingFactory {
    steps: StopSteps,
    close_gate: Mutex<Option<CloseGate>>,
    receiver: Mutex<Option<tokio::sync::mpsc::Receiver<BusMessage>>>,
}

/// Holds a backing's awaited close until the test releases it: `begun` is sent when the close
/// begins, and the close records "closed" only after `release` is sent.
struct CloseGate {
    begun: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

impl Debug for StopRecordingBackingFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(StopRecordingBackingFactory))
            .finish_non_exhaustive()
    }
}

impl MessageBusBackingFactory for StopRecordingBackingFactory {
    fn create(
        &self,
        _trader_id: TraderId,
        _instance_id: UUID4,
        _config: MessageBusConfig,
    ) -> anyhow::Result<Box<dyn MessageBusBacking>> {
        Ok(Box::new(StopRecordingBacking {
            steps: self.steps.clone(),
            closed: false,
            close_gate: self.close_gate.lock().take(),
            receiver: self.receiver.lock().take(),
        }))
    }
}

struct StopRecordingBacking {
    steps: StopSteps,
    closed: bool,
    close_gate: Option<CloseGate>,
    receiver: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
}

impl MessageBusBacking for StopRecordingBacking {
    fn is_closed(&self) -> bool {
        self.closed
    }

    fn publish(&self, message: BusMessage) {
        self.steps
            .lock()
            .push(format!("published {}", message.topic));
    }

    fn stop_read(&mut self) {
        self.steps.lock().push("read stopped".to_string());
    }

    fn take_receiver(&mut self) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
        self.receiver
            .take()
            .ok_or_else(|| anyhow::anyhow!("external ingress receiver unavailable"))
    }

    fn close(&mut self) {
        self.closed = true;
        self.steps.lock().push("closed".to_string());
    }

    fn begin_close(&mut self) -> futures_util::future::LocalBoxFuture<'static, ()> {
        let Some(CloseGate { begun, release }) = self.close_gate.take() else {
            self.close();
            return Box::pin(std::future::ready(()));
        };
        self.closed = true;
        let steps = self.steps.clone();
        begun.send(()).unwrap();

        Box::pin(async move {
            release.await.unwrap();
            steps.lock().push("closed".to_string());
        })
    }
}

#[derive(Debug)]
struct StopRecordingClient {
    steps: StopSteps,
    connected: bool,
    fail_disconnect: bool,
    send_on_disconnect: bool,
}

#[async_trait::async_trait(?Send)]
impl DataClient for StopRecordingClient {
    fn client_id(&self) -> ClientId {
        ClientId::from("STOP-RECORDING")
    }

    fn venue(&self) -> Option<Venue> {
        None
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    // The kernel's finalize stops the engines' clients, so this record is the kernel's
    fn stop(&mut self) -> anyhow::Result<()> {
        msgbus::publish_quote(FINALIZE_TOPIC.into(), &QuoteTick::default());
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn is_disconnected(&self) -> bool {
        !self.connected
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.steps.lock().push("client connected".to_string());
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.steps.lock().push("client disconnected".to_string());
        msgbus::publish_quote(DISCONNECT_TOPIC.into(), &QuoteTick::default());

        if self.send_on_disconnect {
            get_data_event_sender()
                .send(DataEvent::Data(Data::Quote(sent_quote())))
                .unwrap();
        }

        self.connected = false;

        if self.fail_disconnect {
            anyhow::bail!("disconnect refused");
        }
        Ok(())
    }
}
