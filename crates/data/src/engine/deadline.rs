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

//! The deadline of a joined (`cut_warmup`) request: the engine's outer bound over all its legs.
//!
//! Only the engine can see that a source died: a joined request whose source never answers would
//! hold its instrument's live trades for ever. One clock timer is armed at the earliest deadline,
//! as option-chain requests arm theirs. A request's completion and its expiry claim the same
//! `request_deadlines` entry, so exactly one of them finishes the request.

use super::{
    DataEngine, DataResponseEnvelope, Debug, Rc, RefCell, TimeEvent, TimeEventCallback, UUID4,
    UnixNanos, WeakCell,
};

/// The engine clock's timer for the earliest joined-request deadline.
const JOIN_DEADLINE_TIMER: &str = "join-deadline";

/// The reason an expired joined request's one terminal leads with.
const JOIN_DEADLINE_REASON: &str = "deadline";

impl DataEngine {
    /// Returns the count of envelopes dropped because their joined request had expired.
    #[must_use]
    pub const fn late_envelopes(&self) -> u64 {
        self.late_envelopes
    }

    /// Arms the `deadline` of the joined request `request_id` before any of its legs is dispatched.
    ///
    /// # Errors
    ///
    /// Returns an error, refusing the request, when the deadline timer cannot be registered.
    pub(super) fn arm_join_deadline(
        &mut self,
        request_id: UUID4,
        deadline: UnixNanos,
    ) -> anyhow::Result<()> {
        self.request_deadlines.insert(request_id, deadline);
        if self.schedule_join_deadline_timer() {
            return Ok(());
        }

        self.claim_join_deadline(&request_id);
        anyhow::bail!(
            "Cannot handle the `cut_warmup` request {request_id}: its deadline {deadline} could \
             not be armed, the engine's timer did not register"
        )
    }

    /// Claims the deadline of `request_id` for its completion or its dispatch failure, so its
    /// expiry cannot finish it too; the timer moves to the next deadline or is cancelled.
    pub(super) fn claim_join_deadline(&mut self, request_id: &UUID4) {
        if self.request_deadlines.remove(request_id).is_some() {
            self.schedule_join_deadline_timer();
        }
    }

    fn schedule_join_deadline_timer(&self) -> bool {
        let Some(deadline) = self.request_deadlines.values().copied().min() else {
            self.clock.borrow_mut().cancel_timer(JOIN_DEADLINE_TIMER);
            return true;
        };

        if self.clock.borrow().next_time_ns(JOIN_DEADLINE_TIMER) == Some(deadline) {
            return true;
        }

        let Some(timer) = self.join_deadline_timer.clone() else {
            log::error!(
                "Cannot schedule the join deadline: data engine message bus handlers are not registered"
            );
            return false;
        };

        let callback_fn: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_| {
            timer.handle_timeout();
        });

        if let Err(e) = self.clock.borrow_mut().set_time_alert_ns(
            JOIN_DEADLINE_TIMER,
            deadline,
            Some(TimeEventCallback::from(callback_fn)),
            Some(true),
        ) {
            log::error!("Failed to schedule the join deadline: {e}");
            return false;
        }

        true
    }

    fn handle_join_deadline_timeout(&mut self) {
        self.expire_overdue_joins();
        self.schedule_join_deadline_timer();
    }

    /// Expires every joined request whose deadline the clock has reached.
    ///
    /// Runs at each response ingress before any payload effect, so an envelope queued ahead of
    /// the timer's event finds its request already expired.
    pub(super) fn expire_overdue_joins(&mut self) {
        if self.request_deadlines.is_empty() {
            return;
        }

        let now = self.clock.borrow().timestamp_ns();
        let mut overdue: Vec<(UnixNanos, UUID4)> = self
            .request_deadlines
            .iter()
            .filter(|&(_, &deadline)| deadline <= now)
            .map(|(&request_id, &deadline)| (deadline, request_id))
            .collect();

        if overdue.is_empty() {
            return;
        }

        overdue.sort_unstable_by_key(|&(deadline, request_id)| (deadline, request_id.to_string()));

        for (deadline, request_id) in overdue {
            // An expiry finishing an earlier request may already have expired this one
            if self.request_deadlines.remove(&request_id).is_none() {
                continue;
            }
            self.expire_joined_request(request_id, deadline);
        }

        self.schedule_join_deadline_timer();
    }

    /// Expires the joined request `parent_id` at its `deadline`.
    ///
    /// The slots its legs already filled are rebuilt as on completion (the open end kept) into
    /// one completed envelope whose reason leads with "deadline", followed by the reasons the
    /// legs already carry. It then takes the ordinary route: the held live trades are released
    /// through the cut and floor against what was answered, and an interleaving request is
    /// finalized by its entry. Each unfinished leg stays suppressed until its own terminal.
    fn expire_joined_request(&mut self, parent_id: UUID4, deadline: UnixNanos) {
        let unfinished: Vec<UUID4> = self
            .request_pipeline_parent_request_id
            .iter()
            .filter(|(_, (parent, _))| *parent == parent_id)
            .map(|(leg_id, _)| *leg_id)
            .collect();

        for leg_id in &unfinished {
            self.request_pipeline_parent_request_id.remove(leg_id);
            self.inflight_requests.remove(leg_id);
            self.suppressed_legs.insert(*leg_id);
        }

        log::warn!(
            "The `cut_warmup` request {parent_id} reached its deadline {deadline} with {} legs \
             unfinished",
            unfinished.len(),
        );

        let slots = self
            .request_pipeline_responses
            .remove(&parent_id)
            .unwrap_or_default();
        self.request_pipeline_n_components.remove(&parent_id);
        let parent = self.request_pipeline_parent_request.remove(&parent_id);
        let envelope = self.finish_request_pipeline(
            parent_id,
            parent.as_ref(),
            slots,
            Some(JOIN_DEADLINE_REASON),
        );
        self.process_response_envelope(envelope);
    }

    /// Drops an envelope of a leg suppressed by its request's expiry, and counts it. The leg's
    /// own terminal ends its suppression. Returns whether the envelope was dropped.
    pub(super) fn drop_late_envelope(&mut self, envelope: &DataResponseEnvelope) -> bool {
        let leg_id = envelope.correlation_id();
        if !self.suppressed_legs.contains(leg_id) {
            return false;
        }

        if envelope.completed() {
            self.suppressed_legs.remove(leg_id);
        }
        self.late_envelopes += 1;
        log::warn!(
            "Dropped a late envelope of the expired join leg {leg_id} (completed: {}, {} late \
             envelopes)",
            envelope.completed(),
            self.late_envelopes,
        );
        true
    }
}

/// Holds the engine weakly for the deadline timer's callback.
#[derive(Debug)]
pub(super) struct JoinDeadlineTimer {
    engine: WeakCell<DataEngine>,
}

impl JoinDeadlineTimer {
    pub(super) fn new(engine: &Rc<RefCell<DataEngine>>) -> Self {
        Self {
            engine: WeakCell::from(Rc::downgrade(engine)),
        }
    }

    fn handle_timeout(&self) {
        if let Some(engine) = self.engine.upgrade() {
            engine.borrow_mut().handle_join_deadline_timeout();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clock::VirtualClock,
        messages::data::{
            DataResponse, DataResponseEnvelope, RequestCommand, RequestTrades, TradesResponse,
        },
        msgbus::{self, MessageBus, TypedHandler},
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        data::TradeTick,
        enums::AggressorSide,
        identifiers::{ClientId, TradeId, TraderId},
        instruments::{InstrumentAny, stubs::audusd_sim},
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use crate::engine::DataEngine;

    fn trades_envelope(request_id: UUID4, ts: u64, completed: bool) -> DataResponseEnvelope {
        let trade = TradeTick::new(
            audusd_sim().id,
            Price::from("0.65000"),
            Quantity::from(1),
            AggressorSide::Buy,
            TradeId::new(ts.to_string()),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        );
        DataResponseEnvelope::new(
            DataResponse::Trades(TradesResponse::new(
                request_id,
                ClientId::new("SIM"),
                audusd_sim().id,
                vec![trade],
                None,
                Some(UnixNanos::from(ts)),
                UnixNanos::from(ts),
                None,
            )),
            completed,
            None,
        )
    }

    #[rstest]
    fn test_expiry_reason_leads_with_deadline_then_the_reasons_its_legs_carry() {
        let _msgbus = MessageBus::new(TraderId::new("TESTER-001"), UUID4::new(), None, None)
            .register_message_bus();
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CurrencyPair(audusd_sim()))
            .unwrap();
        let engine = Rc::new(RefCell::new(DataEngine::new(
            Rc::<RefCell<VirtualClock>>::clone(&clock),
            cache,
            None,
        )));
        DataEngine::register_msgbus_handlers(&engine);
        let params = serde_json::from_value(serde_json::json!({ "cut_warmup": true })).unwrap();
        let parent = RequestCommand::Trades(RequestTrades::new(
            audusd_sim().id,
            None,
            None,
            None,
            Some(ClientId::new("SIM")),
            UUID4::new(),
            UnixNanos::default(),
            Some(params),
        ));
        let parent_id = *parent.request_id();
        let delivered = Rc::new(RefCell::new(Vec::new()));
        let recorded = Rc::clone(&delivered);
        msgbus::register_response_handler(
            &parent_id,
            TypedHandler::from(move |envelope: &DataResponseEnvelope| {
                let trades = match envelope.response() {
                    Some(DataResponse::Trades(resp)) => Some((resp.data.len(), resp.end)),
                    _ => None,
                };
                recorded.borrow_mut().push((
                    trades,
                    envelope.completed(),
                    envelope.failure_reason().map(str::to_owned),
                ));
            }),
        );
        let (gave_up, unfinished) = (UUID4::new(), UUID4::new());
        {
            let mut engine = engine.borrow_mut();
            engine.new_request_pipeline(parent, 2);
            engine.register_request_pipeline_leg(gave_up, parent_id);
            engine.register_request_pipeline_leg(unfinished, parent_id);
            engine
                .arm_join_deadline(parent_id, UnixNanos::from(10))
                .unwrap();
            engine.response_envelope(DataResponseEnvelope::terminal(
                gave_up,
                Some("a made-up source gave up".to_owned()),
            ));
            engine.response_envelope(trades_envelope(unfinished, 5, false));
        }
        assert!(delivered.borrow().is_empty());

        let events = clock.borrow_mut().advance_time(UnixNanos::from(10), true);
        let handlers = clock.borrow().match_handlers(events);
        for handler in handlers {
            handler.callback.call(handler.event);
        }

        assert_eq!(
            *delivered.borrow(),
            vec![(
                Some((1, None)),
                true,
                Some("deadline; a made-up source gave up".to_owned()),
            )]
        );
        let engine = engine.borrow();
        assert!(engine.request_deadlines.is_empty());
        assert_eq!(
            engine.suppressed_legs.iter().collect::<Vec<_>>(),
            [&unfinished]
        );
        assert_eq!(engine.request_pipeline_count(), 0);
    }
}
