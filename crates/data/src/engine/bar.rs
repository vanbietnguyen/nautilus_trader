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

use std::fmt::Debug;

use anyhow::Context;
use jiff::tz::TimeZone;
use nautilus_common::{
    cache::Cache,
    clock::{Clock, VirtualClock},
    messages::data::{
        BarsResponse, DataResponseEnvelope, QuotesResponse, ReleaseKeptBars, RequestQuotes,
        RequestTrades, SubscribeBars, SubscribeCommand, TradesResponse,
    },
    msgbus::{self, MStr, Topic, TypedHandler},
};
use nautilus_core::{DurationNanos, Params, UUID4, UnixNanos, datetime::get_timezone};
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    identifiers::{ClientId, InstrumentId},
};

use super::{
    AggregationSource, BAR_AGGREGATOR_PRIORITY, BarAggregation, BarAggregator, BarBarHandler,
    BarQuoteHandler, BarTradeHandler, DataCommand, DataEngine, DataResponse,
    DeepChartsRangeBarAggregator, Instrument, InstrumentAny, PriceType, RANGE_BAR_DEFAULT_BOUNDARY,
    RANGE_BAR_DEFAULT_TIME_ZONE, RangeInclusiveBarAggregator, Rc, RefCell, RenkoBarAggregator,
    RequestBarAggregation, RequestCommand, SubscribeQuotes, SubscribeTrades, TickBarAggregator,
    TickImbalanceBarAggregator, TickRunsBarAggregator, TimeBarAggregator, UnsubscribeBars,
    UnsubscribeCommand, UnsubscribeQuotes, UnsubscribeTrades, ValueBarAggregator,
    ValueImbalanceBarAggregator, ValueRunsBarAggregator, VolumeBarAggregator,
    VolumeImbalanceBarAggregator, VolumeRunsBarAggregator,
    handlers::{JoinedRelease, JoinedSlot, JoinedTradeHandler, WarmupBuffer},
    has_continuous_future_params, has_time_range_pipeline_params, log_error_on_cache_insert,
    process_engine_bar, request_bar_aggregation_from_params, request_params,
    requests::{
        FILTER_EXPR, INTERLEAVE, INTERLEAVE_REFUSED, KEPT_ANSWERED_BARE, KEPT_REFUSED,
        UPDATE_SUBSCRIPTIONS, carries_quotes, time_zone_from_params,
    },
    responses::{parent_request_window, quotes_part},
    switchboard,
    time_range::TIME_RANGE_POINT_DATA,
};

/// An interleaving request's own state, from before its aggregators start until its bars and its
/// one terminal are out.
#[derive(Debug, Default)]
pub(super) struct InterleavedRequest {
    /// Bars its aggregators completed and not yet delivered.
    bars: Rc<RefCell<Vec<Bar>>>,
    /// The client the request was dispatched to, once resolved.
    client_id: Option<ClientId>,
    /// The request's window and parameters, carried by bars delivered without a response.
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    params: Option<Params>,
}

/// The replay of a request that builds bars: one clock that its builders run on as a
/// subscription's builders run on the engine clock, driven by the engine as a backtest drives
/// its clock (see [`DataEngine::replay_until`]). A pipeline's replay is its parent's: every
/// child answer drives it.
///
/// It records, for each builder, what drives its clock (this replay), where its bars go (the
/// answer of the request in flight, through the output handler set up with that request: parts
/// of an interleaving request's answer, otherwise the cache; between requests nowhere) and how
/// long it lives: a request's own builders end with it; builders kept by request
/// (`update_subscriptions`, keyed by their bar types alone) outlive it, idle in
/// `DataEngine::kept_replays`, until a later request continues the replay (see
/// [`DataEngine::admit_kept_request`]), a subscription takes them over (see
/// [`DataEngine::hand_over_kept_builders`]) or the request claiming the replay releases them (see
/// [`DataEngine::release_kept_bars`]). Nothing asks a builder whether it is historical.
#[derive(Debug)]
pub(super) struct RequestReplay {
    clock: Rc<RefCell<VirtualClock>>,
    /// The builders it drives, in the request's order.
    builders: Vec<BarAggregatorKey>,
    /// Whether its builders' timers are armed: at the request's `start`, or without one at its
    /// first record.
    armed: bool,
    /// The instant its builders' timers were armed at, once armed.
    opened: Option<UnixNanos>,
    /// Through when its last completed answer drove it (see
    /// [`DataEngine::end_replay_coverage`]): a kept replay is continued from the next instant.
    /// `None` while a request is in flight, or when its last answer fired no end of coverage.
    covered: Option<UnixNanos>,
    /// The request's `end`: its answer covers no later.
    end: Option<UnixNanos>,
    /// The engine clock's time when the request was dispatched: its answer covers no later.
    dispatched: UnixNanos,
    /// Whether its answer may not hold every record of its window, so its end of coverage fires
    /// nothing: the request named a `limit`, a filter expression or `point_data` (see
    /// [`window_not_wholly_known`]), or its answer, or a pipeline child's, came as a bare
    /// response (see [`DataEngine::mark_bare_response`]). A kept replay so marked is removed
    /// when its request ends, not kept.
    partial: bool,
    /// The request that started it or last continued it: the one whose ID releases its kept
    /// builders (see [`DataEngine::release_kept_bars`]). A refused request never changes it.
    claim: UUID4,
    /// The kept builders its claiming request released while that request was in flight (see
    /// [`DataEngine::release_kept_bars`]): the request keeps them to its end, then they are
    /// removed, not kept, unless a subscription held back on one takes it over then.
    released: Vec<BarAggregatorKey>,
}

/// Kept builders of one replay handed over to their subscriptions in one engine turn: the
/// builders whose subscriptions were held back while a request ran, at its successful end (see
/// [`DataEngine::release_kept_builders`]), or the idle builders one subscribe command adopts (see
/// [`DataEngine::adopt_kept_builders`]). They leave the replay together, on one clock of their
/// own (see [`DataEngine::hand_over_clock`]), and catch up together, in deadline order (time,
/// then timer name). A builder adopted by a later subscribe command catches up alone. The
/// command's other bar types are prepared after the idle builders it adopts have caught up, so
/// a composite among them takes none of their catch-up bars; a composite subscribed in an earlier
/// command does take them.
#[derive(Debug)]
#[must_use]
pub(super) struct HandOver {
    bar_types: Vec<BarType>,
    clock: Rc<RefCell<VirtualClock>>,
    /// Their held live records not fed yet (see [`DataEngine::take_held_records`]).
    held: Vec<HeldRecord>,
}

/// What the end of a request whose builders are kept by request leaves to do once its last bars
/// and its terminal are out (see [`DataEngine::release_request_bar_aggregators`]), so nothing a
/// subscription is given comes before the answer's own end.
#[derive(Debug)]
#[must_use]
pub(super) enum Release {
    /// After a success: its kept builders whose subscriptions were held back, to hand over.
    HandOver(HandOver),
    /// After anything else: the new builders of its held-back subscriptions, to feed.
    Restart(Restart),
}

/// The new builders started for the subscriptions held back on the kept builders of a request
/// that failed, expired or was cleaned up (see [`DataEngine::remove_kept_builders`]), with the
/// live records held for them, not fed yet (see [`DataEngine::feed_restarted`]).
#[derive(Debug)]
#[must_use]
pub(super) struct Restart {
    /// Each builder's copy of each held trade, in the request's order.
    held_trades: Vec<(BarType, Vec<Option<TradeTick>>)>,
    /// Each restarted bar type with its held quotes or bars, sources first.
    restarted: Vec<(BarType, Option<BarAggregatorWarmup>)>,
}

/// One live record held for kept builders that leave their replay (see
/// [`DataEngine::take_held_records`]), with its arrival (see [`WarmupBuffer`]).
#[derive(Debug)]
enum HeldRecord {
    /// Each builder's copy of one held trade, in the order live gave them the copies.
    Trade(u64, Vec<(BarType, TradeTick)>),
    Quote(u64, BarType, QuoteTick),
    Bar(u64, BarType, Bar),
}

impl HeldRecord {
    const fn arrival(&self) -> u64 {
        match self {
            Self::Trade(arrival, _) | Self::Quote(arrival, ..) | Self::Bar(arrival, ..) => *arrival,
        }
    }

    /// Returns the instant its builders' clock is readied for before it is fed: for a trade, its
    /// latest copy's.
    fn ts_init(&self) -> UnixNanos {
        match self {
            Self::Trade(_, copies) => copies
                .iter()
                .map(|(_, copy)| copy.ts_init)
                .max()
                .unwrap_or_default(),
            Self::Quote(_, _, quote) => quote.ts_init,
            Self::Bar(_, _, bar) => bar.ts_init,
        }
    }
}

/// The completed answer of an interleaving trades request, delivered by
/// [`DataEngine::finish_interleaved`] after the bars its clean-up completes.
pub(super) struct ClosingPart<'a> {
    /// The completed response, whose identity and window the last parts carry.
    resp: &'a TradesResponse,
    /// Its trades after its last delivered bar.
    pending: Vec<TradeTick>,
    sub_correlation_id: Option<UUID4>,
    /// The cut and the floor of a joined request's answer.
    joined: Option<JoinedRelease>,
}

impl DataEngine {
    /// Admits a request that sets `interleave`, or refuses it with a reason naming the first
    /// shape on its route that cannot be answered in live order.
    ///
    /// Live order is guaranteed only for bars built directly from the request's own trades, so
    /// the one admitted shape is a trades request for one instrument whose every bar type its
    /// trades feed (see [`why_not_fed_by_trades`], the same test that routes a trade to the
    /// request's aggregators), answered by its client, by a date-range plan or by a join
    /// (`cut_warmup`): each answer reaches [`Self::respond_interleaved_trades`]. It sees the
    /// request's route in the engine's routing order: its kind, its client, the pipeline it is
    /// a leg of, the pipelines its params nest it in, its instruments, then its bar types.
    /// Without `interleave`, or with it `false`, the request is admitted at once, unread; so is
    /// a request that aggregates no bars (no `bar_types`, or an empty list), whatever its kind,
    /// route or `interleave` value, which nothing else reads then.
    ///
    /// # Errors
    ///
    /// Returns an error if the request sets `interleave`, aggregates bars (its `bar_types` is
    /// present and not an empty list), and:
    /// - The value is not a boolean.
    /// - It is not a trades request.
    /// - Its client is external (this engine would answer nothing).
    /// - It is a leg of a `RequestJoin` (whose answer is rebuilt as one data type).
    /// - It sets continuous-future or time-range pipeline params (those pipelines feed the
    ///   aggregators apart from the trades the requester is given).
    /// - It does not name exactly one instrument (a batch).
    /// - A bar type it asks is not fed by its trades: a composite (built from other bars),
    ///   a bar type built from quotes, or one of another instrument. The error names the first.
    pub(super) fn admit_interleave(&self, req: &RequestCommand) -> anyhow::Result<()> {
        let params = request_params(req);
        let Some(value) = params.and_then(|params| params.get(INTERLEAVE)) else {
            return Ok(());
        };

        // A request that aggregates no bars is answered as without the parameter: no bars to
        // order. A malformed `bar_types` asks for bars, so it is admitted on and refused below
        let aggregation = request_bar_aggregation_from_params(params);
        if matches!(aggregation, Ok(None)) {
            return Ok(());
        }

        match value.as_bool() {
            Some(true) => {}
            Some(false) => return Ok(()),
            None => anyhow::bail!("{INTERLEAVE_REFUSED}: `interleave` must be a boolean"),
        }

        let RequestCommand::Trades(cmd) = req else {
            anyhow::bail!("{INTERLEAVE_REFUSED}: only a trades request can set `interleave`");
        };

        if let Some(client_id) = req.client_id()
            && self.is_external_client(*client_id)
        {
            anyhow::bail!(
                "{INTERLEAVE_REFUSED}: its client {client_id} is external, so this engine \
                 answers nothing"
            );
        }

        if let Some(&(pipeline_id, _)) = self
            .request_pipeline_parent_request_id
            .get(req.request_id())
        {
            let join_id = self
                .parent_join_request_id
                .get(&pipeline_id)
                .copied()
                .unwrap_or(pipeline_id);
            anyhow::bail!(
                "{INTERLEAVE_REFUSED}: it is a leg of the request join {join_id}, whose answer \
                 is rebuilt as one data type"
            );
        }

        if has_continuous_future_params(params) {
            anyhow::bail!(
                "{INTERLEAVE_REFUSED}: its continuous-future params nest it in a \
                 continuous-future pipeline, which answers bars only"
            );
        }

        if has_time_range_pipeline_params(params) {
            anyhow::bail!(
                "{INTERLEAVE_REFUSED}: its `time_range_generator` nests it in a time-range \
                 pipeline, whose windows feed its bars apart from the trades it is given"
            );
        }

        let [instrument_id] = cmd.instrument_ids.as_slice() else {
            anyhow::bail!(
                "{INTERLEAVE_REFUSED}: it must name exactly one instrument (batches are \
                 refused), received {}",
                cmd.instrument_ids.len(),
            );
        };

        // `None` returned above
        let Some(state) = aggregation? else {
            return Ok(());
        };

        for bar_type in &state.bar_types {
            if let Some(reason) = why_not_fed_by_trades(bar_type, *instrument_id) {
                anyhow::bail!(
                    "{INTERLEAVE_REFUSED}: its bar type {bar_type} is not built from its \
                     trades: {reason}"
                );
            }
        }

        Ok(())
    }

    /// Admits a `RequestJoin` before it adopts `leg_ids`, or refuses it when one of them is an
    /// interleaving request already dispatched.
    ///
    /// A join adopts requests in flight, so such a leg was admitted as a plain request; the join
    /// would rebuild its answer as one data type and lose its bars. The refusal leaves the leg as
    /// it is: it is not adopted, and its own answer delivers its bars, cleans up its entry and
    /// its aggregators, and sends its one terminal.
    ///
    /// # Errors
    ///
    /// Returns an error naming the join and the first such leg.
    pub(super) fn admit_join_legs(&self, join_id: UUID4, leg_ids: &[UUID4]) -> anyhow::Result<()> {
        if let Some(leg_id) = leg_ids
            .iter()
            .find(|leg_id| self.interleaved_requests.contains_key(leg_id))
        {
            anyhow::bail!(
                "{INTERLEAVE_REFUSED}: the request join {join_id} would take {leg_id}, already \
                 dispatched, as a leg and rebuild its answer as one data type"
            );
        }

        Ok(())
    }

    /// Prepares the aggregators of `request_id` (for a pipeline, its parent) as `state`
    /// describes them, on a replay (see [`RequestReplay`]) whose window
    /// [`Self::open_request_replay`] records. Without `update_subscriptions` they are the
    /// request's own and end with it. With it they are kept under their bar types: the request
    /// continues the idle kept replay whose builders are exactly its own, claiming it (admitted
    /// by [`Self::admit_kept_request`]), or starts a new one, which owns only the builders it
    /// makes.
    pub(super) fn prepare_request_bar_aggregators_from_state(
        &mut self,
        request_id: UUID4,
        state: &RequestBarAggregation,
    ) -> anyhow::Result<()> {
        if !self.can_start_request_bar_aggregators(request_id, state) {
            anyhow::bail!(
                "Cannot request aggregated bars: one of the aggregators in `bar_types` is already running"
            );
        }

        let aggregator_request_id = state.aggregator_request_id(request_id);
        let keys: Vec<BarAggregatorKey> = state
            .bar_types
            .iter()
            .map(|bar_type| bar_aggregator_key(*bar_type, aggregator_request_id))
            .collect();
        let dispatched = self.clock.borrow().timestamp_ns();
        let continued = self
            .kept_replays
            .iter()
            .position(|replay| state.update_subscriptions && same_builders(&replay.builders, &keys))
            .map(|index| self.kept_replays.swap_remove(index));

        // A continued replay keeps its clock, timers and grid; only its window is the request's
        let replay = match continued {
            Some(replay) => RequestReplay {
                claim: request_id,
                end: None,
                dispatched,
                partial: false,
                covered: None,
                released: Vec::new(),
                ..replay
            },
            None => RequestReplay {
                clock: Rc::new(RefCell::new(VirtualClock::new())),
                builders: keys
                    .into_iter()
                    .filter(|key| !self.bar_aggregators.contains_key(key))
                    .collect(),
                armed: false,
                opened: None,
                covered: None,
                end: None,
                dispatched,
                partial: false,
                claim: request_id,
                released: Vec::new(),
            },
        };

        self.request_bar_aggregations
            .insert(request_id, state.clone());
        self.request_replays.insert(request_id, replay);

        if let Err(e) = self.init_request_bar_aggregators(request_id, state) {
            self.cleanup_request_bar_aggregators(&request_id);
            return Err(e);
        }

        Ok(())
    }

    pub(super) fn prepare_request_bar_aggregators(
        &mut self,
        req: &RequestCommand,
    ) -> anyhow::Result<()> {
        let request_id = *req.request_id();

        let Some(mut state) = request_bar_aggregation_from_params(request_params(req))? else {
            return Ok(());
        };
        self.admit_kept_request(req, &state)?;

        // The entry exists before the aggregators that deliver into it
        if state.interleave {
            // Fed in the order live delivers the bars of one trade, so they are produced in it
            state.bar_types.sort_by_cached_key(live_delivery_order);

            let (start, end) = parent_request_window(Some(req));
            let interleaved = InterleavedRequest {
                start,
                end,
                params: request_params(req).cloned(),
                ..InterleavedRequest::default()
            };
            self.interleaved_requests.insert(request_id, interleaved);
        }

        let result = self.prepare_request_bar_aggregators_from_state(request_id, &state);
        if result.is_err() {
            self.interleaved_requests.remove(&request_id);
        } else {
            self.open_request_replay(req);
        }

        result
    }

    /// Admits a request that sets `update_subscriptions` on any route, or refuses it with a
    /// reason naming the case, before any of it runs: no replay is claimed, no join deadline
    /// armed and no route dispatched, a merged (`quotes`) request's, an external client's and a
    /// continuous future's included. It runs after the join's own parameter refusals and the
    /// `interleave` admission, and before [`Self::admit_kept_request`], which admits the builders
    /// the request keeps. A request that does not set it to `true`, or that aggregates no bars
    /// (and is not a continuous future, whose bar types its params give otherwise), is admitted
    /// at once; so is one whose `bar_types` cannot be read, which its route refuses as before.
    ///
    /// # Errors
    ///
    /// Returns an error if the request sets `update_subscriptions` and:
    /// - It sets continuous-future params: a continuous future's builders end with its request.
    /// - It is a trades or a quotes request naming several instruments (a batch): the builders
    ///   of its instruments would share one replay, its catalog legs driving that replay through
    ///   the window one instrument after another, and one hand-over of their held records.
    /// - A bar type it asks is built from another bar (a composite): a kept builder is handed
    ///   over to its subscription alone or with builders that share nothing. The error names the
    ///   first.
    /// - A bar type it asks is not fed by its own records (see [`why_not_fed_by_own_records`]):
    ///   its builder would be kept as covering a window it never saw. The error names the first.
    /// - The engine's clock is not a [`VirtualClock`]: at a hand-over the builders' pending
    ///   closes move from the replay's clock to the engine's, which a live clock would fire at
    ///   its own time instead of theirs.
    pub(super) fn admit_kept_route(&self, req: &RequestCommand) -> anyhow::Result<()> {
        let Some(params) = request_params(req) else {
            return Ok(());
        };
        if params.get_bool(UPDATE_SUBSCRIPTIONS) != Some(true) {
            return Ok(());
        }

        anyhow::ensure!(
            !has_continuous_future_params(Some(params)),
            "{KEPT_REFUSED}: it sets continuous-future params, whose builders end with its request"
        );

        let Ok(Some(state)) = request_bar_aggregation_from_params(Some(params)) else {
            return Ok(());
        };
        if let RequestCommand::Trades(RequestTrades { instrument_ids, .. })
        | RequestCommand::Quotes(RequestQuotes { instrument_ids, .. }) = req
            && instrument_ids.len() != 1
        {
            anyhow::bail!(
                "{KEPT_REFUSED}: it must name exactly one instrument (batches are refused, as \
                 their builders would share one replay), received {}",
                instrument_ids.len(),
            );
        }
        if let Some(bar_type) = state
            .bar_types
            .iter()
            .find(|bar_type| bar_type.is_composite())
        {
            anyhow::bail!(
                "{KEPT_REFUSED}: {bar_type} is built from another bar; a kept request names only \
                 bar types built directly from its own records"
            );
        }
        if let Some((bar_type, why)) = state.bar_types.iter().find_map(|bar_type| {
            why_not_fed_by_own_records(req, bar_type).map(|why| (bar_type, why))
        }) {
            anyhow::bail!(
                "{KEPT_REFUSED}: its own records do not feed {bar_type}: {why}; a kept request \
                 names only bar types built directly from its own records"
            );
        }
        anyhow::ensure!(
            self.clock
                .borrow()
                .as_any()
                .downcast_ref::<VirtualClock>()
                .is_some(),
            "{KEPT_REFUSED}: the engine's clock is not a `VirtualClock`, so its builders' pending \
             closes cannot move onto it at a hand-over"
        );

        Ok(())
    }

    /// Admits a request with `update_subscriptions`, whose builders are kept under their bar
    /// types and outlive it, or refuses it, naming the case, before anything of it is made,
    /// changed or removed. Admitted are only: none of its builders exists (it starts a replay,
    /// see [`RequestReplay`]); or its builders are exactly those of one idle kept replay, no
    /// request has claimed that replay, and the request's `start` is the next instant after the
    /// replay's coverage (it continues the replay, claimed when its aggregators are prepared,
    /// before dispatch). A request without `update_subscriptions` is admitted at once. What its
    /// route and its bar types may be is admitted before (see [`Self::admit_kept_route`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the request keeps its builders and: a bar type it asks is already
    /// running for a subscription; a request in flight has claimed one of its builders; it names
    /// a `limit`, a filter expression or `point_data` (see [`window_not_wholly_known`]); it has no
    /// `start`; some of its builders exist and some do not; its time zone is not that of an
    /// existing builder; one exists that no kept replay holds; its builders are kept by two
    /// replays, or are not all the builders of their replay; that replay's last answer fired no
    /// end of coverage; or its `start` is before that replay's start, within its coverage, or
    /// after the next instant. Every one is checked before the replay is claimed or a builder
    /// touched, so a refusal leaves the next valid continuation as it would have been.
    pub(super) fn admit_kept_request(
        &self,
        req: &RequestCommand,
        state: &RequestBarAggregation,
    ) -> anyhow::Result<()> {
        if !state.update_subscriptions {
            return Ok(());
        }

        let keys: Vec<BarAggregatorKey> = state
            .bar_types
            .iter()
            .map(|bar_type| bar_aggregator_key(*bar_type, None))
            .collect();

        if let Some(key) = keys
            .iter()
            .find(|key| self.bar_aggregator_handlers.contains_key(key))
        {
            anyhow::bail!(
                "{KEPT_REFUSED}: {} is already running for a subscription",
                key.0
            );
        }

        if let Some((request_id, key)) = keys.iter().find_map(|key| {
            self.request_replays
                .iter()
                .find(|(_, replay)| replay.builders.contains(key))
                .map(|(request_id, _)| (request_id, key))
        }) {
            anyhow::bail!(
                "{KEPT_REFUSED}: the builder of {} is in use by the request {request_id}",
                key.0
            );
        }

        anyhow::ensure!(
            !names_a_limit(req),
            "{KEPT_REFUSED}: it names a `limit`, so its answer may stop before its window ends"
        );
        anyhow::ensure!(
            !names_a_filter(req),
            "{KEPT_REFUSED}: it names a filter expression (`filter_expr`), so its answer may hold \
             only some records of its window"
        );
        anyhow::ensure!(
            !names_point_data(req),
            "{KEPT_REFUSED}: it names `point_data`, so its time-range pipeline asks only single \
             instants of its window"
        );
        let Some(start) = parent_request_window(Some(req)).0 else {
            anyhow::bail!(
                "{KEPT_REFUSED}: it gives no `start`, at which its builders' clock starts"
            );
        };

        let (existing, missing): (Vec<&BarAggregatorKey>, Vec<&BarAggregatorKey>) = keys
            .iter()
            .partition(|key| self.bar_aggregators.contains_key(*key));
        let [first, ..] = existing.as_slice() else {
            return Ok(());
        };

        if let Some(missing) = missing.first() {
            anyhow::bail!(
                "{KEPT_REFUSED}: {} has a kept builder and {} has none",
                first.0,
                missing.0
            );
        }

        for key in &existing {
            let time_zone = self.bar_aggregator_time_zone(key.0, state.time_zone.clone())?;
            anyhow::ensure!(
                self.bar_aggregator_reusable(key, &time_zone),
                "{KEPT_REFUSED}: its time zone is not the time zone of the kept builder of {}",
                key.0
            );
        }

        let mut holder: Option<(usize, BarAggregatorKey)> = None;
        for key in &keys {
            let Some(index) = self.kept_replay_index(key) else {
                anyhow::bail!(
                    "{KEPT_REFUSED}: {} has a builder no kept replay holds",
                    key.0
                );
            };
            match holder {
                Some((first_index, first_key)) if first_index != index => anyhow::bail!(
                    "{KEPT_REFUSED}: {} and {} are kept by two replays",
                    first_key.0,
                    key.0
                ),
                Some(_) => {}
                None => holder = Some((index, *key)),
            }
        }
        let Some((index, _)) = holder else {
            return Ok(());
        };

        let replay = &self.kept_replays[index];
        anyhow::ensure!(
            same_builders(&replay.builders, &keys),
            "{KEPT_REFUSED}: its bar types are not all the builders of the replay that keeps {}",
            first.0
        );

        let Some(covered) = replay.covered else {
            anyhow::bail!(
                "{KEPT_REFUSED}: the last answer of the replay that keeps {} fired no end of \
                 coverage, so it has nothing to continue from",
                first.0
            );
        };
        let next = covered + DurationNanos::new(1);

        if replay.opened.is_some_and(|opened| start < opened) {
            anyhow::bail!(
                "{KEPT_REFUSED}: its start {start} is before the start of the replay that keeps {}",
                first.0
            );
        }
        anyhow::ensure!(
            start >= next,
            "{KEPT_REFUSED}: its start {start} overlaps the coverage through {covered} of the \
             replay that keeps {}",
            first.0
        );
        anyhow::ensure!(
            start == next,
            "{KEPT_REFUSED}: its start {start} leaves a gap after the coverage through {covered} \
             of the replay that keeps {}",
            first.0
        );

        Ok(())
    }

    /// Returns the index of the idle kept replay holding the builder under `key`, if any.
    fn kept_replay_index(&self, key: &BarAggregatorKey) -> Option<usize> {
        self.kept_replays
            .iter()
            .position(|replay| replay.builders.contains(key))
    }

    /// Releases the claim of the request `cmd.request_id` on the builder of `cmd.bar_type` kept by
    /// request (see [`ReleaseKeptBars`]), acting only when that request claims the builder's
    /// replay (see [`RequestReplay::claim`]): an idle builder is stopped and removed, and its
    /// replay with it when it is the last; one that request still runs is marked, and removed at
    /// its end (see [`RequestReplay::released`]), its answer untouched. Only the released bar
    /// type's builder goes: the others of the replay are left as they are. A release naming any
    /// other request, or a builder no replay holds any more (a subscription took it over), is
    /// ignored.
    pub(super) fn release_kept_bars(&mut self, cmd: &ReleaseKeptBars) {
        let key = bar_aggregator_key(cmd.bar_type, None);

        if let Some(index) = self
            .kept_replays
            .iter()
            .position(|replay| replay.claim == cmd.request_id && replay.builders.contains(&key))
        {
            if let Err(e) = self.stop_bar_aggregator(cmd.bar_type, None) {
                log::error!(
                    "Error stopping kept bar aggregator for {}: {e}",
                    cmd.bar_type
                );
            }
            let replay = &mut self.kept_replays[index];
            replay.builders.retain(|kept| *kept != key);
            if replay.builders.is_empty() {
                self.kept_replays.swap_remove(index);
            }
            return;
        }

        // A replay in flight is keyed by the request claiming it
        if let Some(replay) = self.request_replays.get_mut(&cmd.request_id)
            && replay.builders.contains(&key)
        {
            if !replay.released.contains(&key) {
                replay.released.push(key);
            }
            return;
        }

        log::debug!(
            "Ignoring release of {} by request {}: it claims no kept builder of it",
            cmd.bar_type,
            cmd.request_id
        );
    }

    /// Returns whether the builder of `bar_type` is kept by request and idle on its replay, so a
    /// subscription to it adopts it (see [`Self::adopt_kept_builders`]).
    pub(super) fn is_idle_kept_builder(&self, bar_type: BarType) -> bool {
        self.kept_replay_index(&bar_aggregator_key(bar_type, None))
            .is_some()
    }

    /// Returns whether a replay drives the clock of the builder under `key`: a request's own,
    /// or one kept by request, in flight or idle.
    pub(super) fn is_replay_driven(&self, key: &BarAggregatorKey) -> bool {
        self.request_replays
            .values()
            .chain(&self.kept_replays)
            .any(|replay| replay.builders.contains(key))
    }

    /// Records the window of the replay of `req` (for a pipeline, its parent), if it has one:
    /// its `end` and whether its window is not wholly known (see [`window_not_wholly_known`]).
    /// Then arms it at the request's `start`; without one it is armed at its first record.
    pub(super) fn open_request_replay(&mut self, req: &RequestCommand) {
        let request_id = *req.request_id();
        let (start, end) = parent_request_window(Some(req));
        let Some(replay) = self.request_replays.get_mut(&request_id) else {
            return;
        };
        replay.end = end;
        replay.partial = window_not_wholly_known(req);

        if let Some(start) = start {
            self.arm_replay(request_id, start);
        }
    }

    /// Marks the replay that `request_id` feeds, if any, as answered by a bare response, which
    /// does not certify that it holds every record of the window (see
    /// [`RequestReplay::partial`]): the request's own, or for a child of a time-range or
    /// continuous-future pipeline its parent's. A leg of a request's plan feeds no replay of its
    /// own: its answer, empty or not, is a source.
    pub(super) fn mark_bare_response(&mut self, request_id: UUID4) {
        let owner = self
            .time_range_pipeline_parent_request_id
            .get(&request_id)
            .or_else(|| self.continuous_future_parent_request_id.get(&request_id))
            .copied()
            .unwrap_or(request_id);

        if let Some(replay) = self.request_replays.get_mut(&owner) {
            replay.partial = true;
        }
    }

    /// Returns whether `req` builds bars on a replay (see [`RequestReplay`]): its own, or the one
    /// of the time-range pipeline it is a child of. A request whose replay is not made yet is
    /// read from its parameters: any bar types, kept by request or not.
    #[cfg(feature = "streaming")]
    pub(super) fn builds_bars_on_a_replay(&self, req: &RequestCommand) -> bool {
        let request_id = *req.request_id();
        let owner = self
            .time_range_pipeline_parent_request_id
            .get(&request_id)
            .copied()
            .unwrap_or(request_id);

        self.request_replays.contains_key(&owner)
            || request_bar_aggregation_from_params(request_params(req))
                .ok()
                .flatten()
                .is_some()
    }

    /// Returns the replay clock of `request_id`, `None` for a request without a replay.
    fn replay_clock(&self, request_id: UUID4) -> Option<Rc<RefCell<VirtualClock>>> {
        self.request_replays
            .get(&request_id)
            .map(|replay| Rc::clone(&replay.clock))
    }

    /// Arms the timers of the replay of `request_id` at `instant`, once: the replay clock is set
    /// there first, then each builder with a timer starts it as a subscription's builder does,
    /// in the request's order. Then, as for a subscription's builders, each time bar built from
    /// another time bar of the request is linked to that source's builder, so at one instant
    /// the source closes first and its bar is in the dependent's (see
    /// [`Self::connect_time_bar_source`]).
    fn arm_replay(&mut self, request_id: UUID4, instant: UnixNanos) {
        let Some(replay) = self
            .request_replays
            .get_mut(&request_id)
            .filter(|replay| !replay.armed)
        else {
            return;
        };
        replay.armed = true;
        replay.opened = Some(instant);
        replay.clock.borrow().set_time(instant);

        for key in &replay.builders {
            if let Some(aggregator) = self.bar_aggregators.get(key)
                && has_timer(&key.0)
            {
                aggregator
                    .borrow_mut()
                    .start_timer(Some(Rc::clone(aggregator)));
            }
        }

        let (bar_types, aggregator_request_id) = self
            .request_bar_aggregations
            .get(&request_id)
            .map(|state| {
                (
                    state.bar_types.clone(),
                    state.aggregator_request_id(request_id),
                )
            })
            .unwrap_or_default();

        for bar_type in bar_types {
            self.connect_time_bar_source(bar_type, aggregator_request_id);
        }
    }

    /// Readies the replay of `request_id` for a record stamped `ts`, as a backtest readies its
    /// clock for a datum: arms it at `ts` when it is not armed yet, otherwise fires every
    /// deadline strictly before `ts` (see [`Self::drive_replay`]), then sets the replay clock
    /// to `ts`. A deadline at `ts` stays armed until a later record or the end of coverage, so
    /// it fires after every record of its instant. The clock never moves back: a record stamped
    /// before it fires nothing and is the builders' stale case. Does nothing for a request
    /// without a replay.
    pub(super) fn replay_until(&mut self, request_id: UUID4, ts: UnixNanos) {
        let Some(armed) = self
            .request_replays
            .get(&request_id)
            .map(|replay| replay.armed)
        else {
            return;
        };

        if !armed {
            self.arm_replay(request_id, ts);
            return;
        }

        if let Some(clock) = self.replay_clock(request_id) {
            step_clock(&clock, ts);
        }
    }

    /// Drives the replay of `request_id` through `through`, inclusive (see [`drive_clock`]).
    fn drive_replay(&self, request_id: UUID4, through: UnixNanos) {
        if let Some(clock) = self.replay_clock(request_id) {
            drive_clock(&clock, through);
        }
    }

    /// Drives the replay of `request_id` through the end of its coverage at its completed answer
    /// with no failure reason: `min(end, dispatched)` (`dispatched` without an end), so every
    /// close the answer covered fires and nothing later, and records it as the replay's
    /// coverage. An answer that may not hold every record of the window (see
    /// [`RequestReplay::partial`]) does not drive it, nor does a joined request's
    /// (`cut_warmup`), whose held live trades drive it on (see
    /// [`Self::release_kept_builders`]).
    pub(super) fn end_replay_coverage(&mut self, request_id: UUID4) {
        let joined = self
            .request_bar_aggregations
            .get(&request_id)
            .is_some_and(|state| state.cut_warmup);
        let Some(replay) = self
            .request_replays
            .get_mut(&request_id)
            .filter(|replay| !replay.partial && !joined)
        else {
            return;
        };
        let covered = replay
            .end
            .map_or(replay.dispatched, |end| end.min(replay.dispatched));
        replay.covered = Some(covered);

        self.drive_replay(request_id, covered);
    }

    /// Records the client an interleaving request (or a leg of its pipeline) was dispatched to.
    pub(super) fn record_interleaved_client(&mut self, request_id: UUID4, client_id: ClientId) {
        let owner = self
            .request_pipeline_parent_request_id
            .get(&request_id)
            .map_or(request_id, |&(parent_id, _)| parent_id);

        if let Some(interleaved) = self.interleaved_requests.get_mut(&owner) {
            interleaved.client_id = Some(client_id);
        }
    }

    fn can_start_request_bar_aggregators(
        &self,
        request_id: UUID4,
        state: &RequestBarAggregation,
    ) -> bool {
        let aggregator_request_id = state.aggregator_request_id(request_id);
        state.bar_types.iter().all(|bar_type| {
            let key = bar_aggregator_key(*bar_type, aggregator_request_id);
            self.bar_aggregators
                .get(&key)
                .is_none_or(|aggregator| !aggregator.borrow().is_running())
        })
    }

    fn init_request_bar_aggregators(
        &mut self,
        request_id: UUID4,
        state: &RequestBarAggregation,
    ) -> anyhow::Result<()> {
        let aggregator_request_id = state.aggregator_request_id(request_id);
        let Some(replay) = self.request_replays.get(&request_id) else {
            anyhow::bail!("Cannot request aggregated bars: the request {request_id} has no replay");
        };
        let replay_clock = Rc::clone(&replay.clock);
        // A continued kept replay's builders were set up by the request that started it
        let continued = replay.armed;

        for bar_type in &state.bar_types {
            // An existing builder is only checked for its time zone
            self.create_bar_aggregator_for_key(
                *bar_type,
                aggregator_request_id,
                state.skip_first_non_full_bar,
                state.time_zone.clone(),
            )?;

            let key = bar_aggregator_key(*bar_type, aggregator_request_id);
            let Some(aggregator) = self.bar_aggregators.get(&key) else {
                continue;
            };

            // Set up as a subscription's builder, on the replay clock; armed at its start
            if !continued {
                if has_timer(bar_type) {
                    let clock: Rc<RefCell<dyn Clock>> = Rc::clone(&replay_clock) as _;
                    aggregator.borrow_mut().set_clock(clock);
                }

                if state.disable_build_with_no_updates {
                    aggregator.borrow_mut().set_build_with_no_updates(false);
                }
            }

            aggregator.borrow_mut().set_is_running(true);
        }

        self.set_request_bar_aggregator_chain_handlers(request_id, state);

        Ok(())
    }

    fn set_request_bar_aggregator_chain_handlers(
        &self,
        request_id: UUID4,
        state: &RequestBarAggregation,
    ) {
        let aggregator_request_id = state.aggregator_request_id(request_id);

        for bar_type in &state.bar_types {
            let key = bar_aggregator_key(*bar_type, aggregator_request_id);

            let Some(aggregator) = self.bar_aggregators.get(&key).cloned() else {
                continue;
            };

            let downstream: Vec<_> = state
                .bar_types
                .iter()
                .filter(|candidate| {
                    candidate.is_composite()
                        && candidate.composite().standard() == bar_type.standard()
                })
                .filter_map(|candidate| {
                    let key = bar_aggregator_key(*candidate, aggregator_request_id);
                    self.bar_aggregators.get(&key).cloned()
                })
                .collect();

            let cache = Rc::clone(&self.cache);
            let interleaved = self.interleaved_requests.get(&request_id);
            let interleaved_bars = interleaved.map(|interleaved| Rc::clone(&interleaved.bars));

            let handler: Box<dyn FnMut(Bar)> = Box::new(move |bar: Bar| {
                // Request-generated bars are delivered only through the cache, or as response
                // parts when the request interleaves, each part's bars entering the cache as it
                // is delivered.
                if let Some(interleaved_bars) = &interleaved_bars {
                    interleaved_bars.borrow_mut().push(bar);
                } else if let Err(e) = cache.as_ref().borrow_mut().add_bar_historical(bar) {
                    log_error_on_cache_insert(&e);
                }

                for aggregator in &downstream {
                    aggregator.borrow_mut().handle_bar(bar);
                }
            });

            // A replay's builder computes as a subscription's, not in historical mode
            aggregator.borrow_mut().set_historical_mode(false, handler);
        }
    }

    /// Ends the request's aggregators on a failure or a clean-up, before its terminal goes out:
    /// what the end leaves to do (see [`Release`]) waits for that terminal (see
    /// [`Self::finish_release`]).
    pub(super) fn cleanup_request_bar_aggregators(&mut self, request_id: &UUID4) -> bool {
        let aggregates = self.request_bar_aggregations.contains_key(request_id);
        if let Some(release) = self.release_request_bar_aggregators(request_id, None, None, false) {
            self.pending_releases.insert(*request_id, release);
        }
        aggregates
    }

    /// Does what the end of `request_id` left to do (see [`Release`]), once its last bars and its
    /// terminal are out: `release`, or what its clean-up left waiting.
    pub(super) fn finish_release(&mut self, request_id: UUID4, release: Option<Release>) {
        let pending = self.pending_releases.remove(&request_id);
        match release.or(pending) {
            Some(Release::HandOver(hand_over)) => self.hand_over_kept_builders(hand_over),
            Some(Release::Restart(restart)) => self.feed_restarted(restart),
            None => {}
        }
    }

    /// Returns the failure reason the completed answer of `request_id` ends with when the request
    /// keeps its builders by request (`update_subscriptions`) and its answer, or a pipeline
    /// child's, came as a bare response (see [`Self::mark_bare_response`]): its replay is removed,
    /// not kept, so its requester is told it holds no builder.
    pub(super) fn kept_bare_answer_failure(&self, request_id: UUID4) -> Option<String> {
        let kept = self
            .request_bar_aggregations
            .get(&request_id)
            .is_some_and(|state| state.update_subscriptions);
        let partial = self
            .request_replays
            .get(&request_id)
            .is_some_and(|replay| replay.partial);
        (kept && partial).then(|| KEPT_ANSWERED_BARE.to_owned())
    }

    /// Ends the request's aggregators: on its completed answer with no failure reason
    /// (`succeeded`), or on a failure, an expiry or a clean-up.
    ///
    /// A request's own builders stop with it. Builders kept by request (`update_subscriptions`)
    /// stay on their replay after a success (see [`Self::release_kept_builders`]), which returns
    /// the builders it hands over to their subscriptions; after anything else, after an answer
    /// that may not hold every record of its window (a bare response, see
    /// [`RequestReplay::partial`]), the replay is removed with them (see
    /// [`Self::remove_kept_builders`]), which returns the new builders of their held-back
    /// subscriptions. Either is returned (see [`Release`]),
    /// for the caller to do once the answer's bars and its terminal are out (see
    /// [`Self::finish_release`]). With `interleaved`, the entry of an interleaving request
    /// holding its undelivered bars, the bars its held live trades complete go there; the entry
    /// still held here goes with the builders, undelivered: only [`Self::finish_interleaved`],
    /// which takes the entry first, delivers its bars. A joined builder releases its held live
    /// trades through its own copy of `joined`.
    pub(super) fn release_request_bar_aggregators(
        &mut self,
        request_id: &UUID4,
        joined: Option<JoinedRelease>,
        interleaved: Option<&InterleavedRequest>,
        succeeded: bool,
    ) -> Option<Release> {
        self.interleaved_requests.remove(request_id);
        let replay = self.request_replays.remove(request_id);

        let state = self.request_bar_aggregations.remove(request_id)?;

        if state.update_subscriptions {
            let answer = interleaved.map(|interleaved| Rc::clone(&interleaved.bars));
            return match replay {
                Some(replay) if succeeded && !replay.partial => self
                    .release_kept_builders(&state, replay, joined, answer.as_deref())
                    .map(Release::HandOver),
                replay => self
                    .remove_kept_builders(&state, replay.as_ref())
                    .map(Release::Restart),
            };
        }

        for bar_type in &state.bar_types {
            if let Err(e) = self.stop_bar_aggregator(*bar_type, Some(*request_id)) {
                log::error!("Error stopping request bar aggregator for {bar_type}: {e}");
            }
        }

        None
    }

    /// Keeps the builders of a kept request that succeeded on its replay, idle: no output
    /// handler, nothing drives them, their armed deadlines stay armed, until a later request
    /// continues the replay from its coverage, a subscription takes them over or the request
    /// claiming the replay releases them (see [`Self::release_kept_bars`]). Those the request
    /// released while it ran (see [`RequestReplay::released`]) are removed now, after its answer,
    /// and the replay goes with its last builder.
    ///
    /// The builders whose subscriptions were held back while the answer was in flight leave the
    /// replay now, together, on a clock of their own (see [`Self::hand_over_clock`]), and are
    /// returned to be handed over to their subscriptions (see [`Self::hand_over_kept_builders`])
    /// once the answer's bars and its terminal are out. With `answer`, an interleaving request's
    /// undelivered bars, their held live trades are fed now (see [`Self::feed_held_records`]),
    /// and the bars they complete go to the answer; otherwise every held record is fed at the
    /// hand-over, and the bars it completes are published. A held record stamped at or before
    /// the replay's coverage is not fed: the answer held every record of its window up to then
    /// (a joined request's answer certifies no coverage; its cut and floor decide instead).
    fn release_kept_builders(
        &mut self,
        state: &RequestBarAggregation,
        mut replay: RequestReplay,
        joined: Option<JoinedRelease>,
        answer: Option<&RefCell<Vec<Bar>>>,
    ) -> Option<HandOver> {
        let held_back: Vec<BarType> = sources_before_dependents(&state.bar_types)
            .into_iter()
            .filter(|bar_type| {
                self.bar_aggregator_handlers
                    .contains_key(&bar_aggregator_key(*bar_type, None))
            })
            .collect();

        let hand_over = if held_back.is_empty() {
            None
        } else {
            let clock = self.hand_over_clock(&replay.clock, &held_back);
            let mut held = self.take_held_records(&held_back, joined, replay.covered);
            if let Some(answer) = answer {
                self.feed_held_records(std::mem::take(&mut held), &clock, Some(answer));
            }
            replay.builders.retain(|key| !held_back.contains(&key.0));
            Some(HandOver {
                bar_types: held_back,
                clock,
                held,
            })
        };

        for key in std::mem::take(&mut replay.released) {
            if !replay.builders.contains(&key) {
                continue;
            }

            if let Err(e) = self.stop_bar_aggregator(key.0, None) {
                log::error!(
                    "Error stopping released kept bar aggregator for {}: {e}",
                    key.0
                );
            }
            replay.builders.retain(|kept| *kept != key);
        }

        for key in &replay.builders {
            if let Some(aggregator) = self.bar_aggregators.get(key) {
                let mut aggregator = aggregator.borrow_mut();
                aggregator.set_historical_mode(false, Box::new(|_: Bar| {}));
                aggregator.set_is_running(false);
            }
        }

        if !replay.builders.is_empty() {
            self.kept_replays.push(replay);
        }

        hand_over
    }

    /// Removes the replay of a kept request that failed, expired or was cleaned up, its builders
    /// stopped and removed: none of them is handed over. A subscription held back on one of them
    /// gets a new builder, started as a plain subscription now (see
    /// [`Self::restart_held_back_subscription`]); the live records held for it are returned with
    /// it (see [`Restart`]), to be fed once the request's terminal is out (see
    /// [`Self::feed_restarted`]). A builder the replay did not make is left as it is.
    fn remove_kept_builders(
        &mut self,
        state: &RequestBarAggregation,
        replay: Option<&RequestReplay>,
    ) -> Option<Restart> {
        let held_trades = self.take_held_trades(&state.bar_types, None);
        let mut restarted = Vec::new();

        for bar_type in sources_before_dependents(&state.bar_types) {
            let key = bar_aggregator_key(bar_type, None);
            let subscribed = self.bar_aggregator_handlers.contains_key(&key);
            if !subscribed && !replay.is_some_and(|replay| replay.builders.contains(&key)) {
                continue;
            }

            let held = self.bar_aggregator_warmups.remove(&key);
            if let Err(e) = self.stop_bar_aggregator(bar_type, None) {
                log::error!("Error stopping kept request bar aggregator for {bar_type}: {e}");
            }

            if subscribed {
                self.restart_held_back_subscription(bar_type);
                restarted.push((bar_type, held));
            }
        }

        (!restarted.is_empty()).then_some(Restart {
            held_trades,
            restarted,
        })
    }

    /// Feeds the new builders of `restart` (see [`Restart`]) the live records held for them, as
    /// live feeds them: the held trades trade by trade across bar types in the request's order
    /// (see [`Self::take_held_trades`]), each trade's deferred `RANGE_INCLUSIVE` bars published
    /// after it; then any held quotes or bars one builder after the other, sources first.
    fn feed_restarted(&self, restart: Restart) {
        let Restart {
            held_trades,
            restarted,
        } = restart;
        let aggregator = |engine: &Self, bar_type: BarType| {
            engine
                .bar_aggregators
                .get(&bar_aggregator_key(bar_type, None))
                .map(Rc::clone)
        };

        for row in held_trade_rows(&held_trades) {
            for (index, copy) in row {
                if let Some(aggregator) = aggregator(self, held_trades[index].0) {
                    aggregator.borrow_mut().handle_trade(copy);
                }
            }
            self.publish_deferred_bars();
        }

        for (bar_type, held) in restarted {
            if let Some(held) = held
                && let Some(aggregator) = aggregator(self, bar_type)
            {
                held.release(aggregator.borrow_mut().as_mut());
            }
        }
    }

    /// Starts a new builder for the subscription to `bar_type` whose request's kept builder was
    /// removed, as [`Self::subscribe_bars`] starts one.
    fn restart_held_back_subscription(&mut self, bar_type: BarType) {
        let params = self
            .subscriptions_bar_aggregation
            .get(&bar_type.standard())
            .and_then(|subscription| subscription.command.params.clone());
        let skip_first_non_full_bar = params
            .as_ref()
            .and_then(|params| params.get_bool("skip_first_non_full_bar"));
        let started = time_zone_from_params(params.as_ref()).and_then(|time_zone| {
            self.start_bar_aggregator(bar_type, None, skip_first_non_full_bar, time_zone)
        });

        if let Err(e) = started {
            log::error!("Error starting the subscription bar aggregator for {bar_type}: {e}");
        }
    }

    /// Returns a new clock for the kept builders of `bar_types` leaving the replay whose clock is
    /// `replay_clock`, set to its time: each builder with a timer continues on it with its
    /// pending deadline as armed (see [`BarAggregator::continue_on_clock`]), so the replay's
    /// other builders are not driven with them and keep their own deadlines.
    fn hand_over_clock(
        &self,
        replay_clock: &Rc<RefCell<VirtualClock>>,
        bar_types: &[BarType],
    ) -> Rc<RefCell<VirtualClock>> {
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock
            .borrow()
            .set_time(replay_clock.borrow().timestamp_ns());

        for bar_type in bar_types.iter().filter(|bar_type| has_timer(bar_type)) {
            let key = bar_aggregator_key(*bar_type, None);
            debug_assert!(
                timer_armed(&replay_clock.borrow(), key.0),
                "Invariant: the kept builder of {} is never stopped while its replay drives it",
                key.0,
            );
            if let Some(aggregator) = self.bar_aggregators.get(&key) {
                let to: Rc<RefCell<dyn Clock>> = Rc::clone(&clock) as _;
                aggregator.borrow_mut().continue_on_clock(to);
            }
        }

        clock
    }

    /// Hands the kept builders of `hand_over` (see [`HandOver`]) over to their subscriptions, as
    /// live builders, together: each gets the publishing handler a subscription's builder gets
    /// (see [`Self::engine_bar_handler`]); then their held records not fed yet are fed (see
    /// [`Self::feed_held_records`]) and their clock is driven through the engine clock's time
    /// minus 1 ns, so the bars due by then are published in deadline order, stamped at their own
    /// close; then each builder with a timer continues on the engine clock with its pending
    /// deadline as armed (see [`BarAggregator::continue_on_clock`]): no new origin, open or first
    /// close, no flush. A deadline at the engine clock's time fires there once, after the records
    /// stamped then.
    pub(super) fn hand_over_kept_builders(&self, hand_over: HandOver) {
        let HandOver {
            bar_types,
            clock,
            held,
        } = hand_over;

        for bar_type in &bar_types {
            let handler = self.engine_bar_handler(*bar_type, true);
            if let Some(aggregator) = self
                .bar_aggregators
                .get(&bar_aggregator_key(*bar_type, None))
            {
                aggregator.borrow_mut().set_historical_mode(false, handler);
            }
        }

        self.feed_held_records(held, &clock, None);
        let now = self.clock.borrow().timestamp_ns();
        drive_clock(&clock, now.saturating_sub(DurationNanos::new(1)));

        for bar_type in &bar_types {
            if let Some(aggregator) = self
                .bar_aggregators
                .get(&bar_aggregator_key(*bar_type, None))
            {
                let mut aggregator = aggregator.borrow_mut();
                if has_timer(bar_type) {
                    aggregator.continue_on_clock(Rc::clone(&self.clock));
                }
                aggregator.set_is_running(true);
            }
            self.connect_time_bar_sources(*bar_type);
        }
    }

    /// Hands the idle kept builders of `bar_types` over to the subscriptions of one subscribe
    /// command that adopt them: the builders of each kept replay together, on one clock of their
    /// own (see [`HandOver`]), so they catch up in deadline order; each replay goes with its last
    /// builder. A bar type no kept replay holds any more is skipped. Idle, they hold no live
    /// records: those are held only for a builder a request runs.
    pub(super) fn adopt_kept_builders(&mut self, bar_types: &[BarType]) {
        let mut remaining = bar_types.to_vec();
        while let Some(&first) = remaining.first() {
            let Some(index) = self.kept_replay_index(&bar_aggregator_key(first, None)) else {
                remaining.remove(0);
                continue;
            };

            let replay = &mut self.kept_replays[index];
            let (adopted, rest): (Vec<BarType>, Vec<BarType>) =
                remaining.into_iter().partition(|bar_type| {
                    replay
                        .builders
                        .contains(&bar_aggregator_key(*bar_type, None))
                });
            remaining = rest;
            replay.builders.retain(|kept| {
                !adopted
                    .iter()
                    .any(|bar_type| *kept == bar_aggregator_key(*bar_type, None))
            });
            let replay_clock = Rc::clone(&replay.clock);
            if replay.builders.is_empty() {
                self.kept_replays.swap_remove(index);
            }

            let clock = self.hand_over_clock(&replay_clock, &adopted);
            self.hand_over_kept_builders(HandOver {
                bar_types: adopted,
                clock,
                held: Vec::new(),
            });
        }
    }

    /// Takes the live records held for the kept builders of `bar_types` (see
    /// [`BarAggregatorWarmup`]) as one list in arrival order (see [`WarmupBuffer`]), across
    /// builders and kinds: each held trade once, with every builder's copy of it through its own
    /// copy of `joined` (see [`Self::take_held_trades`]), in the order live gave them the copies;
    /// each held quote and bar. A copy stamped at or before `covered`, the coverage of the
    /// answer the builders leave, is dropped: the answer held it.
    fn take_held_records(
        &mut self,
        bar_types: &[BarType],
        joined: Option<JoinedRelease>,
        covered: Option<UnixNanos>,
    ) -> Vec<HeldRecord> {
        let owned = |ts_init: UnixNanos| covered.is_some_and(|covered| ts_init <= covered);
        let arrivals: Vec<(BarType, Vec<u64>)> = bar_types
            .iter()
            .filter_map(|bar_type| {
                self.bar_aggregator_warmups
                    .get(&bar_aggregator_key(*bar_type, None))
                    .map(|warmup| (*bar_type, warmup.arrivals()))
            })
            .collect();
        let arrival = |bar_type: BarType, own: usize| {
            arrivals
                .iter()
                .find(|(held, _)| *held == bar_type)
                .and_then(|(_, arrivals)| arrivals.get(own).copied())
        };

        // A builder subscribed later holds the last of the trades the first one holds
        let held_trades = self.take_held_trades(bar_types, joined);
        let longest = held_trades
            .iter()
            .map(|(_, copies)| copies.len())
            .max()
            .unwrap_or_default();
        let mut records: Vec<HeldRecord> = (0..longest)
            .filter_map(|row| {
                let mut copies: Vec<(u64, BarType, TradeTick)> = held_trades
                    .iter()
                    .filter_map(|(bar_type, held)| {
                        let own = (row + held.len()).checked_sub(longest)?;
                        let copy = held[own].filter(|copy| !owned(copy.ts_init))?;
                        Some((arrival(*bar_type, own)?, *bar_type, copy))
                    })
                    .collect();
                copies.sort_by_key(|(arrival, ..)| *arrival);
                let (first, ..) = copies.first()?;
                Some(HeldRecord::Trade(
                    *first,
                    copies
                        .into_iter()
                        .map(|(_, bar_type, copy)| (bar_type, copy))
                        .collect(),
                ))
            })
            .collect();

        for bar_type in bar_types {
            let key = bar_aggregator_key(*bar_type, None);
            let Some(warmup) = self.bar_aggregator_warmups.remove(&key) else {
                continue;
            };

            match warmup {
                BarAggregatorWarmup::Quote(buffer) => records.extend(
                    buffer
                        .release_with_arrivals()
                        .into_iter()
                        .filter(|(_, quote)| !owned(quote.ts_init))
                        .map(|(arrival, quote)| HeldRecord::Quote(arrival, *bar_type, quote)),
                ),
                BarAggregatorWarmup::Bar(buffer) => records.extend(
                    buffer
                        .release_with_arrivals()
                        .into_iter()
                        .filter(|(_, bar)| !owned(bar.ts_init))
                        .map(|(arrival, bar)| HeldRecord::Bar(arrival, *bar_type, bar)),
                ),
                BarAggregatorWarmup::Trade(buffer)
                | BarAggregatorWarmup::JoinedTrade(buffer, _) => records.extend(
                    buffer
                        .release_with_arrivals()
                        .into_iter()
                        .filter(|(_, trade)| !owned(trade.ts_init))
                        .map(|(arrival, trade)| {
                            HeldRecord::Trade(arrival, vec![(*bar_type, trade)])
                        }),
                ),
            }
        }

        records.sort_by_key(HeldRecord::arrival);
        records
    }

    /// Feeds `records` (see [`Self::take_held_records`]) to their builders in that order, as live
    /// fed them, each trade's copies one after the other: before each record, `clock`, the
    /// builders' driver (see [`Self::hand_over_clock`]), is readied for it as for a record of the
    /// answer (see [`step_clock`]), at the latest of a trade's copies, so the closes due before
    /// it fire first, once for all the records of an instant. With `answer`, an interleaving
    /// request's undelivered bars, the bars fired and the bars one record completes go to its end
    /// by the rule of [`Self::respond_interleaved_trades`]: a `RANGE_INCLUSIVE` bar after the
    /// others. Otherwise the builders publish (see [`Self::hand_over_kept_builders`]), a record's
    /// deferred `RANGE_INCLUSIVE` bars after it.
    fn feed_held_records(
        &self,
        records: Vec<HeldRecord>,
        clock: &Rc<RefCell<VirtualClock>>,
        answer: Option<&RefCell<Vec<Bar>>>,
    ) {
        let aggregator = |bar_type: BarType| {
            self.bar_aggregators
                .get(&bar_aggregator_key(bar_type, None))
                .map(Rc::clone)
        };

        for record in records {
            let start = answer.map_or(0, |bars| bars.borrow().len());
            step_clock(clock, record.ts_init());

            match record {
                HeldRecord::Trade(_, copies) => {
                    for (bar_type, copy) in copies {
                        if let Some(aggregator) = aggregator(bar_type) {
                            aggregator.borrow_mut().handle_trade(copy);
                        }
                    }
                }
                HeldRecord::Quote(_, bar_type, quote) => {
                    if let Some(aggregator) = aggregator(bar_type) {
                        aggregator.borrow_mut().handle_quote(quote);
                    }
                }
                HeldRecord::Bar(_, bar_type, bar) => {
                    if let Some(aggregator) = aggregator(bar_type) {
                        aggregator.borrow_mut().handle_bar(bar);
                    }
                }
            }

            match answer {
                Some(bars) => {
                    let completed = bars.borrow_mut().split_off(start);
                    let (after, before): (Vec<Bar>, Vec<Bar>) = completed
                        .into_iter()
                        .partition(is_delivered_after_its_trade);
                    bars.borrow_mut().extend(before.into_iter().chain(after));
                }
                None => self.publish_deferred_bars(),
            }
        }
    }

    /// Feeds an answer of a request that does not interleave to its aggregators (see
    /// [`Self::feed_request_bar_aggregators`]), their bars entering the cache as they are built.
    ///
    /// A `completed` answer ends the request. With no `failure` reason it first drives the
    /// request's replay through the end of its coverage (see [`Self::end_replay_coverage`]),
    /// once: for a pipeline, this is its parent's terminal, after every child answer. Then its
    /// aggregators are cleaned up; what that leaves to do for the subscriptions of kept builders
    /// is returned, to be done once the answer's terminal is out (see [`Release`]).
    pub(super) fn process_request_bar_aggregation_response(
        &mut self,
        resp: &DataResponse,
        completed: bool,
        failure: Option<&str>,
    ) -> Option<Release> {
        let correlation_id = *resp.correlation_id();

        let state = self
            .request_bar_aggregations
            .get(&correlation_id)
            .cloned()?;

        self.feed_request_bar_aggregators(&state, correlation_id, resp);

        if completed {
            if failure.is_none() {
                self.end_replay_coverage(correlation_id);
            }

            let joined = match resp {
                DataResponse::Trades(r) => joined_release(&state, &r.data),
                _ => None,
            };
            return self.release_request_bar_aggregators(
                &correlation_id,
                joined,
                None,
                failure.is_none(),
            );
        }

        None
    }

    /// Feeds the quotes, trades or bars of `resp` to the aggregators of `request_id` (for a
    /// pipeline, its parent), one record at a time. Before each record its replay fires the
    /// closes due strictly before the record's `ts_init` (see [`Self::replay_until`]).
    pub(super) fn feed_request_bar_aggregators(
        &mut self,
        state: &RequestBarAggregation,
        request_id: UUID4,
        resp: &DataResponse,
    ) {
        match resp {
            DataResponse::Quotes(r) => {
                for quote in r.data.iter() {
                    self.replay_until(request_id, quote.ts_init);
                    self.update_request_bar_aggregators_from_quote(state, request_id, *quote);
                }
            }
            DataResponse::Trades(r) => {
                for trade in r.data.iter() {
                    self.replay_until(request_id, trade.ts_init);
                    self.update_request_bar_aggregators_from_trade(state, request_id, *trade);
                }
            }
            DataResponse::Bars(r) => {
                for bar in r.data.iter() {
                    self.replay_until(request_id, bar.ts_init);
                    self.update_request_bar_aggregators_from_bar(state, request_id, *bar);
                }
            }
            _ => {}
        }
    }

    /// Takes the trade warm-ups of the subscribed builders of `bar_types` (kept by request), in
    /// that order (the request's; for an interleaving request live's, see
    /// [`live_delivery_order`]): each builder's bar type and its copy of each held trade, in
    /// arrival order, through its own copy of `joined`, as when it releases alone (`None` where
    /// the cut drops one). Any other warm-up stays.
    ///
    /// Admission leaves a request that holds trades for several builders one instrument, so
    /// every builder holds the trades of one topic from its subscription to this release: a
    /// builder subscribed later holds the last of the trades the first one holds.
    fn take_held_trades(
        &mut self,
        bar_types: &[BarType],
        joined: Option<JoinedRelease>,
    ) -> Vec<(BarType, Vec<Option<TradeTick>>)> {
        let mut builders = Vec::new();

        for bar_type in bar_types {
            let key = bar_aggregator_key(*bar_type, None);
            if !self.bar_aggregator_handlers.contains_key(&key) {
                continue;
            }

            let Some(copies) = self
                .bar_aggregator_warmups
                .get(&key)
                .and_then(|warmup| warmup.release_trades(joined, key.0))
            else {
                continue;
            };
            self.bar_aggregator_warmups.remove(&key);
            builders.push((*bar_type, copies));
        }

        // One topic's trades held from each subscription on: a shorter buffer is a longer's tail
        debug_assert!(builders.iter().all(|(_, shorter)| {
            let without_floor =
                |copy: &Option<TradeTick>| copy.map(|trade| (trade.trade_id, trade.ts_event));
            builders.iter().all(|(_, longer)| {
                longer.len() < shorter.len()
                    || longer[longer.len() - shorter.len()..]
                        .iter()
                        .map(without_floor)
                        .eq(shorter.iter().map(without_floor))
            })
        }));

        builders
    }

    /// Answers a trades request that set `interleave` with response parts in live order.
    ///
    /// Trades feed the request aggregators one at a time; admission leaves only bar types its
    /// trades feed (see [`Self::admit_interleave`]). Before each trade (and, in a merged answer, before each quote: see [`Self::send_interleaved_quotes`])
    /// the request's replay fires the closes due strictly before it (see
    /// [`Self::replay_until`]): time bars and `RANGE` bars completed at their daily boundary go
    /// out before the trade, in the order of their closes. A bar a trade completes is delivered
    /// before that trade, except a `RANGE_INCLUSIVE` bar, which contains its completing trade and
    /// is delivered after it (the live publication order). The aggregators are fed in live's order (see [`live_delivery_order`]), so bars of
    /// different types one trade completes come in the order live publishes them. Each bar
    /// delivery first sends the trades gathered since the previous one; each bar is a part of
    /// its own, in the order the aggregators produced it, and enters the cache only as
    /// its part is sent, so a trade's or a bar's handler finds in the cache the bars live
    /// publication has cached by then. The last part of a partial envelope, the trades after
    /// the last completed bar (possibly none), is sent as a part; a completed envelope ends the
    /// request through [`Self::finish_interleaved`], with its reason.
    pub(super) fn respond_interleaved_trades(
        &mut self,
        resp: &TradesResponse,
        state: &RequestBarAggregation,
        envelope: &DataResponseEnvelope,
    ) {
        let request_id = resp.correlation_id;
        let bars = self
            .interleaved_requests
            .get(&request_id)
            .map(|interleaved| Rc::clone(&interleaved.bars))
            .unwrap_or_default();
        let mut pending = Vec::new();

        for trade in resp.data.iter() {
            // The closes of the request's replay due before the trade, each bar before it
            self.replay_until(request_id, trade.ts_init);
            let fired = std::mem::take(&mut *bars.borrow_mut());
            send_interleaved_bars(&self.cache, resp, &mut pending, fired);

            self.update_request_bar_aggregators_from_trade(state, request_id, *trade);

            let completed = std::mem::take(&mut *bars.borrow_mut());
            let (after, before): (Vec<Bar>, Vec<Bar>) = completed
                .into_iter()
                .partition(is_delivered_after_its_trade);

            send_interleaved_bars(&self.cache, resp, &mut pending, before);
            pending.push(*trade);
            send_interleaved_bars(&self.cache, resp, &mut pending, after);
        }

        if envelope.completed() {
            let closing = ClosingPart {
                resp,
                pending,
                sub_correlation_id: envelope.sub_correlation_id(),
                joined: joined_release(state, &resp.data),
            };
            let failure = envelope.failure_reason().map(str::to_owned);
            self.finish_interleaved(request_id, failure, Some(closing));
            return;
        }

        let last = DataResponseEnvelope::new(
            DataResponse::Trades(trades_part(resp, pending)),
            false,
            envelope.sub_correlation_id(),
        )
        .with_correlation_id(*envelope.correlation_id());
        msgbus::send_response_envelope(&last);
    }

    /// Sends `quotes`, one run of quotes of a `quotes` request's merged answer (see
    /// [`Self::send_quote_parts`]), through the ordinary response route.
    ///
    /// For an interleaving request with its own replay the run is split where a close is due:
    /// before each quote, as before a trade (see [`Self::respond_interleaved_trades`]), the
    /// request's replay fires the closes due strictly before the quote's `ts_init` (see
    /// [`Self::replay_until`]), and their bars go out before the quote, each a part of its own
    /// with the identity of the answer's `trades` and entering the cache as it is sent (see
    /// [`send_interleaved_bars`]); the quotes before them are sent first, as one part. A close
    /// at the quote's own instant stays armed, so the quote comes before the bar closing then,
    /// as a backtest runs a timer due at a datum's stamp after that datum. Otherwise the quotes
    /// go out as before: in their order, each stretch one quotes part through the route, which
    /// caches it. Any other request's run goes out whole.
    ///
    /// The quotes feed none of the request's builders: admission leaves only bar types its
    /// trades feed (see [`Self::admit_interleave`]), so a quote moves the request's time and
    /// changes no bar.
    pub(super) fn send_interleaved_quotes(
        &mut self,
        trades: &TradesResponse,
        quotes: QuotesResponse,
    ) {
        let request_id = quotes.correlation_id;
        let bars = self
            .interleaved_requests
            .get(&request_id)
            .filter(|_| self.request_replays.contains_key(&request_id))
            .map(|interleaved| Rc::clone(&interleaved.bars));

        let Some(bars) = bars else {
            let part = DataResponse::Quotes(quotes);
            self.process_response_envelope(DataResponseEnvelope::new(part, false, None));
            return;
        };

        let mut run = Vec::new();

        for quote in quotes.data.iter() {
            // The closes of the request's replay due before the quote, each bar before it
            self.replay_until(request_id, quote.ts_init);
            let fired = std::mem::take(&mut *bars.borrow_mut());

            if !fired.is_empty() {
                if !run.is_empty() {
                    let part = DataResponse::Quotes(quotes_part(&quotes, std::mem::take(&mut run)));
                    self.process_response_envelope(DataResponseEnvelope::new(part, false, None));
                }
                send_interleaved_bars(&self.cache, trades, &mut Vec::new(), fired);
            }
            run.push(*quote);
        }

        if !run.is_empty() {
            let part = DataResponse::Quotes(quotes_part(&quotes, run));
            self.process_response_envelope(DataResponseEnvelope::new(part, false, None));
        }
    }

    /// Ends an interleaving request: on its completed answer (`closing`), on a payload-free
    /// answer, or on expiry.
    ///
    /// At a completed answer with no failure reason, first drives the request's replay through
    /// the end of its coverage (see [`Self::end_replay_coverage`]). Then takes the request's
    /// entry, cleans up its aggregators (a joined request's builders releasing through the
    /// answer's cut and floor), and delivers the bars that end and that clean-up completed, each
    /// a part of its own in the order they were produced, entering the cache just before its
    /// part is sent: with the answer's identity, after the answer's trades; or, when there is
    /// no answer and a client was resolved, with the dispatched client's identity and the
    /// request's window (none otherwise, and they do not enter the cache). Then sends exactly
    /// one terminal, the answer's last trades part when there is one, carrying `failure` when
    /// there is a reason (for a kept request answered bare, the reason it was answered so: see
    /// [`Self::kept_bare_answer_failure`]). Then does what the end left to do for the
    /// subscriptions of its kept builders (see [`Self::finish_release`]): their catch-up, or the
    /// bars their new builders' held records complete, comes after the answer.
    ///
    /// A payload-free answer with no failure reason ends the request as a successful answer with
    /// a payload does: it certifies that the request's window holds no more records.
    pub(super) fn finish_interleaved(
        &mut self,
        request_id: UUID4,
        failure: Option<String>,
        closing: Option<ClosingPart<'_>>,
    ) {
        let failure = failure.or_else(|| self.kept_bare_answer_failure(request_id));
        let succeeded = failure.is_none();
        if succeeded {
            self.end_replay_coverage(request_id);
        }

        let interleaved = self
            .interleaved_requests
            .remove(&request_id)
            .unwrap_or_default();
        let joined = closing.as_ref().and_then(|closing| closing.joined);
        let release = self.release_request_bar_aggregators(
            &request_id,
            joined,
            Some(&interleaved),
            succeeded,
        );
        let remaining = std::mem::take(&mut *interleaved.bars.borrow_mut());

        let terminal = if let Some(ClosingPart {
            resp,
            mut pending,
            sub_correlation_id,
            ..
        }) = closing
        {
            send_interleaved_bars(&self.cache, resp, &mut pending, remaining);
            DataResponseEnvelope::new(
                DataResponse::Trades(trades_part(resp, pending)),
                true,
                sub_correlation_id,
            )
            .with_correlation_id(request_id)
        } else {
            if let Some(client_id) = interleaved.client_id {
                let ts_init = self.clock.borrow().timestamp_ns();
                for bar in remaining {
                    send_bar_part(
                        &self.cache,
                        request_id,
                        client_id,
                        (interleaved.start, interleaved.end),
                        ts_init,
                        interleaved.params.as_ref(),
                        bar,
                    );
                }
            }
            DataResponseEnvelope::terminal(request_id, None)
        };

        let terminal = match failure {
            Some(reason) => terminal.with_failure(reason),
            None => terminal,
        };
        msgbus::send_response_envelope(&terminal);

        // What its kept builders' subscriptions are given comes after its last bars and terminal
        self.finish_release(request_id, release);
    }

    pub(super) fn update_request_bar_aggregators_from_quote(
        &self,
        state: &RequestBarAggregation,
        request_id: UUID4,
        quote: QuoteTick,
    ) {
        let aggregator_request_id = state.aggregator_request_id(request_id);

        for bar_type in &state.bar_types {
            if bar_type.is_composite()
                || bar_type.instrument_id() != quote.instrument_id
                || bar_type.spec().price_type == PriceType::Last
            {
                continue;
            }

            self.update_request_bar_aggregator(*bar_type, aggregator_request_id, |aggregator| {
                aggregator.handle_quote(quote);
            });
        }
    }

    pub(super) fn update_request_bar_aggregators_from_trade(
        &self,
        state: &RequestBarAggregation,
        request_id: UUID4,
        trade: TradeTick,
    ) {
        let aggregator_request_id = state.aggregator_request_id(request_id);

        for bar_type in &state.bar_types {
            if why_not_fed_by_trades(bar_type, trade.instrument_id).is_some() {
                continue;
            }

            self.update_request_bar_aggregator(*bar_type, aggregator_request_id, |aggregator| {
                aggregator.handle_trade(trade);
            });
        }
    }

    pub(super) fn update_request_bar_aggregators_from_bar(
        &self,
        state: &RequestBarAggregation,
        request_id: UUID4,
        bar: Bar,
    ) {
        let aggregator_request_id = state.aggregator_request_id(request_id);

        for bar_type in &state.bar_types {
            if !bar_type.is_composite()
                || bar_type.composite().standard() != bar.bar_type.standard()
            {
                continue;
            }

            self.update_request_bar_aggregator(*bar_type, aggregator_request_id, |aggregator| {
                aggregator.handle_bar(bar);
            });
        }
    }

    pub(super) fn update_request_bar_aggregator<F>(
        &self,
        bar_type: BarType,
        request_id: Option<UUID4>,
        update: F,
    ) where
        F: FnOnce(&mut dyn BarAggregator),
    {
        let key = bar_aggregator_key(bar_type, request_id);

        let Some(aggregator) = self.bar_aggregators.get(&key) else {
            log::error!("Cannot update request bar aggregator: no aggregator found for {bar_type}");
            return;
        };

        update(aggregator.borrow_mut().as_mut());
    }

    pub(super) fn subscribe_bars(&mut self, cmd: &SubscribeBars) -> anyhow::Result<()> {
        let bar_type = cmd
            .single_bar_type()
            .expect("native engine receives one bar type");

        match bar_type.aggregation_source() {
            AggregationSource::Internal => self.start_bar_aggregation(cmd)?,
            AggregationSource::External => {
                if bar_type.instrument_id().is_synthetic() {
                    anyhow::bail!(
                        "Cannot subscribe for externally aggregated synthetic instrument bar data"
                    );
                }
            }
        }

        Ok(())
    }

    pub(super) fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) {
        let bar_type = cmd
            .single_bar_type()
            .expect("native engine receives one bar type");

        // A builder kept by request that no subscription took over is the claiming request's: an
        // unsubscribe never removes it (see `release_kept_bars`)
        let key = bar_aggregator_key(bar_type, None);
        if self.kept_replay_index(&key).is_some() {
            return;
        }

        // A kept builder a request is still running stays the request's: a subscription held
        // back on it is released now, and only that subscription
        let in_flight = self
            .request_replays
            .values()
            .any(|replay| replay.builders.contains(&key));
        let held_back = in_flight && self.bar_aggregator_handlers.contains_key(&key);
        if in_flight && !held_back {
            return;
        }

        // Don't remove aggregator if other exact-topic subscribers still exist
        let topic = switchboard::get_bars_topic(bar_type.standard());
        if msgbus::exact_subscriber_count_bars(topic) > 0 {
            return;
        }

        let retained = self
            .subscriptions_bar_aggregation
            .get(&bar_type.standard())
            .map(|subscription| subscription.command.clone());

        let command = retained.map_or_else(
            || cmd.clone(),
            |subscribe| {
                UnsubscribeBars::new(
                    subscribe.bar_types,
                    subscribe.client_id,
                    subscribe.venue,
                    cmd.command_id,
                    cmd.ts_init,
                    Some(subscribe.command_id),
                    subscribe.params,
                )
            },
        );

        if held_back {
            self.release_bar_aggregator_handlers(&key);
            self.subscriptions_bar_aggregation
                .remove(&bar_type.standard());
            self.unsubscribe_bar_aggregator(&command);
        } else if self
            .bar_aggregators
            .contains_key(&bar_aggregator_key(bar_type, None))
        {
            match self.stop_bar_aggregator(bar_type, None) {
                Ok(()) => {
                    self.subscriptions_bar_aggregation
                        .remove(&bar_type.standard());
                    self.unsubscribe_bar_aggregator(&command);
                }
                Err(e) => log::error!("Error stopping bar aggregator for {bar_type}: {e}"),
            }
        }

        // After stopping a composite, release its source through `unsubscribe_bars`, which frees
        // the client feed recorded in the source's retained command.
        if command
            .single_bar_type()
            .expect("retained aggregation has one bar type")
            .is_composite()
        {
            let source_type = command
                .single_bar_type()
                .expect("retained aggregation has one bar type")
                .composite();

            if self
                .bar_aggregators
                .contains_key(&bar_aggregator_key(source_type, None))
            {
                self.unsubscribe_bars(&UnsubscribeBars::new(
                    source_type,
                    command.client_id,
                    command.venue,
                    UUID4::new(),
                    command.ts_init,
                    Some(command.command_id),
                    command.params.clone(),
                ));
            }
        }
    }

    fn create_bar_aggregator(
        &self,
        instrument: &InstrumentAny,
        bar_type: BarType,
        skip_first_non_full_bar: Option<bool>,
        time_zone: TimeZone,
    ) -> Box<dyn BarAggregator> {
        let cache = Rc::clone(&self.cache);
        let validate_sequence = self.config.validate_data_sequence;

        let handler = move |bar: Bar| {
            process_engine_bar(&cache, validate_sequence, true, bar);
        };

        let clock = Rc::clone(&self.clock);
        let config = self.config.clone();

        let price_precision = instrument.price_precision();
        let size_precision = instrument.size_precision();

        if bar_type.spec().is_time_aggregated() {
            let time_bars_origin_offset = config
                .time_bars_origin_offset
                .get(&bar_type.spec().aggregation)
                .map(|duration| jiff::SignedDuration::try_from(*duration).unwrap_or_default());

            Box::new(TimeBarAggregator::new(
                bar_type,
                price_precision,
                size_precision,
                clock,
                handler,
                config.time_bars_build_with_no_updates,
                config.time_bars_timestamp_on_close,
                config.time_bars_interval_type,
                time_bars_origin_offset,
                time_zone,
                config.time_bars_build_delay,
                skip_first_non_full_bar.unwrap_or(config.time_bars_skip_first_non_full_bar),
            ))
        } else {
            match bar_type.spec().aggregation {
                BarAggregation::Tick => Box::new(TickBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::TickImbalance => Box::new(TickImbalanceBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::TickRuns => Box::new(TickRunsBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::Volume => Box::new(VolumeBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::VolumeImbalance => Box::new(VolumeImbalanceBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::VolumeRuns => Box::new(VolumeRunsBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::Value => Box::new(ValueBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::ValueImbalance => Box::new(ValueImbalanceBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::ValueRuns => Box::new(ValueRunsBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::Renko => Box::new(RenkoBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    instrument.price_increment(),
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::Range => Box::new(DeepChartsRangeBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    instrument.price_increment(),
                    time_zone,
                    config
                        .time_bars_origin_offset
                        .get(&BarAggregation::Range)
                        .map_or(RANGE_BAR_DEFAULT_BOUNDARY, |duration| {
                            jiff::SignedDuration::try_from(*duration).unwrap_or_default()
                        }),
                    handler,
                )) as Box<dyn BarAggregator>,
                BarAggregation::RangeInclusive => Box::new(RangeInclusiveBarAggregator::new(
                    bar_type,
                    price_precision,
                    size_precision,
                    instrument.price_increment(),
                    time_zone,
                    config
                        .time_bars_origin_offset
                        .get(&BarAggregation::RangeInclusive)
                        .map_or(RANGE_BAR_DEFAULT_BOUNDARY, |duration| {
                            jiff::SignedDuration::try_from(*duration).unwrap_or_default()
                        }),
                    handler,
                )) as Box<dyn BarAggregator>,
                other => unreachable!(
                    "Unsupported internal bar aggregation dispatch for {other:?}; update `create_bar_aggregator`"
                ),
            }
        }
    }

    pub(super) fn create_bar_aggregator_for_key(
        &mut self,
        bar_type: BarType,
        request_id: Option<UUID4>,
        skip_first_non_full_bar: Option<bool>,
        time_zone: Option<TimeZone>,
    ) -> anyhow::Result<()> {
        let key = bar_aggregator_key(bar_type, request_id);
        let time_zone = self.bar_aggregator_time_zone(bar_type, time_zone)?;

        if self.bar_aggregators.contains_key(&key) {
            anyhow::ensure!(
                self.bar_aggregator_reusable(&key, &time_zone),
                "Cannot reuse bar aggregator {bar_type} with a different time zone"
            );

            return Ok(());
        }

        let instrument = {
            let cache = self.cache.borrow();
            cache
                .instrument(&bar_type.instrument_id())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Cannot start bar aggregation: no instrument found for {}",
                        bar_type.instrument_id(),
                    )
                })?
                .clone()
        };

        let aggregator =
            self.create_bar_aggregator(&instrument, bar_type, skip_first_non_full_bar, time_zone);
        debug_assert_eq!(
            aggregator.bar_type(),
            key.0,
            "aggregator bar type must match its standardized key"
        );
        self.bar_aggregators
            .insert(key, Rc::new(RefCell::new(aggregator)));

        Ok(())
    }

    /// Returns the time zone a builder of `bar_type` is made with: `time_zone`, else the
    /// configured `time_bars_time_zone`, else its aggregation's default.
    ///
    /// # Errors
    ///
    /// Returns an error if the configured `time_bars_time_zone` is invalid.
    fn bar_aggregator_time_zone(
        &self,
        bar_type: BarType,
        time_zone: Option<TimeZone>,
    ) -> anyhow::Result<TimeZone> {
        if let Some(zone) = time_zone {
            return Ok(zone);
        }

        let default_zone = if matches!(
            bar_type.spec().aggregation,
            BarAggregation::Range | BarAggregation::RangeInclusive
        ) {
            RANGE_BAR_DEFAULT_TIME_ZONE
        } else {
            "UTC"
        };
        get_timezone(
            self.config
                .time_bars_time_zone
                .as_deref()
                .unwrap_or(default_zone),
        )
        .context("invalid `time_bars_time_zone` configuration")
    }

    /// Returns whether the builder under `key` can be used with `time_zone`: a time bar's builder
    /// only with its own.
    fn bar_aggregator_reusable(&self, key: &BarAggregatorKey, time_zone: &TimeZone) -> bool {
        self.bar_aggregators.get(key).is_none_or(|aggregator| {
            aggregator
                .borrow()
                .as_any()
                .downcast_ref::<TimeBarAggregator>()
                .is_none_or(|aggregator| aggregator.time_zone() == time_zone)
        })
    }

    fn start_bar_aggregation(&mut self, cmd: &SubscribeBars) -> anyhow::Result<()> {
        let bar_type = cmd
            .single_bar_type()
            .expect("native engine receives one bar type");
        let time_zone = time_zone_from_params(cmd.params.as_ref())?;
        let skip_first_non_full_bar = cmd
            .params
            .as_ref()
            .and_then(|params| params.get_bool("skip_first_non_full_bar"));
        self.create_bar_aggregator_for_key(
            bar_type,
            None,
            skip_first_non_full_bar,
            time_zone.clone(),
        )?;
        let key = bar_aggregator_key(bar_type, None);

        if self
            .bar_aggregators
            .get(&key)
            .is_some_and(|aggregator| aggregator.borrow().is_running())
            && self.bar_aggregator_handlers.contains_key(&key)
        {
            if let Some(source_command) = self
                .subscriptions_bar_aggregation
                .get(&bar_type.standard())
                .and_then(|subscription| subscription.source.clone())
            {
                self.execute(DataCommand::Subscribe(source_command));
            }

            log::debug!("Reusing running bar aggregator for {bar_type}");
            return Ok(());
        }

        self.start_bar_aggregator(bar_type, None, skip_first_non_full_bar, time_zone)?;
        let source = self.subscribe_bar_aggregator(cmd);
        self.subscriptions_bar_aggregation.insert(
            bar_type.standard(),
            BarAggregationSubscription {
                command: cmd.clone(),
                source,
            },
        );

        Ok(())
    }

    fn start_bar_aggregator(
        &mut self,
        bar_type: BarType,
        request_id: Option<UUID4>,
        skip_first_non_full_bar: Option<bool>,
        time_zone: Option<TimeZone>,
    ) -> anyhow::Result<()> {
        let key = bar_aggregator_key(bar_type, request_id);
        let bar_type_std = bar_type.standard();

        self.create_bar_aggregator_for_key(
            bar_type,
            request_id,
            skip_first_non_full_bar,
            time_zone,
        )?;
        let aggregator = self
            .bar_aggregators
            .get(&key)
            .map(Rc::clone)
            .ok_or_else(|| anyhow::anyhow!("Cannot start bar aggregation for {bar_type}"))?;
        let defer_subscription_activation = request_id.is_none()
            && aggregator.borrow().is_running()
            && !self.bar_aggregator_handlers.contains_key(&key);

        if !self.bar_aggregator_handlers.contains_key(&key) {
            // Subscribe to underlying data topics
            let mut subscriptions = Vec::new();

            // A deferred aggregator is still building its kept request's answer on the request's
            // replay, so live data is held back and fed to it once the request ends
            let defer = defer_subscription_activation;
            let warmup;

            if bar_type.is_composite() {
                let topic = switchboard::get_bars_topic(bar_type.composite());
                let buffer = defer.then(WarmupBuffer::new);
                let handler = TypedHandler::new(
                    BarBarHandler::new(&aggregator, bar_type_std).with_warmup(buffer.clone()),
                );
                warmup = buffer.map(BarAggregatorWarmup::Bar);
                msgbus::subscribe_bars(topic.into(), handler.clone(), None);
                subscriptions.push(BarAggregatorSubscription::Bar { topic, handler });
            } else if bar_type.spec().price_type == PriceType::Last {
                let topic = switchboard::get_trades_topic(bar_type.instrument_id());
                // Chosen once here: only a joined warm-up's builder takes trades through a slot
                let handler = if defer && self.is_joined_warmup(&key) {
                    let buffer = WarmupBuffer::new();
                    let joined = JoinedSlot::default();
                    warmup = Some(BarAggregatorWarmup::JoinedTrade(
                        buffer.clone(),
                        joined.clone(),
                    ));
                    TypedHandler::new(JoinedTradeHandler::new(
                        &aggregator,
                        bar_type_std,
                        buffer,
                        joined,
                    ))
                } else {
                    let buffer = defer.then(WarmupBuffer::new);
                    let handler = TypedHandler::new(
                        BarTradeHandler::new(&aggregator, bar_type_std).with_warmup(buffer.clone()),
                    );
                    warmup = buffer.map(BarAggregatorWarmup::Trade);
                    handler
                };
                msgbus::subscribe_trades(
                    topic.into(),
                    handler.clone(),
                    Some(BAR_AGGREGATOR_PRIORITY),
                );
                subscriptions.push(BarAggregatorSubscription::Trade { topic, handler });
            } else {
                // Warn if imbalance/runs aggregation is wired to quotes (needs aggressor_side from trades)
                if matches!(
                    bar_type.spec().aggregation,
                    BarAggregation::TickImbalance
                        | BarAggregation::VolumeImbalance
                        | BarAggregation::ValueImbalance
                        | BarAggregation::TickRuns
                        | BarAggregation::VolumeRuns
                        | BarAggregation::ValueRuns
                ) {
                    log::warn!(
                        "Bar type {bar_type} uses imbalance/runs aggregation which requires trade \
                         data with `aggressor_side`, but `price_type` is not LAST so it will receive \
                         quote data: bars will not emit correctly",
                    );
                }

                let topic = switchboard::get_quotes_topic(bar_type.instrument_id());
                let buffer = defer.then(WarmupBuffer::new);
                let handler = TypedHandler::new(
                    BarQuoteHandler::new(&aggregator, bar_type_std).with_warmup(buffer.clone()),
                );
                warmup = buffer.map(BarAggregatorWarmup::Quote);
                msgbus::subscribe_quotes(
                    topic.into(),
                    handler.clone(),
                    Some(BAR_AGGREGATOR_PRIORITY),
                );
                subscriptions.push(BarAggregatorSubscription::Quote { topic, handler });
            }

            self.bar_aggregator_handlers.insert(key, subscriptions);

            if let Some(warmup) = warmup {
                self.bar_aggregator_warmups.insert(key, warmup);
            }
        }

        if defer_subscription_activation {
            return Ok(());
        }

        // A builder kept by request and idle on its replay is taken over, not set up again: with
        // the others of its subscribe command, once that command's last one is prepared
        if request_id.is_none() && self.kept_replay_index(&key).is_some() {
            match &mut self.pending_adoptions {
                Some(pending) if !pending.contains(&bar_type) => pending.push(bar_type),
                Some(_) => {}
                None => self.adopt_kept_builders(&[bar_type]),
            }
            return Ok(());
        }

        self.setup_bar_aggregator(bar_type, request_id)?;

        aggregator.borrow_mut().set_is_running(true);

        Ok(())
    }

    /// Returns whether the live aggregator under `key` is warming up for a joined request
    /// (`cut_warmup`): asked once when its deferred subscription is set up, never per trade.
    fn is_joined_warmup(&self, key: &BarAggregatorKey) -> bool {
        self.request_bar_aggregations.values().any(|state| {
            state.cut_warmup
                && state.update_subscriptions
                && state
                    .bar_types
                    .iter()
                    .any(|bar_type| bar_aggregator_key(*bar_type, None) == *key)
        })
    }

    fn subscribe_bar_aggregator(&mut self, cmd: &SubscribeBars) -> Option<SubscribeCommand> {
        let subscribe = self.bar_aggregator_source_command(cmd)?;
        self.execute(DataCommand::Subscribe(subscribe.clone()));
        Some(subscribe)
    }

    fn bar_aggregator_source_command(&self, cmd: &SubscribeBars) -> Option<SubscribeCommand> {
        let bar_type = cmd
            .single_bar_type()
            .expect("native engine receives one bar type");
        let key = bar_aggregator_key(bar_type, None);
        if !self.bar_aggregators.contains_key(&key) {
            log::error!("Cannot subscribe bar aggregator: no aggregator found for {bar_type}");
            return None;
        }

        if bar_type.is_composite() {
            let composite_bar_type = bar_type.composite();
            if composite_bar_type.is_externally_aggregated() {
                let subscribe = SubscribeBars::new(
                    composite_bar_type,
                    cmd.client_id,
                    cmd.venue,
                    UUID4::new(),
                    cmd.ts_init,
                    Some(cmd.command_id),
                    cmd.params.clone(),
                );
                return Some(SubscribeCommand::Bars(subscribe));
            }
        } else if bar_type.spec().price_type == PriceType::Last {
            let subscribe = SubscribeTrades::new(
                bar_type.instrument_id(),
                cmd.client_id,
                cmd.venue,
                UUID4::new(),
                cmd.ts_init,
                Some(cmd.command_id),
                cmd.params.clone(),
            );
            return Some(SubscribeCommand::Trades(subscribe));
        } else {
            let subscribe = SubscribeQuotes::new(
                bar_type.instrument_id(),
                cmd.client_id,
                cmd.venue,
                UUID4::new(),
                cmd.ts_init,
                Some(cmd.command_id),
                cmd.params.clone(),
            );
            return Some(SubscribeCommand::Quotes(subscribe));
        }

        None
    }

    /// Sets up a bar aggregator as a live subscription's: the publishing output handler (see
    /// [`Self::engine_bar_handler`]), then, for a time bar or a `RANGE` bar, its timer started on
    /// the engine clock, and, for a subscription's builder (no `request_id`), its link to the
    /// builder of its time bar source (see [`Self::connect_time_bar_sources`]). A request's
    /// builders are set up on its replay instead (see [`RequestReplay`]).
    pub(super) fn setup_bar_aggregator(
        &self,
        bar_type: BarType,
        request_id: Option<UUID4>,
    ) -> anyhow::Result<()> {
        let key = bar_aggregator_key(bar_type, request_id);

        let aggregator = self.bar_aggregators.get(&key).ok_or_else(|| {
            anyhow::anyhow!("Cannot setup bar aggregator: no aggregator found for {bar_type}")
        })?;

        let handler = self.engine_bar_handler(bar_type, true);
        aggregator.borrow_mut().set_historical_mode(false, handler);

        // Time bars close, and range bars arm their completion alerts, on the engine clock
        if has_timer(&bar_type) {
            aggregator.borrow_mut().set_clock(Rc::clone(&self.clock));
            aggregator
                .borrow_mut()
                .start_timer(Some(Rc::clone(aggregator)));
        }

        if request_id.is_none() {
            self.connect_time_bar_sources(bar_type);
        }

        Ok(())
    }

    /// Returns the output handler of a builder of `bar_type` the engine runs: it adds each bar to
    /// the cache and, with `publish`, publishes it.
    fn engine_bar_handler(&self, bar_type: BarType, publish: bool) -> Box<dyn FnMut(Bar)> {
        let cache = Rc::clone(&self.cache);
        let validate_sequence = self.config.validate_data_sequence;

        // A live update-fed RANGE_INCLUSIVE bar contains its completing update, so it is
        // published after that update has reached every subscriber (see `publish_deferred_bars`)
        let defer_until_update_delivered = publish
            && bar_type.spec().aggregation == BarAggregation::RangeInclusive
            && !bar_type.is_composite();

        if defer_until_update_delivered {
            let deferred_bars = Rc::clone(&self.deferred_bars);
            Box::new(move |bar: Bar| deferred_bars.borrow_mut().push_back(bar))
        } else {
            Box::new(move |bar: Bar| {
                process_engine_bar(&cache, validate_sequence, publish, bar);
            })
        }
    }

    fn connect_time_bar_sources(&self, bar_type: BarType) {
        let bar_types = self
            .subscriptions_bar_aggregation
            .values()
            .map(|subscription| {
                subscription
                    .command
                    .single_bar_type()
                    .expect("retained aggregation has one bar type")
            })
            .chain(std::iter::once(bar_type));

        for bar_type in bar_types {
            self.connect_time_bar_source(bar_type, None);
        }
    }

    /// Links the builder of `bar_type` keyed by `request_id` (`None` for a subscription's) to
    /// the builder of its source under the same key, when both exist and `bar_type` is a time
    /// bar built from an internally aggregated time bar: when both close at one instant, the
    /// source closes first, so its bar is in the dependent's.
    fn connect_time_bar_source(&self, bar_type: BarType, request_id: Option<UUID4>) {
        if !bar_type.is_composite()
            || !bar_type.spec().is_time_aggregated()
            || !bar_type.composite().is_internally_aggregated()
            || !bar_type.composite().spec().is_time_aggregated()
        {
            return;
        }

        let Some(source) = self
            .bar_aggregators
            .get(&bar_aggregator_key(bar_type.composite(), request_id))
        else {
            return;
        };

        let Some(aggregator) = self
            .bar_aggregators
            .get(&bar_aggregator_key(bar_type, request_id))
        else {
            return;
        };

        if let Some(aggregator) = aggregator
            .borrow_mut()
            .as_any_mut()
            .downcast_mut::<TimeBarAggregator>()
        {
            aggregator.set_timer_source(Rc::downgrade(source));
        }
    }

    fn unsubscribe_bar_aggregator(&mut self, cmd: &UnsubscribeBars) {
        let bar_type = cmd
            .single_bar_type()
            .expect("native engine receives one bar type");

        if bar_type.is_composite() {
            let composite_bar_type = bar_type.composite();
            if composite_bar_type.is_externally_aggregated() {
                let unsubscribe = UnsubscribeBars::new(
                    composite_bar_type,
                    cmd.client_id,
                    cmd.venue,
                    UUID4::new(),
                    cmd.ts_init,
                    Some(cmd.command_id),
                    cmd.params.clone(),
                );
                self.execute(DataCommand::Unsubscribe(UnsubscribeCommand::Bars(
                    unsubscribe,
                )));
            }
        } else if bar_type.spec().price_type == PriceType::Last {
            let unsubscribe = UnsubscribeTrades::new(
                bar_type.instrument_id(),
                cmd.client_id,
                cmd.venue,
                UUID4::new(),
                cmd.ts_init,
                Some(cmd.command_id),
                cmd.params.clone(),
            );
            self.execute(DataCommand::Unsubscribe(UnsubscribeCommand::Trades(
                unsubscribe,
            )));
        } else {
            let unsubscribe = UnsubscribeQuotes::new(
                bar_type.instrument_id(),
                cmd.client_id,
                cmd.venue,
                UUID4::new(),
                cmd.ts_init,
                Some(cmd.command_id),
                cmd.params.clone(),
            );
            self.execute(DataCommand::Unsubscribe(UnsubscribeCommand::Quotes(
                unsubscribe,
            )));
        }
    }

    pub(super) fn stop_bar_aggregator(
        &mut self,
        bar_type: BarType,
        request_id: Option<UUID4>,
    ) -> anyhow::Result<()> {
        let key = bar_aggregator_key(bar_type, request_id);

        let aggregator = self.bar_aggregators.shift_remove(&key).ok_or_else(|| {
            anyhow::anyhow!("Cannot stop bar aggregator: no aggregator to stop for {bar_type}")
        })?;

        aggregator.borrow_mut().stop();
        self.release_bar_aggregator_handlers(&key);

        Ok(())
    }

    /// Unsubscribes the message handlers of the builder under `key` and drops the live data held
    /// back for it, so nothing holds more for it (see [`BarAggregatorWarmup::discard`]).
    fn release_bar_aggregator_handlers(&mut self, key: &BarAggregatorKey) {
        if let Some(warmup) = self.bar_aggregator_warmups.remove(key) {
            warmup.discard();
        }

        // Unsubscribe any registered message handlers
        if let Some(subs) = self.bar_aggregator_handlers.remove(key) {
            for sub in subs {
                match sub {
                    BarAggregatorSubscription::Bar { topic, handler } => {
                        msgbus::unsubscribe_bars(topic.into(), &handler);
                    }
                    BarAggregatorSubscription::Trade { topic, handler } => {
                        msgbus::unsubscribe_trades(topic.into(), &handler);
                    }
                    BarAggregatorSubscription::Quote { topic, handler } => {
                        msgbus::unsubscribe_quotes(topic.into(), &handler);
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct BarAggregationSubscription {
    pub(super) command: SubscribeBars,
    pub(super) source: Option<SubscribeCommand>,
}

/// Identifies a bar aggregator instance.
///
/// Live subscriptions key on `(bar_type.standard(), None)`. Request-scoped
/// aggregators carrying a `request_id` key on `(bar_type.standard(), Some(id))`
/// so they can run alongside a live aggregator on the same bar type.
pub(crate) type BarAggregatorKey = (BarType, Option<UUID4>);

#[inline]
pub(crate) fn bar_aggregator_key(bar_type: BarType, request_id: Option<UUID4>) -> BarAggregatorKey {
    (bar_type.standard(), request_id)
}

/// Returns `bar_types` ordered so that every composite follows the bar type it aggregates from.
///
/// Bar types at the same depth of the chain keep their original order.
fn sources_before_dependents(bar_types: &[BarType]) -> Vec<BarType> {
    // Depth is bounded by the number of bar types so a cyclic chain cannot loop
    let depth = |bar_type: &BarType| {
        let mut depth = 0;
        let mut current = *bar_type;

        while current.is_composite() && depth < bar_types.len() {
            let Some(source) = bar_types
                .iter()
                .find(|candidate| candidate.standard() == current.composite().standard())
            else {
                break;
            };
            current = *source;
            depth += 1;
        }

        depth
    };

    let mut ordered = bar_types.to_vec();
    ordered.sort_by_cached_key(depth);
    ordered
}

/// Returns why the request aggregator of `bar_type` is not fed by the trades of `instrument_id`,
/// or `None` when it is: a trade reaches only the aggregators of bar types that are not
/// composites, price `LAST` and are of its instrument.
fn why_not_fed_by_trades(bar_type: &BarType, instrument_id: InstrumentId) -> Option<&'static str> {
    if bar_type.is_composite() {
        Some("it is a composite, built from other bars")
    } else if bar_type.spec().price_type != PriceType::Last {
        Some("it is built from quotes")
    } else if bar_type.instrument_id() != instrument_id {
        Some("it is of another instrument")
    } else {
        None
    }
}

/// Returns why the builder of `bar_type` is not fed by the records `req` asks, or `None` when it
/// is: only a trades request and a quotes request feed bar builders. A trades request's trades
/// feed the `LAST` bars of its instruments (its quotes, when it carries them, the others below);
/// a quotes request's quotes feed their `BID`, `ASK` and `MID` bars; a bars request's bars feed
/// only bars built from other bars; the records of any other kind feed none.
fn why_not_fed_by_own_records(req: &RequestCommand, bar_type: &BarType) -> Option<&'static str> {
    let (instrument_ids, feeds_last, feeds_quotes) = match req {
        RequestCommand::Trades(r) => (&r.instrument_ids, true, carries_quotes(req)),
        RequestCommand::Quotes(r) => (&r.instrument_ids, false, true),
        RequestCommand::Bars(_) => {
            return Some("a bars request feeds only bars built from other bars");
        }
        _ => {
            return Some("only a trades request or a quotes request feeds bar builders");
        }
    };

    if !instrument_ids.contains(&bar_type.instrument_id()) {
        return Some("it is of another instrument");
    }
    match bar_type.spec().price_type {
        PriceType::Last if !feeds_last => {
            Some("it is built from trades and the request asks quotes")
        }
        PriceType::Bid | PriceType::Ask | PriceType::Mid if !feeds_quotes => {
            Some("it is built from quotes and the request asks trades")
        }
        PriceType::Last | PriceType::Bid | PriceType::Ask | PriceType::Mid => None,
        PriceType::Mark => Some("neither trades nor quotes feed its price type"),
    }
}

/// Returns the key that orders the bar types of an interleaving request as live delivers the bars
/// one trade completes, so its aggregators are fed, and produce their bars, in that order.
///
/// Every bar type admitted is fed by the trades (see [`why_not_fed_by_trades`]). A request's
/// builders, its own or kept, run on a replay (see [`RequestReplay`]) and build no time bar
/// from a trade: the replay fires every close before the trade, in close order, ties in timer
/// name order (`RANGE_BAR_<bar type>` before `TIME_BAR_<bar type>`), so the key orders only
/// the other bar types. The trades topic's router calls those aggregators at one priority in
/// handler ID order, `BarTradeHandler|<bar type>`: the bar type's string order (a builder
/// subscribed during a joined warm-up has
/// `JoinedTradeHandler|<bar type>`, so the order holds among builders subscribed alike). A
/// `RANGE_INCLUSIVE` bar is published after the trade, its aggregator having been called in
/// that same order.
fn live_delivery_order(bar_type: &BarType) -> (bool, String) {
    (!bar_type.spec().is_time_aggregated(), bar_type.to_string())
}

/// Returns whether the window of `req` is not wholly known by the asker's own choice: it names a
/// `limit`, so its answer may stop before its window ends (only the kinds whose records feed
/// aggregators are read), a filter expression (`filter_expr`), so rows of its window are left
/// out, or `point_data`, so its time-range pipeline asks only single instants of its window.
fn window_not_wholly_known(req: &RequestCommand) -> bool {
    names_a_limit(req) || names_a_filter(req) || names_point_data(req)
}

/// Returns whether `req` runs a time-range pipeline with `point_data`, whose children ask only
/// the instant at the start of each of their windows.
fn names_point_data(req: &RequestCommand) -> bool {
    let params = request_params(req);
    has_time_range_pipeline_params(params)
        && params.and_then(|params| params.get_bool(TIME_RANGE_POINT_DATA)) == Some(true)
}

/// Returns whether `req` names a non-empty filter expression (`filter_expr`), so its answer may
/// hold only some records of its window.
fn names_a_filter(req: &RequestCommand) -> bool {
    request_params(req)
        .and_then(|params| params.get_str(FILTER_EXPR))
        .is_some_and(|filter_expr| !filter_expr.is_empty())
}

/// Returns whether `req` names a `limit` (only the kinds whose records feed aggregators are
/// read), so its answer may stop before its window ends.
fn names_a_limit(req: &RequestCommand) -> bool {
    match req {
        RequestCommand::Quotes(cmd) => cmd.limit.is_some(),
        RequestCommand::Trades(cmd) => cmd.limit.is_some(),
        RequestCommand::Bars(cmd) => cmd.limit.is_some(),
        _ => false,
    }
}

/// Returns whether `bar_type`'s builder runs on a clock: a time bar closes, and a `RANGE` bar
/// completes at its daily boundary, when a timer fires.
fn has_timer(bar_type: &BarType) -> bool {
    bar_type.spec().is_time_aggregated() || bar_type.spec().aggregation == BarAggregation::Range
}

/// Returns the earliest deadline armed on `clock`, if any.
fn next_deadline(clock: &VirtualClock) -> Option<UnixNanos> {
    clock
        .timer_names()
        .into_iter()
        .filter_map(|name| clock.next_time_ns(name))
        .min()
}

/// Drives `clock`, a replay's, through `through`, inclusive, as a backtest drives its clock:
/// takes the earliest armed deadline, sets the clock to it and runs its handlers (ties in the
/// clock's order, time then timer name) with no borrow of the clock or a builder held, then
/// looks again, since a handler may arm a new deadline, until none is left at or before
/// `through`.
fn drive_clock(clock: &Rc<RefCell<VirtualClock>>, through: UnixNanos) {
    loop {
        let handlers = {
            let mut clock = clock.borrow_mut();
            let Some(deadline) = next_deadline(&clock).filter(|deadline| *deadline <= through)
            else {
                break;
            };
            let events = clock.advance_time(deadline, true);
            clock.match_handlers(events)
        };

        for handler in handlers {
            handler.run();
        }
    }
}

/// Readies `clock`, a replay's, for a record stamped `ts`: fires every deadline strictly before
/// `ts` (see [`drive_clock`]), then sets the clock to `ts`; it never moves back. A deadline at
/// `ts` stays armed, so it fires after every record of its instant.
fn step_clock(clock: &Rc<RefCell<VirtualClock>>, ts: UnixNanos) {
    drive_clock(clock, ts.saturating_sub(DurationNanos::new(1)));

    let clock = clock.borrow();
    if ts > clock.timestamp_ns() {
        clock.set_time(ts);
    }
}

/// Returns, for each trade held for the builders of `held` (see
/// [`DataEngine::take_held_trades`]), in arrival order, the copy each builder takes, by its
/// index in `held`, in that order: a builder subscribed later did not hold the first trades,
/// and a copy the cut drops is not taken.
fn held_trade_rows(held: &[(BarType, Vec<Option<TradeTick>>)]) -> Vec<Vec<(usize, TradeTick)>> {
    let longest = held
        .iter()
        .map(|(_, copies)| copies.len())
        .max()
        .unwrap_or_default();

    (0..longest)
        .map(|index| {
            held.iter()
                .enumerate()
                .filter_map(|(builder, (_, copies))| {
                    (index + copies.len())
                        .checked_sub(longest)
                        .and_then(|own| copies.get(own).copied().flatten())
                        .map(|copy| (builder, copy))
                })
                .collect()
        })
        .collect()
}

/// Returns whether `builders` and `keys` name the same builders, in any order.
fn same_builders(builders: &[BarAggregatorKey], keys: &[BarAggregatorKey]) -> bool {
    builders.iter().all(|key| keys.contains(key)) && keys.iter().all(|key| builders.contains(key))
}

/// Returns whether `clock` arms the timer of the builder of `bar_type` (a time bar's close or a
/// `RANGE` bar's daily boundary): a kept builder's stays armed until it is handed over, since
/// nothing stops a builder a replay drives (see [`DataEngine::stop`]).
fn timer_armed(clock: &VirtualClock, bar_type: BarType) -> bool {
    let names = [
        format!("TIME_BAR_{bar_type}"),
        format!("RANGE_BAR_{bar_type}"),
    ];
    clock
        .timer_names()
        .into_iter()
        .any(|name| names.iter().any(|armed| armed == name))
}

/// Returns whether a bar an interleaving request's trade completes is delivered after that trade:
/// a `RANGE_INCLUSIVE` bar, which contains it; every other bar is delivered before it.
fn is_delivered_after_its_trade(bar: &Bar) -> bool {
    bar.bar_type.spec().aggregation == BarAggregation::RangeInclusive
}

/// Live data held back for a builder a kept request (`update_subscriptions`) is still running.
#[derive(Debug)]
pub(super) enum BarAggregatorWarmup {
    Bar(WarmupBuffer<Bar>),
    Trade(WarmupBuffer<TradeTick>),
    Quote(WarmupBuffer<QuoteTick>),
    /// A joined warm-up's held trades, and the slot through which its handler takes later trades
    /// once the warm-up is released.
    JoinedTrade(WarmupBuffer<TradeTick>, JoinedSlot),
}

impl BarAggregatorWarmup {
    /// Ends the warm-up and feeds the held data to `aggregator` in arrival order.
    pub(super) fn release(&self, aggregator: &mut dyn BarAggregator) {
        match self {
            Self::Bar(buffer) => buffer
                .release()
                .into_iter()
                .for_each(|bar| aggregator.handle_bar(bar)),
            Self::Trade(buffer) => buffer
                .release()
                .into_iter()
                .for_each(|trade| aggregator.handle_trade(trade)),
            Self::Quote(buffer) => buffer
                .release()
                .into_iter()
                .for_each(|quote| aggregator.handle_quote(quote)),
            Self::JoinedTrade(buffer, _) => buffer
                .release()
                .into_iter()
                .for_each(|trade| aggregator.handle_trade(trade)),
        }
    }

    /// Returns the arrival of each record held (see [`WarmupBuffer`]), in arrival order.
    pub(super) fn arrivals(&self) -> Vec<u64> {
        match self {
            Self::Bar(buffer) => buffer.arrivals(),
            Self::Trade(buffer) | Self::JoinedTrade(buffer, _) => buffer.arrivals(),
            Self::Quote(buffer) => buffer.arrivals(),
        }
    }

    /// Ends the warm-up and drops what it held: whoever held into it holds nothing more.
    pub(super) fn discard(&self) {
        match self {
            Self::Bar(buffer) => drop(buffer.release()),
            Self::Trade(buffer) | Self::JoinedTrade(buffer, _) => drop(buffer.release()),
            Self::Quote(buffer) => drop(buffer.release()),
        }
    }

    /// Ends a trade warm-up and returns the builder's copy of each held trade in arrival order,
    /// through `joined`'s cut and floor (see [`JoinedRelease`]) or as [`Self::release`] would
    /// feed them, `None` where the cut drops one; `None` for a warm-up of bars or quotes, which
    /// stays held.
    pub(super) fn release_trades(
        &self,
        joined: Option<JoinedRelease>,
        bar_type: BarType,
    ) -> Option<Vec<Option<TradeTick>>> {
        match (self, joined) {
            (Self::JoinedTrade(buffer, slot), Some(release)) => {
                Some(buffer.release_joined_copies(slot, release, bar_type))
            }
            (Self::Trade(buffer) | Self::JoinedTrade(buffer, _), _) => {
                Some(buffer.release().into_iter().map(Some).collect())
            }
            (Self::Bar(_) | Self::Quote(_), _) => None,
        }
    }
}

/// Returns the cut and the floor that release the held live trades of a joined request's
/// builders, from its complete `answered` trades; `None` for every other request.
fn joined_release(state: &RequestBarAggregation, answered: &[TradeTick]) -> Option<JoinedRelease> {
    state
        .cut_warmup
        .then(|| JoinedRelease::from_answer(answered))
}

/// Typed subscription for bar aggregator handlers.
///
/// Stores the topic and handler for each data type so we can properly
/// unsubscribe from the typed routers.
#[derive(Clone)]
pub enum BarAggregatorSubscription {
    Bar {
        topic: MStr<Topic>,
        handler: TypedHandler<Bar>,
    },
    Trade {
        topic: MStr<Topic>,
        handler: TypedHandler<TradeTick>,
    },
    Quote {
        topic: MStr<Topic>,
        handler: TypedHandler<QuoteTick>,
    },
}

impl Debug for BarAggregatorSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bar { topic, handler } => f
                .debug_struct(stringify!(Bar))
                .field("topic", topic)
                .field("handler_id", &handler.id())
                .finish(),
            Self::Trade { topic, handler } => f
                .debug_struct(stringify!(Trade))
                .field("topic", topic)
                .field("handler_id", &handler.id())
                .finish(),
            Self::Quote { topic, handler } => f
                .debug_struct(stringify!(Quote))
                .field("topic", topic)
                .field("handler_id", &handler.id())
                .finish(),
        }
    }
}

/// Builds a trades part of an interleaved response, carrying the request's identity and window.
pub(super) fn trades_part(resp: &TradesResponse, data: Vec<TradeTick>) -> TradesResponse {
    TradesResponse::new(
        resp.correlation_id,
        resp.client_id,
        resp.instrument_ids.clone(),
        data,
        resp.start,
        resp.end,
        resp.ts_init,
        resp.params.clone(),
    )
}

/// Sends the trades gathered in `pending`, then each of `bars` as a bars part of its own, in the
/// order the aggregators produced them. A bar enters the cache just before its part is sent, so
/// a handler of one bar never finds a later bar in the cache, even one of the same type that
/// the same trade completed. Sends nothing when `bars` is empty.
fn send_interleaved_bars(
    cache: &Rc<RefCell<Cache>>,
    resp: &TradesResponse,
    pending: &mut Vec<TradeTick>,
    bars: Vec<Bar>,
) {
    if bars.is_empty() {
        return;
    }

    if !pending.is_empty() {
        let trades = trades_part(resp, std::mem::take(pending));
        msgbus::send_response_part(&resp.correlation_id, &DataResponse::Trades(trades));
    }

    for bar in bars {
        send_bar_part(
            cache,
            resp.correlation_id,
            resp.client_id,
            (resp.start, resp.end),
            resp.ts_init,
            resp.params.as_ref(),
            bar,
        );
    }
}

/// Sends `bar` as a bars part of its own, carrying the request's identity and window. The bar
/// enters the cache just before the part is sent.
fn send_bar_part(
    cache: &Rc<RefCell<Cache>>,
    correlation_id: UUID4,
    client_id: ClientId,
    (start, end): (Option<UnixNanos>, Option<UnixNanos>),
    ts_init: UnixNanos,
    params: Option<&Params>,
    bar: Bar,
) {
    if let Err(e) = cache.as_ref().borrow_mut().add_bar_historical(bar) {
        log_error_on_cache_insert(&e);
    }

    let part = BarsResponse::new(
        correlation_id,
        client_id,
        bar.bar_type,
        vec![bar],
        start,
        end,
        ts_init,
        params.cloned(),
    );
    msgbus::send_response_part(&correlation_id, &DataResponse::Bars(part));
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use indexmap::IndexMap;
    use jiff::Timestamp;
    use nautilus_common::{
        cache::Cache,
        clock::{Clock, VirtualClock},
        messages::data::{
            DataResponse, DataResponseEnvelope, RequestCommand, RequestTrades, SubscribeBars,
            TradesResponse,
        },
        msgbus::MessageBus,
    };
    use nautilus_core::{UUID4, UnixNanos, datetime::get_timezone};
    use nautilus_model::{
        data::{Bar, BarType, TradeTick},
        enums::AggressorSide,
        identifiers::{ClientId, TradeId, TraderId},
        instruments::{InstrumentAny, stubs::audusd_sim},
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::{
        BarAggregatorSubscription, BarAggregatorWarmup, bar_aggregator_key,
        request_bar_aggregation_from_params, sources_before_dependents, time_zone_from_params,
    };
    use crate::engine::{DataEngine, config::DataEngineConfig};

    #[rstest]
    #[case::utc_default(None, None, "2026-11-02T00:00:00Z")]
    #[case::config(Some("America/New_York"), None, "2026-11-02T05:00:00Z")]
    #[case::utc_override(Some("America/New_York"), Some("UTC"), "2026-11-02T00:00:00Z")]
    fn test_calendar_zone_precedence_and_reuse(
        #[case] config_zone: Option<&str>,
        #[case] override_zone: Option<&str>,
        #[case] expected: &str,
    ) {
        let _msgbus = MessageBus::new(TraderId::new("TESTER-001"), UUID4::new(), None, None)
            .register_message_bus();
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock.borrow_mut().set_time(UnixNanos::from(
            "2026-11-01T12:00:00Z".parse::<Timestamp>().unwrap(),
        ));
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CurrencyPair(audusd_sim()))
            .unwrap();
        let config = DataEngineConfig::builder()
            .maybe_time_bars_time_zone(config_zone.map(str::to_owned))
            .time_bars_skip_first_non_full_bar(true)
            .build();
        let mut engine = DataEngine::new(
            Rc::clone(&clock) as Rc<RefCell<dyn Clock>>,
            Rc::clone(&cache),
            Some(config),
        );
        let bar_type = BarType::from("AUD/USD.SIM-1-DAY-LAST-INTERNAL");
        let mut params = IndexMap::from([(
            "skip_first_non_full_bar".to_owned(),
            serde_json::json!(false),
        )]);

        if let Some(zone) = override_zone {
            params.insert("time_zone".to_owned(), serde_json::json!(zone));
        }

        let cmd = SubscribeBars::new(
            bar_type,
            None,
            Some(bar_type.instrument_id().venue),
            UUID4::new(),
            UnixNanos::default(),
            None,
            Some(nautilus_core::Params::new(params)),
        );
        engine.subscribe_bars(&cmd).unwrap();
        engine.subscribe_bars(&cmd).unwrap();
        let mut params = IndexMap::clone(cmd.params.as_ref().unwrap());
        params.insert("time_zone".to_owned(), serde_json::json!("Asia/Tokyo"));

        let cmd = SubscribeBars {
            params: Some(nautilus_core::Params::new(params)),
            ..cmd
        };

        let e = engine.subscribe_bars(&cmd).unwrap_err();

        // Request-local instances can use another zone without replacing the live instance
        engine
            .create_bar_aggregator_for_key(
                bar_type,
                Some(UUID4::new()),
                None,
                Some(get_timezone("Asia/Tokyo").unwrap()),
            )
            .unwrap();
        assert_eq!(
            e.to_string(),
            format!("Cannot reuse bar aggregator {bar_type} with a different time zone")
        );
        assert_eq!(
            clock.borrow().next_time_ns(&format!("TIME_BAR_{bar_type}")),
            Some(UnixNanos::from(expected.parse::<Timestamp>().unwrap()))
        );
        let now = clock.borrow().timestamp_ns();
        engine
            .bar_aggregators
            .get(&bar_aggregator_key(bar_type, None))
            .unwrap()
            .borrow_mut()
            .update(Price::from("0.65000"), Quantity::from(2), now);
        let close = UnixNanos::from(expected.parse::<Timestamp>().unwrap());
        let events = clock.borrow_mut().advance_time(close, true);
        let handlers = clock.borrow().match_handlers(events);
        for handler in handlers {
            handler.callback.call(handler.event);
        }

        assert_eq!(
            cache.borrow().bar(&bar_type).copied(),
            Some(Bar::new(
                bar_type,
                Price::from("0.65000"),
                Price::from("0.65000"),
                Price::from("0.65000"),
                Price::from("0.65000"),
                Quantity::from(2),
                close,
                close
            ))
        );
    }

    #[rstest]
    #[case::wrong_type(serde_json::json!(17), "`time_zone` parameter must be a string")]
    #[case::unknown_zone(serde_json::json!("Not/A_Zone"), "invalid `time_zone` parameter \"Not/A_Zone\"")]
    fn test_calendar_zone_parameter_rejects_invalid_value(
        #[case] value: serde_json::Value,
        #[case] expected: &str,
    ) {
        let params = nautilus_core::Params::new([("time_zone".to_owned(), value)].into());
        assert_eq!(
            time_zone_from_params(Some(&params))
                .unwrap_err()
                .to_string(),
            expected
        );
    }

    #[rstest]
    fn test_calendar_request_zone_checks_shared_instance() {
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CurrencyPair(audusd_sim()))
            .unwrap();
        let mut engine = DataEngine::new(clock, cache, None);
        let bar_type = BarType::from("AUD/USD.SIM-1-MONTH-LAST-INTERNAL");
        engine
            .create_bar_aggregator_for_key(bar_type, None, None, None)
            .unwrap();
        let params: nautilus_core::Params = serde_json::from_value(serde_json::json!({
            "bar_types": [bar_type.to_string()], "update_subscriptions": true,
            "time_zone": "America/New_York"
        }))
        .unwrap();
        let state = request_bar_aggregation_from_params(Some(&params))
            .unwrap()
            .unwrap();
        let id = UUID4::new();
        let error = engine
            .prepare_request_bar_aggregators_from_state(id, &state)
            .unwrap_err();
        let mut params = IndexMap::clone(&params);
        params.insert("update_subscriptions".to_owned(), serde_json::json!(false));
        let params = nautilus_core::Params::new(params);
        let state = request_bar_aggregation_from_params(Some(&params))
            .unwrap()
            .unwrap();
        engine
            .prepare_request_bar_aggregators_from_state(id, &state)
            .unwrap();
        let instance = engine
            .bar_aggregators
            .get(&bar_aggregator_key(bar_type, Some(id)))
            .unwrap()
            .borrow();
        let request_zone = instance
            .as_any()
            .downcast_ref::<crate::aggregation::TimeBarAggregator>()
            .unwrap()
            .time_zone()
            .clone();
        let live = engine
            .bar_aggregators
            .get(&bar_aggregator_key(bar_type, None))
            .unwrap()
            .borrow();
        let live_zone = live
            .as_any()
            .downcast_ref::<crate::aggregation::TimeBarAggregator>()
            .unwrap()
            .time_zone()
            .clone();
        assert_eq!(
            error.to_string(),
            format!("Cannot reuse bar aggregator {bar_type} with a different time zone")
        );
        assert_eq!(request_zone, get_timezone("America/New_York").unwrap());
        assert_eq!(live_zone, jiff::tz::TimeZone::UTC);
        assert_eq!(engine.bar_aggregators.len(), 2);
    }

    #[rstest]
    #[case::source_first(&["1-MINUTE-MID-INTERNAL", "5-MINUTE-MID-INTERNAL@1-MINUTE-INTERNAL"])]
    #[case::composite_first(&["5-MINUTE-MID-INTERNAL@1-MINUTE-INTERNAL", "1-MINUTE-MID-INTERNAL"])]
    fn test_sources_before_dependents_orders_two_level_chain(#[case] raw: &[&str]) {
        let bar_types = to_bar_types(raw);

        let ordered = sources_before_dependents(&bar_types);

        assert_eq!(
            ordered,
            to_bar_types(&[
                "1-MINUTE-MID-INTERNAL",
                "5-MINUTE-MID-INTERNAL@1-MINUTE-INTERNAL",
            ])
        );
    }

    #[rstest]
    fn test_sources_before_dependents_orders_three_level_chain_and_keeps_unrelated_order() {
        let bar_types = to_bar_types(&[
            "15-MINUTE-MID-INTERNAL@5-MINUTE-INTERNAL",
            "100-TICK-LAST-INTERNAL",
            "5-MINUTE-MID-INTERNAL@1-MINUTE-INTERNAL",
            "1-MINUTE-MID-INTERNAL",
            "50-TICK-LAST-INTERNAL",
        ]);

        let ordered = sources_before_dependents(&bar_types);

        assert_eq!(
            ordered,
            to_bar_types(&[
                "100-TICK-LAST-INTERNAL",
                "1-MINUTE-MID-INTERNAL",
                "50-TICK-LAST-INTERNAL",
                "5-MINUTE-MID-INTERNAL@1-MINUTE-INTERNAL",
                "15-MINUTE-MID-INTERNAL@5-MINUTE-INTERNAL",
            ])
        );
    }

    fn to_bar_types(raw: &[&str]) -> Vec<BarType> {
        raw.iter()
            .map(|bar_type| BarType::from(format!("AUD/USD.SIM-{bar_type}").as_str()))
            .collect()
    }

    /// An engine without clients, and an interleaved trades request for AUD/USD 1-TICK bars.
    fn interleaved_engine() -> (DataEngine, RequestCommand) {
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CurrencyPair(audusd_sim()))
            .unwrap();
        let engine = DataEngine::new(clock, cache, None);
        let params: nautilus_core::Params = serde_json::from_value(serde_json::json!({
            "bar_types": ["AUD/USD.SIM-1-TICK-LAST-INTERNAL"],
            "update_subscriptions": false,
            "interleave": true,
        }))
        .unwrap();
        let request = RequestCommand::Trades(RequestTrades::new(
            audusd_sim().id,
            None,
            None,
            None,
            Some(ClientId::new("SIM")),
            UUID4::new(),
            UnixNanos::default(),
            Some(params),
        ));
        (engine, request)
    }

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
                None,
                UnixNanos::from(ts),
                None,
            )),
            completed,
            None,
        )
    }

    #[rstest]
    #[case::completed_answer(true)]
    #[case::payload_free_failure(false)]
    fn test_interleaved_entry_lives_through_partial_envelopes_until_the_request_ends(
        #[case] completed_answer: bool,
    ) {
        let _msgbus = MessageBus::new(TraderId::new("TESTER-001"), UUID4::new(), None, None)
            .register_message_bus();
        let (mut engine, request) = interleaved_engine();
        let request_id = *request.request_id();
        engine.prepare_request_bar_aggregators(&request).unwrap();

        for ts in 1..=3 {
            assert!(engine.interleaved_requests.contains_key(&request_id));
            engine.response_envelope(trades_envelope(request_id, ts, false));
        }
        assert!(engine.interleaved_requests.contains_key(&request_id));
        if completed_answer {
            engine.response_envelope(trades_envelope(request_id, 4, true));
        } else {
            engine.response_envelope(DataResponseEnvelope::terminal(
                request_id,
                Some("the source stopped".to_owned()),
            ));
        }

        assert!(engine.interleaved_requests.is_empty());
        assert!(engine.request_bar_aggregations.is_empty());
    }

    #[rstest]
    fn test_interleaved_dispatch_error_leaves_no_entry() {
        let _msgbus = MessageBus::new(TraderId::new("TESTER-001"), UUID4::new(), None, None)
            .register_message_bus();
        let (mut engine, request) = interleaved_engine();

        let error = engine.execute_request(request).unwrap_err();

        assert!(error.to_string().contains("no client found"), "{error}");
        assert!(engine.interleaved_requests.is_empty());
        assert!(engine.request_bar_aggregations.is_empty());
    }

    #[rstest]
    #[case::plain_live(None, "BarTradeHandler", None)]
    #[case::ordinary_warmup(Some(false), "BarTradeHandler", Some("Trade"))]
    #[case::joined_warmup(Some(true), "JoinedTradeHandler", Some("JoinedTrade"))]
    fn test_only_a_joined_warmup_subscribes_the_joined_trade_handler(
        #[case] cut_warmup: Option<bool>,
        #[case] handler_name: &str,
        #[case] warmup: Option<&str>,
    ) {
        let _msgbus = MessageBus::new(TraderId::new("TESTER-001"), UUID4::new(), None, None)
            .register_message_bus();
        let (mut engine, _) = interleaved_engine();
        let bar_type = BarType::from("AUD/USD.SIM-1-TICK-LAST-INTERNAL");

        // A running `update_subscriptions` request defers the live subscription to its warm-up
        if let Some(cut_warmup) = cut_warmup {
            let params: nautilus_core::Params = serde_json::from_value(serde_json::json!({
                "bar_types": [bar_type.to_string()],
                "update_subscriptions": true,
                "cut_warmup": cut_warmup,
            }))
            .unwrap();
            let request = RequestCommand::Trades(RequestTrades::new(
                audusd_sim().id,
                Some(UnixNanos::default().to_datetime_utc()),
                None,
                None,
                Some(ClientId::new("SIM")),
                UUID4::new(),
                UnixNanos::default(),
                Some(params),
            ));
            engine.prepare_request_bar_aggregators(&request).unwrap();
        }
        engine
            .subscribe_bars(&SubscribeBars::new(
                bar_type,
                None,
                Some(bar_type.instrument_id().venue),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();

        let key = bar_aggregator_key(bar_type, None);
        let handler_ids: Vec<String> = engine.bar_aggregator_handlers[&key]
            .iter()
            .map(|subscription| match subscription {
                BarAggregatorSubscription::Trade { handler, .. } => handler.id().to_string(),
                other => panic!("not a trade subscription: {other:?}"),
            })
            .collect();
        assert_eq!(handler_ids, [format!("{handler_name}|{bar_type}")]);
        let warmup_kind = engine
            .bar_aggregator_warmups
            .get(&key)
            .map(|warmup| match warmup {
                BarAggregatorWarmup::Trade(_) => "Trade",
                BarAggregatorWarmup::JoinedTrade(..) => "JoinedTrade",
                other => panic!("not a trade warm-up: {other:?}"),
            });
        assert_eq!(warmup_kind, warmup);
    }
}
