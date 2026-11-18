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

use std::{cmp, str::FromStr};

use anyhow::Context;
use indexmap::IndexMap;
use jiff::{civil, tz::TimeZone};
use nautilus_common::messages::data::{DataResponse, RequestBars, RequestCommand, SubscribeBars};
use nautilus_core::{Params, UUID4, UnixNanos, datetime::get_timezone};
use nautilus_model::{
    data::BarType,
    enums::{AggregationSource, ContinuousFutureAdjustmentType, PriceType},
    identifiers::{ClientId, InstrumentId},
};
use rust_decimal::Decimal;
use serde_json::Value;

use super::time_range::TIME_RANGE_GENERATOR;

pub(super) const CONTINUOUS_FUTURE_PARENT_REQUEST_ID: &str = "continuous_future_parent_request_id";

const CONTINUOUS_FUTURE_TRANSITIONS: &str = "continuous_future_transitions";
const CONTINUOUS_FUTURE_ADJUSTMENT_MODE: &str = "continuous_future_adjustment_mode";
const LAST_POST_INSTRUMENT_ID: &str = "last_post_instrument_id";
const FIRST_PRE_INSTRUMENT_ID: &str = "first_pre_instrument_id";
const BAR_TYPES: &str = "bar_types";
pub(super) const INTERLEAVE: &str = "interleave";
/// The request parameter naming a filter expression, which leaves rows of the window out.
pub(super) const FILTER_EXPR: &str = "filter_expr";
const UPDATE_SUBSCRIPTIONS: &str = "update_subscriptions";
const SCHEMA: &str = "schema";
const CUT_WARMUP: &str = "cut_warmup";
const DEADLINE: &str = "deadline";
const SESSION_OPENS: &str = "session_opens";
const QUOTES: &str = "quotes";
const CUT_WARMUP_REFUSED: &str = "Cannot admit a `cut_warmup` request";
pub(super) const QUOTES_REFUSED: &str = "Cannot answer the quotes of a `quotes` request";
pub(super) const INTERLEAVE_REFUSED: &str = "Cannot answer an `interleave` request in live order";
const JOIN_SCHEMA: &str = "tbbo";
const SESSION_TIME_ZONE: &str = "America/New_York";
const SESSION_OPEN_TIME: civil::Time = civil::time(18, 0, 0, 0);

#[derive(Debug, Clone)]
pub(super) struct RequestBarAggregation {
    pub(super) bar_types: Vec<BarType>,
    pub(super) update_subscriptions: bool,
    pub(super) disable_build_with_no_updates: bool,
    pub(super) skip_first_non_full_bar: Option<bool>,
    pub(super) time_zone: Option<TimeZone>,
    /// Answer a trades request as trades and aggregated bars parts in live order (admitted by
    /// `DataEngine::admit_interleave`; what callers get is documented on
    /// `DataActor::request_trades`, `nautilus_common::actor`).
    pub(super) interleave: bool,
    /// Release the held live trades through the join's cut and floor (an admitted `cut_warmup`
    /// request, the history/live join).
    pub(super) cut_warmup: bool,
}

impl RequestBarAggregation {
    pub(super) const fn aggregator_request_id(&self, request_id: UUID4) -> Option<UUID4> {
        if self.update_subscriptions {
            None
        } else {
            Some(request_id)
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ContinuousFutureRequest {
    pub(super) primary_bar_type: BarType,
    pub(super) request_bar_aggregation: RequestBarAggregation,
    pub(super) transitions: Vec<ContinuousFutureTransition>,
    pub(super) adjustment_mode: ContinuousFutureAdjustmentType,
    first_pre_instrument_id: Option<InstrumentId>,
    last_post_instrument_id: Option<InstrumentId>,
}

#[derive(Debug, Clone)]
pub(super) struct ContinuousFutureRequestState {
    pub(super) parent: RequestBars,
    pub(super) request: ContinuousFutureRequest,
    pub(super) start_ns: UnixNanos,
    pub(super) cursor_ns: UnixNanos,
    pub(super) end_ns: UnixNanos,
    pub(super) response_client_id: ClientId,
    pub(super) data_count: u64,
}

impl ContinuousFutureRequest {
    pub(super) fn first_segment_instrument_id(&self) -> InstrumentId {
        self.transitions[0].pre_instrument_id
    }

    pub(super) fn next_segment(
        &self,
        cursor_ns: u64,
        end_ns: u64,
    ) -> Option<ContinuousFutureSegment> {
        if cursor_ns > end_ns || self.transitions.is_empty() {
            return None;
        }

        for (index, row) in self.transitions.iter().enumerate() {
            if cursor_ns < row.transition_time_ns {
                return Some(ContinuousFutureSegment {
                    index,
                    instrument_id: row.pre_instrument_id,
                    start_ns: cursor_ns,
                    end_ns: cmp::min(end_ns, row.transition_time_ns - 1),
                });
            }
        }

        let row = self.transitions.last()?;
        Some(ContinuousFutureSegment {
            index: self.transitions.len(),
            instrument_id: row.post_instrument_id,
            start_ns: cursor_ns,
            end_ns,
        })
    }

    pub(super) fn adjustment_for_segment(&self, segment_index: usize) -> Decimal {
        let is_ratio = self.adjustment_mode.is_ratio();
        let is_backward = self.adjustment_mode.is_backward();
        let mut cumulative = if is_ratio {
            Decimal::ONE
        } else {
            Decimal::ZERO
        };

        let transition_start_index = self
            .first_pre_instrument_id
            .and_then(|instrument_id| {
                self.transitions
                    .iter()
                    .position(|row| row.pre_instrument_id == instrument_id)
            })
            .unwrap_or(0);
        let transition_stop_index = self
            .last_post_instrument_id
            .and_then(|instrument_id| {
                self.transitions
                    .iter()
                    .position(|row| row.post_instrument_id == instrument_id)
                    .map(|index| index + 1)
            })
            .unwrap_or(self.transitions.len());

        let clamped_segment_index = cmp::max(
            transition_start_index,
            cmp::min(segment_index, transition_stop_index),
        );
        let range: Box<dyn Iterator<Item = usize>> = if is_backward {
            Box::new(clamped_segment_index..transition_stop_index)
        } else {
            Box::new(transition_start_index..clamped_segment_index)
        };

        for index in range {
            let row = &self.transitions[index];

            if is_ratio {
                cumulative *= if is_backward {
                    row.post_price / row.pre_price
                } else {
                    row.pre_price / row.post_price
                };
            } else {
                cumulative += if is_backward {
                    row.post_price - row.pre_price
                } else {
                    row.pre_price - row.post_price
                };
            }
        }

        cumulative
    }

    pub(super) fn source_for_segment(
        &self,
        segment_instrument_id: InstrumentId,
    ) -> ContinuousFutureSource {
        let reference_bar_type = if self.primary_bar_type.is_composite() {
            self.primary_bar_type.composite()
        } else {
            self.primary_bar_type
        };

        if reference_bar_type.is_externally_aggregated() {
            let source_bar_type = BarType::new(
                segment_instrument_id,
                reference_bar_type.spec(),
                AggregationSource::External,
            );
            return ContinuousFutureSource::Bars(source_bar_type);
        }

        if reference_bar_type.spec().price_type == PriceType::Last {
            ContinuousFutureSource::Trades
        } else {
            ContinuousFutureSource::Quotes
        }
    }

    pub(super) fn child_params(&self, parent_params: Option<&Params>, parent_id: UUID4) -> Params {
        let mut child_params = parent_params
            .map(|params| IndexMap::clone(params))
            .unwrap_or_default();
        child_params.shift_remove(CONTINUOUS_FUTURE_TRANSITIONS);
        child_params.shift_remove(CONTINUOUS_FUTURE_ADJUSTMENT_MODE);
        child_params.shift_remove(LAST_POST_INSTRUMENT_ID);
        child_params.shift_remove(FIRST_PRE_INSTRUMENT_ID);
        child_params.shift_remove(BAR_TYPES);

        // The parent consumes each segment as one response, so a segment must not fan out into
        // time-range windows.
        child_params.shift_remove(TIME_RANGE_GENERATOR);
        child_params.insert(
            CONTINUOUS_FUTURE_PARENT_REQUEST_ID.to_string(),
            Value::String(parent_id.to_string()),
        );
        Params::new(child_params)
    }
}

#[derive(Debug, Clone)]
pub(super) struct ContinuousFutureTransition {
    pub(super) transition_time_ns: u64,
    pub(super) pre_instrument_id: InstrumentId,
    pub(super) post_instrument_id: InstrumentId,
    pre_price: Decimal,
    post_price: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContinuousFutureSource {
    Bars(BarType),
    Trades,
    Quotes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ContinuousFutureSegment {
    pub(super) index: usize,
    pub(super) instrument_id: InstrumentId,
    pub(super) start_ns: u64,
    pub(super) end_ns: u64,
}

pub(super) fn request_params(req: &RequestCommand) -> Option<&Params> {
    match req {
        RequestCommand::Data(cmd) => cmd.params.as_ref(),
        RequestCommand::Instrument(cmd) => cmd.params.as_ref(),
        RequestCommand::Instruments(cmd) => cmd.params.as_ref(),
        RequestCommand::BookSnapshot(cmd) => cmd.params.as_ref(),
        RequestCommand::BookDeltas(cmd) => cmd.params.as_ref(),
        RequestCommand::BookDepth(cmd) => cmd.params.as_ref(),
        RequestCommand::Quotes(cmd) => cmd.params.as_ref(),
        RequestCommand::Trades(cmd) => cmd.params.as_ref(),
        RequestCommand::FundingRates(cmd) => cmd.params.as_ref(),
        RequestCommand::OptionChainReferencePrice(cmd) => cmd.params.as_ref(),
        RequestCommand::Bars(cmd) => cmd.params.as_ref(),
        RequestCommand::Join(cmd) => cmd.params.as_ref(),
    }
}

pub(super) fn response_params(resp: &DataResponse) -> Option<&Params> {
    match resp {
        DataResponse::Data(resp) => resp.params.as_ref(),
        DataResponse::Instrument(resp) => resp.params.as_ref(),
        DataResponse::Instruments(resp) => resp.params.as_ref(),
        DataResponse::Book(resp) => resp.params.as_ref(),
        DataResponse::BookDeltas(resp) => resp.params.as_ref(),
        DataResponse::BookDepth(resp) => resp.params.as_ref(),
        DataResponse::Quotes(resp) => resp.params.as_ref(),
        DataResponse::Trades(resp) => resp.params.as_ref(),
        DataResponse::FundingRates(resp) => resp.params.as_ref(),
        DataResponse::OptionChainReferencePrice(resp) => resp.params.as_ref(),
        DataResponse::Bars(resp) => resp.params.as_ref(),
    }
}

pub(super) fn request_bar_aggregation_from_params(
    params: Option<&Params>,
) -> anyhow::Result<Option<RequestBarAggregation>> {
    let Some(params) = params else {
        return Ok(None);
    };
    let Some(value) = params.get(BAR_TYPES) else {
        return Ok(None);
    };

    let bar_types = parse_bar_types(value)?;

    if bar_types.is_empty() {
        return Ok(None);
    }

    if let Some(bar_type) = bar_types
        .iter()
        .find(|bar_type| !bar_type.is_internally_aggregated())
    {
        anyhow::bail!("Cannot request aggregated bars: {bar_type} must be internally aggregated");
    }

    let mut unique_bar_types = Vec::with_capacity(bar_types.len());
    for bar_type in bar_types {
        if !unique_bar_types.contains(&bar_type) {
            unique_bar_types.push(bar_type);
        }
    }

    let update_subscriptions = params.get_bool(UPDATE_SUBSCRIPTIONS).unwrap_or(false);
    let skip_first_non_full_bar = params.get_bool("skip_first_non_full_bar");
    let interleave = params.get_bool(INTERLEAVE).unwrap_or(false);
    // Only an admitted request reaches here with `cut_warmup` set: admission refuses the others
    let cut_warmup = params.get_bool(CUT_WARMUP) == Some(true);

    Ok(Some(RequestBarAggregation {
        bar_types: unique_bar_types,
        update_subscriptions,
        disable_build_with_no_updates: false,
        skip_first_non_full_bar,
        time_zone: time_zone_from_params(Some(params))?,
        interleave,
        cut_warmup,
    }))
}

fn parse_bar_types(value: &Value) -> anyhow::Result<Vec<BarType>> {
    let values = value
        .as_array()
        .context("`bar_types` request parameter must be an array")?;
    let mut bar_types = Vec::with_capacity(values.len());
    for value in values {
        let raw = value
            .as_str()
            .context("`bar_types` request parameter must contain strings")?;
        bar_types
            .push(BarType::from_str(raw).context("failed to parse `bar_types` request parameter")?);
    }
    Ok(bar_types)
}

/// A request the engine admitted for the history/live join (`cut_warmup`): the trades of one
/// instrument, past whole sessions and the current one, cut against the live stream.
#[derive(Debug)]
pub(super) struct AdmittedJoin {
    pub(super) instrument_id: InstrumentId,
    /// The engine's deadline for the whole request (UNIX nanoseconds, after the clock's time
    /// at admission).
    pub(super) deadline: UnixNanos,
    /// The trading-day opens of the requested depth, strictly increasing, each at civil 18:00
    /// `America/New_York`; the last is the current trading day's open (`H`).
    pub(super) session_opens: Vec<UnixNanos>,
}

/// Admits a request that sets `cut_warmup`, or refuses it with a reason naming the first
/// condition it fails.
///
/// A request without `cut_warmup` (or with it false) returns `None` and takes the ordinary path:
/// only the absent parameter is read, and `now` is not called. An admitted request is a trades
/// request for one instrument that asks for internal, non-composite `LAST` bar types, sets
/// `update_subscriptions`, gives a `deadline` after `now`, is open-ended (no end), sets `schema`
/// to `tbbo` and gives verified `session_opens` (see [`verified_session_opens`]).
///
/// # Errors
///
/// Returns an error if the request sets `cut_warmup` and is not admitted; the error names the
/// failed condition.
pub(super) fn admit_cut_warmup(
    req: &RequestCommand,
    now: impl FnOnce() -> UnixNanos,
) -> anyhow::Result<Option<AdmittedJoin>> {
    let Some(params) = request_params(req) else {
        return Ok(None);
    };
    let Some(value) = params.get(CUT_WARMUP) else {
        return Ok(None);
    };
    match value.as_bool() {
        Some(true) => {}
        Some(false) => return Ok(None),
        None => anyhow::bail!("{CUT_WARMUP_REFUSED}: `cut_warmup` must be a boolean"),
    }

    let RequestCommand::Trades(cmd) = req else {
        anyhow::bail!("{CUT_WARMUP_REFUSED}: only a trades request can set `cut_warmup`");
    };
    let [instrument_id] = cmd.instrument_ids[..] else {
        anyhow::bail!(
            "{CUT_WARMUP_REFUSED}: it must name exactly one instrument (batches are refused), \
             received {}",
            cmd.instrument_ids.len(),
        );
    };

    let bar_types = params
        .get(BAR_TYPES)
        .map(parse_bar_types)
        .transpose()?
        .unwrap_or_default();
    anyhow::ensure!(
        !bar_types.is_empty(),
        "{CUT_WARMUP_REFUSED}: it must ask for `bar_types`"
    );
    if let Some(bar_type) = bar_types.iter().find(|bar_type| {
        !bar_type.is_internally_aggregated()
            || bar_type.is_composite()
            || bar_type.spec().price_type != PriceType::Last
    }) {
        anyhow::bail!(
            "{CUT_WARMUP_REFUSED}: {bar_type} is not an internal, non-composite LAST bar type"
        );
    }

    anyhow::ensure!(
        params.get_bool(UPDATE_SUBSCRIPTIONS) == Some(true),
        "{CUT_WARMUP_REFUSED}: it must set `update_subscriptions`"
    );

    let now = now();
    let deadline = params
        .get_u64(DEADLINE)
        .map(UnixNanos::from)
        .with_context(|| {
            format!("{CUT_WARMUP_REFUSED}: it must give a `deadline` in UNIX nanoseconds")
        })?;
    anyhow::ensure!(
        deadline > now,
        "{CUT_WARMUP_REFUSED}: its `deadline` {deadline} is not after the clock's time {now}"
    );

    anyhow::ensure!(
        cmd.end.is_none(),
        "{CUT_WARMUP_REFUSED}: it must be open-ended (no end)"
    );
    anyhow::ensure!(
        params.get_str(SCHEMA) == Some(JOIN_SCHEMA),
        "{CUT_WARMUP_REFUSED}: it must set `schema` to `{JOIN_SCHEMA}`"
    );

    let session_opens = verified_session_opens(params, now)?;

    Ok(Some(AdmittedJoin {
        instrument_id,
        deadline,
        session_opens,
    }))
}

/// Returns the `session_opens` parameter, verified: strictly increasing instants, each at civil
/// 18:00 `America/New_York` of its day (read through the time zone, so a daylight-saving change
/// moves the UTC instant), the last one the current trading day's open by `now`.
fn verified_session_opens(params: &Params, now: UnixNanos) -> anyhow::Result<Vec<UnixNanos>> {
    let values = params
        .get(SESSION_OPENS)
        .and_then(Value::as_array)
        .with_context(|| {
            format!("{CUT_WARMUP_REFUSED}: `session_opens` must be an array of UNIX nanoseconds")
        })?;
    let time_zone = get_timezone(SESSION_TIME_ZONE)?;

    let mut opens: Vec<UnixNanos> = Vec::with_capacity(values.len());
    for value in values {
        let open = value.as_u64().map(UnixNanos::from).with_context(|| {
            format!("{CUT_WARMUP_REFUSED}: `session_opens` must contain UNIX nanoseconds")
        })?;
        if let Some(previous) = opens.last() {
            anyhow::ensure!(
                open > *previous,
                "{CUT_WARMUP_REFUSED}: `session_opens` must be strictly increasing, {open} follows \
                 {previous}"
            );
        }
        let civil = jiff::Timestamp::from(open).to_zoned(time_zone.clone());
        anyhow::ensure!(
            civil.time() == SESSION_OPEN_TIME,
            "{CUT_WARMUP_REFUSED}: `session_opens` {open} is at {civil}, not at 18:00 \
             {SESSION_TIME_ZONE}"
        );
        opens.push(open);
    }

    let Some(&last) = opens.last() else {
        anyhow::bail!(
            "{CUT_WARMUP_REFUSED}: `session_opens` must name the current trading day's open"
        );
    };
    let current = current_session_open(now, &time_zone)?;
    anyhow::ensure!(
        last == current,
        "{CUT_WARMUP_REFUSED}: the last `session_opens` {last} is not the current trading day's \
         open {current}"
    );

    Ok(opens)
}

/// Returns the latest civil 18:00 `America/New_York` at or before `now`.
fn current_session_open(now: UnixNanos, time_zone: &TimeZone) -> anyhow::Result<UnixNanos> {
    let now_ts = jiff::Timestamp::from(now);
    let today = now_ts.to_zoned(time_zone.clone()).date();
    let open_today = today
        .to_datetime(SESSION_OPEN_TIME)
        .to_zoned(time_zone.clone())?;
    let open = if open_today.timestamp() <= now_ts {
        open_today
    } else {
        today
            .yesterday()?
            .to_datetime(SESSION_OPEN_TIME)
            .to_zoned(time_zone.clone())?
    };
    Ok(UnixNanos::from(open.timestamp()))
}

/// Returns whether `req` sets `cut_warmup`: past admission, a request of the history/live join.
pub(super) fn sets_cut_warmup(req: &RequestCommand) -> bool {
    request_params(req).and_then(|params| params.get_bool(CUT_WARMUP)) == Some(true)
}

/// Returns whether `req` is a trades request that sets `quotes`: its catalog legs also answer the
/// instrument's quotes, delivered with its trades in one time order.
///
/// Only an admitted request reaches its legs: one that cannot carry its quotes is refused first
/// (see [`carries_quotes`]). What callers get, and every refusal, is documented on
/// `DataActor::request_trades` (`nautilus_common::actor`).
pub(super) fn sets_quotes(req: &RequestCommand) -> bool {
    matches!(req, RequestCommand::Trades(_))
        && request_params(req).and_then(|params| params.get_bool(QUOTES)) == Some(true)
}

/// Returns whether `req` is a trades request whose `quotes` param is present and not `false`
/// (set, or not a boolean): the engine admits it on its whole route and plan before anything of
/// it runs, and refuses it by name when it cannot carry its quotes.
pub(super) fn carries_quotes(req: &RequestCommand) -> bool {
    matches!(req, RequestCommand::Trades(_))
        && request_params(req)
            .and_then(|params| params.get(QUOTES))
            .is_some_and(|value| value.as_bool() != Some(false))
}

/// Returns the trading days from the one opening at `first` up to `end` (exclusive), each as its
/// open and the next trading day's open, both at civil 18:00 `America/New_York` read through the
/// time zone (never by adding 24 hours), so a day across a daylight-saving change lasts 23 or 25
/// hours.
#[cfg(feature = "streaming")]
pub(super) fn trading_days(
    first: UnixNanos,
    end: UnixNanos,
) -> anyhow::Result<Vec<(UnixNanos, UnixNanos)>> {
    let time_zone = get_timezone(SESSION_TIME_ZONE)?;
    let mut date = jiff::Timestamp::from(first)
        .to_zoned(time_zone.clone())
        .date();
    let mut open = session_open_on(date, &time_zone)?;
    let mut days = Vec::new();
    while open < end {
        date = date.tomorrow()?;
        let next = session_open_on(date, &time_zone)?;
        days.push((open, next));
        open = next;
    }
    Ok(days)
}

#[cfg(feature = "streaming")]
fn session_open_on(date: civil::Date, time_zone: &TimeZone) -> anyhow::Result<UnixNanos> {
    let open = date
        .to_datetime(SESSION_OPEN_TIME)
        .to_zoned(time_zone.clone())?;
    Ok(UnixNanos::from(open.timestamp()))
}

/// Returns the failure reason of a request that builds bars when no source covers `[start, end]`
/// of its window, the first such part, for `cause`.
pub(super) fn uncovered_window_reason(start: UnixNanos, end: UnixNanos, cause: &str) -> String {
    format!(
        "No source covers {} to {} of the request's window: {cause}",
        start.to_datetime_utc(),
        end.to_datetime_utc(),
    )
}

/// Names and counts the trading days a join did not serve, by their opens in New York time.
#[cfg(feature = "streaming")]
pub(super) fn describe_missing_sessions(missing: &[UnixNanos]) -> anyhow::Result<String> {
    let time_zone = get_timezone(SESSION_TIME_ZONE)?;
    let opens = missing
        .iter()
        .map(|open| {
            jiff::Timestamp::from(*open)
                .to_zoned(time_zone.clone())
                .strftime("%Y-%m-%dT%H:%M%:z")
                .to_string()
        })
        .collect::<Vec<_>>();
    Ok(format!(
        "{} missing sessions (`missing_sessions`), the trading days opening {}",
        missing.len(),
        opens.join(", "),
    ))
}

pub(super) fn time_zone_from_params(params: Option<&Params>) -> anyhow::Result<Option<TimeZone>> {
    let Some(value) = params.and_then(|params| params.get("time_zone")) else {
        return Ok(None);
    };

    let name = value
        .as_str()
        .context("`time_zone` parameter must be a string")?;
    Ok(Some(get_timezone(name).with_context(|| {
        format!("invalid `time_zone` parameter {name:?}")
    })?))
}

pub(super) fn remove_request_bar_aggregation_params(params: &mut IndexMap<String, Value>) {
    params.shift_remove(BAR_TYPES);
}

pub(super) fn has_continuous_future_params(params: Option<&Params>) -> bool {
    params.is_some_and(|params| params.contains_key(CONTINUOUS_FUTURE_TRANSITIONS))
}

pub(super) fn continuous_future_request_from_bars(
    request: &RequestBars,
) -> anyhow::Result<Option<ContinuousFutureRequest>> {
    let Some(params) = request.params.as_ref() else {
        return Ok(None);
    };

    if !params.contains_key(CONTINUOUS_FUTURE_TRANSITIONS) {
        return Ok(None);
    }

    let bar_types = continuous_future_bar_types(request, params)?;
    parse_continuous_future(bar_types, params).map(Some)
}

pub(super) fn continuous_future_subscription_from_bars(
    cmd: &SubscribeBars,
) -> anyhow::Result<Option<ContinuousFutureRequest>> {
    let Some(params) = cmd.params.as_ref() else {
        return Ok(None);
    };

    if !params.contains_key(CONTINUOUS_FUTURE_TRANSITIONS) {
        return Ok(None);
    }

    if params.contains_key(BAR_TYPES) {
        anyhow::bail!(
            "Continuous future bar subscriptions must not include `bar_types`; pass the chain in continuous_future_transitions instead"
        );
    }

    parse_continuous_future(vec![cmd.single_bar_type()?], params).map(Some)
}

fn parse_continuous_future(
    bar_types: Vec<BarType>,
    params: &Params,
) -> anyhow::Result<ContinuousFutureRequest> {
    let primary_bar_type = bar_types[0];
    if !primary_bar_type.is_internally_aggregated() {
        anyhow::bail!("Continuous future target {primary_bar_type} must be internally aggregated");
    }

    let transitions_value = params
        .get(CONTINUOUS_FUTURE_TRANSITIONS)
        .context("missing `continuous_future_transitions`")?;
    let adjustment_mode = parse_adjustment_mode(params.get(CONTINUOUS_FUTURE_ADJUSTMENT_MODE))
        .with_context(|| {
            format!("Invalid continuous future adjustment mode for {primary_bar_type}")
        })?;
    let first_pre_instrument_id = parse_optional_chain_bound(
        params,
        FIRST_PRE_INSTRUMENT_ID,
        primary_bar_type.instrument_id(),
    )?;
    let last_post_instrument_id = parse_optional_chain_bound(
        params,
        LAST_POST_INSTRUMENT_ID,
        primary_bar_type.instrument_id(),
    )?;
    let transitions = parse_transitions(
        primary_bar_type,
        transitions_value,
        adjustment_mode.is_ratio(),
    )?;

    if transitions.is_empty() {
        anyhow::bail!("Continuous future transitions must not be empty");
    }

    if let Some(instrument_id) = first_pre_instrument_id
        && !transitions
            .iter()
            .any(|row| row.pre_instrument_id == instrument_id)
    {
        anyhow::bail!(
            "Continuous future first_pre_instrument_id {instrument_id} was not found in transitions"
        );
    }

    if let Some(instrument_id) = last_post_instrument_id
        && !transitions
            .iter()
            .any(|row| row.post_instrument_id == instrument_id)
    {
        anyhow::bail!(
            "Continuous future last_post_instrument_id {instrument_id} was not found in transitions"
        );
    }

    Ok(ContinuousFutureRequest {
        primary_bar_type,
        request_bar_aggregation: RequestBarAggregation {
            bar_types,
            update_subscriptions: false,
            // Roll gaps and weekends must not emit synthetic last-close bars (v1 parity)
            disable_build_with_no_updates: true,
            skip_first_non_full_bar: None,
            time_zone: time_zone_from_params(Some(params))?,
            interleave: false,
            cut_warmup: false,
        },
        transitions,
        adjustment_mode,
        first_pre_instrument_id,
        last_post_instrument_id,
    })
}

fn continuous_future_bar_types(
    request: &RequestBars,
    params: &Params,
) -> anyhow::Result<Vec<BarType>> {
    let Some(value) = params.get(BAR_TYPES) else {
        return Ok(vec![request.single_bar_type()?]);
    };

    let values = value
        .as_array()
        .context("`bar_types` request parameter must be an array")?;
    let mut bar_types = Vec::with_capacity(values.len());
    for value in values {
        let raw = value
            .as_str()
            .context("`bar_types` request parameter must contain strings")?;
        let bar_type =
            BarType::from_str(raw).context("failed to parse `bar_types` request parameter")?;

        if !bar_type.is_internally_aggregated() {
            anyhow::bail!(
                "Cannot request aggregated bars: {bar_type} must be internally aggregated"
            );
        }

        if !bar_types.contains(&bar_type) {
            bar_types.push(bar_type);
        }
    }

    if bar_types.is_empty() {
        anyhow::bail!("Continuous future `bar_types` request parameter must not be empty");
    }

    Ok(bar_types)
}

fn parse_adjustment_mode(value: Option<&Value>) -> anyhow::Result<ContinuousFutureAdjustmentType> {
    let Some(value) = value else {
        return Ok(ContinuousFutureAdjustmentType::default());
    };

    if let Some(raw) = value.as_str() {
        return ContinuousFutureAdjustmentType::from_str(raw)
            .with_context(|| format!("failed to parse `{CONTINUOUS_FUTURE_ADJUSTMENT_MODE}`"));
    }

    match value.as_u64() {
        Some(1) => Ok(ContinuousFutureAdjustmentType::BackwardSpread),
        Some(2) => Ok(ContinuousFutureAdjustmentType::ForwardSpread),
        Some(3) => Ok(ContinuousFutureAdjustmentType::BackwardRatio),
        Some(4) => Ok(ContinuousFutureAdjustmentType::ForwardRatio),
        _ => anyhow::bail!("failed to parse `{CONTINUOUS_FUTURE_ADJUSTMENT_MODE}`"),
    }
}

fn parse_optional_chain_bound(
    params: &Params,
    key: &str,
    target_instrument_id: InstrumentId,
) -> anyhow::Result<Option<InstrumentId>> {
    let Some(raw) = params.get_str(key) else {
        return Ok(None);
    };

    let instrument_id = InstrumentId::from_str(raw)
        .with_context(|| format!("Invalid continuous future {key} for {target_instrument_id}"))?;
    if instrument_id.venue != target_instrument_id.venue {
        anyhow::bail!(
            "Continuous future {key} venue mismatch for {target_instrument_id}: target venue {}, {key} venue {}",
            target_instrument_id.venue,
            instrument_id.venue,
        );
    }

    Ok(Some(instrument_id))
}

fn parse_transitions(
    target_bar_type: BarType,
    value: &Value,
    is_ratio: bool,
) -> anyhow::Result<Vec<ContinuousFutureTransition>> {
    let values = value
        .as_array()
        .context("Continuous future transitions must be an array")?;
    let target_venue = target_bar_type.instrument_id().venue;
    let mut transitions = Vec::with_capacity(values.len());
    let mut previous_transition_time_ns = None;
    let mut previous_post_instrument_id = None;

    for row_value in values {
        let row = row_value.as_object().with_context(|| {
            format!("Continuous future transition must be an object, was {row_value}")
        })?;
        let transition_time_ns = row
            .get("transition_time_ns")
            .and_then(Value::as_u64)
            .with_context(|| {
                format!("Invalid continuous future transition_time_ns, was {row_value}")
            })?;

        if let Some(previous) = previous_transition_time_ns
            && transition_time_ns <= previous
        {
            anyhow::bail!(
                "Continuous future transition times must be strictly increasing, was {value}"
            );
        }
        previous_transition_time_ns = Some(transition_time_ns);

        let pre_instrument_id =
            parse_transition_instrument_id(row.get("pre_instrument_id"), target_bar_type)?;
        let post_instrument_id =
            parse_transition_instrument_id(row.get("post_instrument_id"), target_bar_type)?;
        if pre_instrument_id.venue != target_venue || post_instrument_id.venue != target_venue {
            anyhow::bail!(
                "Continuous future segment venue mismatch for {target_bar_type}: target venue {target_venue}, segment venues pre={}, post={}",
                pre_instrument_id.venue,
                post_instrument_id.venue,
            );
        }

        if let Some(previous) = previous_post_instrument_id
            && pre_instrument_id != previous
        {
            anyhow::bail!(
                "Continuous future chain discontinuity for {target_bar_type}: previous post {previous} != current pre {pre_instrument_id}",
            );
        }
        previous_post_instrument_id = Some(post_instrument_id);

        let pre_price = parse_transition_price(row.get("pre_price"), row_value, "pre_price")?;
        let post_price = parse_transition_price(row.get("post_price"), row_value, "post_price")?;
        if is_ratio && (pre_price <= Decimal::ZERO || post_price <= Decimal::ZERO) {
            anyhow::bail!(
                "Continuous future ratio adjustment requires positive prices, was {row_value}"
            );
        }

        transitions.push(ContinuousFutureTransition {
            transition_time_ns,
            pre_instrument_id,
            post_instrument_id,
            pre_price,
            post_price,
        });
    }

    Ok(transitions)
}

fn parse_transition_instrument_id(
    value: Option<&Value>,
    target_bar_type: BarType,
) -> anyhow::Result<InstrumentId> {
    let raw = value.and_then(Value::as_str).with_context(|| {
        format!("Continuous future transition missing pre/post_instrument_id for {target_bar_type}")
    })?;
    InstrumentId::from_str(raw).with_context(|| {
        format!("Invalid continuous future transition instrument id for {target_bar_type}")
    })
}

fn parse_transition_price(
    value: Option<&Value>,
    row: &Value,
    key: &str,
) -> anyhow::Result<Decimal> {
    let value =
        value.with_context(|| format!("Continuous future transition missing {key}, was {row}"))?;
    let raw = value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string);
    Decimal::from_str(&raw)
        .with_context(|| format!("Invalid continuous future transition price, was {row}"))
}

#[cfg(test)]
mod tests {
    use nautilus_core::{Params, UnixNanos};
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    fn request_with_params(params: Params) -> RequestBars {
        RequestBars::new(
            BarType::from("ES.GLBX-1-TICK-LAST-INTERNAL"),
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            Some(params),
        )
    }

    fn transition_params(mode: &str) -> Params {
        serde_json::from_value(json!({
            "continuous_future_adjustment_mode": mode,
            "continuous_future_transitions": [
                {
                    "transition_time_ns": 10,
                    "pre_instrument_id": "ESH24.GLBX",
                    "post_instrument_id": "ESM24.GLBX",
                    "pre_price": "100.00",
                    "post_price": "110.00"
                },
                {
                    "transition_time_ns": 20,
                    "pre_instrument_id": "ESM24.GLBX",
                    "post_instrument_id": "ESU24.GLBX",
                    "pre_price": "120.00",
                    "post_price": "150.00"
                }
            ]
        }))
        .unwrap()
    }

    #[rstest]
    fn test_continuous_future_request_rejects_chain_discontinuity() {
        let params: Params = serde_json::from_value(json!({
            "continuous_future_transitions": [
                {
                    "transition_time_ns": 10,
                    "pre_instrument_id": "ESH24.GLBX",
                    "post_instrument_id": "ESM24.GLBX",
                    "pre_price": "100.00",
                    "post_price": "110.00"
                },
                {
                    "transition_time_ns": 20,
                    "pre_instrument_id": "ESZ24.GLBX",
                    "post_instrument_id": "ESU24.GLBX",
                    "pre_price": "120.00",
                    "post_price": "150.00"
                }
            ]
        }))
        .unwrap();

        let result = continuous_future_request_from_bars(&request_with_params(params));

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("chain discontinuity")
        );
    }

    #[rstest]
    fn test_continuous_future_request_rejects_unsorted_transition_times() {
        let params: Params = serde_json::from_value(json!({
            "continuous_future_transitions": [
                {
                    "transition_time_ns": 20,
                    "pre_instrument_id": "ESH24.GLBX",
                    "post_instrument_id": "ESM24.GLBX",
                    "pre_price": "100.00",
                    "post_price": "110.00"
                },
                {
                    "transition_time_ns": 10,
                    "pre_instrument_id": "ESM24.GLBX",
                    "post_instrument_id": "ESU24.GLBX",
                    "pre_price": "120.00",
                    "post_price": "150.00"
                }
            ]
        }))
        .unwrap();

        let result = continuous_future_request_from_bars(&request_with_params(params));

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("strictly increasing")
        );
    }

    #[rstest]
    fn test_continuous_future_request_rejects_empty_transitions() {
        let params: Params = serde_json::from_value(json!({
            "continuous_future_transitions": []
        }))
        .unwrap();

        let result = continuous_future_request_from_bars(&request_with_params(params));

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must not be empty")
        );
    }

    #[rstest]
    #[case(
        json!({"pre_instrument_id": "ESH24"}),
        json!({}),
        "Invalid continuous future transition instrument id"
    )]
    #[case(
        json!({"transition_time_ns": null}),
        json!({}),
        "Invalid continuous future transition_time_ns"
    )]
    #[case(
        json!({"pre_price": "not-a-price"}),
        json!({}),
        "Invalid continuous future transition price"
    )]
    #[case(
        json!({"post_price": "0"}),
        json!({"continuous_future_adjustment_mode": "FORWARD_RATIO"}),
        "requires positive prices"
    )]
    #[case(
        json!({"pre_price": "0"}),
        json!({"continuous_future_adjustment_mode": "BACKWARD_RATIO"}),
        "requires positive prices"
    )]
    #[case(
        json!({}),
        json!({"last_post_instrument_id": "ESZ24.GLBX"}),
        "last_post_instrument_id"
    )]
    #[case(
        json!({}),
        json!({"first_pre_instrument_id": "ESZ24.GLBX"}),
        "first_pre_instrument_id"
    )]
    fn test_continuous_future_request_rejects_invalid_transition_metadata(
        #[case] transition_overrides: serde_json::Value,
        #[case] params_overrides: serde_json::Value,
        #[case] expected: &str,
    ) {
        let mut transition = json!({
            "transition_time_ns": 10,
            "pre_instrument_id": "ESH24.GLBX",
            "post_instrument_id": "ESM24.GLBX",
            "pre_price": "100.00",
            "post_price": "110.00"
        });
        let transition_row = transition.as_object_mut().unwrap();
        for (key, value) in transition_overrides.as_object().unwrap() {
            transition_row.insert(key.clone(), value.clone());
        }

        let mut params_value = json!({
            "continuous_future_adjustment_mode": "BACKWARD_SPREAD",
            "continuous_future_transitions": [transition]
        });
        let params_row = params_value.as_object_mut().unwrap();
        for (key, value) in params_overrides.as_object().unwrap() {
            params_row.insert(key.clone(), value.clone());
        }
        let params: Params = serde_json::from_value(params_value).unwrap();

        let result = continuous_future_request_from_bars(&request_with_params(params));

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(expected));
    }

    #[rstest]
    fn test_continuous_future_next_segment_selects_contract_ranges() {
        let request = continuous_future_request_from_bars(&request_with_params(transition_params(
            "BACKWARD_SPREAD",
        )))
        .unwrap()
        .unwrap();

        assert_eq!(
            request.next_segment(0, 25),
            Some(ContinuousFutureSegment {
                index: 0,
                instrument_id: InstrumentId::from("ESH24.GLBX"),
                start_ns: 0,
                end_ns: 9,
            })
        );
        assert_eq!(
            request.next_segment(10, 25),
            Some(ContinuousFutureSegment {
                index: 1,
                instrument_id: InstrumentId::from("ESM24.GLBX"),
                start_ns: 10,
                end_ns: 19,
            })
        );
        assert_eq!(
            request.next_segment(20, 25),
            Some(ContinuousFutureSegment {
                index: 2,
                instrument_id: InstrumentId::from("ESU24.GLBX"),
                start_ns: 20,
                end_ns: 25,
            })
        );
    }

    #[rstest]
    #[case("BACKWARD_SPREAD", 0, "40.00")]
    #[case("BACKWARD_SPREAD", 1, "30.00")]
    #[case("BACKWARD_SPREAD", 2, "0")]
    #[case("FORWARD_SPREAD", 0, "0")]
    #[case("FORWARD_SPREAD", 1, "-10.00")]
    #[case("FORWARD_SPREAD", 2, "-40.00")]
    fn test_continuous_future_adjustment_for_spread_modes(
        #[case] mode: &str,
        #[case] segment_index: usize,
        #[case] expected: &str,
    ) {
        let request =
            continuous_future_request_from_bars(&request_with_params(transition_params(mode)))
                .unwrap()
                .unwrap();

        assert_eq!(
            request.adjustment_for_segment(segment_index),
            Decimal::from_str(expected).unwrap()
        );
    }

    #[rstest]
    #[case(
        "BACKWARD_SPREAD",
        json!({"last_post_instrument_id": "ESM24.GLBX"}),
        0,
        "10.00"
    )]
    #[case(
        "BACKWARD_SPREAD",
        json!({"last_post_instrument_id": "ESM24.GLBX"}),
        1,
        "0"
    )]
    #[case(
        "FORWARD_SPREAD",
        json!({"first_pre_instrument_id": "ESM24.GLBX"}),
        1,
        "0"
    )]
    #[case(
        "FORWARD_SPREAD",
        json!({"first_pre_instrument_id": "ESM24.GLBX"}),
        2,
        "-30.00"
    )]
    fn test_continuous_future_adjustment_honors_chain_bounds(
        #[case] mode: &str,
        #[case] bound: serde_json::Value,
        #[case] segment_index: usize,
        #[case] expected: &str,
    ) {
        let mut params = IndexMap::clone(&transition_params(mode));
        for (key, value) in bound.as_object().unwrap() {
            params.insert(key.clone(), value.clone());
        }

        let request =
            continuous_future_request_from_bars(&request_with_params(Params::new(params)))
                .unwrap()
                .unwrap();

        assert_eq!(
            request.adjustment_for_segment(segment_index),
            Decimal::from_str(expected).unwrap()
        );
    }

    #[rstest]
    fn test_continuous_future_adjustment_for_backward_ratio() {
        let params: Params = serde_json::from_value(json!({
            "continuous_future_adjustment_mode": "BACKWARD_RATIO",
            "continuous_future_transitions": [
                {
                    "transition_time_ns": 10,
                    "pre_instrument_id": "ESH24.GLBX",
                    "post_instrument_id": "ESM24.GLBX",
                    "pre_price": "100.00",
                    "post_price": "200.00"
                },
                {
                    "transition_time_ns": 20,
                    "pre_instrument_id": "ESM24.GLBX",
                    "post_instrument_id": "ESU24.GLBX",
                    "pre_price": "50.00",
                    "post_price": "150.00"
                }
            ]
        }))
        .unwrap();
        let request = continuous_future_request_from_bars(&request_with_params(params))
            .unwrap()
            .unwrap();

        assert_eq!(request.adjustment_for_segment(0), Decimal::from(6));
        assert_eq!(request.adjustment_for_segment(1), Decimal::from(3));
        assert_eq!(request.adjustment_for_segment(2), Decimal::ONE);
    }

    #[rstest]
    fn test_continuous_future_source_for_external_reference_uses_segment_instrument() {
        let mut params = IndexMap::clone(&transition_params("BACKWARD_SPREAD"));
        params.insert(
            "bar_types".to_string(),
            json!(["ES.GLBX-5-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL"]),
        );
        let request =
            continuous_future_request_from_bars(&request_with_params(Params::new(params)))
                .unwrap()
                .unwrap();

        assert_eq!(
            request.source_for_segment(InstrumentId::from("ESH24.GLBX")),
            ContinuousFutureSource::Bars(BarType::from("ESH24.GLBX-1-MINUTE-LAST-EXTERNAL"))
        );
    }

    #[rstest]
    fn test_continuous_future_source_for_bid_target_uses_quotes() {
        let params: Params = serde_json::from_value(json!({
            "bar_types": ["ES.GLBX-1-TICK-BID-INTERNAL"],
            "continuous_future_transitions": [
                {
                    "transition_time_ns": 10,
                    "pre_instrument_id": "ESH24.GLBX",
                    "post_instrument_id": "ESM24.GLBX",
                    "pre_price": "100.00",
                    "post_price": "110.00"
                }
            ]
        }))
        .unwrap();
        let request = continuous_future_request_from_bars(&request_with_params(params))
            .unwrap()
            .unwrap();

        assert_eq!(
            request.source_for_segment(InstrumentId::from("ESH24.GLBX")),
            ContinuousFutureSource::Quotes
        );
    }
}
