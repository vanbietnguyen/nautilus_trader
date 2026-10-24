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

//! `DeepCharts`-style range bar aggregation.

use std::{
    cell::RefCell,
    fmt::Debug,
    rc::{Rc, Weak},
};

use jiff::{SignedDuration, tz::TimeZone};
use nautilus_common::{
    clock::Clock,
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{
    UnixNanos,
    correctness::{self, FAILED},
};
use nautilus_model::{
    data::{Bar, BarType},
    enums::ContinuousFutureAdjustmentType,
    types::{Price, Quantity, price::PriceRaw},
};
use rust_decimal::Decimal;

use super::{BarAggregator, BarAggregatorCore};

/// The default time zone of the daily range bar boundary.
pub const RANGE_BAR_DEFAULT_TIME_ZONE: &str = "America/New_York";

/// The default daily range bar boundary, as an offset from local midnight (18:00).
pub const RANGE_BAR_DEFAULT_BOUNDARY: SignedDuration = SignedDuration::from_hours(18);

/// Provides a means of building range bars, following the `DeepCharts` convention.
///
/// The range is the bar specification step multiplied by the instrument price increment. An
/// update that would make the forming bar's high minus low exceed the range completes the forming
/// bar at the update's timestamp, then opens the next bar with the whole update. An update that
/// makes high minus low exactly equal to the range stays in the forming bar. The completing update
/// is therefore never part of the bar it completes, and completed bars never exceed the range.
///
/// A fixed daily boundary, an offset from local midnight in a time zone, also completes the
/// forming bar. With a clock (see [`BarAggregator::set_clock`] and [`BarAggregator::start_timer`])
/// the aggregator arms a time alert for the next boundary and completes the bar when it fires.
/// Without one, the first update at or after the boundary completes the bar before it is applied.
/// Either way the bar completes at the boundary, and a boundary with no forming bar completes
/// nothing.
///
/// Completed bars carry the completion time as both `ts_event` and `ts_init`: the completing
/// update's `ts_init`, or the boundary.
pub struct DeepChartsRangeBarAggregator {
    core: BarAggregatorCore,
    range: Price,
    time_zone: TimeZone,
    boundary_offset: SignedDuration,
    timer_name: String,
    clock: Option<Rc<RefCell<dyn Clock>>>,
    aggregator_weak: Option<Weak<RefCell<Box<dyn BarAggregator>>>>,
    next_boundary_ns: Option<UnixNanos>,
}

impl Debug for DeepChartsRangeBarAggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(DeepChartsRangeBarAggregator))
            .field("core", &self.core)
            .field("range", &self.range)
            .field("time_zone", &self.time_zone)
            .field("boundary_offset", &self.boundary_offset)
            .field("timer_name", &self.timer_name)
            .field("next_boundary_ns", &self.next_boundary_ns)
            .finish()
    }
}

impl DeepChartsRangeBarAggregator {
    /// Creates a new [`DeepChartsRangeBarAggregator`] instance.
    ///
    /// `boundary_offset` is the daily boundary as an offset from local midnight in `time_zone`.
    ///
    /// # Panics
    ///
    /// Panics if `bar_type.aggregation_source` is not `AggregationSource::Internal`, or if
    /// `boundary_offset` is negative or not less than one day.
    pub fn new<H: FnMut(Bar) + 'static>(
        bar_type: BarType,
        price_precision: u8,
        size_precision: u8,
        price_increment: Price,
        time_zone: TimeZone,
        boundary_offset: SignedDuration,
        handler: H,
    ) -> Self {
        correctness::check_predicate_true(
            !boundary_offset.is_negative() && boundary_offset < SignedDuration::from_hours(24),
            "`boundary_offset` must be within [0, 24) hours",
        )
        .expect(FAILED);

        let range = Price::from_raw(
            price_increment.raw() * bar_type.spec().step.get() as PriceRaw,
            price_increment.precision,
        );
        let core = BarAggregatorCore::new(bar_type, price_precision, size_precision, handler);
        let timer_name = format!("RANGE_BAR_{}", core.builder.bar_type);

        Self {
            core,
            range,
            time_zone,
            boundary_offset,
            timer_name,
            clock: None,
            aggregator_weak: None,
            next_boundary_ns: None,
        }
    }

    /// Returns the first daily boundary strictly after `ts`.
    fn next_boundary_after(&self, ts: UnixNanos) -> UnixNanos {
        let now = ts.to_datetime_utc();
        let mut date = self.time_zone.to_datetime(now).date();

        loop {
            let civil = date
                .at(0, 0, 0, 0)
                .checked_add(self.boundary_offset)
                .expect("valid range bar boundary");
            let boundary = self
                .time_zone
                .to_timestamp(civil)
                .expect("valid range bar boundary");

            if boundary > now {
                return UnixNanos::from(boundary);
            }

            date = date.tomorrow().expect("valid range bar boundary date");
        }
    }

    /// Completes the forming bar at the pending boundary once `now` reaches it, then schedules
    /// the next boundary after `now`.
    fn roll_boundary(&mut self, now: UnixNanos) {
        match self.next_boundary_ns {
            Some(boundary) if now < boundary => return,
            Some(boundary) => self.complete_forming_bar(boundary),
            None => {}
        }

        let next = self.next_boundary_after(now);
        self.next_boundary_ns = Some(next);
        self.arm_boundary_alert(next);
    }

    fn arm_boundary_alert(&self, alert_time_ns: UnixNanos) {
        let (Some(clock), Some(aggregator_weak)) = (&self.clock, &self.aggregator_weak) else {
            return;
        };

        let aggregator_weak = Weak::clone(aggregator_weak);
        let callback = TimeEventCallback::RustLocal(Rc::new(move |event: TimeEvent| {
            if let Some(aggregator) = aggregator_weak.upgrade() {
                aggregator.borrow_mut().build_bar(&event);
            }
        }));

        let mut clock = clock.borrow_mut();
        clock.cancel_timer(&self.timer_name);
        if let Err(e) =
            clock.set_time_alert_ns(&self.timer_name, alert_time_ns, Some(callback), Some(true))
        {
            log::error!("Cannot arm {} at {alert_time_ns}: {e}", self.timer_name);
        }
    }

    fn exceeds_range(&self, high: Price, low: Price) -> bool {
        let Some((bar_high, bar_low)) = self.core.builder.high.zip(self.core.builder.low) else {
            return false; // No forming bar
        };
        let high = high.max(bar_high);
        let low = low.min(bar_low);
        high.raw() - low.raw() > self.range.raw()
    }

    fn complete_forming_bar(&mut self, ts: UnixNanos) {
        if self.core.builder.open.is_some() {
            self.core.build_and_send(ts, ts);
        }
    }
}

impl BarAggregator for DeepChartsRangeBarAggregator {
    fn bar_type(&self) -> BarType {
        self.core.builder.bar_type
    }

    fn is_running(&self) -> bool {
        self.core.is_running
    }

    fn set_is_running(&mut self, value: bool) {
        self.core.set_is_running(value);
    }

    /// Apply the given update to the aggregator.
    fn update(&mut self, price: Price, size: Quantity, ts_init: UnixNanos) {
        if self.core.is_stale(ts_init) {
            return;
        }

        self.roll_boundary(ts_init);

        let adjusted = self.core.builder.apply_adjustment_to_price(price);
        if self.exceeds_range(adjusted, adjusted) {
            self.complete_forming_bar(ts_init);
        }

        self.core.builder.update(price, size, ts_init);
    }

    fn update_bar(&mut self, bar: Bar, volume: Quantity, ts_init: UnixNanos) {
        if self.core.is_stale(ts_init) {
            return;
        }

        self.roll_boundary(ts_init);

        let high = self.core.builder.apply_adjustment_to_price(bar.high);
        let low = self.core.builder.apply_adjustment_to_price(bar.low);
        if self.exceeds_range(high, low) {
            self.complete_forming_bar(ts_init);
        }

        self.core.builder.update_bar(bar, volume, ts_init);
    }

    /// Cancels the boundary alert; later updates still complete a bar at a passed boundary.
    fn stop(&mut self) {
        if let Some(clock) = &self.clock {
            clock.borrow_mut().cancel_timer(&self.timer_name);
        }
    }

    fn set_historical_mode(&mut self, _historical_mode: bool, handler: Box<dyn FnMut(Bar)>) {
        self.core.set_handler(handler);
    }

    fn set_clock(&mut self, clock: Rc<RefCell<dyn Clock>>) {
        self.clock = Some(clock);
    }

    /// Completes the forming bar at the boundary the alert was armed for.
    fn build_bar(&mut self, event: &TimeEvent) {
        if event.name.as_str() != self.timer_name {
            return;
        }

        // The clock may already be past the event when several boundaries elapse at once
        let now = self
            .clock
            .as_ref()
            .map_or(event.ts_event, |clock| clock.borrow().timestamp_ns())
            .max(event.ts_event);
        self.roll_boundary(now);
    }

    /// Arms the alert for the next boundary on the clock set by [`BarAggregator::set_clock`].
    fn start_timer(&mut self, aggregator_rc: Option<Rc<RefCell<Box<dyn BarAggregator>>>>) {
        if let Some(aggregator_rc) = aggregator_rc {
            self.aggregator_weak = Some(Rc::downgrade(&aggregator_rc));
        }

        let Some(now) = self
            .clock
            .as_ref()
            .map(|clock| clock.borrow().timestamp_ns())
        else {
            return;
        };

        match self.next_boundary_ns {
            // Restarted before the pending boundary: re-arm it
            Some(boundary) if now < boundary => self.arm_boundary_alert(boundary),
            _ => self.roll_boundary(now),
        }
    }

    fn set_aggregator_weak(&mut self, weak: Weak<RefCell<Box<dyn BarAggregator>>>) {
        self.aggregator_weak = Some(weak);
    }

    fn set_adjustment(&mut self, adjustment: Decimal, mode: ContinuousFutureAdjustmentType) {
        self.core.set_adjustment(adjustment, mode);
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::clock::VirtualClock;
    use nautilus_core::datetime::get_timezone;
    use rstest::rstest;

    use super::*;
    use crate::aggregation::tests::recording_handler;

    type SharedAggregator = Rc<RefCell<Box<dyn BarAggregator>>>;

    const SEC: u64 = 1_000_000_000;
    const DAY: u64 = 86_400;
    // Boundaries fall at 10 seconds past each UTC midnight
    const BOUNDARY: u64 = 10;

    fn bar_type() -> BarType {
        BarType::from("MNQZ5.XCME-40-RANGE-LAST-INTERNAL")
    }

    fn ts(secs: u64) -> UnixNanos {
        UnixNanos::from(secs * SEC)
    }

    fn utc(value: &str) -> UnixNanos {
        UnixNanos::from(value.parse::<jiff::Timestamp>().unwrap())
    }

    fn bar(open: &str, high: &str, low: &str, close: &str, volume: u64, secs: u64) -> Bar {
        Bar::new(
            bar_type(),
            Price::from(open),
            Price::from(high),
            Price::from(low),
            Price::from(close),
            Quantity::from(volume),
            ts(secs),
            ts(secs),
        )
    }

    fn aggregator<H: FnMut(Bar) + 'static>(handler: H) -> DeepChartsRangeBarAggregator {
        DeepChartsRangeBarAggregator::new(
            bar_type(),
            2,
            0,
            Price::from("0.25"),
            TimeZone::UTC,
            SignedDuration::from_secs(BOUNDARY as i64),
            handler,
        )
    }

    fn started<H: FnMut(Bar) + 'static>(
        handler: H,
    ) -> (SharedAggregator, Rc<RefCell<VirtualClock>>) {
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let rc: SharedAggregator = Rc::new(RefCell::new(Box::new(aggregator(handler))));
        rc.borrow_mut()
            .set_clock(Rc::clone(&clock) as Rc<RefCell<dyn Clock>>);
        rc.borrow_mut().start_timer(Some(Rc::clone(&rc)));
        (rc, clock)
    }

    fn forming(rc: &SharedAggregator) -> (Option<Price>, Quantity) {
        let agg = rc.borrow();
        let agg = agg
            .as_any()
            .downcast_ref::<DeepChartsRangeBarAggregator>()
            .unwrap();
        (agg.core.builder.open, agg.core.builder.volume)
    }

    fn timer_name() -> String {
        format!("RANGE_BAR_{}", bar_type())
    }

    /// Fires the clock's due alerts, as the engine does before applying data at `secs`.
    fn advance(clock: &Rc<RefCell<VirtualClock>>, secs: u64) {
        let events = clock.borrow_mut().advance_time(ts(secs), true);
        let handlers = clock.borrow().match_handlers(events);
        for handler in handlers {
            handler.callback.call(handler.event);
        }
    }

    fn trade(
        rc: &SharedAggregator,
        clock: &Rc<RefCell<VirtualClock>>,
        price: &str,
        size: u64,
        secs: u64,
    ) {
        advance(clock, secs);
        rc.borrow_mut()
            .update(Price::from(price), Quantity::from(size), ts(secs));
    }

    #[rstest]
    fn test_range_is_step_ticks() {
        let agg = aggregator(|_| {});

        assert_eq!(agg.range, Price::from("10.00"));
    }

    #[rstest]
    #[should_panic(expected = "`boundary_offset` must be within [0, 24) hours")]
    fn test_boundary_offset_must_be_within_a_day() {
        let _ = DeepChartsRangeBarAggregator::new(
            bar_type(),
            2,
            0,
            Price::from("0.25"),
            TimeZone::UTC,
            SignedDuration::from_hours(24),
            |_| {},
        );
    }

    #[rstest]
    // Friday before the spring-forward Sunday: 18:00 EST
    #[case("2026-03-06T17:00:00Z", "2026-03-06T23:00:00Z")]
    // Exactly at a boundary: the next one
    #[case("2026-03-06T23:00:00Z", "2026-03-07T23:00:00Z")]
    // Monday after: 18:00 EDT
    #[case("2026-03-09T17:00:00Z", "2026-03-09T22:00:00Z")]
    // After the boundary, before UTC midnight
    #[case("2026-08-21T22:30:00Z", "2026-08-22T22:00:00Z")]
    fn test_default_boundary_follows_new_york_clock(#[case] now: &str, #[case] expected: &str) {
        let agg = DeepChartsRangeBarAggregator::new(
            bar_type(),
            2,
            0,
            Price::from("0.25"),
            get_timezone(RANGE_BAR_DEFAULT_TIME_ZONE).unwrap(),
            RANGE_BAR_DEFAULT_BOUNDARY,
            |_| {},
        );

        assert_eq!(agg.next_boundary_after(utc(now)), utc(expected));
    }

    #[rstest]
    fn test_first_trade_opens_bar() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        agg.update(Price::from("100.00"), Quantity::from(2), ts(1));

        assert!(bars.lock().is_empty());
        assert_eq!(agg.core.builder.open, Some(Price::from("100.00")));
        assert_eq!(agg.core.builder.volume, Quantity::from(2));
    }

    #[rstest]
    fn test_range_equal_to_step_stays_in_bar() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        agg.update(Price::from("100.00"), Quantity::from(1), ts(1));
        agg.update(Price::from("110.00"), Quantity::from(1), ts(2));
        agg.update(Price::from("100.00"), Quantity::from(1), ts(3));

        assert!(bars.lock().is_empty());
        assert_eq!(agg.core.builder.count, 3);
    }

    #[rstest]
    #[case::up("110.00", "110.25", "110.00", "100.00")]
    #[case::down("90.00", "89.75", "100.00", "90.00")]
    fn test_overflow_completes_bar_and_opens_next_with_whole_trade(
        #[case] extreme: &str,
        #[case] overflow: &str,
        #[case] bar_high: &str,
        #[case] bar_low: &str,
    ) {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        agg.update(Price::from("100.00"), Quantity::from(1), ts(1));
        agg.update(Price::from(extreme), Quantity::from(2), ts(2));
        agg.update(Price::from(overflow), Quantity::from(5), ts(3));

        assert_eq!(
            *bars.lock(),
            vec![bar("100.00", bar_high, bar_low, extreme, 3, 3)]
        );
        assert_eq!(agg.core.builder.open, Some(Price::from(overflow)));
        assert_eq!(agg.core.builder.high, Some(Price::from(overflow)));
        assert_eq!(agg.core.builder.low, Some(Price::from(overflow)));
        assert_eq!(agg.core.builder.volume, Quantity::from(5));
        assert_eq!(agg.core.builder.count, 1);
    }

    #[rstest]
    fn test_boundary_alert_completes_forming_bar() {
        let (bars, record) = recording_handler();
        let (rc, clock) = started(record);

        trade(&rc, &clock, "100.00", 1, 1);
        trade(&rc, &clock, "104.50", 2, 2);
        advance(&clock, BOUNDARY);

        assert_eq!(
            *bars.lock(),
            vec![bar("100.00", "104.50", "100.00", "104.50", 3, BOUNDARY)]
        );
        assert_eq!(forming(&rc), (None, Quantity::from(0)));
    }

    #[rstest]
    fn test_boundary_alert_rearms_daily() {
        let (bars, record) = recording_handler();
        let (rc, clock) = started(record);

        advance(&clock, BOUNDARY);
        trade(&rc, &clock, "100.00", 1, DAY);
        advance(&clock, DAY + BOUNDARY);

        assert_eq!(
            *bars.lock(),
            vec![bar(
                "100.00",
                "100.00",
                "100.00",
                "100.00",
                1,
                DAY + BOUNDARY
            )]
        );
        assert_eq!(
            clock.borrow().next_time_ns(&timer_name()),
            Some(ts(2 * DAY + BOUNDARY))
        );
    }

    #[rstest]
    fn test_boundary_without_forming_bar_is_noop() {
        let (bars, record) = recording_handler();
        let (_rc, clock) = started(record);

        advance(&clock, BOUNDARY);
        advance(&clock, DAY + BOUNDARY);

        assert!(bars.lock().is_empty());
    }

    #[rstest]
    fn test_first_trade_after_boundary_opens_new_bar() {
        let (bars, record) = recording_handler();
        let (rc, clock) = started(record);

        trade(&rc, &clock, "100.00", 1, 1);
        trade(&rc, &clock, "150.00", 4, 20);

        assert_eq!(
            *bars.lock(),
            vec![bar("100.00", "100.00", "100.00", "100.00", 1, BOUNDARY)]
        );
        assert_eq!(
            forming(&rc),
            (Some(Price::from("150.00")), Quantity::from(4))
        );
    }

    #[rstest]
    fn test_boundary_completes_before_trade_at_boundary_without_alert() {
        let (bars, record) = recording_handler();
        let (rc, clock) = started(record);

        trade(&rc, &clock, "100.00", 1, 1);
        // The trade reaches the aggregator before the alert fires
        rc.borrow_mut()
            .update(Price::from("101.00"), Quantity::from(1), ts(BOUNDARY));
        advance(&clock, BOUNDARY + 1);

        assert_eq!(
            *bars.lock(),
            vec![bar("100.00", "100.00", "100.00", "100.00", 1, BOUNDARY)]
        );
        assert_eq!(
            forming(&rc),
            (Some(Price::from("101.00")), Quantity::from(1))
        );
    }

    #[rstest]
    fn test_without_clock_next_trade_completes_bar_at_boundary() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        agg.update(Price::from("100.00"), Quantity::from(1), ts(1));
        agg.update(Price::from("101.00"), Quantity::from(2), ts(5));
        agg.update(Price::from("120.00"), Quantity::from(3), ts(2 * DAY));

        assert_eq!(
            *bars.lock(),
            vec![bar("100.00", "101.00", "100.00", "101.00", 3, BOUNDARY)]
        );
        assert_eq!(agg.core.builder.open, Some(Price::from("120.00")));
        assert_eq!(agg.next_boundary_ns, Some(ts(2 * DAY + BOUNDARY)));
    }

    #[rstest]
    fn test_stop_cancels_boundary_alert() {
        let (bars, record) = recording_handler();
        let (rc, clock) = started(record);

        trade(&rc, &clock, "100.00", 1, 1);
        rc.borrow_mut().stop();
        advance(&clock, BOUNDARY);

        assert!(bars.lock().is_empty());
        assert_eq!(clock.borrow().timer_count(), 0);
    }

    #[rstest]
    fn test_restart_after_boundary_completes_forming_bar() {
        let (bars, record) = recording_handler();
        let (rc, clock) = started(record);

        trade(&rc, &clock, "100.00", 1, 1);
        rc.borrow_mut().stop();
        advance(&clock, 20);
        rc.borrow_mut().start_timer(None);

        assert_eq!(
            *bars.lock(),
            vec![bar("100.00", "100.00", "100.00", "100.00", 1, BOUNDARY)]
        );
        assert_eq!(
            clock.borrow().next_time_ns(&timer_name()),
            Some(ts(DAY + BOUNDARY))
        );
    }

    /// One synthetic tape through every construction rule, against bars computed by hand from the
    /// `DeepCharts` convention: overflow completes the prior bar and the whole trade opens the
    /// next; equality stays; the daily boundary completes the forming bar; a boundary with nothing
    /// forming is a no-op; the first trade after a boundary opens a fresh bar; a trade at the
    /// boundary arrives after the boundary's completion; a forming bar at the end of the tape is
    /// not completed.
    #[rstest]
    fn test_range_bar_digest() {
        let (bars, record) = recording_handler();
        let (rc, clock) = started(record);

        // Session ending at 10s
        trade(&rc, &clock, "100.00", 1, 1); // Opens A
        trade(&rc, &clock, "105.00", 2, 2);
        trade(&rc, &clock, "110.00", 1, 3); // Range 10.00, equal: stays in A
        trade(&rc, &clock, "108.00", 3, 4);
        trade(&rc, &clock, "110.25", 1, 5); // Range 10.25: completes A at 5s, opens B
        trade(&rc, &clock, "100.25", 2, 6); // Range 10.00, equal: stays in B
        trade(&rc, &clock, "100.00", 1, 7); // Range 10.25: completes B at 7s, opens C
        trade(&rc, &clock, "95.00", 4, 8);
        advance(&clock, BOUNDARY); // Completes C at 10s

        // Session ending at DAY + 10s has no trades
        advance(&clock, DAY + BOUNDARY); // Nothing forming: no bar

        // Session ending at 2 * DAY + 10s
        let open = DAY + 20;
        trade(&rc, &clock, "200.00", 1, open); // Opens D at its own price, not the last close
        trade(&rc, &clock, "189.75", 1, open + 1); // Range 10.25: completes D, opens E
        // The trade at the boundary reaches the aggregator before the alert fires
        rc.borrow_mut().update(
            Price::from("190.00"),
            Quantity::from(6),
            ts(2 * DAY + BOUNDARY),
        ); // Completes E at the boundary, opens F
        advance(&clock, 2 * DAY + BOUNDARY + 1); // Already applied: F keeps forming

        assert_eq!(
            *bars.lock(),
            vec![
                bar("100.00", "110.00", "100.00", "108.00", 7, 5), // A
                bar("110.25", "110.25", "100.25", "100.25", 3, 7), // B
                bar("100.00", "100.00", "95.00", "95.00", 5, BOUNDARY), // C
                bar("200.00", "200.00", "200.00", "200.00", 1, open + 1), // D
                bar(
                    "189.75",
                    "189.75",
                    "189.75",
                    "189.75",
                    1,
                    2 * DAY + BOUNDARY
                ), // E
            ]
        );
        assert_eq!(
            forming(&rc),
            (Some(Price::from("190.00")), Quantity::from(6))
        );
    }
}
