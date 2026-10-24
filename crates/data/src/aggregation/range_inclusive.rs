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

//! Range bar aggregation in which the completing update is part of the bar.

use std::fmt::Debug;

use jiff::{SignedDuration, tz::TimeZone};
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

use super::{BarAggregator, BarAggregatorCore, range::next_daily_boundary_after};

/// Provides a means of building range bars that include their completing update.
///
/// The range is the bar specification step multiplied by the instrument price increment. Each
/// update joins the forming bar first; the bar completes once its high minus low reaches or
/// exceeds the range. The completing update is therefore part of the bar, any overshoot past the
/// range stays in it, and the next update opens a fresh bar at its own price.
///
/// A fixed daily boundary, an offset from local midnight in a time zone, discards the forming bar:
/// the first update at or after the boundary drops whatever was forming, emits nothing, and opens
/// a fresh bar. A discard emits no bar, so the aggregator needs no clock.
///
/// Completed bars carry the completing update's `ts_init` as both `ts_event` and `ts_init`.
pub struct RangeInclusiveBarAggregator {
    core: BarAggregatorCore,
    range: Price,
    time_zone: TimeZone,
    boundary_offset: SignedDuration,
    next_boundary_ns: Option<UnixNanos>,
}

impl Debug for RangeInclusiveBarAggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RangeInclusiveBarAggregator))
            .field("core", &self.core)
            .field("range", &self.range)
            .field("time_zone", &self.time_zone)
            .field("boundary_offset", &self.boundary_offset)
            .field("next_boundary_ns", &self.next_boundary_ns)
            .finish()
    }
}

impl RangeInclusiveBarAggregator {
    /// Creates a new [`RangeInclusiveBarAggregator`] instance.
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

        Self {
            core: BarAggregatorCore::new(bar_type, price_precision, size_precision, handler),
            range,
            time_zone,
            boundary_offset,
            next_boundary_ns: None,
        }
    }

    /// Discards the forming bar once `ts` reaches the pending boundary, then schedules the next
    /// boundary after `ts`.
    fn roll_boundary(&mut self, ts: UnixNanos) {
        match self.next_boundary_ns {
            Some(boundary) if ts < boundary => return,
            Some(_) => self.core.builder.reset(),
            None => {}
        }

        self.next_boundary_ns = Some(next_daily_boundary_after(
            &self.time_zone,
            self.boundary_offset,
            ts,
        ));
    }

    fn range_reached(&self) -> bool {
        let Some((high, low)) = self.core.builder.high.zip(self.core.builder.low) else {
            return false; // No forming bar
        };
        high.raw() - low.raw() >= self.range.raw()
    }
}

impl BarAggregator for RangeInclusiveBarAggregator {
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
        self.core.builder.update(price, size, ts_init);

        if self.range_reached() {
            self.core.build_and_send(ts_init, ts_init);
        }
    }

    fn update_bar(&mut self, bar: Bar, volume: Quantity, ts_init: UnixNanos) {
        if self.core.is_stale(ts_init) {
            return;
        }

        self.roll_boundary(ts_init);
        self.core.builder.update_bar(bar, volume, ts_init);

        if self.range_reached() {
            self.core.build_and_send(ts_init, ts_init);
        }
    }

    fn set_historical_mode(&mut self, _historical_mode: bool, handler: Box<dyn FnMut(Bar)>) {
        self.core.set_handler(handler);
    }

    fn set_adjustment(&mut self, adjustment: Decimal, mode: ContinuousFutureAdjustmentType) {
        self.core.set_adjustment(adjustment, mode);
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::datetime::get_timezone;
    use rstest::rstest;

    use super::*;
    use crate::aggregation::{
        RANGE_BAR_DEFAULT_BOUNDARY, RANGE_BAR_DEFAULT_TIME_ZONE, tests::recording_handler,
    };

    const SEC: u64 = 1_000_000_000;
    const DAY: u64 = 86_400;
    // Boundaries fall at 10 seconds past each UTC midnight
    const BOUNDARY: u64 = 10;

    fn bar_type() -> BarType {
        BarType::from("MNQZ5.XCME-40-RANGE_INCLUSIVE-LAST-INTERNAL")
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

    fn aggregator<H: FnMut(Bar) + 'static>(handler: H) -> RangeInclusiveBarAggregator {
        RangeInclusiveBarAggregator::new(
            bar_type(),
            2,
            0,
            Price::from("0.25"),
            TimeZone::UTC,
            SignedDuration::from_secs(BOUNDARY as i64),
            handler,
        )
    }

    fn trade(agg: &mut RangeInclusiveBarAggregator, price: &str, size: u64, secs: u64) {
        agg.update(Price::from(price), Quantity::from(size), ts(secs));
    }

    fn forming(agg: &RangeInclusiveBarAggregator) -> (Option<Price>, Quantity) {
        (agg.core.builder.open, agg.core.builder.volume)
    }

    #[rstest]
    fn test_range_is_step_ticks() {
        let agg = aggregator(|_| {});

        assert_eq!(agg.range, Price::from("10.00"));
    }

    #[rstest]
    #[should_panic(expected = "`boundary_offset` must be within [0, 24) hours")]
    fn test_boundary_offset_must_be_within_a_day() {
        let _ = RangeInclusiveBarAggregator::new(
            bar_type(),
            2,
            0,
            Price::from("0.25"),
            TimeZone::UTC,
            SignedDuration::from_hours(-1),
            |_| {},
        );
    }

    #[rstest]
    fn test_first_trade_opens_bar() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 2, 1);

        assert!(bars.lock().is_empty());
        assert_eq!(
            forming(&agg),
            (Some(Price::from("100.00")), Quantity::from(2))
        );
    }

    #[rstest]
    fn test_range_below_step_keeps_forming() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 1, 1);
        trade(&mut agg, "109.75", 1, 2);
        trade(&mut agg, "100.25", 1, 3);

        assert!(bars.lock().is_empty());
        assert_eq!(agg.core.builder.count, 3);
    }

    #[rstest]
    #[case::up("110.00", "110.00", "100.00")]
    #[case::down("90.00", "100.00", "90.00")]
    fn test_range_equal_to_step_completes_bar_with_trade(
        #[case] completing: &str,
        #[case] bar_high: &str,
        #[case] bar_low: &str,
    ) {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 1, 1);
        trade(&mut agg, completing, 4, 2);

        assert_eq!(
            *bars.lock(),
            vec![bar("100.00", bar_high, bar_low, completing, 5, 2)]
        );
        assert_eq!(forming(&agg), (None, Quantity::from(0)));
    }

    #[rstest]
    #[case::up("105.00", "112.50", "112.50", "100.00")]
    #[case::down("95.00", "87.25", "100.00", "87.25")]
    fn test_overshoot_stays_in_completed_bar(
        #[case] inside: &str,
        #[case] completing: &str,
        #[case] bar_high: &str,
        #[case] bar_low: &str,
    ) {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 1, 1);
        trade(&mut agg, inside, 2, 2);
        trade(&mut agg, completing, 3, 3);

        assert_eq!(
            *bars.lock(),
            vec![bar("100.00", bar_high, bar_low, completing, 6, 3)]
        );
        assert_eq!(forming(&agg), (None, Quantity::from(0)));
    }

    #[rstest]
    fn test_trade_after_completion_opens_bar_at_own_price() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 1, 1);
        trade(&mut agg, "110.25", 1, 2);
        trade(&mut agg, "105.00", 7, 3);

        assert_eq!(bars.lock().len(), 1);
        assert_eq!(agg.core.builder.open, Some(Price::from("105.00")));
        assert_eq!(agg.core.builder.high, Some(Price::from("105.00")));
        assert_eq!(agg.core.builder.low, Some(Price::from("105.00")));
        assert_eq!(agg.core.builder.volume, Quantity::from(7));
        assert_eq!(agg.core.builder.count, 1);
    }

    #[rstest]
    fn test_boundary_discards_forming_bar() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 1, 1);
        trade(&mut agg, "105.00", 2, 2);
        trade(&mut agg, "150.00", 4, 20);

        assert!(bars.lock().is_empty());
        assert_eq!(
            forming(&agg),
            (Some(Price::from("150.00")), Quantity::from(4))
        );
        assert_eq!(agg.core.builder.count, 1);
        assert_eq!(agg.next_boundary_ns, Some(ts(DAY + BOUNDARY)));
    }

    #[rstest]
    fn test_trade_at_boundary_instant_opens_next_session_bar() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 1, 1);
        trade(&mut agg, "109.00", 1, BOUNDARY);

        assert!(bars.lock().is_empty());
        assert_eq!(
            forming(&agg),
            (Some(Price::from("109.00")), Quantity::from(1))
        );
    }

    #[rstest]
    fn test_first_trade_after_boundary_measures_range_from_its_own_price() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        // The discarded bar's low (100.00) must not count toward the next session's range
        trade(&mut agg, "100.00", 1, 1);
        trade(&mut agg, "106.00", 1, 20);
        trade(&mut agg, "110.25", 1, 21);

        assert!(bars.lock().is_empty());

        trade(&mut agg, "116.00", 2, 22);

        assert_eq!(
            *bars.lock(),
            vec![bar("106.00", "116.00", "106.00", "116.00", 4, 22)]
        );
    }

    #[rstest]
    fn test_boundary_without_forming_bar_is_noop() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 1, 1);
        trade(&mut agg, "110.00", 1, 2); // Completes a bar: nothing forming at the boundary
        trade(&mut agg, "120.00", 1, 20);

        assert_eq!(
            *bars.lock(),
            vec![bar("100.00", "110.00", "100.00", "110.00", 2, 2)]
        );
        assert_eq!(
            forming(&agg),
            (Some(Price::from("120.00")), Quantity::from(1))
        );
    }

    #[rstest]
    fn test_several_elapsed_boundaries_discard_once() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 1, 1);
        trade(&mut agg, "101.00", 1, 3 * DAY + 20);

        assert!(bars.lock().is_empty());
        assert_eq!(
            forming(&agg),
            (Some(Price::from("101.00")), Quantity::from(1))
        );
        assert_eq!(agg.next_boundary_ns, Some(ts(4 * DAY + BOUNDARY)));
    }

    #[rstest]
    fn test_end_of_tape_leaves_forming_bar_unemitted() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 1, 1);
        trade(&mut agg, "104.00", 3, 2);
        agg.stop();

        assert!(bars.lock().is_empty());
        assert_eq!(
            forming(&agg),
            (Some(Price::from("100.00")), Quantity::from(4))
        );
    }

    #[rstest]
    fn test_stale_update_is_ignored() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        trade(&mut agg, "100.00", 1, 5);
        trade(&mut agg, "110.00", 1, 4);

        assert!(bars.lock().is_empty());
        assert_eq!(
            forming(&agg),
            (Some(Price::from("100.00")), Quantity::from(1))
        );
    }

    #[rstest]
    fn test_update_bar_completes_when_range_reached() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        agg.update_bar(
            bar("100.00", "104.00", "99.00", "103.00", 5, 1),
            Quantity::from(5),
            ts(1),
        );
        assert!(bars.lock().is_empty());

        agg.update_bar(
            bar("103.00", "109.00", "102.00", "108.00", 3, 2),
            Quantity::from(3),
            ts(2),
        );

        assert_eq!(
            *bars.lock(),
            vec![bar("100.00", "109.00", "99.00", "108.00", 8, 2)]
        );
    }

    #[rstest]
    // Friday before the spring-forward Sunday: 18:00 EST
    #[case("2026-03-06T17:00:00Z", "2026-03-06T23:00:00Z")]
    // Monday after: 18:00 EDT
    #[case("2026-03-09T17:00:00Z", "2026-03-09T22:00:00Z")]
    fn test_default_boundary_discards_at_new_york_clock(
        #[case] open: &str,
        #[case] boundary: &str,
    ) {
        let (bars, record) = recording_handler();
        let mut agg = RangeInclusiveBarAggregator::new(
            bar_type(),
            2,
            0,
            Price::from("0.25"),
            get_timezone(RANGE_BAR_DEFAULT_TIME_ZONE).unwrap(),
            RANGE_BAR_DEFAULT_BOUNDARY,
            record,
        );
        let boundary = utc(boundary);

        agg.update(Price::from("100.00"), Quantity::from(1), utc(open));
        agg.update(
            Price::from("105.00"),
            Quantity::from(1),
            UnixNanos::from(boundary.as_u64() - 1),
        );
        assert_eq!(agg.next_boundary_ns, Some(boundary));

        agg.update(Price::from("110.00"), Quantity::from(1), boundary);

        assert!(bars.lock().is_empty());
        assert_eq!(
            forming(&agg),
            (Some(Price::from("110.00")), Quantity::from(1))
        );
    }

    /// One synthetic tape through every construction rule, against bars computed by hand: the
    /// first trade opens; a range below the step keeps forming; equality completes the bar with the
    /// completing trade inside it; overshoot stays in the completed bar; the next trade opens at its
    /// own price; the daily boundary discards the forming bar; a boundary with nothing forming is a
    /// no-op; the first trade after a boundary measures the range from its own price; a forming bar
    /// at the end of the tape is not completed.
    #[rstest]
    fn test_range_inclusive_bar_digest() {
        let (bars, record) = recording_handler();
        let mut agg = aggregator(record);

        // Session ending at 10s
        trade(&mut agg, "100.00", 1, 1); // Opens A
        trade(&mut agg, "105.00", 2, 2);
        trade(&mut agg, "109.75", 1, 3); // Range 9.75: stays in A
        trade(&mut agg, "110.00", 3, 4); // Range 10.00, equal: completes A with this trade
        trade(&mut agg, "104.00", 1, 5); // Opens B at its own price
        trade(&mut agg, "101.00", 2, 6);
        trade(&mut agg, "93.50", 1, 7); // Range 10.50: completes B, overshoot stays
        trade(&mut agg, "95.00", 4, 8); // Opens C
        trade(&mut agg, "99.00", 1, 9); // C is still forming at the boundary

        // Session ending at DAY + 10s: C is discarded by the first trade past the boundary
        trade(&mut agg, "100.00", 1, 20); // Opens D (C's low 95.00 no longer counts)
        trade(&mut agg, "104.75", 2, 21);
        trade(&mut agg, "90.00", 1, 22); // Range 14.75: completes D, overshoot stays
        trade(&mut agg, "90.00", 5, DAY + 9); // Opens E, which completes nothing

        // Session ending at 2 * DAY + 10s: E is discarded, F forms and the tape ends
        trade(&mut agg, "200.00", 6, DAY + 20); // Opens F
        trade(&mut agg, "205.00", 1, DAY + 21);

        assert_eq!(
            *bars.lock(),
            vec![
                bar("100.00", "110.00", "100.00", "110.00", 7, 4), // A
                bar("104.00", "104.00", "93.50", "93.50", 4, 7),   // B
                bar("100.00", "104.75", "90.00", "90.00", 4, 22),  // D
            ]
        );
        assert_eq!(
            forming(&agg),
            (Some(Price::from("200.00")), Quantity::from(7))
        );
    }
}
