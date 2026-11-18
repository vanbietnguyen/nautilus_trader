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

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use nautilus_common::msgbus::Handler;
use nautilus_core::{UnixNanos, WeakCell, datetime::NANOSECONDS_IN_MILLISECOND};
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    identifiers::InstrumentId,
};
use ustr::Ustr;

use crate::aggregation::{BarAggregator, SpreadQuoteAggregator};

// Between built-in handlers (10) and default user handlers (0)
pub(super) const BAR_AGGREGATOR_PRIORITY: u32 = 5;

thread_local! {
    /// The number of records the warm-up buffers of this thread have held: an engine runs on one
    /// thread, so it orders the records several of its buffers held as they arrived.
    static ARRIVALS: Cell<u64> = const { Cell::new(0) };
}

/// Holds live data back from a bar aggregator that is still replaying a historical request.
///
/// The buffer collects while it is `Some` and is emptied by [`WarmupBuffer::release`], after which
/// the handlers holding it pass data straight through again. Each record is held with its
/// arrival: its place among the records every buffer of the thread held.
#[derive(Debug)]
pub(super) struct WarmupBuffer<T>(Rc<RefCell<Option<Vec<Arrived<T>>>>>);

/// A held record with its arrival (see [`WarmupBuffer`]).
type Arrived<T> = (u64, T);

impl<T> Clone for WarmupBuffer<T> {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<T: Copy> WarmupBuffer<T> {
    pub(super) fn new() -> Self {
        Self(Rc::new(RefCell::new(Some(Vec::new()))))
    }

    /// Holds `item` and returns `true` while warming up, otherwise returns `false`.
    pub(super) fn hold(&self, item: &T) -> bool {
        match self.0.borrow_mut().as_mut() {
            Some(held) => {
                let arrival = ARRIVALS.with(|arrivals| {
                    let arrival = arrivals.get();
                    arrivals.set(arrival + 1);
                    arrival
                });
                held.push((arrival, *item));
                true
            }
            None => false,
        }
    }

    /// Ends the warm-up and returns the data held in arrival order.
    pub(super) fn release(&self) -> Vec<T> {
        self.release_with_arrivals()
            .into_iter()
            .map(|(_, item)| item)
            .collect()
    }

    /// Ends the warm-up and returns the data held in arrival order, each with its arrival.
    pub(super) fn release_with_arrivals(&self) -> Vec<Arrived<T>> {
        self.0.borrow_mut().take().unwrap_or_default()
    }

    /// Returns the arrival of each record held, in arrival order, and keeps holding.
    pub(super) fn arrivals(&self) -> Vec<u64> {
        self.0
            .borrow()
            .iter()
            .flatten()
            .map(|(arrival, _)| *arrival)
            .collect()
    }
}

/// What the release of one joined warm-up did with the live trades it held (the history/live join).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct WarmupCounts {
    /// Live trades held while the answer was in flight.
    pub(super) held: u64,
    /// Trades past the cut, fed to the builder.
    pub(super) flushed: u64,
    /// Trades in or before the cut's millisecond: the answer holds them already.
    pub(super) dropped: u64,
    /// Fed trades whose `ts_init` was raised to the floor.
    pub(super) floored: u64,
    /// Answered trades whose `ts_event` equals their `ts_init`: a hint of the adapter's fallback
    /// to the receive time, not a count of it (a defined exchange time can equal it too).
    pub(super) equal_stamp_rows: u64,
}

/// The cut and the rolling floor through which one joined builder takes live trades after its
/// answer completes (the history/live join).
///
/// The cut is the largest answered `ts_event` at whole milliseconds: a live trade in or before
/// that millisecond is in the answer already and is dropped. The floor starts at the largest
/// answered `ts_init` and rolls to each admitted trade's: a builder silently refuses a `ts_init`
/// below the last it took, so the builder gets a private copy with `ts_init` raised to the floor.
///
/// Invariant: every live trade admitted after the cut reaches the builder with a `ts_init` no
/// lower than any input that builder already took.
#[derive(Clone, Copy, Debug)]
pub(super) struct JoinedRelease {
    pub(super) cut_ms: Option<u64>,
    pub(super) floor: Option<UnixNanos>,
    pub(super) counts: WarmupCounts,
}

impl JoinedRelease {
    /// Returns the cut and the floor of a complete `answered` set of trades.
    #[must_use]
    pub(super) fn from_answer(answered: &[TradeTick]) -> Self {
        Self {
            cut_ms: answered
                .iter()
                .map(|trade| trade.ts_event.as_u64() / NANOSECONDS_IN_MILLISECOND)
                .max(),
            floor: answered.iter().map(|trade| trade.ts_init).max(),
            counts: WarmupCounts {
                equal_stamp_rows: answered
                    .iter()
                    .filter(|trade| trade.ts_event == trade.ts_init)
                    .count() as u64,
                ..WarmupCounts::default()
            },
        }
    }

    /// Returns whether `trade` falls in or before the cut's millisecond.
    fn is_cut(&self, trade: &TradeTick) -> bool {
        self.cut_ms
            .is_some_and(|cut_ms| trade.ts_event.as_u64() / NANOSECONDS_IN_MILLISECOND <= cut_ms)
    }

    /// Returns the builder's copy of a live `trade`: `None` when the cut drops it, otherwise the
    /// trade with its `ts_init` raised to the floor, which then rolls to the copy's.
    fn admit(&mut self, trade: &TradeTick) -> Option<TradeTick> {
        if self.is_cut(trade) {
            self.counts.dropped += 1;
            return None;
        }

        let mut copy = *trade;
        if let Some(floor) = self.floor
            && copy.ts_init < floor
        {
            copy.ts_init = floor;
            self.counts.floored += 1;
        }
        self.floor = Some(copy.ts_init);
        self.counts.flushed += 1;
        Some(copy)
    }
}

/// The release of a joined warm-up, shared by the engine, which sets it when the answer is
/// complete, and the trade handler of the joined builder, which takes every later live trade
/// through it until the aggregator stops. Stays empty for every other warm-up.
#[derive(Clone, Debug, Default)]
pub(super) struct JoinedSlot(Rc<RefCell<Option<JoinedRelease>>>);

impl JoinedSlot {
    /// Returns the builder's copy of a live `trade`: the trade itself while the slot is empty.
    fn admit(&self, trade: &TradeTick) -> Option<TradeTick> {
        match self.0.borrow_mut().as_mut() {
            Some(release) => release.admit(trade),
            None => Some(*trade),
        }
    }
}

impl WarmupBuffer<TradeTick> {
    /// Ends a joined warm-up: returns the builder's copy of each held trade in arrival order
    /// through `release`'s cut and floor, `None` where the cut drops it, for its caller to feed;
    /// logs the counts, and leaves the release in `joined` for the live trades after.
    pub(super) fn release_joined_copies(
        &self,
        joined: &JoinedSlot,
        mut release: JoinedRelease,
        bar_type: BarType,
    ) -> Vec<Option<TradeTick>> {
        let held = self.release();
        release.counts.held = held.len() as u64;

        if held.first().is_some_and(|trade| !release.is_cut(trade)) {
            log::warn!(
                "Joined warm-up of {bar_type}: no overlap observed, the first held live trade is \
                 after the answer's cut (a gap is possible)"
            );
        }

        let copies = held.iter().map(|trade| release.admit(trade)).collect();
        let counts = release.counts;

        log::info!(
            "Released the joined warm-up of {bar_type}: held={}, flushed={}, dropped={}, \
             floored={}, equal_stamp_rows={}",
            counts.held,
            counts.flushed,
            counts.dropped,
            counts.floored,
            counts.equal_stamp_rows,
        );

        *joined.0.borrow_mut() = Some(release);
        copies
    }
}

/// Message handler for processing quote ticks through bar aggregators.
///
/// This handler receives quote tick messages and forwards them to the underlying
/// bar aggregator for processing. It's used as part of the data engine's message
/// routing infrastructure to build bars from incoming quote data.
#[derive(Debug)]
pub(super) struct BarQuoteHandler {
    aggregator: WeakCell<Box<dyn BarAggregator>>,
    bar_type: BarType,
    warmup: Option<WarmupBuffer<QuoteTick>>,
}

impl BarQuoteHandler {
    pub(crate) fn new(aggregator: &Rc<RefCell<Box<dyn BarAggregator>>>, bar_type: BarType) -> Self {
        Self {
            aggregator: WeakCell::from(Rc::downgrade(aggregator)),
            bar_type,
            warmup: None,
        }
    }

    /// Holds incoming data in `warmup` until the buffer is released.
    pub(super) fn with_warmup(mut self, warmup: Option<WarmupBuffer<QuoteTick>>) -> Self {
        self.warmup = warmup;
        self
    }
}

impl Handler<QuoteTick> for BarQuoteHandler {
    fn id(&self) -> Ustr {
        Ustr::from(&format!("BarQuoteHandler|{}", self.bar_type))
    }

    fn handle(&self, quote: &QuoteTick) {
        if self
            .warmup
            .as_ref()
            .is_some_and(|warmup| warmup.hold(quote))
        {
            return;
        }

        if let Some(agg) = self.aggregator.upgrade() {
            agg.borrow_mut().handle_quote(*quote);
        }
    }
}

/// Message handler for processing trade ticks through bar aggregators.
///
/// This handler receives trade tick messages and forwards them to the underlying
/// bar aggregator for processing. It's used as part of the data engine's message
/// routing infrastructure to build bars from incoming trade data.
#[derive(Debug)]
pub(super) struct BarTradeHandler {
    aggregator: WeakCell<Box<dyn BarAggregator>>,
    bar_type: BarType,
    warmup: Option<WarmupBuffer<TradeTick>>,
}

impl BarTradeHandler {
    pub(crate) fn new(aggregator: &Rc<RefCell<Box<dyn BarAggregator>>>, bar_type: BarType) -> Self {
        Self {
            aggregator: WeakCell::from(Rc::downgrade(aggregator)),
            bar_type,
            warmup: None,
        }
    }

    /// Holds incoming data in `warmup` until the buffer is released.
    pub(super) fn with_warmup(mut self, warmup: Option<WarmupBuffer<TradeTick>>) -> Self {
        self.warmup = warmup;
        self
    }
}

impl Handler<TradeTick> for BarTradeHandler {
    fn id(&self) -> Ustr {
        Ustr::from(&format!("BarTradeHandler|{}", self.bar_type))
    }

    fn handle(&self, trade: &TradeTick) {
        if self
            .warmup
            .as_ref()
            .is_some_and(|warmup| warmup.hold(trade))
        {
            return;
        }

        if let Some(agg) = self.aggregator.upgrade() {
            agg.borrow_mut().handle_trade(*trade);
        }
    }
}

/// Message handler for the trades of a joined warm-up's builder (the history/live join).
///
/// Holds the live trades while the request replays, then takes every later live trade through
/// the release its slot receives: the builder gets a private copy, every other subscriber keeps
/// the original. Only a joined warm-up gets this handler; every other trade-fed builder gets a
/// [`BarTradeHandler`].
#[derive(Debug)]
pub(super) struct JoinedTradeHandler {
    aggregator: WeakCell<Box<dyn BarAggregator>>,
    bar_type: BarType,
    warmup: WarmupBuffer<TradeTick>,
    joined: JoinedSlot,
}

impl JoinedTradeHandler {
    pub(super) fn new(
        aggregator: &Rc<RefCell<Box<dyn BarAggregator>>>,
        bar_type: BarType,
        warmup: WarmupBuffer<TradeTick>,
        joined: JoinedSlot,
    ) -> Self {
        Self {
            aggregator: WeakCell::from(Rc::downgrade(aggregator)),
            bar_type,
            warmup,
            joined,
        }
    }
}

impl Handler<TradeTick> for JoinedTradeHandler {
    fn id(&self) -> Ustr {
        Ustr::from(&format!("JoinedTradeHandler|{}", self.bar_type))
    }

    fn handle(&self, trade: &TradeTick) {
        if self.warmup.hold(trade) {
            return;
        }

        if let Some(agg) = self.aggregator.upgrade()
            && let Some(copy) = self.joined.admit(trade)
        {
            agg.borrow_mut().handle_trade(copy);
        }
    }
}

/// Message handler for processing bars through composite bar aggregators.
///
/// This handler receives bar messages and forwards them to the underlying
/// bar aggregator for further processing. It's used for building composite
/// bars from existing bars, such as creating higher timeframe bars from
/// lower timeframe bars.
#[derive(Debug)]
pub(super) struct BarBarHandler {
    aggregator: WeakCell<Box<dyn BarAggregator>>,
    bar_type: BarType,
}

impl BarBarHandler {
    pub(crate) fn new(aggregator: &Rc<RefCell<Box<dyn BarAggregator>>>, bar_type: BarType) -> Self {
        Self {
            aggregator: WeakCell::from(Rc::downgrade(aggregator)),
            bar_type,
        }
    }
}

impl Handler<Bar> for BarBarHandler {
    fn id(&self) -> Ustr {
        Ustr::from(&format!("BarBarHandler|{}", self.bar_type))
    }

    fn handle(&self, bar: &Bar) {
        if let Some(agg) = self.aggregator.upgrade() {
            agg.borrow_mut().handle_bar(*bar);
        }
    }
}

/// Message handler for processing leg quotes through spread quote aggregators.
#[derive(Debug)]
pub(super) struct SpreadQuoteHandler {
    aggregator: WeakCell<SpreadQuoteAggregator>,
    spread_instrument_id: InstrumentId,
    leg_instrument_id: InstrumentId,
}

impl SpreadQuoteHandler {
    pub(crate) fn new(
        aggregator: &Rc<RefCell<SpreadQuoteAggregator>>,
        spread_instrument_id: InstrumentId,
        leg_instrument_id: InstrumentId,
    ) -> Self {
        Self {
            aggregator: WeakCell::from(Rc::downgrade(aggregator)),
            spread_instrument_id,
            leg_instrument_id,
        }
    }
}

impl Handler<QuoteTick> for SpreadQuoteHandler {
    fn id(&self) -> Ustr {
        Ustr::from(&format!(
            "SpreadQuoteHandler|{}|{}",
            self.spread_instrument_id, self.leg_instrument_id,
        ))
    }

    fn handle(&self, quote: &QuoteTick) {
        if let Some(agg) = self.aggregator.upgrade() {
            agg.borrow_mut().handle_quote_tick(*quote);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, sync::Once};

    use log::{Level, LevelFilter, Log, Metadata, Record};
    use nautilus_common::msgbus::Handler;
    use nautilus_core::{UnixNanos, datetime::NANOSECONDS_IN_MILLISECOND};
    use nautilus_model::{
        data::{Bar, BarType, TradeTick},
        enums::AggressorSide,
        identifiers::{InstrumentId, TradeId},
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::{JoinedRelease, JoinedSlot, JoinedTradeHandler, WarmupBuffer, WarmupCounts};
    use crate::aggregation::{BarAggregator, TickBarAggregator};

    const MS: u64 = NANOSECONDS_IN_MILLISECOND;

    thread_local! {
        static RELEASE_LOGS: RefCell<Vec<(Level, String)>> = const { RefCell::new(Vec::new()) };
    }

    struct ReleaseLogCapture;

    impl Log for ReleaseLogCapture {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.target() == module_path!().trim_end_matches("::tests")
        }

        fn log(&self, record: &Record<'_>) {
            if self.enabled(record.metadata()) {
                RELEASE_LOGS.with_borrow_mut(|logs| {
                    logs.push((record.level(), record.args().to_string()));
                });
            }
        }

        fn flush(&self) {}
    }

    fn capture_release_logs() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            log::set_logger(&ReleaseLogCapture).expect("test logger already installed");
            log::set_max_level(LevelFilter::Info);
        });
        RELEASE_LOGS.with_borrow_mut(Vec::clear);
    }

    fn take_release_logs() -> Vec<(Level, String)> {
        RELEASE_LOGS.with_borrow_mut(std::mem::take)
    }

    fn trade(ts_event: u64, ts_init: u64, id: &str) -> TradeTick {
        TradeTick::new(
            InstrumentId::from("AUD/USD.SIM"),
            Price::from("0.65000"),
            Quantity::from("1000"),
            AggressorSide::Buy,
            TradeId::new(id),
            UnixNanos::from(ts_event),
            UnixNanos::from(ts_init),
        )
    }

    /// A one-tick builder that has taken `answered`, its trade handler holding a joined warm-up,
    /// and the bars it builds.
    struct HeldBuilder {
        bar_type: BarType,
        aggregator: Rc<RefCell<Box<dyn BarAggregator>>>,
        handler: JoinedTradeHandler,
        buffer: WarmupBuffer<TradeTick>,
        joined: JoinedSlot,
        bars: Rc<RefCell<Vec<Bar>>>,
    }

    fn held_builder(answered: &[TradeTick], held: &[TradeTick]) -> HeldBuilder {
        let bar_type = BarType::from("AUD/USD.SIM-1-TICK-LAST-INTERNAL");
        let bars = Rc::new(RefCell::new(Vec::new()));
        let sink = Rc::clone(&bars);
        let aggregator: Rc<RefCell<Box<dyn BarAggregator>>> = Rc::new(RefCell::new(Box::new(
            TickBarAggregator::new(bar_type, 5, 0, move |bar| sink.borrow_mut().push(bar)),
        )));
        for trade in answered {
            aggregator.borrow_mut().handle_trade(*trade);
        }

        let buffer = WarmupBuffer::new();
        let joined = JoinedSlot::default();
        let handler =
            JoinedTradeHandler::new(&aggregator, bar_type, buffer.clone(), joined.clone());
        for trade in held {
            handler.handle(trade);
        }

        HeldBuilder {
            bar_type,
            aggregator,
            handler,
            buffer,
            joined,
            bars,
        }
    }

    impl HeldBuilder {
        fn release(&self, answered: &[TradeTick]) {
            let copies = self.buffer.release_joined_copies(
                &self.joined,
                JoinedRelease::from_answer(answered),
                self.bar_type,
            );
            for copy in copies.into_iter().flatten() {
                self.aggregator.borrow_mut().handle_trade(copy);
            }
        }

        /// The `ts_init` of each bar built, in order: a one-tick bar carries its trade's.
        fn built(&self) -> Vec<UnixNanos> {
            self.bars.borrow().iter().map(|bar| bar.ts_init).collect()
        }
    }

    #[rstest]
    fn test_joined_release_counts_what_it_held() {
        // The answer's cut is 160 ms; its second row has equal stamps
        let answered = [
            trade(100 * MS, 150 * MS, "a1"),
            trade(160 * MS, 160 * MS, "a2"),
        ];
        let held = [
            trade(150 * MS, 170 * MS, "in-the-answer"),
            trade(160 * MS + MS / 2, 175 * MS, "same-millisecond"),
            trade(161 * MS, 158 * MS, "below-the-floor"),
            trade(170 * MS, 180 * MS, "after"),
        ];
        let builder = held_builder(&answered, &held);
        capture_release_logs();

        builder.release(&answered);

        let release = (*builder.joined.0.borrow()).unwrap();
        assert_eq!(
            release.counts,
            WarmupCounts {
                held: 4,
                flushed: 2,
                dropped: 2,
                floored: 1,
                equal_stamp_rows: 1,
            }
        );
        assert_eq!(
            take_release_logs(),
            vec![(
                Level::Info,
                format!(
                    "Released the joined warm-up of {}: held=4, flushed=2, dropped=2, floored=1, \
                     equal_stamp_rows=1",
                    builder.bar_type
                ),
            )]
        );
        // The answer's two bars, then the floored copy at the floor and the trade after
        assert_eq!(
            builder.built(),
            [150 * MS, 160 * MS, 160 * MS, 180 * MS].map(UnixNanos::from)
        );
    }

    #[rstest]
    fn test_equal_stamp_rows_counts_a_defined_exchange_time_equal_to_the_receive_time() {
        // The engine sees two stamps, not their provenance: a row whose exchange time is defined
        // and happens to equal its receive time is counted like a fallback row
        let answered = [
            trade(100 * MS, 150 * MS, "distinct-stamps"),
            trade(160 * MS + 7, 160 * MS + 7, "defined-and-equal"),
            trade(170 * MS + 1, 170 * MS, "one-nanosecond-apart"),
        ];

        let release = JoinedRelease::from_answer(&answered);

        assert_eq!(release.counts.equal_stamp_rows, 1);
    }

    #[rstest]
    fn test_joined_release_floors_a_held_trade_below_the_answer() {
        // Answered (100 ms, 200), held (101 ms, 199): past the cut, below the builder's last input
        let answered = [trade(100 * MS, 200 * MS, "a1")];
        let builder = held_builder(&answered, &[trade(101 * MS, 199 * MS, "held")]);

        builder.release(&answered);

        assert_eq!(builder.built(), [200 * MS, 200 * MS].map(UnixNanos::from));
        let release = (*builder.joined.0.borrow()).unwrap();
        assert_eq!(release.counts.floored, 1);
        assert_eq!(release.counts.flushed, 1);
    }

    #[rstest]
    fn test_joined_release_logs_no_overlap_observed_when_the_first_held_trade_is_after_the_cut() {
        let answered = [trade(100 * MS, 150 * MS, "a1")];
        let builder = held_builder(&answered, &[trade(101 * MS, 160 * MS, "after")]);
        capture_release_logs();

        builder.release(&answered);

        let logs = take_release_logs();
        assert_eq!(logs.len(), 2, "{logs:?}");
        assert_eq!(logs[0].0, Level::Warn);
        assert!(logs[0].1.contains("no overlap observed"), "{logs:?}");
        assert_eq!(
            logs[1].1,
            format!(
                "Released the joined warm-up of {}: held=1, flushed=1, dropped=0, floored=0, \
                 equal_stamp_rows=0",
                builder.bar_type
            )
        );
    }

    #[rstest]
    fn test_joined_release_logs_counts_only_when_the_first_held_trade_is_in_the_answer() {
        let answered = [trade(100 * MS, 150 * MS, "a1")];
        let held = [
            trade(100 * MS, 155 * MS, "a1-live"),
            trade(101 * MS, 160 * MS, "after"),
        ];
        let builder = held_builder(&answered, &held);
        capture_release_logs();

        builder.release(&answered);

        let logs = take_release_logs();
        assert_eq!(logs.len(), 1, "{logs:?}");
        assert_eq!(logs[0].0, Level::Info);
    }

    #[rstest]
    fn test_joined_handler_takes_later_live_trades_through_the_cut_and_the_floor() {
        let answered = [trade(100 * MS, 200 * MS, "a1")];
        let builder = held_builder(&answered, &[]);
        builder.release(&answered);

        // After the release: a late trade of the cut's millisecond, then one below the floor
        builder
            .handler
            .handle(&trade(100 * MS + 1, 210 * MS, "late"));
        builder
            .handler
            .handle(&trade(101 * MS, 199 * MS, "below-the-floor"));

        assert_eq!(builder.built(), [200 * MS, 200 * MS].map(UnixNanos::from));
        let release = (*builder.joined.0.borrow()).unwrap();
        assert_eq!(release.counts.dropped, 1);
        assert_eq!(release.counts.floored, 1);
    }

    #[rstest]
    fn test_joined_release_of_an_empty_answer_cuts_nothing() {
        let release = JoinedRelease::from_answer(&[]);

        assert_eq!(release.cut_ms, None);
        assert_eq!(release.floor, None);
        assert_eq!(release.counts, WarmupCounts::default());
    }
}
