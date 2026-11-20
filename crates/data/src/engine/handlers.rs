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
use nautilus_core::WeakCell;
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
    warmup: Option<WarmupBuffer<Bar>>,
}

impl BarBarHandler {
    pub(crate) fn new(aggregator: &Rc<RefCell<Box<dyn BarAggregator>>>, bar_type: BarType) -> Self {
        Self {
            aggregator: WeakCell::from(Rc::downgrade(aggregator)),
            bar_type,
            warmup: None,
        }
    }

    /// Holds incoming data in `warmup` until the buffer is released.
    pub(super) fn with_warmup(mut self, warmup: Option<WarmupBuffer<Bar>>) -> Self {
        self.warmup = warmup;
        self
    }
}

impl Handler<Bar> for BarBarHandler {
    fn id(&self) -> Ustr {
        Ustr::from(&format!("BarBarHandler|{}", self.bar_type))
    }

    fn handle(&self, bar: &Bar) {
        if self.warmup.as_ref().is_some_and(|warmup| warmup.hold(bar)) {
            return;
        }

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
