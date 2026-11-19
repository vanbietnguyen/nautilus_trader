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

use std::{iter::Fuse, ops::Range, sync::Arc};

use nautilus_common::messages::data::{InstrumentsResponse, QuotesResponse, TradesResponse};
use nautilus_model::data::{BatchView, DataBatch, QuoteTick};

use super::{
    BookDeltasResponse, BookType, CustomData, CustomDataResponse, Data, DataEngine, DataResponse,
    DataResponseEnvelope, Display, FromStr, HasTsInit, InstrumentAny, InstrumentId,
    NANOSECONDS_IN_DAY, OrderBook, OrderBookDeltas, RecordFlag, RequestCommand, UUID4, UnixNanos,
    bar::trades_part, type_name,
};
use crate::replay::{DataIterator, ReplayBatch, ReplayPoll, ReplaySource};

impl DataEngine {
    // Replays a day-start snapshot forward to the request's original start: when the first delta
    // is an F_SNAPSHOT on a UTC day boundary, rebuilds the book from the deltas before the start
    // and replaces them with one snapshot keyed at the start, then forwards the rest unchanged.
    pub(super) fn book_deltas_snapshot_replay(
        &self,
        resp: &mut BookDeltasResponse,
    ) -> anyhow::Result<()> {
        let Some(original_start_ns) = resp.start else {
            return Ok(());
        };

        let Some(first) = resp.data.first().copied() else {
            return Ok(());
        };

        if !RecordFlag::F_SNAPSHOT.matches(first.flags) {
            return Ok(());
        }

        if first.ts_init.as_u64() % NANOSECONDS_IN_DAY != 0 {
            return Ok(());
        }

        // Nothing to fast-forward when the request starts at or before the day-start snapshot
        if original_start_ns <= first.ts_init {
            return Ok(());
        }

        if self
            .cache
            .borrow()
            .instrument(&resp.instrument_id)
            .is_none()
        {
            log::warn!(
                "Instrument {} not found in cache, skipping snapshot replay",
                resp.instrument_id,
            );
            return Ok(());
        }

        let book_type = resp
            .params
            .as_ref()
            .and_then(|p| p.get_str("book_type"))
            .and_then(|s| BookType::from_str(s).ok())
            .unwrap_or(BookType::L2_MBP);

        let split = resp
            .data
            .iter()
            .position(|delta| delta.ts_init >= original_start_ns)
            .unwrap_or(resp.data.len());
        let (before, after) = resp.data.split_at(split);

        let mut book = OrderBook::new(resp.instrument_id, book_type);
        let batch = OrderBookDeltas::new(resp.instrument_id, before.to_vec());
        if let Err(e) = book.apply_deltas(&batch) {
            anyhow::bail!(
                "Failed to rebuild order book {} for snapshot replay: {e}",
                resp.instrument_id,
            );
        }

        let mut new_data = book.to_deltas(original_start_ns, original_start_ns).deltas;
        new_data.extend_from_slice(after);
        resp.data = new_data.into();

        Ok(())
    }
}

// Keeps an earlier failure reason when a completed envelope fails again
pub(super) fn append_failure(
    envelope: DataResponseEnvelope,
    reason: String,
) -> DataResponseEnvelope {
    let reason = match envelope.failure_reason() {
        Some(existing) => format!("{existing}; {reason}"),
        None => reason,
    };

    envelope.with_failure(reason)
}

#[inline(always)]
pub(super) fn log_if_empty_response<T, I: Display>(
    data: &[T],
    id: &I,
    correlation_id: &UUID4,
) -> bool {
    if data.is_empty() {
        let name = type_name::<T>();
        let short_name = name.rsplit("::").next().unwrap_or(name);
        log::warn!("Received empty {short_name} response for {id} {correlation_id}");
        return true;
    }

    false
}

// A book-delta batch carries one instrument ID, so different instruments must stay separate
pub(super) fn rebuild_pipeline_response(
    parent_id: UUID4,
    parent: Option<&RequestCommand>,
    legs: Vec<DataResponse>,
) -> Option<DataResponse> {
    let (start, end) = parent_request_window(parent);
    let mut iter = legs.into_iter();
    let first = iter.next()?;

    let response = match first {
        DataResponse::Data(mut acc) => {
            let mut data = custom_response_data(&acc, parent_id)?;

            for leg in iter {
                let DataResponse::Data(other) = leg else {
                    return None;
                };

                data.extend(custom_response_data(&other, parent_id)?);
            }

            data.sort_by_key(CustomData::ts_init);
            acc.data = Arc::new(data);
            acc.start = start.or(acc.start);
            acc.end = end.or(acc.end);
            DataResponse::Data(acc)
        }
        DataResponse::Instrument(acc) => {
            let data = merge_instrument_data(vec![acc.data], iter)?;
            DataResponse::Instruments(InstrumentsResponse::new(
                parent_id,
                acc.client_id,
                acc.instrument_id.venue,
                data,
                start.or(acc.start),
                end.or(acc.end),
                acc.ts_init,
                acc.params,
            ))
        }
        DataResponse::Instruments(mut acc) => {
            acc.data = merge_instrument_data(acc.data, iter)?;
            acc.start = start.or(acc.start);
            acc.end = end.or(acc.end);
            DataResponse::Instruments(acc)
        }
        DataResponse::Quotes(mut acc) => {
            acc.data = merge_series(
                acc.data,
                iter.map(|r| {
                    if let DataResponse::Quotes(r) = r {
                        Some(r.data)
                    } else {
                        None
                    }
                }),
                |row| (row.ts_init, row.instrument_id),
            )?;

            acc.start = start.or(acc.start);
            acc.end = end.or(acc.end);
            if let Some(RequestCommand::Quotes(parent)) = parent {
                acc.instrument_ids.clone_from(&parent.instrument_ids);
            }

            DataResponse::Quotes(acc)
        }
        DataResponse::Trades(mut acc) => {
            acc.data = merge_series(
                acc.data,
                iter.map(|r| {
                    if let DataResponse::Trades(r) = r {
                        Some(r.data)
                    } else {
                        None
                    }
                }),
                |row| (row.ts_init, row.instrument_id),
            )?;

            acc.start = start.or(acc.start);
            acc.end = end.or(acc.end);
            if let Some(RequestCommand::Trades(parent)) = parent {
                acc.instrument_ids.clone_from(&parent.instrument_ids);
            }

            DataResponse::Trades(acc)
        }
        DataResponse::FundingRates(mut acc) => {
            acc.data = merge_series(
                acc.data,
                iter.map(|r| {
                    if let DataResponse::FundingRates(r) = r {
                        Some(r.data)
                    } else {
                        None
                    }
                }),
                |row| row.ts_init,
            )?;

            acc.start = start.or(acc.start);
            acc.end = end.or(acc.end);
            DataResponse::FundingRates(acc)
        }
        DataResponse::Bars(mut acc) => {
            acc.data = merge_series(
                acc.data,
                iter.map(|r| {
                    if let DataResponse::Bars(r) = r {
                        Some(r.data)
                    } else {
                        None
                    }
                }),
                |row| row.ts_init,
            )?;

            acc.start = start.or(acc.start);
            acc.end = end.or(acc.end);
            DataResponse::Bars(acc)
        }
        DataResponse::BookDeltas(mut acc) => {
            let instrument_id = acc.instrument_id;
            acc.data = merge_series(
                acc.data,
                iter.map(|r| {
                    let DataResponse::BookDeltas(r) = r else {
                        return None;
                    };

                    same_instrument(r.instrument_id, instrument_id).then_some(r.data)
                }),
                |row| row.ts_init,
            )?;

            acc.start = start.or(acc.start);
            acc.end = end.or(acc.end);
            DataResponse::BookDeltas(acc)
        }
        DataResponse::BookDepth(mut acc) => {
            acc.data = merge_series(
                acc.data,
                iter.map(|r| {
                    if let DataResponse::BookDepth(r) = r {
                        Some(r.data)
                    } else {
                        None
                    }
                }),
                |row| row.ts_init,
            )?;

            acc.start = start.or(acc.start);
            acc.end = end.or(acc.end);
            DataResponse::BookDepth(acc)
        }
        _ => return None,
    };

    Some(rebind_response_correlation(response, parent_id))
}

/// Keeps a join's rebuilt answer open-ended: its trades `end` is cleared, so the answer keeps the
/// replay tail's open end and is not trimmed to the catalog leg's finite one. Runs between
/// [`rebuild_pipeline_response`] and the trim of every rebuild of a join.
pub(super) fn keep_open_end(rebuilt: &mut DataResponse) {
    if let DataResponse::Trades(response) = rebuilt {
        response.end = None;
    }
}

/// The most rows one part of a merged answer carries (the catalog's chunk size): a run of one
/// type that reaches it is sent as a part, and a new run starts.
const MERGED_PART_ROWS: usize = 10_000;

/// Sends the rows of `quote_sources` and `trade_sources` in one time order through `send` as
/// typed parts, and returns the last run of trades, possibly empty, which completes the answer.
///
/// Each source is one leg's rows and goes into the replay iterator on its own, by `ts_init`:
/// every quotes source first, then every trades source, each feed's in slot order, so a quote
/// comes before a trade at an equal stamp (the backtest's order), also where a source's next
/// batch starts at that stamp, and rows of one feed at an equal stamp go in slot order. A run of
/// one type is cut where the type changes and where it reaches 10,000 rows; each run is sent as
/// it is cut, so no two parts are held at once. Each part carries the identity and window of
/// `quotes` or `trades`, whose rows are not read.
///
/// # Errors
///
/// Returns an error if a source cannot be admitted or the replay iterator fails; the parts sent
/// before it stay sent.
pub(super) fn quote_and_trade_runs(
    quotes: &QuotesResponse,
    trades: &TradesResponse,
    quote_sources: Vec<Box<dyn ReplaySource>>,
    trade_sources: Vec<Box<dyn ReplaySource>>,
    send: impl FnMut(DataResponse),
) -> anyhow::Result<TradesResponse> {
    let mut replay = DataIterator::new();
    let quote_sources = quote_sources.into_iter().map(|source| ("quotes", source));
    let trade_sources = trade_sources.into_iter().map(|source| ("trades", source));

    for (slot, (feed, source)) in quote_sources.chain(trade_sources).enumerate() {
        replay.add_source(&format!("{feed}-{slot}"), source, true)?;
    }

    cut_quote_and_trade_runs(&mut replay, quotes, trades, send)
}

/// Takes the rows out of the quotes and trades legs of a merged answer, in leg order, each leg's
/// rows in the order the rebuild sorts them (by `ts_init`, then instrument; copied only when they
/// come unsorted). Each leg is left with no rows, so the rebuild of the legs makes the answer's
/// identity alone. Any other leg keeps its rows and adds none.
pub(super) fn take_merge_rows(legs: &mut [DataResponse]) -> Vec<DataBatch> {
    legs.iter_mut()
        .filter_map(|leg| match leg {
            DataResponse::Quotes(r) => {
                Some(DataBatch::Quote(take_sorted_rows(&mut r.data, |row| {
                    (row.ts_init, row.instrument_id)
                })))
            }
            DataResponse::Trades(r) => {
                Some(DataBatch::Trade(take_sorted_rows(&mut r.data, |row| {
                    (row.ts_init, row.instrument_id)
                })))
            }
            _ => None,
        })
        .collect()
}

fn take_sorted_rows<T: Clone, K: Ord>(
    data: &mut BatchView<T>,
    key: impl Fn(&T) -> K,
) -> BatchView<T> {
    let mut rows = std::mem::replace(data, Vec::new().into());

    if !rows.is_sorted_by_key(&key) {
        rows.make_mut().sort_by_key(key);
    }

    rows
}

/// Makes each leg's collected rows, its one chunk, a source of the merge narrowed to
/// `[start, end]` (see [`WindowSource`]).
///
/// # Errors
///
/// Returns an error if a leg's first chunk cannot be read.
pub(super) fn leg_sources(
    rows: Vec<DataBatch>,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> anyhow::Result<Vec<Box<dyn ReplaySource>>> {
    rows.into_iter()
        .map(|rows| {
            let source = WindowSource::new(std::iter::once(Ok(rows)), start, end)?;
            Ok(Box::new(source) as Box<dyn ReplaySource>)
        })
        .collect()
}

/// One leg of a merged answer as a replay source: its chunks, each in `ts_init` order, narrowed
/// to `[start, end]` as views of their rows (the rows the answer's trim keeps), a chunk left
/// empty skipped. It reads its first nonempty chunk when it is made, so a leg that cannot be
/// read fails before any part is sent, and passes a chunk's error on unchanged.
pub(super) struct WindowSource<I: Iterator> {
    chunks: Fuse<I>,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    first: Option<DataBatch>,
}

impl<I: Iterator<Item = anyhow::Result<DataBatch>>> WindowSource<I> {
    /// Creates a source over `chunks`, reading up to its first chunk with a row in the window.
    ///
    /// # Errors
    ///
    /// Returns the error of a chunk read before a chunk with a row in the window.
    pub(super) fn new(
        chunks: I,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
    ) -> anyhow::Result<Self> {
        let mut source = Self {
            chunks: chunks.fuse(),
            start,
            end,
            first: None,
        };
        source.first = source.next_chunk()?;
        Ok(source)
    }

    fn next_chunk(&mut self) -> anyhow::Result<Option<DataBatch>> {
        for chunk in self.chunks.by_ref() {
            let chunk = chunk?;
            let rows = window_rows(&chunk, self.start, self.end);

            if !rows.is_empty() {
                return Ok(Some(chunk.slice(rows.start, rows.end)));
            }
        }

        Ok(None)
    }
}

impl<I: Iterator<Item = anyhow::Result<DataBatch>>> ReplaySource for WindowSource<I> {
    fn poll_batch(&mut self) -> anyhow::Result<ReplayPoll<ReplayBatch>> {
        let chunk = match self.first.take() {
            Some(chunk) => Some(chunk),
            None => self.next_chunk()?,
        };

        Ok(chunk.map_or(ReplayPoll::Complete, |chunk| {
            ReplayPoll::Ready(ReplayBatch::Typed(chunk))
        }))
    }
}

// The rows of `chunk`, in `ts_init` order, from the first at or after `start` through the last
// at or before `end`, found as the answer's trim finds them
fn window_rows(
    chunk: &DataBatch,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) -> Range<usize> {
    let stamp_at = |index: usize| chunk.get(index).map(|row| row.ts_init());
    let len = chunk.len();
    let first = start.map_or(0, |start| {
        (0..len)
            .position(|index| stamp_at(index).is_some_and(|ts| ts >= start))
            .unwrap_or(len)
    });
    let last = end.map_or(len, |end| {
        (0..len)
            .rposition(|index| stamp_at(index).is_some_and(|ts| ts <= end))
            .map_or(0, |index| index + 1)
    });

    first..last.max(first)
}

// Takes `replay`'s rows in order, sending each run as it is cut (see `quote_and_trade_runs`)
fn cut_quote_and_trade_runs(
    replay: &mut DataIterator,
    quotes: &QuotesResponse,
    trades: &TradesResponse,
    mut send: impl FnMut(DataResponse),
) -> anyhow::Result<TradesResponse> {
    let mut quote_run = Vec::new();
    let mut trade_run = Vec::new();

    loop {
        match replay.next()? {
            ReplayPoll::Ready(Data::Quote(quote)) => {
                if !trade_run.is_empty() {
                    let run = std::mem::take(&mut trade_run);
                    send(DataResponse::Trades(trades_part(trades, run)));
                }
                quote_run.push(quote);

                if quote_run.len() == MERGED_PART_ROWS {
                    let run = std::mem::take(&mut quote_run);
                    send(DataResponse::Quotes(quotes_part(quotes, run)));
                }
            }
            ReplayPoll::Ready(Data::Trade(trade)) => {
                if !quote_run.is_empty() {
                    let run = std::mem::take(&mut quote_run);
                    send(DataResponse::Quotes(quotes_part(quotes, run)));
                }
                trade_run.push(trade);

                if trade_run.len() == MERGED_PART_ROWS {
                    let run = std::mem::take(&mut trade_run);
                    send(DataResponse::Trades(trades_part(trades, run)));
                }
            }
            ReplayPoll::Ready(other) => {
                anyhow::bail!("The replay of quotes and trades yielded {other:?}")
            }
            ReplayPoll::Pending { .. } => {
                anyhow::bail!("The replay of quotes and trades is pending on static input")
            }
            ReplayPoll::Complete => break,
        }
    }

    if !quote_run.is_empty() {
        send(DataResponse::Quotes(quotes_part(quotes, quote_run)));
    }

    Ok(trades_part(trades, trade_run))
}

/// Builds a quotes part of a merged answer, carrying the quotes response's identity and window.
pub(super) fn quotes_part(resp: &QuotesResponse, data: Vec<QuoteTick>) -> QuotesResponse {
    QuotesResponse::new(
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

pub(super) fn same_instrument(left: InstrumentId, right: InstrumentId) -> bool {
    left == right
        || (left.symbol.as_str() == right.symbol.as_str()
            && left.venue.as_str() == right.venue.as_str())
}

fn merge_series<T: Clone, K: Ord>(
    mut data: BatchView<T>,
    legs: impl Iterator<Item = Option<BatchView<T>>>,
    key: impl Fn(&T) -> K,
) -> Option<BatchView<T>> {
    let mut batches = Vec::new();

    for leg in legs {
        let leg = leg?;

        if leg.is_empty() {
            continue;
        }

        if data.is_empty() {
            data = leg;
        } else {
            batches.push(leg);
        }
    }

    if !batches.is_empty() {
        let len = data.len() + batches.iter().map(|batch| batch.len()).sum::<usize>();
        let mut rows = Vec::with_capacity(len);
        rows.extend_from_slice(&data);
        for batch in batches {
            rows.extend_from_slice(&batch);
        }

        data = rows.into();
    }

    if !data.is_sorted_by_key(&key) {
        data.make_mut().sort_by_key(key);
    }

    Some(data)
}

fn merge_instrument_data(
    mut data: Vec<InstrumentAny>,
    legs: impl Iterator<Item = DataResponse>,
) -> Option<Vec<InstrumentAny>> {
    for leg in legs {
        match leg {
            DataResponse::Instrument(r) => data.push(r.data),
            DataResponse::Instruments(r) => data.extend(r.data),
            _ => return None,
        }
    }

    Some(data)
}

fn custom_response_data(resp: &CustomDataResponse, parent_id: UUID4) -> Option<Vec<CustomData>> {
    if let Some(data) = resp.data.as_ref().downcast_ref::<Vec<CustomData>>() {
        return Some(data.clone());
    }

    if let Some(data) = resp.data.as_ref().downcast_ref::<CustomData>() {
        return Some(vec![data.clone()]);
    }

    if let Some(data) = resp.data.as_ref().downcast_ref::<Vec<Data>>() {
        let mut custom = Vec::with_capacity(data.len());
        for item in data {
            let Data::Custom(value) = item else {
                log::error!("Custom data pipeline {parent_id} received non-custom data {item:?}");
                return None;
            };

            custom.push(value.clone());
        }

        return Some(custom);
    }

    log::error!(
        "Custom data pipeline {parent_id} received unsupported payload for {}",
        resp.data_type,
    );
    None
}

pub(super) fn parent_request_window(
    parent: Option<&RequestCommand>,
) -> (Option<UnixNanos>, Option<UnixNanos>) {
    let Some(parent) = parent else {
        return (None, None);
    };

    let (start, end) = match parent {
        RequestCommand::Data(cmd) => (cmd.start, cmd.end),
        RequestCommand::Instrument(cmd) => (cmd.start, cmd.end),
        RequestCommand::Instruments(cmd) => (cmd.start, cmd.end),
        RequestCommand::BookDeltas(cmd) => (cmd.start, cmd.end),
        RequestCommand::BookDepth(cmd) => (cmd.start, cmd.end),
        RequestCommand::Quotes(cmd) => (cmd.start, cmd.end),
        RequestCommand::Trades(cmd) => (cmd.start, cmd.end),
        RequestCommand::FundingRates(cmd) => (cmd.start, cmd.end),
        RequestCommand::Bars(cmd) => (cmd.start, cmd.end),
        RequestCommand::Join(cmd) => (cmd.start, cmd.end),
        RequestCommand::BookSnapshot(_) | RequestCommand::OptionChainReferencePrice(_) => {
            return (None, None);
        }
    };

    (
        start.map(datetime_to_unix_nanos_or_zero),
        end.map(datetime_to_unix_nanos_or_zero),
    )
}

pub(super) fn datetime_to_unix_nanos_or_zero(dt: jiff::Timestamp) -> UnixNanos {
    UnixNanos::from(u64::try_from(dt.as_nanosecond().max(0)).unwrap_or(0))
}

pub(super) fn rebind_response_correlation(mut resp: DataResponse, new_id: UUID4) -> DataResponse {
    match &mut resp {
        DataResponse::Data(r) => r.correlation_id = new_id,
        DataResponse::Instrument(r) => r.correlation_id = new_id,
        DataResponse::Instruments(r) => r.correlation_id = new_id,
        DataResponse::Book(r) => r.correlation_id = new_id,
        DataResponse::BookDeltas(r) => r.correlation_id = new_id,
        DataResponse::BookDepth(r) => r.correlation_id = new_id,
        DataResponse::Quotes(r) => r.correlation_id = new_id,
        DataResponse::Trades(r) => r.correlation_id = new_id,
        DataResponse::FundingRates(r) => r.correlation_id = new_id,
        DataResponse::OptionChainReferencePrice(r) => r.correlation_id = new_id,
        DataResponse::Bars(r) => r.correlation_id = new_id,
    }

    resp
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, rc::Rc};

    use nautilus_model::{
        data::TradeTick,
        enums::AggressorSide,
        identifiers::{ClientId, TradeId},
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::*;
    use crate::replay::{ReplayBatch, ReplaySource};

    const PAGE_ROWS: usize = 1_000;

    /// Hands out quotes stamped 1 to `total` in pages of [`PAGE_ROWS`], counting the rows it
    /// has handed out.
    struct CountedQuotes {
        handed_out: Rc<Cell<usize>>,
        total: usize,
    }

    impl ReplaySource for CountedQuotes {
        fn poll_batch(&mut self) -> anyhow::Result<ReplayPoll<ReplayBatch>> {
            let start = self.handed_out.get();

            if start == self.total {
                return Ok(ReplayPoll::Complete);
            }
            let end = (start + PAGE_ROWS).min(self.total);
            let page = (start..end)
                .map(|row| {
                    let ts = UnixNanos::from(row as u64 + 1);
                    QuoteTick::new(
                        InstrumentId::from("AUD/USD.SIM"),
                        Price::from("1.0000"),
                        Price::from("1.0002"),
                        Quantity::from(1),
                        Quantity::from(1),
                        ts,
                        ts,
                    )
                })
                .collect::<Vec<_>>();
            self.handed_out.set(end);
            Ok(ReplayPoll::Ready(ReplayBatch::Typed(DataBatch::Quote(
                page.into(),
            ))))
        }
    }

    #[rstest]
    fn test_merged_answer_sends_its_first_part_before_it_cuts_its_last_run() {
        let total = 25_000;
        let handed_out = Rc::new(Cell::new(0));
        let mut replay = DataIterator::new();
        let source = CountedQuotes {
            handed_out: Rc::clone(&handed_out),
            total,
        };
        replay.add_source("quotes", Box::new(source), true).unwrap();
        let instrument_id = InstrumentId::from("AUD/USD.SIM");
        let (client_id, ts_init) = (ClientId::new("SIM"), UnixNanos::default());
        let quotes = QuotesResponse::new(
            UUID4::new(),
            client_id,
            vec![instrument_id],
            Vec::new(),
            None,
            None,
            ts_init,
            None,
        );
        let trades = TradesResponse::new(
            quotes.correlation_id,
            client_id,
            vec![instrument_id],
            Vec::new(),
            None,
            None,
            ts_init,
            None,
        );

        // Each part with the rows the source had handed out when it was sent
        let mut sent = Vec::new();
        let last = cut_quote_and_trade_runs(&mut replay, &quotes, &trades, |part| {
            let DataResponse::Quotes(part) = part else {
                panic!("a quotes part: {part:?}");
            };
            let stamps = part.data.iter().map(|quote| quote.ts_init.as_u64());
            sent.push((stamps.collect::<Vec<_>>(), handed_out.get()));
        })
        .unwrap();

        assert!(last.data.is_empty());
        let sizes = sent.iter().map(|(rows, _)| rows.len()).collect::<Vec<_>>();
        assert_eq!(sizes, [10_000, 10_000, 5_000]);
        let delivered = sent.iter().flat_map(|(rows, _)| rows.iter().copied());
        assert!(delivered.eq(1..=total as u64));

        // The first part goes out before any row of the last run is handed out, and no part
        // waits on more than the cap and one page beyond the rows sent before it
        assert!(sent[0].1 <= 2 * MERGED_PART_ROWS);
        let mut sent_before = 0;

        for (rows, handed_out) in &sent {
            assert!(handed_out - sent_before <= MERGED_PART_ROWS + PAGE_ROWS);
            sent_before += rows.len();
        }
    }

    // A quote at `ts`, its slot carried as its `ts_event`
    fn quote(ts: u64, slot: u64) -> QuoteTick {
        QuoteTick::new(
            InstrumentId::from("AUD/USD.SIM"),
            Price::from("1.0000"),
            Price::from("1.0002"),
            Quantity::from(1),
            Quantity::from(1),
            UnixNanos::from(slot),
            UnixNanos::from(ts),
        )
    }

    // A trade at `ts`, its slot carried as its `ts_event`
    fn trade(ts: u64, slot: u64) -> TradeTick {
        TradeTick::new(
            InstrumentId::from("AUD/USD.SIM"),
            Price::from("1.0001"),
            Quantity::from(1),
            AggressorSide::Buy,
            TradeId::new(ts.to_string()),
            UnixNanos::from(slot),
            UnixNanos::from(ts),
        )
    }

    fn chunks(rows: Vec<DataBatch>) -> impl Iterator<Item = anyhow::Result<DataBatch>> + 'static {
        rows.into_iter().map(Ok)
    }

    fn source(rows: Vec<DataBatch>) -> Box<dyn ReplaySource> {
        Box::new(WindowSource::new(chunks(rows), None, None).unwrap())
    }

    // Each part as its type and its rows' (stamp, slot), the last trades run included
    fn merged_parts(
        quote_sources: Vec<Box<dyn ReplaySource>>,
        trade_sources: Vec<Box<dyn ReplaySource>>,
    ) -> Vec<(&'static str, Vec<(u64, u64)>)> {
        let instrument_id = InstrumentId::from("AUD/USD.SIM");
        let (client_id, ts_init) = (ClientId::new("SIM"), UnixNanos::default());
        let quotes = QuotesResponse::new(
            UUID4::new(),
            client_id,
            vec![instrument_id],
            Vec::new(),
            None,
            None,
            ts_init,
            None,
        );
        let trades = TradesResponse::new(
            quotes.correlation_id,
            client_id,
            vec![instrument_id],
            Vec::new(),
            None,
            None,
            ts_init,
            None,
        );
        let trade_rows = |part: &TradesResponse| {
            let rows = part.data.iter();
            rows.map(|row| (row.ts_init.as_u64(), row.ts_event.as_u64()))
                .collect()
        };

        let mut parts = Vec::new();
        let last =
            quote_and_trade_runs(
                &quotes,
                &trades,
                quote_sources,
                trade_sources,
                |part| match part {
                    DataResponse::Quotes(part) => {
                        let rows = part.data.iter();
                        let rows = rows.map(|row| (row.ts_init.as_u64(), row.ts_event.as_u64()));
                        parts.push(("quotes", rows.collect()));
                    }
                    DataResponse::Trades(part) => parts.push(("trades", trade_rows(&part))),
                    part => panic!("a quotes or trades part: {part:?}"),
                },
            )
            .unwrap();
        parts.push(("trades", trade_rows(&last)));
        parts
    }

    #[rstest]
    fn test_merged_answer_takes_a_quote_before_a_trade_at_the_stamp_its_next_batch_starts() {
        // The quotes' first batch ends at 9; the trades already hold a trade at 10
        let quotes = vec![
            DataBatch::Quote(vec![quote(8, 1), quote(9, 1)].into()),
            DataBatch::Quote(vec![quote(10, 1), quote(11, 1)].into()),
        ];
        let trades = vec![DataBatch::Trade(vec![trade(10, 1)].into())];

        let parts = merged_parts(vec![source(quotes)], vec![source(trades)]);

        assert_eq!(
            parts,
            [
                ("quotes", vec![(8, 1), (9, 1), (10, 1)]),
                ("trades", vec![(10, 1)]),
                ("quotes", vec![(11, 1)]),
                ("trades", vec![]),
            ]
        );
    }

    #[rstest]
    fn test_merged_answer_sends_the_rows_of_one_feed_at_one_stamp_in_slot_order() {
        // Two sources of each feed, as two files of it, each with a row at 5
        let quote_sources = vec![
            source(vec![DataBatch::Quote(
                vec![quote(5, 1), quote(6, 1)].into(),
            )]),
            source(vec![DataBatch::Quote(vec![quote(5, 2)].into())]),
        ];
        let trade_sources = vec![
            source(vec![DataBatch::Trade(vec![trade(5, 3)].into())]),
            source(vec![DataBatch::Trade(vec![trade(5, 4)].into())]),
        ];

        let parts = merged_parts(quote_sources, trade_sources);

        assert_eq!(
            parts,
            [
                ("quotes", vec![(5, 1), (5, 2)]),
                ("trades", vec![(5, 3), (5, 4)]),
                ("quotes", vec![(6, 1)]),
                ("trades", vec![]),
            ]
        );
    }

    #[rstest]
    fn test_merged_answer_leaves_out_a_leg_empty_in_its_window() {
        let window = (Some(UnixNanos::from(10)), Some(UnixNanos::from(20)));
        let rows = |rows: Vec<QuoteTick>| vec![DataBatch::Quote(rows.into())];
        let outside = rows(vec![quote(5, 1), quote(25, 1)]);
        let inside = rows(vec![quote(9, 2), quote(10, 2), quote(20, 2), quote(21, 2)]);
        let mut quote_sources = leg_sources(outside, window.0, window.1).unwrap();
        quote_sources.extend(leg_sources(inside, window.0, window.1).unwrap());
        let trades = vec![DataBatch::Trade(vec![trade(15, 3)].into())];
        let trade_sources = leg_sources(trades, window.0, window.1).unwrap();

        let parts = merged_parts(quote_sources, trade_sources);

        assert_eq!(
            parts,
            [
                ("quotes", vec![(10, 2)]),
                ("trades", vec![(15, 3)]),
                ("quotes", vec![(20, 2)]),
                ("trades", vec![]),
            ]
        );
    }

    #[rstest]
    fn test_window_source_skips_a_chunk_empty_in_its_window_and_completes() {
        let page =
            |stamps: [u64; 2]| DataBatch::Quote(stamps.map(|ts| quote(ts, 1)).to_vec().into());
        let pages = vec![page([1, 2]), page([3, 4]), page([5, 6]), page([7, 8])];
        let (start, end) = (Some(UnixNanos::from(4)), Some(UnixNanos::from(5)));
        let mut source = WindowSource::new(chunks(pages), start, end).unwrap();

        let mut stamps = Vec::new();
        while let ReplayPoll::Ready(ReplayBatch::Typed(chunk)) = source.poll_batch().unwrap() {
            let chunk = (0..chunk.len()).filter_map(|index| chunk.get(index));
            stamps.push(chunk.map(|row| row.ts_init().as_u64()).collect::<Vec<_>>());
        }

        assert_eq!(stamps, [vec![4], vec![5]]);
        assert!(matches!(source.poll_batch(), Ok(ReplayPoll::Complete)));
    }

    #[rstest]
    fn test_window_source_passes_a_chunk_error_on_unchanged() {
        let failing = || {
            let page = DataBatch::Quote(vec![quote(1, 1)].into());
            [Ok(page), Err(anyhow::anyhow!("page 2 cannot be decoded"))].into_iter()
        };

        let mut source = WindowSource::new(failing(), None, None).unwrap();
        assert!(matches!(source.poll_batch(), Ok(ReplayPoll::Ready(_))));
        let error = source.poll_batch().unwrap_err();
        assert_eq!(error.to_string(), "page 2 cannot be decoded");

        // Read before a row in the window, the error fails the source as it is made
        let outside = Some(UnixNanos::from(2));
        let error = WindowSource::new(failing(), outside, None).err().unwrap();
        assert_eq!(error.to_string(), "page 2 cannot be decoded");
    }

    #[rstest]
    #[case::sorted(vec![9, 1, 2, 8], true)]
    #[case::unsorted(vec![9, 2, 1, 8], false)]
    fn test_merge_series_preserves_single_visible_leg(
        #[case] rows: Vec<u64>,
        #[case] shared: bool,
    ) {
        let backing = Arc::new(rows.clone());
        let view = BatchView::new(Arc::clone(&backing), 1..3);
        let empty: BatchView<u64> = Vec::new().into();

        let merged = merge_series(
            empty.clone(),
            [Some(view), Some(empty)].into_iter(),
            |row| *row,
        )
        .unwrap();

        assert_eq!(merged.as_ref(), &[1, 2]);
        assert_eq!(Arc::ptr_eq(merged.arc(), &backing), shared);
        assert_eq!(merged.range(), if shared { 1..3 } else { 0..2 });
        assert_eq!(backing.as_slice(), rows.as_slice());
    }

    #[rstest]
    fn test_merge_series_materializes_visible_rows_in_stable_leg_order() {
        let first = Arc::new(vec![(0, 90), (2, 11), (1, 12), (9, 91)]);
        let second = Arc::new(vec![(0, 92), (1, 21), (2, 22), (9, 93)]);

        let merged = merge_series(
            BatchView::new(Arc::clone(&first), 1..3),
            [Some(BatchView::new(Arc::clone(&second), 1..3))].into_iter(),
            |row| row.0,
        )
        .unwrap();

        assert_eq!(merged.as_ref(), &[(1, 12), (1, 21), (2, 11), (2, 22)]);
        assert_eq!(merged.range(), 0..4);
        assert_eq!(first.as_slice(), &[(0, 90), (2, 11), (1, 12), (9, 91)]);
        assert_eq!(second.as_slice(), &[(0, 92), (1, 21), (2, 22), (9, 93)]);
    }

    #[rstest]
    fn test_merge_series_rejects_missing_variant_after_empty_legs() {
        let data: BatchView<u64> = vec![1, 2].into();
        let empty: BatchView<u64> = Vec::new().into();

        let merged = merge_series(data, [Some(empty), None].into_iter(), |row| *row);

        assert!(merged.is_none());
    }
}
