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
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
//  the specific language governing permissions and limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
};

use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use nautilus_core::UnixNanos;
use nautilus_model::data::HasTsInit;
use nautilus_serialization::arrow::DecodeTypedFromRecordBatch;

pub(super) type TypedPages<T> = Box<dyn Iterator<Item = anyhow::Result<Vec<T>>> + Send>;

pub(super) type OpenPages<T> = Box<dyn FnOnce() -> anyhow::Result<TypedPages<T>> + Send>;

/// A page source that is opened only when the merge reaches its first possible row.
pub(super) struct PendingPages<T> {
    label: String,
    start: Option<UnixNanos>,
    open: OpenPages<T>,
}

impl<T> PendingPages<T> {
    /// Creates a source whose rows all have `ts_init >= start`.
    ///
    /// A source without a `start` opens before the first row is emitted.
    pub(super) fn new(label: String, start: Option<UnixNanos>, open: OpenPages<T>) -> Self {
        Self { label, start, open }
    }
}

struct ActivePages<T> {
    pages: TypedPages<T>,
    buffer: VecDeque<T>,
}

/// Merges ordered page sources by `ts_init`, breaking ties by source position.
///
/// A source opens once the merge frontier reaches its declared start and is dropped as soon as it
/// is exhausted, so only sources whose ranges cover the frontier hold resources.
pub(super) struct MergedPages<T> {
    pending: VecDeque<(usize, PendingPages<T>)>,
    active: Vec<Option<ActivePages<T>>>,
    heads: BinaryHeap<Reverse<(UnixNanos, usize)>>,
    page_size: usize,
    finished: bool,
}

impl<T: HasTsInit> MergedPages<T> {
    pub(super) fn new(sources: Vec<PendingPages<T>>, page_size: usize) -> Self {
        let mut pending: Vec<_> = sources.into_iter().enumerate().collect();
        pending.sort_by_key(|(index, source)| (source.start, *index));

        Self {
            active: (0..pending.len()).map(|_| None).collect(),
            pending: pending.into(),
            heads: BinaryHeap::new(),
            page_size: page_size.max(1),
            finished: false,
        }
    }

    fn read_page(&mut self) -> anyhow::Result<Option<Vec<T>>> {
        let mut page = Vec::with_capacity(self.page_size);
        while page.len() < self.page_size {
            self.open_due()?;

            let Some(Reverse((_, index))) = self.heads.pop() else {
                break;
            };

            let source = self.active[index]
                .as_mut()
                .expect("queued source is active");
            page.push(source.buffer.pop_front().expect("queued source has a row"));
            self.advance(index)?;
        }

        Ok((!page.is_empty()).then_some(page))
    }

    // Opens every pending source whose start is at or before the earliest queued row, so no
    // unopened source can hold a row that precedes or ties the next one emitted
    fn open_due(&mut self) -> anyhow::Result<()> {
        while let Some((_, source)) = self.pending.front() {
            let due = match self.heads.peek() {
                Some(Reverse((head, _))) => source.start.is_none_or(|start| start <= *head),
                None => true,
            };

            if !due {
                return Ok(());
            }

            let (index, source) = self.pending.pop_front().expect("pending source exists");
            self.active[index] = Some(ActivePages {
                pages: (source.open)()?,
                buffer: VecDeque::new(),
            });
            self.advance(index)?;

            if let (Some(start), Some(first)) = (source.start, self.first_row_ts(index)) {
                anyhow::ensure!(
                    first >= start,
                    "Catalog source {} returned ts_init {first} before its declared start {start}",
                    source.label,
                );
            }
        }

        Ok(())
    }

    fn first_row_ts(&self, index: usize) -> Option<UnixNanos> {
        self.active[index]
            .as_ref()
            .and_then(|source| source.buffer.front())
            .map(HasTsInit::ts_init)
    }

    fn advance(&mut self, index: usize) -> anyhow::Result<()> {
        let source = self.active[index]
            .as_mut()
            .expect("advanced source is active");

        while source.buffer.is_empty() {
            let Some(page) = source.pages.next() else {
                // Releases the buffer allocation and the source's resources
                self.active[index] = None;
                return Ok(());
            };
            source.buffer.extend(page?);
        }

        let row = source.buffer.front().expect("source has a row");
        self.heads.push(Reverse((row.ts_init(), index)));
        Ok(())
    }

    fn release(&mut self) {
        self.pending.clear();
        self.active.iter_mut().for_each(|source| *source = None);
        self.heads.clear();
    }
}

impl<T: HasTsInit> Iterator for MergedPages<T> {
    type Item = anyhow::Result<Vec<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match self.read_page() {
            Ok(Some(page)) => Some(Ok(page)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(e) => {
                self.finished = true;
                self.release();
                Some(Err(e))
            }
        }
    }
}

pub(super) fn decode_typed_pages<T>(
    stream: SendableRecordBatchStream,
) -> impl Stream<Item = anyhow::Result<Vec<T>>>
where
    T: DecodeTypedFromRecordBatch,
{
    let metadata = stream.schema().metadata().clone();
    futures::stream::try_unfold((stream, metadata), |(mut stream, metadata)| async move {
        let Some(batch) = stream.next().await else {
            return Ok(None);
        };

        let rows = T::decode_typed_batch(&metadata, batch?)?;
        Ok(Some((rows, (stream, metadata))))
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
    };

    use datafusion::{error::DataFusionError, physical_plan::stream::RecordBatchStreamAdapter};
    use nautilus_model::data::{QuoteTick, stubs::quote_audusd};
    use nautilus_serialization::arrow::{EncodeToRecordBatch, EncodingError};
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::stream_error(false)]
    #[case::decode_error(true)]
    fn typed_pages_stop_polling_after_error(#[case] decode_error: bool) {
        let quote = quote_audusd();
        let batch = QuoteTick::encode_batch(&quote.metadata(), &[quote]).unwrap();
        let schema = batch.schema();

        let failure = if decode_error {
            Ok(batch.project(&[0]).unwrap())
        } else {
            Err(DataFusionError::Execution(
                "injected stream failure".to_string(),
            ))
        };

        let mut batches = vec![Ok(batch.clone()), failure, Ok(batch)].into_iter();
        let polls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&polls);

        let inner = futures::stream::poll_fn(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(batches.next())
        });

        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, inner));
        let decoded = decode_typed_pages::<QuoteTick>(stream);
        let mut items: Vec<_> = futures::executor::block_on_stream(Box::pin(decoded)).collect();

        assert_eq!(items.len(), 2);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(items.remove(0).unwrap(), vec![quote]);
        let error = items.remove(0).unwrap_err();

        if decode_error {
            assert!(matches!(
                error.downcast_ref::<EncodingError>(),
                Some(EncodingError::MissingColumn("ask_price", 1))
            ));
        } else {
            assert!(matches!(error.downcast_ref::<DataFusionError>(),
                Some(DataFusionError::Execution(message)) if message == "injected stream failure"));
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Row {
        ts: u64,
        source: usize,
        seq: usize,
    }

    impl HasTsInit for Row {
        fn ts_init(&self) -> UnixNanos {
            UnixNanos::from(self.ts)
        }
    }

    #[derive(Default)]
    struct Counter {
        opened: AtomicUsize,
        live: AtomicUsize,
        max_live: AtomicUsize,
    }

    struct CountedPages {
        pages: std::vec::IntoIter<anyhow::Result<Vec<Row>>>,
        counter: Arc<Counter>,
    }

    impl Iterator for CountedPages {
        type Item = anyhow::Result<Vec<Row>>;

        fn next(&mut self) -> Option<Self::Item> {
            self.pages.next()
        }
    }

    impl Drop for CountedPages {
        fn drop(&mut self) {
            self.counter.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct Source {
        start: Option<u64>,
        pages: Vec<anyhow::Result<Vec<Row>>>,
        open_error: Option<&'static str>,
    }

    impl Source {
        fn new(source: usize, start: Option<u64>, pages: &[&[u64]]) -> Self {
            let mut seq = 0;
            let pages = pages
                .iter()
                .map(|page| {
                    Ok(page
                        .iter()
                        .map(|&ts| {
                            seq += 1;
                            Row { ts, source, seq }
                        })
                        .collect())
                })
                .collect();

            Self {
                start,
                pages,
                open_error: None,
            }
        }

        fn failing_page(mut self, message: &'static str) -> Self {
            self.pages.push(Err(anyhow::anyhow!(message)));
            self
        }

        fn failing_open(mut self, message: &'static str) -> Self {
            self.open_error = Some(message);
            self
        }
    }

    fn merged(sources: Vec<Source>, page_size: usize, counter: &Arc<Counter>) -> MergedPages<Row> {
        let pending = sources
            .into_iter()
            .enumerate()
            .map(|(index, source)| {
                let counter = Arc::clone(counter);
                let open = move || {
                    if let Some(message) = source.open_error {
                        anyhow::bail!(message);
                    }

                    counter.opened.fetch_add(1, Ordering::SeqCst);
                    let live = counter.live.fetch_add(1, Ordering::SeqCst) + 1;
                    counter.max_live.fetch_max(live, Ordering::SeqCst);
                    Ok(Box::new(CountedPages {
                        pages: source.pages.into_iter(),
                        counter,
                    }) as TypedPages<Row>)
                };

                PendingPages::new(
                    format!("source-{index}"),
                    source.start.map(UnixNanos::from),
                    Box::new(open),
                )
            })
            .collect();

        MergedPages::new(pending, page_size)
    }

    fn drain(pages: MergedPages<Row>) -> Vec<Vec<Row>> {
        pages.map(Result::unwrap).collect()
    }

    #[rstest]
    fn merged_pages_opens_disjoint_sources_one_at_a_time() {
        let counter = Arc::new(Counter::default());
        let sources = (0..50_usize)
            .map(|index| {
                let ts = 100 * u64::try_from(index).unwrap();
                Source::new(index, Some(ts), &[&[ts, ts + 1], &[ts + 2]])
            })
            .collect();

        let pages = merged(sources, 2, &counter);
        let rows: Vec<Row> = drain(pages).into_iter().flatten().collect();

        assert_eq!(rows.len(), 150);
        assert!(rows.windows(2).all(|pair| pair[0].ts < pair[1].ts));
        assert_eq!(counter.opened.load(Ordering::SeqCst), 50);
        assert_eq!(counter.max_live.load(Ordering::SeqCst), 1);
        assert_eq!(counter.live.load(Ordering::SeqCst), 0);
    }

    #[rstest]
    fn merged_pages_defers_sources_until_the_frontier_reaches_their_start() {
        let counter = Arc::new(Counter::default());
        let sources = vec![
            Source::new(0, Some(10), &[&[10, 20]]),
            Source::new(1, Some(30), &[&[30]]),
        ];
        let mut pages = merged(sources, 1, &counter);

        assert_eq!(pages.next().unwrap().unwrap()[0].ts, 10);
        assert_eq!(counter.opened.load(Ordering::SeqCst), 1);
        assert_eq!(pages.next().unwrap().unwrap()[0].ts, 20);
        assert_eq!(counter.opened.load(Ordering::SeqCst), 1);
        assert_eq!(pages.next().unwrap().unwrap()[0].ts, 30);
        assert_eq!(counter.opened.load(Ordering::SeqCst), 2);
        assert!(pages.next().is_none());
    }

    #[rstest]
    fn merged_pages_opens_every_source_starting_at_the_head_before_emitting_it() {
        let counter = Arc::new(Counter::default());
        let sources = vec![
            Source::new(0, Some(10), &[&[10, 20]]),
            Source::new(1, Some(20), &[&[20, 30]]),
            Source::new(2, Some(20), &[&[20]]),
            Source::new(3, Some(5), &[&[20]]),
        ];

        let rows: Vec<_> = drain(merged(sources, 10, &counter))
            .into_iter()
            .flatten()
            .map(|row| (row.ts, row.source))
            .collect();

        assert_eq!(
            rows,
            vec![(10, 0), (20, 0), (20, 1), (20, 2), (20, 3), (30, 1)]
        );
    }

    // A small deterministic generator keeps the comparison reproducible without a new dependency
    fn next_random(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    fn random_sources(seed: u64, declare_starts: bool) -> Vec<Source> {
        let mut state = seed;
        (0..12)
            .map(|index| {
                let start = next_random(&mut state) % 200;
                let len = usize::try_from(next_random(&mut state) % 6).unwrap();
                let mut ts = start;
                let mut rows = Vec::with_capacity(len);
                for _ in 0..len {
                    rows.push(ts);
                    ts += next_random(&mut state) % 3;
                }
                let split = rows.len() / 2;
                let declared = !next_random(&mut state).is_multiple_of(4);
                let slack = next_random(&mut state) % 3;
                let declared = (declare_starts && declared).then(|| start - start.min(slack));
                Source::new(index, declared, &[&rows[..split], &rows[split..]])
            })
            .collect()
    }

    #[rstest]
    fn merged_pages_matches_the_eager_merge_order(
        #[values(1, 2, 3, 7, 64)] page_size: usize,
        #[values(1, 2, 3, 4, 5, 6, 7, 8)] seed: u64,
    ) {
        let counter = Arc::new(Counter::default());

        let eager = drain(merged(random_sources(seed, false), page_size, &counter));
        let lazy = drain(merged(random_sources(seed, true), page_size, &counter));

        assert_eq!(lazy, eager);
        assert_eq!(counter.live.load(Ordering::SeqCst), 0);
    }

    #[rstest]
    fn merged_pages_drains_empty_sources_in_place() {
        let counter = Arc::new(Counter::default());
        let sources = vec![
            Source::new(0, Some(0), &[]),
            Source::new(1, Some(5), &[&[]]),
            Source::new(2, Some(10), &[&[10]]),
            Source::new(3, Some(15), &[]),
            Source::new(4, Some(20), &[&[20]]),
        ];

        let rows: Vec<_> = drain(merged(sources, 1, &counter))
            .into_iter()
            .flatten()
            .map(|row| row.ts)
            .collect();

        assert_eq!(rows, vec![10, 20]);
        assert_eq!(counter.opened.load(Ordering::SeqCst), 5);
        assert_eq!(counter.max_live.load(Ordering::SeqCst), 1);
        assert_eq!(counter.live.load(Ordering::SeqCst), 0);
    }

    #[rstest]
    fn merged_pages_rejects_a_first_row_before_the_declared_start() {
        let counter = Arc::new(Counter::default());
        let sources = vec![
            Source::new(0, Some(10), &[&[10, 30]]),
            Source::new(1, Some(20), &[&[15]]),
        ];
        let mut pages = merged(sources, 1, &counter);

        assert_eq!(pages.next().unwrap().unwrap()[0].ts, 10);
        let error = pages.next().unwrap().unwrap_err();

        assert_eq!(
            error.to_string(),
            "Catalog source source-1 returned ts_init 15 before its declared start 20"
        );
        assert!(pages.next().is_none());
        assert_eq!(counter.live.load(Ordering::SeqCst), 0);
    }

    #[rstest]
    fn merged_pages_surfaces_a_deferred_open_failure_on_the_pull_that_reaches_it() {
        let counter = Arc::new(Counter::default());
        let sources = vec![
            Source::new(0, Some(10), &[&[10, 20]]),
            Source::new(1, Some(30), &[&[30]]).failing_open("file is missing"),
            Source::new(2, Some(40), &[&[40]]),
        ];
        let mut pages = merged(sources, 1, &counter);

        assert_eq!(pages.next().unwrap().unwrap()[0].ts, 10);
        assert_eq!(pages.next().unwrap().unwrap()[0].ts, 20);
        assert_eq!(
            pages.next().unwrap().unwrap_err().to_string(),
            "file is missing"
        );
        assert!(pages.next().is_none());
        assert_eq!(counter.opened.load(Ordering::SeqCst), 1);
        assert_eq!(counter.live.load(Ordering::SeqCst), 0);
    }

    #[rstest]
    fn merged_pages_releases_every_active_source_after_a_page_failure() {
        let counter = Arc::new(Counter::default());
        let sources = vec![
            Source::new(0, Some(10), &[&[10], &[30]]),
            Source::new(1, Some(20), &[&[20]]).failing_page("page failed"),
        ];
        let mut pages = merged(sources, 1, &counter);

        assert_eq!(pages.next().unwrap().unwrap()[0].ts, 10);
        assert_eq!(
            pages.next().unwrap().unwrap_err().to_string(),
            "page failed"
        );

        assert_eq!(counter.live.load(Ordering::SeqCst), 0);
        assert!(pages.next().is_none());
    }

    #[rstest]
    fn merged_pages_drop_releases_open_sources_and_never_opens_pending_ones() {
        let counter = Arc::new(Counter::default());
        let sources = vec![
            Source::new(0, Some(10), &[&[10, 20]]),
            Source::new(1, Some(15), &[&[15, 25]]),
            Source::new(2, Some(100), &[&[100]]),
        ];
        let mut pages = merged(sources, 1, &counter);

        assert_eq!(pages.next().unwrap().unwrap()[0].ts, 10);
        assert_eq!(counter.live.load(Ordering::SeqCst), 1);
        assert_eq!(pages.next().unwrap().unwrap()[0].ts, 15);
        assert_eq!(counter.live.load(Ordering::SeqCst), 2);
        drop(pages);

        assert_eq!(counter.live.load(Ordering::SeqCst), 0);
        assert_eq!(counter.opened.load(Ordering::SeqCst), 2);
    }
}
