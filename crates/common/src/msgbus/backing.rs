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

//! External message bus backing traits.
//!
//! Ingress and egress are named from the local message bus boundary. Egress carries serialized
//! [`BusMessage`] values from the local bus to external streams. Ingress exposes serialized
//! [`BusMessage`] values read from external streams so a live bridge can republish them on the
//! local bus.
//!
//! - `MessageBusBackingFactory` creates concrete backing technology for a bus runtime.
//! - `MessageBusBacking` owns the runtime facade used by core bus wiring.
//! - `MessageBusExternalEgress` accepts outbound messages from the local bus. Its `reserve_source`
//!   asks the backing for the source of the next entry it writes for a topic, so the bus numbers a
//!   publication before it encodes it.
//! - `MessageBusExternalIngress` exposes the inbound external stream receiver.
//! - `external_egress_from_backing` adapts a backing into an egress surface.
//! - `external_io_from_backing` adapts one backing into shared egress and ingress surfaces.

use std::{
    cell::{Cell, RefCell},
    fmt::Debug,
    rc::Rc,
};

use futures::future::LocalBoxFuture;
use nautilus_core::UUID4;
use nautilus_model::identifiers::TraderId;
use ustr::Ustr;

use super::config::MessageBusConfig;
use crate::msgbus::{BusMessage, BusMessageSource};

/// Receiver for external message bus ingress publications.
#[cfg(feature = "live")]
pub type MessageBusExternalReceiver = tokio::sync::mpsc::Receiver<BusMessage>;

/// Factory for constructing external message bus backings.
///
/// Implementations own concrete backing configuration and return the [`MessageBusBacking`] surface
/// used by the core bus runtime.
pub trait MessageBusBackingFactory: Debug + Send + Sync {
    /// Creates a message bus backing for the given bus runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if backing construction or connection setup fails.
    fn create(
        &self,
        trader_id: TraderId,
        instance_id: UUID4,
        config: MessageBusConfig,
    ) -> anyhow::Result<Box<dyn MessageBusBacking>>;
}

/// External message bus backing facade.
///
/// Implementations own the concrete backing technology and provide the runtime-facing publication
/// surface used by the core bus. With the `live` feature, the same backing can also hand an
/// inbound receiver to the live bridge.
pub trait MessageBusBacking {
    /// Returns `true` if the backing has been closed.
    fn is_closed(&self) -> bool;

    /// Reserves the source of the next entry this backing writes for `topic`.
    ///
    /// The bus calls this once for each publication it forwards, before it encodes the message,
    /// and sets the returned source on the message it then publishes. Only the backing knows the
    /// stream an entry of `topic` is written on, so it gives the number. A publication that is then
    /// not published keeps its number unused.
    ///
    /// The default returns `None`: a backing that does not number its entries publishes them
    /// without a source, as every backing did before this call existed.
    ///
    /// # Errors
    ///
    /// Returns an error if the backing cannot give another number for the stream of `topic`.
    fn reserve_source(&self, topic: Ustr) -> anyhow::Result<Option<BusMessageSource>> {
        let _ = topic;
        Ok(None)
    }

    /// Queues a serialized bus message for external egress.
    fn publish(&self, message: BusMessage);

    /// Takes the inbound message receiver for live bridge consumption.
    ///
    /// # Errors
    ///
    /// Returns an error if the receiver has already been taken or is unavailable.
    #[cfg(feature = "live")]
    fn take_receiver(&mut self) -> anyhow::Result<MessageBusExternalReceiver> {
        anyhow::bail!("external ingress receiver unavailable")
    }

    /// Stops the inbound read and nothing else: publications are still written until
    /// [`Self::close`].
    ///
    /// The default does nothing: a backing with no read of its own has nothing to stop.
    #[cfg(feature = "live")]
    fn stop_read(&mut self) {}

    /// Closes the backing and releases any owned resources.
    fn close(&mut self);

    /// Begins closing the backing and returns a future that completes when the close has ended.
    ///
    /// The future owns everything it waits for and borrows nothing of the backing, so the caller
    /// can await it without blocking its thread or holding the backing.
    ///
    /// The default calls [`Self::close`] and returns a future that is already complete.
    fn begin_close(&mut self) -> LocalBoxFuture<'static, ()> {
        self.close();
        Box::pin(std::future::ready(()))
    }
}

/// External egress surface for serialized message bus publications.
///
/// The core bus passes each outbound message as a [`BusMessage`] carrying the
/// `topic`, `payload_type`, and serialized `payload`. Implementations must not block the publishing
/// thread. If the underlying channel is full, drop the message in the implementation rather than
/// applying back-pressure to the node.
pub trait MessageBusExternalEgress {
    /// Returns `true` if egress has been closed.
    fn is_closed(&self) -> bool;

    /// Reserves the source of the next entry this egress writes for `topic`.
    ///
    /// The bus calls this once for each publication it forwards, before it encodes the message,
    /// and sets the returned source on the message it then publishes. Only the backing behind the
    /// egress knows the stream an entry of `topic` is written on, so it gives the number. A
    /// publication that is then not published keeps its number unused.
    ///
    /// The default returns `None`: an egress that does not number its entries publishes them
    /// without a source, as every egress did before this call existed.
    ///
    /// # Errors
    ///
    /// Returns an error if the egress cannot give another number for the stream of `topic`.
    fn reserve_source(&self, topic: Ustr) -> anyhow::Result<Option<BusMessageSource>> {
        let _ = topic;
        Ok(None)
    }

    /// Queues a serialized bus message for external egress.
    fn publish(&self, message: BusMessage);

    /// Closes egress and stops accepting outbound messages.
    fn close(&mut self);

    /// Begins closing egress and returns a future that completes when the close has ended.
    ///
    /// The future owns everything it waits for and borrows nothing of the egress.
    ///
    /// The default calls [`Self::close`] and returns a future that is already complete.
    fn begin_close(&mut self) -> LocalBoxFuture<'static, ()> {
        self.close();
        Box::pin(std::future::ready(()))
    }
}

/// External ingress surface for serialized message bus publications.
///
/// The live bridge consumes each inbound [`BusMessage`] as a topic and serialized
/// payload. The receiver can be taken only once so ingress can hand ownership of the external stream
/// to the bridge without exposing concrete backing details.
#[cfg(feature = "live")]
pub trait MessageBusExternalIngress {
    /// Returns `true` if ingress has been closed.
    fn is_closed(&self) -> bool;

    /// Takes the inbound message receiver for live bridge consumption.
    ///
    /// # Errors
    ///
    /// Returns an error if the receiver has already been taken or is unavailable.
    fn take_receiver(&mut self) -> anyhow::Result<MessageBusExternalReceiver>;

    /// Closes ingress and stops accepting inbound messages.
    fn close(&mut self);
}

type SharedMessageBusBacking = Rc<RefCell<Box<dyn MessageBusBacking>>>;
type SharedMessageBusCloseState = Rc<Cell<bool>>;

/// Wraps a message bus backing for external egress installation.
#[must_use]
pub fn external_egress_from_backing(
    backing: Box<dyn MessageBusBacking>,
) -> Box<dyn MessageBusExternalEgress> {
    Box::new(BackingExternalEgress {
        backing: Rc::new(RefCell::new(backing)),
        closed: Rc::new(Cell::new(false)),
    })
}

/// Splits a message bus backing into external egress and ingress surfaces.
///
/// Closing the ingress stops only the backing's read ([`MessageBusBacking::stop_read`]); closing
/// the egress closes the backing.
#[cfg(feature = "live")]
#[must_use]
pub fn external_io_from_backing(
    backing: Box<dyn MessageBusBacking>,
) -> (
    Box<dyn MessageBusExternalEgress>,
    Box<dyn MessageBusExternalIngress>,
) {
    let backing = Rc::new(RefCell::new(backing));
    let closed = Rc::new(Cell::new(false));
    (
        Box::new(BackingExternalEgress {
            backing: backing.clone(),
            closed: closed.clone(),
        }),
        Box::new(BackingExternalIngress {
            backing,
            read_stopped: Cell::new(false),
            egress_closed: closed,
        }),
    )
}

struct BackingExternalEgress {
    backing: SharedMessageBusBacking,
    closed: SharedMessageBusCloseState,
}

impl MessageBusExternalEgress for BackingExternalEgress {
    fn is_closed(&self) -> bool {
        self.backing.borrow().is_closed()
    }

    fn reserve_source(&self, topic: Ustr) -> anyhow::Result<Option<BusMessageSource>> {
        self.backing.borrow().reserve_source(topic)
    }

    fn publish(&self, message: BusMessage) {
        self.backing.borrow().publish(message);
    }

    fn close(&mut self) {
        if !self.closed.replace(true) {
            self.backing.borrow_mut().close();
        }
    }

    fn begin_close(&mut self) -> LocalBoxFuture<'static, ()> {
        if self.closed.replace(true) {
            return Box::pin(std::future::ready(()));
        }
        self.backing.borrow_mut().begin_close()
    }
}

#[cfg(feature = "live")]
struct BackingExternalIngress {
    backing: SharedMessageBusBacking,
    read_stopped: Cell<bool>,
    egress_closed: SharedMessageBusCloseState,
}

#[cfg(feature = "live")]
impl MessageBusExternalIngress for BackingExternalIngress {
    // Closed when its read was stopped or the egress closed the backing. A backing that reports
    // itself closed for another reason (its publishing ended) still has a read to stop
    fn is_closed(&self) -> bool {
        self.read_stopped.get() || self.egress_closed.get()
    }

    fn take_receiver(&mut self) -> anyhow::Result<MessageBusExternalReceiver> {
        self.backing.borrow_mut().take_receiver()
    }

    fn close(&mut self) {
        if !self.read_stopped.replace(true) {
            self.backing.borrow_mut().stop_read();
        }
    }
}

#[cfg(all(test, feature = "live"))]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };

    use bytes::Bytes;
    use rstest::*;

    use super::{MessageBusBacking, external_egress_from_backing, external_io_from_backing};
    use crate::{
        enums::SerializationEncoding,
        msgbus::{
            BusMessage, BusPayloadType,
            MessageBusExternalIngress as ReexportedMessageBusExternalIngress,
        },
    };

    struct CapturingBacking {
        rx: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
        closed: bool,
    }

    impl MessageBusBacking for CapturingBacking {
        fn is_closed(&self) -> bool {
            self.closed
        }

        fn publish(&self, _message: BusMessage) {}

        fn take_receiver(&mut self) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
            self.rx
                .take()
                .ok_or_else(|| anyhow::anyhow!("Stream receiver already taken"))
        }

        fn close(&mut self) {
            self.closed = true;
        }
    }

    struct CapturingPublishBacking {
        publications: Rc<RefCell<Vec<BusMessage>>>,
        closed: Rc<Cell<bool>>,
        close_count: Rc<Cell<u32>>,
    }

    impl MessageBusBacking for CapturingPublishBacking {
        fn is_closed(&self) -> bool {
            self.closed.get()
        }

        fn publish(&self, message: BusMessage) {
            self.publications.borrow_mut().push(message);
        }

        fn close(&mut self) {
            self.close_count.set(self.close_count.get() + 1);
            self.closed.set(true);
        }
    }

    struct CapturingExternalIngress {
        rx: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
        closed: bool,
    }

    impl ReexportedMessageBusExternalIngress for CapturingExternalIngress {
        fn is_closed(&self) -> bool {
            self.closed
        }

        fn take_receiver(&mut self) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
            self.rx
                .take()
                .ok_or_else(|| anyhow::anyhow!("Stream receiver already taken"))
        }

        fn close(&mut self) {
            self.closed = true;
        }
    }

    #[rstest]
    fn test_message_bus_external_ingress_reexport_accepts_bus_messages() {
        let (tx, rx) = tokio::sync::mpsc::channel::<BusMessage>(1);
        let mut ingress = CapturingExternalIngress {
            rx: Some(rx),
            closed: false,
        };
        let message = BusMessage::with_str_topic(
            "events/data",
            BusPayloadType::QuoteTick,
            Bytes::from_static(b"payload"),
            SerializationEncoding::Json,
        );

        tx.try_send(message.clone()).unwrap();
        let mut stream_rx =
            ReexportedMessageBusExternalIngress::take_receiver(&mut ingress).unwrap();
        let received = stream_rx.try_recv().unwrap();

        assert_eq!(received.topic, message.topic);
        assert_eq!(received.payload, message.payload);
        assert!(ReexportedMessageBusExternalIngress::take_receiver(&mut ingress).is_err());

        ReexportedMessageBusExternalIngress::close(&mut ingress);
        assert!(ReexportedMessageBusExternalIngress::is_closed(&ingress));
    }

    #[rstest]
    fn test_external_io_from_backing_shares_close_state() {
        let (tx, rx) = tokio::sync::mpsc::channel::<BusMessage>(1);
        let backing = CapturingBacking {
            rx: Some(rx),
            closed: false,
        };
        let message = BusMessage::with_str_topic(
            "events/data",
            BusPayloadType::QuoteTick,
            Bytes::from_static(b"payload"),
            SerializationEncoding::Json,
        );
        let (mut egress, mut ingress) = external_io_from_backing(Box::new(backing));

        tx.try_send(message.clone()).unwrap();
        let mut stream_rx = ingress.take_receiver().unwrap();
        let received = stream_rx.try_recv().unwrap();

        assert_eq!(received.topic, message.topic);
        assert!(!egress.is_closed());
        assert!(!ingress.is_closed());

        egress.close();

        assert!(egress.is_closed());
        assert!(ingress.is_closed());
    }

    #[rstest]
    fn test_external_egress_from_backing_forwards_publications() {
        let publications = Rc::new(RefCell::new(Vec::new()));
        let closed = Rc::new(Cell::new(false));
        let close_count = Rc::new(Cell::new(0));
        let backing = CapturingPublishBacking {
            publications: publications.clone(),
            closed: closed.clone(),
            close_count,
        };
        let mut egress = external_egress_from_backing(Box::new(backing));
        let message = BusMessage::with_str_topic(
            "events/data",
            BusPayloadType::QuoteTick,
            Bytes::from_static(b"payload"),
            SerializationEncoding::Json,
        );

        egress.publish(message.clone());
        egress.close();

        let publications = publications.borrow();
        assert_eq!(publications.len(), 1);
        assert_eq!(publications[0].topic, message.topic);
        assert!(closed.get());
    }

    #[rstest]
    fn test_external_io_from_backing_closes_shared_backing_once() {
        let publications = Rc::new(RefCell::new(Vec::new()));
        let closed = Rc::new(Cell::new(false));
        let close_count = Rc::new(Cell::new(0));
        let backing = CapturingPublishBacking {
            publications,
            closed,
            close_count: close_count.clone(),
        };
        let (mut egress, mut ingress) = external_io_from_backing(Box::new(backing));

        egress.close();
        ingress.close();

        assert_eq!(close_count.get(), 1);
    }

    #[rstest]
    fn test_external_io_from_backing_close_does_not_depend_on_backing_is_closed() {
        let publications = Rc::new(RefCell::new(Vec::new()));
        let closed = Rc::new(Cell::new(true));
        let close_count = Rc::new(Cell::new(0));
        let backing = CapturingPublishBacking {
            publications,
            closed,
            close_count: close_count.clone(),
        };
        let (mut egress, mut ingress) = external_io_from_backing(Box::new(backing));

        egress.close();
        ingress.close();

        assert_eq!(close_count.get(), 1);
    }

    #[rstest]
    fn test_external_io_from_backing_default_receiver_is_unavailable() {
        let publications = Rc::new(RefCell::new(Vec::new()));
        let closed = Rc::new(Cell::new(false));
        let close_count = Rc::new(Cell::new(0));
        let backing = CapturingPublishBacking {
            publications,
            closed,
            close_count,
        };
        let (_egress, mut ingress) = external_io_from_backing(Box::new(backing));

        let error = ingress
            .take_receiver()
            .expect_err("egress-only backing should not provide ingress receiver");

        assert_eq!(error.to_string(), "external ingress receiver unavailable");
    }
}
