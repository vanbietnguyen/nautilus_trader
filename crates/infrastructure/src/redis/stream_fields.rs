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

//! The fields of one external message bus stream entry.
//!
//! Every entry carries `topic`, `type`, `payload` and `encoding`, and `payload_kind` = `typed` for a
//! typed message ([`BusPayloadType::is_typed_message`]). An entry whose message has a source
//! carries three more: `publisher_id` (the publishing trader ID), `instance_id` (the instance of
//! that publisher's program start) and `sequence` (the entry's number, counted from 1 per stream
//! key, publisher and instance). An entry without them has no source, as older writers wrote none.
//! The stream key itself is not a field: a reader takes it from the stream it reads.
//!
//! [`BusPayloadType::is_typed_message`]: nautilus_common::msgbus::BusPayloadType::is_typed_message

use nautilus_common::msgbus::{BusMessage, BusMessageSource};
use redis::{RedisWrite, ToRedisArgs};

/// The field marking the payload type of a typed message.
pub const PAYLOAD_KIND_FIELD: &str = "payload_kind";
/// The value of [`PAYLOAD_KIND_FIELD`] for a typed message.
pub const PAYLOAD_KIND_TYPED: &str = "typed";
/// The field carrying the trader ID of an entry's publisher.
pub const PUBLISHER_ID_FIELD: &str = "publisher_id";
/// The field carrying the instance ID of an entry's publisher.
pub const INSTANCE_ID_FIELD: &str = "instance_id";
/// The field carrying an entry's number.
pub const SEQUENCE_FIELD: &str = "sequence";
const FIELD_COUNT_BASE: usize = 4;
const FIELD_COUNT_TYPED: usize = 5;

type BusMessageField<'a> = (&'static str, &'a [u8]);

/// The fields of the stream entry of one [`BusMessage`].
///
/// As [`ToRedisArgs`] it writes every field of the entry as field and value pairs: those of
/// [`Self::as_slice`], then the three of the message's source when it has one.
#[derive(Debug)]
pub struct BusMessageFields<'a> {
    fields: [BusMessageField<'a>; FIELD_COUNT_TYPED],
    len: usize,
    source: Option<&'a BusMessageSource>,
}

impl<'a> BusMessageFields<'a> {
    /// Returns the fields of the message itself: four, or five for a typed message, without the
    /// fields of its source.
    #[must_use]
    pub fn as_slice(&self) -> &[BusMessageField<'a>] {
        &self.fields[..self.len]
    }
}

impl ToRedisArgs for BusMessageFields<'_> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        for (field, value) in self.as_slice() {
            out.write_arg(field.as_bytes());
            out.write_arg(value);
        }

        if let Some(source) = self.source {
            out.write_arg(PUBLISHER_ID_FIELD.as_bytes());
            out.write_arg(source.publisher_id.as_str().as_bytes());
            out.write_arg(INSTANCE_ID_FIELD.as_bytes());
            out.write_arg(source.instance_id.as_str().as_bytes());
            out.write_arg(SEQUENCE_FIELD.as_bytes());
            source.sequence.write_redis_args(out);
        }
    }
}

/// Returns the fields of the stream entry of `message`, its payload encoded as `encoding`.
#[must_use]
pub fn bus_message_fields<'a>(message: &'a BusMessage, encoding: &'a str) -> BusMessageFields<'a> {
    let len = if message.payload_type.is_typed_message() {
        FIELD_COUNT_TYPED
    } else {
        FIELD_COUNT_BASE
    };

    BusMessageFields {
        fields: [
            ("topic", message.topic.as_ref()),
            ("type", message.payload_type.as_str().as_bytes()),
            ("payload", message.payload.as_ref()),
            ("encoding", encoding.as_bytes()),
            (PAYLOAD_KIND_FIELD, PAYLOAD_KIND_TYPED.as_bytes()),
        ],
        len,
        source: message.source.as_ref(),
    }
}
