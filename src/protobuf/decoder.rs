//! The prost merge adapter that keeps closed-enum and nested-message
//! semantics while decoding into a [`DynamicMessage`].

use prost::bytes::{Buf, BufMut};
use prost::encoding::{self, DecodeContext, WireType};
use prost::{DecodeError, Message};
use prost_reflect::{
    DynamicMessage, EnumDescriptor, ExtensionDescriptor, FieldDescriptor, Kind, MessageDescriptor,
    ReflectMessage, Syntax, Value,
};

// prost-reflect treats every enum as open and does not check required fields.
// Intercept enums before mutation, and route nested merges through this adapter
// so an unknown closed-enum value cannot replace a known value or select a oneof.
// Framing, scalar decoding, group matching and recursion limits remain prost's.
#[derive(Debug)]
pub(super) struct ProjectionDecoder<'a> {
    message: &'a mut DynamicMessage,
    unknown_closed_enum: bool,
    // Retain the innermost known field once, rather than copying a growing
    // diagnostic through each recursive merge. Never retain input values.
    error_field: &'a mut Option<Field>,
}

impl<'a> ProjectionDecoder<'a> {
    pub(super) fn new(message: &'a mut DynamicMessage, error_field: &'a mut Option<Field>) -> Self {
        Self {
            message,
            unknown_closed_enum: false,
            error_field,
        }
    }
}

#[derive(Debug)]
pub(super) enum Field {
    Declared(FieldDescriptor),
    Extension(ExtensionDescriptor),
}

impl Field {
    pub(super) fn full_name(&self) -> &str {
        match self {
            Self::Declared(f) => f.full_name(),
            Self::Extension(f) => f.full_name(),
        }
    }

    fn kind(&self) -> Kind {
        match self {
            Self::Declared(f) => f.kind(),
            Self::Extension(f) => f.kind(),
        }
    }

    fn is_list(&self) -> bool {
        match self {
            Self::Declared(f) => f.is_list(),
            Self::Extension(f) => f.is_list(),
        }
    }

    fn is_map(&self) -> bool {
        match self {
            Self::Declared(f) => f.is_map(),
            Self::Extension(f) => f.is_map(),
        }
    }

    fn is_group(&self) -> bool {
        match self {
            Self::Declared(f) => f.is_group(),
            Self::Extension(f) => f.is_group(),
        }
    }

    fn value_mut<'a>(&self, message: &'a mut DynamicMessage) -> &'a mut Value {
        match self {
            Self::Declared(f) => message.get_field_mut(f),
            Self::Extension(f) => message.get_extension_mut(f),
        }
    }
}

impl Message for ProjectionDecoder<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        self.message.encode_raw(buf);
    }

    fn encoded_len(&self) -> usize {
        self.message.encoded_len()
    }

    fn clear(&mut self) {
        self.message.clear();
        self.unknown_closed_enum = false;
        *self.error_field = None;
    }

    fn merge_field(
        &mut self,
        number: u32,
        wire: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        let descriptor = self.message.descriptor();
        let field = descriptor
            .get_field(number)
            .map(Field::Declared)
            .or_else(|| descriptor.get_extension(number).map(Field::Extension));
        let Some(field) = field else {
            return self.message.merge_field(number, wire, buf, ctx);
        };
        let kind = field.kind();
        let expected_wire = if field.is_group() {
            WireType::StartGroup
        } else {
            kind.wire_type()
        };
        let packed = field.is_list()
            && wire == WireType::LengthDelimited
            && matches!(
                expected_wire,
                WireType::Varint | WireType::ThirtyTwoBit | WireType::SixtyFourBit
            );
        if wire != expected_wire && !packed {
            // A known number with an incompatible wire type is still unknown
            // data. Skip before selecting a oneof or establishing presence.
            // The source archive retains the original bytes independently.
            return encoding::skip_field(wire, number, buf, ctx);
        }
        let result = match kind {
            Kind::Enum(enumeration) if enumeration.parent_file().syntax() == Syntax::Proto2 => {
                self.merge_closed_enum(&field, &enumeration, wire, buf, ctx)
            }
            Kind::Message(entry) if field.is_map() => {
                self.merge_map_entry(&field, &entry, wire, buf, ctx)
            }
            Kind::Message(child) => self.merge_message(&field, child, number, wire, buf, ctx),
            _ => self.message.merge_field(number, wire, buf, ctx),
        };
        if result.is_err() && self.error_field.is_none() {
            *self.error_field = Some(field);
        }
        result
    }
}

impl ProjectionDecoder<'_> {
    /// Merges a proto2 enum field, keeping known values and recording an
    /// unknown one without mutating the message.
    fn merge_closed_enum(
        &mut self,
        field: &Field,
        enumeration: &EnumDescriptor,
        wire: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        let mut values = Vec::new();
        if field.is_list() {
            encoding::int32::merge_repeated(wire, &mut values, buf, ctx)?;
        } else {
            let mut value = 0;
            encoding::int32::merge(wire, &mut value, buf, ctx)?;
            values.push(value);
        }
        for value in values {
            if enumeration.get_value(value).is_none() {
                self.unknown_closed_enum = true;
                continue;
            }
            let target = field.value_mut(self.message);
            if field.is_list() {
                target
                    .as_list_mut()
                    .expect("enum list")
                    .push(Value::EnumNumber(value));
            } else {
                *target = Value::EnumNumber(value);
            }
        }
        Ok(())
    }

    fn merge_map_entry(
        &mut self,
        field: &Field,
        child_descriptor: &MessageDescriptor,
        wire: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        let mut entry = DynamicMessage::new(child_descriptor.clone());
        let mut decoder = ProjectionDecoder::new(&mut entry, self.error_field);
        encoding::message::merge(wire, &mut decoder, buf, ctx)?;
        // Closed enum map values move the entire entry to unknown
        // fields, leaving any prior entry for this key unchanged.
        if decoder.unknown_closed_enum {
            return Ok(());
        }
        let key = entry
            .get_field(&child_descriptor.map_entry_key_field())
            .into_owned()
            .into_map_key()
            .expect("validated map key");
        let value = entry
            .get_field(&child_descriptor.map_entry_value_field())
            .into_owned();
        field
            .value_mut(self.message)
            .as_map_mut()
            .expect("map field")
            .insert(key, value);
        Ok(())
    }

    fn merge_message(
        &mut self,
        field: &Field,
        child_descriptor: MessageDescriptor,
        number: u32,
        wire: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        if field.is_list() {
            let mut child = DynamicMessage::new(child_descriptor);
            merge_child(
                number,
                wire,
                field.is_group(),
                &mut child,
                self.error_field,
                buf,
                ctx,
            )?;
            field
                .value_mut(self.message)
                .as_list_mut()
                .expect("message list")
                .push(Value::Message(child));
        } else {
            let child = field
                .value_mut(self.message)
                .as_message_mut()
                .expect("message field");
            merge_child(
                number,
                wire,
                field.is_group(),
                child,
                self.error_field,
                buf,
                ctx,
            )?;
        }
        Ok(())
    }
}

fn merge_child(
    number: u32,
    wire: WireType,
    group: bool,
    child: &mut DynamicMessage,
    error_field: &mut Option<Field>,
    buf: &mut impl Buf,
    ctx: DecodeContext,
) -> Result<(), DecodeError> {
    let mut decoder = ProjectionDecoder::new(child, error_field);
    if group {
        encoding::group::merge(number, wire, &mut decoder, buf, ctx)
    } else {
        encoding::message::merge(wire, &mut decoder, buf, ctx)
    }
}
