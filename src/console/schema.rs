//! The descriptor pool, the exposed-method table, and the method schemas the
//! explorer renders.

use super::*;

/// The descriptor pool decoded once from the compiled set.
pub fn descriptor_pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| {
        DescriptorPool::decode(DESCRIPTOR_SET).expect("the compiled descriptor set decodes")
    })
}

/// The exposed services and their methods, for `/api/config` and for
/// tests: `(service, method, server_streaming)`.
pub fn exposed_methods() -> Vec<(String, String, bool)> {
    let pool = descriptor_pool();
    let mut out = Vec::new();
    for (service, _) in EXPOSED {
        let Some(desc) = pool.get_service_by_name(&format!("{PACKAGE}.{service}")) else {
            continue;
        };
        for method in desc.methods() {
            if method.is_client_streaming() || !is_exposed(service, method.name()) {
                continue;
            }
            out.push((
                service.to_string(),
                method.name().to_string(),
                method.is_server_streaming(),
            ));
        }
    }
    out
}

pub(super) fn resolve_method(service: &str, method: &str) -> Result<MethodDescriptor, Reply> {
    if !is_exposed(service, method) {
        return Err(Reply::error(
            404,
            "NOT_FOUND",
            format!(
                "{service}.{method} is not exposed by the console; the exposed surface is {}",
                exposed_summary()
            ),
        ));
    }
    let desc = descriptor_pool()
        .get_service_by_name(&format!("{PACKAGE}.{service}"))
        .ok_or_else(|| {
            Reply::error(
                404,
                "NOT_FOUND",
                format!("service {service:?} is not in the descriptor set"),
            )
        })?;
    let found = desc.methods().find(|m| m.name() == method).ok_or_else(|| {
        Reply::error(
            404,
            "NOT_FOUND",
            format!("{service} has no method {method:?}"),
        )
    })?;
    if found.is_client_streaming() {
        return Err(Reply::error(
            501,
            "UNIMPLEMENTED",
            format!("{service}.{method} is client-streaming; the console does not expose it"),
        ));
    }
    Ok(found)
}

/// The comment written above a descriptor in its `.proto`, when the
/// compiled set kept source info: the location whose path is the
/// descriptor's own.
pub(super) fn leading_comment(file: &prost_types::FileDescriptorProto, path: &[i32]) -> String {
    file.source_code_info
        .as_ref()
        .and_then(|info| info.location.iter().find(|l| l.path == path))
        .and_then(|l| l.leading_comments.as_deref())
        .map(|c| {
            c.lines()
                .map(str::trim)
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string()
        })
        .unwrap_or_default()
}

pub(super) fn kind_name(kind: &Kind) -> String {
    match kind {
        Kind::Message(m) => m.full_name().to_string(),
        Kind::Enum(e) => e.full_name().to_string(),
        scalar => format!("{scalar:?}").to_lowercase(),
    }
}

/// Add `message` and every message and enum its fields reach to the
/// two maps, each once.
pub(super) fn describe_message(
    message: &MessageDescriptor,
    messages: &mut serde_json::Map<String, Value>,
    enums: &mut serde_json::Map<String, Value>,
) {
    if messages.contains_key(message.full_name()) {
        return;
    }
    // Reserve the name first: a message that reaches itself stops here.
    messages.insert(message.full_name().to_string(), Value::Null);
    let file = message.parent_file_descriptor_proto();
    let mut fields = Vec::new();
    for field in message.fields() {
        let mut entry = json!({
            "name": field.name(),
            "json_name": field.json_name(),
            "number": field.number(),
            "type": kind_name(&field.kind()),
            "repeated": field.is_list(),
            "comment": leading_comment(file, field.path()),
        });
        if let Some(oneof) = field.containing_oneof().filter(|o| !o.is_synthetic()) {
            entry["oneof"] = json!(oneof.name());
        }
        if field.is_map() {
            if let Kind::Message(pair) = field.kind() {
                let (key, value) = (pair.map_entry_key_field(), pair.map_entry_value_field());
                entry["map"] =
                    json!({ "key": kind_name(&key.kind()), "value": kind_name(&value.kind()) });
                entry["type"] = json!(kind_name(&value.kind()));
                match value.kind() {
                    Kind::Message(m) => describe_message(&m, messages, enums),
                    Kind::Enum(e) => describe_enum(&e, enums),
                    _ => {}
                }
            }
        } else {
            match field.kind() {
                Kind::Message(m) => describe_message(&m, messages, enums),
                Kind::Enum(e) => describe_enum(&e, enums),
                _ => {}
            }
        }
        fields.push(entry);
    }
    messages.insert(
        message.full_name().to_string(),
        json!({ "comment": leading_comment(file, message.path()), "fields": fields }),
    );
}

pub(super) fn describe_enum(desc: &EnumDescriptor, enums: &mut serde_json::Map<String, Value>) {
    if !enums.contains_key(desc.full_name()) {
        let values: Vec<Value> = desc.values().map(|v| json!(v.name())).collect();
        enums.insert(desc.full_name().to_string(), Value::Array(values));
    }
}

/// `GET /api/describe/<Service>/<Method>`: the shapes of an exposed
/// method's request and response messages, from the descriptor set the
/// transcoder reads, with the comments the `.proto` carries.
pub(super) fn describe(service: &str, method: &str) -> Result<Value, Reply> {
    let found = resolve_method(service, method)?;
    let (mut messages, mut enums) = (serde_json::Map::new(), serde_json::Map::new());
    describe_message(&found.input(), &mut messages, &mut enums);
    describe_message(&found.output(), &mut messages, &mut enums);
    Ok(json!({
        "service": service,
        "method": method,
        "server_streaming": found.is_server_streaming(),
        "comment": leading_comment(found.parent_service().parent_file_descriptor_proto(), found.path()),
        "request": found.input().full_name(),
        "response": found.output().full_name(),
        "messages": messages,
        "enums": enums,
    }))
}
