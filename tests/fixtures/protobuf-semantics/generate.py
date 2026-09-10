"""Refresh with protoc and Python protobuf==6.33.5; neither runs in cargo test."""

import json
import pathlib
import struct
import subprocess

import google.protobuf
from google.protobuf import descriptor, descriptor_pb2, descriptor_pool, message_factory, text_format
from google.protobuf.internal import api_implementation

assert google.protobuf.__version__ == "6.33.5"
assert api_implementation.Type() == "upb"
assert subprocess.check_output(["protoc", "--version"], text=True).strip() == "libprotoc 25.1"
root = pathlib.Path(__file__).resolve().parent
subprocess.run(
    ["protoc", f"-I{root}", "--include_imports",
     f"--descriptor_set_out={root / 'descriptor.bin'}", "closed.proto"],
    check=True,
)
pool = descriptor_pool.DescriptorPool()
files = descriptor_pb2.FileDescriptorSet.FromString((root / "descriptor.bin").read_bytes())
for file in files.file:
    pool.Add(file)
doc_type = message_factory.GetMessageClass(pool.FindMessageTypeByName("semantics.Doc"))


def varint(n):
    n &= (1 << 64) - 1
    result = bytearray()
    while n > 127:
        result.append((n & 127) | 128)
        n >>= 7
    result.append(n)
    return bytes(result)


def scalar(tag, value):
    return varint(tag << 3) + varint(value)


def payload(tag, value):
    return varint((tag << 3) | 2) + varint(len(value)) + value


def group(tag, value, end=None):
    return varint((tag << 3) | 3) + value + varint(((end or tag) << 3) | 4)


def fixed32(tag, value=1):
    return varint((tag << 3) | 5) + struct.pack("<I", value)


def fixed64(tag, value=1):
    return varint((tag << 3) | 1) + struct.pack("<Q", value)


def by_wire(tag, wire_type):
    return {
        0: scalar(tag, 1),
        1: fixed64(tag),
        2: payload(tag, b"\x08\x01"),
        3: group(tag, scalar(99, 1)),
        5: fixed32(tag),
    }[wire_type]


def entry(key, value):
    return payload(1, key.encode()) + value


def values(message):
    def value(field, item):
        if field.type in (descriptor.FieldDescriptor.TYPE_MESSAGE, descriptor.FieldDescriptor.TYPE_GROUP):
            return values(item)
        if field.type == descriptor.FieldDescriptor.TYPE_BYTES:
            return item.hex()
        return item

    result = {}
    for field, item in message.ListFields():
        if field.message_type and field.message_type.GetOptions().map_entry:
            result[str(field.number)] = {
                str(key): value(field.message_type.fields_by_name["value"], val)
                for key, val in item.items()
            }
        elif field.is_repeated:
            result[str(field.number)] = [value(field, val) for val in item]
        else:
            result[str(field.number)] = value(field, item)
    return result


base_without_required = scalar(1, 1) + payload(2, b"x") + payload(3, struct.pack("<f", 1))
base = base_without_required + payload(15, b"")
complete = scalar(1, 3) + scalar(2, 4)
left = scalar(1, 3)
right = scalar(2, 4)
cases = {
    "fixed64 scalar present": base + fixed64(16, 0x3ff0000000000000),
    "required bytes present empty": base,
    "required unindexed bytes missing": base_without_required,
    "closed enum absent default": base,
    "closed enum unknown only": base + scalar(7, 99),
    "closed enum known then unknown": base + scalar(7, 1) + scalar(7, 99),
    "closed enum unknown then explicit zero": base + scalar(7, 99) + scalar(7, 0),
    "closed enum negative unknown": base + scalar(7, -1),
    "closed enum truncates to int32": base + scalar(7, (1 << 32) + 1),
    "oneof unknown does not select": base + scalar(4, 99),
    "oneof unknown preserves another member": base + scalar(5, 7) + scalar(4, 99),
    "oneof unknown preserves same member": base + scalar(4, 1) + scalar(4, 99),
    "oneof explicit zero selects": base + scalar(5, 7) + scalar(4, 0),
    "open imported enum accepts unknown": base + scalar(14, 99),
    "open imported enum accepts negative": base + scalar(14, -1),
    "closed repeated packed and unpacked": base + payload(8, bytes([0, 99, 1, 99])) + scalar(8, 0),
    "closed repeated only unknown": base + payload(8, bytes([99, 98])),
    "closed map unknown discarded": base + payload(9, entry("a", scalar(2, 99))),
    "closed map unknown preserves old entry": base + payload(9, entry("a", scalar(2, 1))) + payload(9, entry("a", scalar(2, 99))),
    "closed map known unknown within entry": base + payload(9, entry("a", scalar(2, 1) + scalar(2, 99))),
    "closed map unknown known within entry": base + payload(9, entry("a", scalar(2, 99) + scalar(2, 1))),
    "closed map omitted value default": base + payload(9, entry("a", b"")),
    "required nested partial fragments merge": base + payload(11, left) + payload(11, right),
    "required nested field absent": base + payload(11, left),
    "required repeated elements complete": base + payload(10, complete) + payload(10, complete),
    "required repeated elements do not merge": base + payload(10, left) + payload(10, right),
    "required map entry value absent": base + payload(12, entry("a", b"")),
    "required map message fragments merge": base + payload(12, entry("a", payload(2, left) + payload(2, right))),
    "required map duplicate entry replaces": base + payload(12, entry("a", payload(2, complete))) + payload(12, entry("a", payload(2, left))),
    "oneof message fragments merge": base + payload(6, left) + payload(6, right),
    "oneof replaced incomplete message ignored": base + payload(6, left) + scalar(5, 7),
    "oneof switching back starts fresh": base + payload(6, complete) + scalar(5, 7) + payload(6, left),
    "oneof unknown preserves message merge": base + payload(6, left) + scalar(4, 99) + payload(6, right),
    "group required explicit zero": base + group(13, scalar(1, 0)),
    "group required absent": base + group(13, b""),
    "group fragments merge": base + group(13, payload(2, b"label")) + group(13, scalar(1, 1)),
    "group mismatched end": base + group(13, scalar(1, 1), 12),
    "group missing end": base + group(13, scalar(1, 1))[:-1],
    "unknown group accepted": base + group(99, scalar(1, 1)),
    "required extension fragments merge": base + payload(100, left) + payload(100, right),
    "required extension absent child": base + payload(100, left),
    "closed enum extension unknown preserves known": base + scalar(101, 1) + scalar(101, 99),
    "closed repeated enum extension": base + payload(102, bytes([0, 99, 1])) + scalar(102, 99),
    "malformed unindexed repeated child": base + payload(10, b"\x08\x80"),
}
for label, tag, compatible in [
    ("scalar", 1, {0}),
    ("repeated packable", 3, {2, 5}),
    ("string", 2, {2}),
    ("message", 11, {2}),
    ("map", 9, {2}),
    ("group", 13, {3}),
    ("enum", 7, {0}),
    ("message extension", 100, {2}),
    ("fixed64", 16, {1}),
]:
    for wire_type in sorted({0, 1, 2, 3, 5} - compatible):
        prefix = base + fixed64(16, 0x3ff0000000000000) if label == "fixed64" else base
        cases[f"wrong wire {label} as type {wire_type}"] = prefix + by_wire(tag, wire_type)

for wire_type in [1, 2, 3, 5]:
    cases[f"wrong wire oneof preserves member type {wire_type}"] = (
        base + scalar(4, 1) + by_wire(5, wire_type)
    )
for wire_type in [0, 1, 3, 5]:
    cases[f"wrong wire required bytes absent type {wire_type}"] = base_without_required + by_wire(15, wire_type)
    cases[f"wrong wire nested scalar preserves value type {wire_type}"] = (
        base + payload(11, complete + by_wire(1, wire_type))
    )
    cases[f"wrong wire nested required scalar absent type {wire_type}"] = (
        base + payload(11, scalar(2, 4) + by_wire(1, wire_type))
    )

for wire_type in [0, 1, 3, 5]:
    cases[f"closed enum map entry key wire type {wire_type}"] = (
        base + payload(9, by_wire(1, wire_type) + scalar(2, 1))
    )
for wire_type in [1, 2, 3, 5]:
    cases[f"closed enum map entry value wire type {wire_type}"] = (
        base + payload(9, payload(1, b"a") + by_wire(2, wire_type))
    )

known_enum_entry = payload(9, entry("a", scalar(2, 1)))
known_detail_entry = payload(12, entry("a", payload(2, complete)))
cases.update({
    "closed enum map valid prior then key wire type 1": (
        base + known_enum_entry + payload(9, by_wire(1, 1) + scalar(2, 0))
    ),
    "closed enum map valid prior then value wire type 1": (
        base + known_enum_entry + payload(9, payload(1, b"a") + by_wire(2, 1))
    ),
    "closed enum map entry synthetic field 3": (
        base + payload(9, entry("a", scalar(2, 1)) + scalar(3, 7))
    ),
    "closed enum map valid prior then synthetic field 3": (
        base + known_enum_entry
        + payload(9, entry("a", scalar(2, 0)) + scalar(3, 7))
    ),
    "message map valid prior then key wire type 1": (
        base + known_detail_entry
        + payload(12, by_wire(1, 1) + payload(2, complete))
    ),
    "message map valid prior then value wire type 1": (
        base + known_detail_entry
        + payload(12, payload(1, b"a") + by_wire(2, 1))
    ),
    "message map entry synthetic field 3": (
        base + payload(12, entry("a", payload(2, complete)) + scalar(3, 7))
    ),
    "message map valid prior then synthetic field 3": (
        base + known_detail_entry
        + payload(12, entry("a", payload(2, scalar(1, 8) + scalar(2, 9))) + scalar(3, 7))
    ),
    "message map complete value plus nested left wire type 1": (
        base + payload(12, entry("a", payload(2, complete + by_wire(1, 1))))
    ),
})

cpp_map_cases = {
    *(f"closed enum map entry key wire type {wire_type}" for wire_type in [0, 1, 3, 5]),
    *(f"closed enum map entry value wire type {wire_type}" for wire_type in [1, 2, 3, 5]),
    "closed enum map valid prior then key wire type 1",
    "closed enum map valid prior then value wire type 1",
    "closed enum map entry synthetic field 3",
    "closed enum map valid prior then synthetic field 3",
    "message map valid prior then key wire type 1",
    "message map valid prior then value wire type 1",
    "message map entry synthetic field 3",
    "message map valid prior then synthetic field 3",
    "message map complete value plus nested left wire type 1",
}
assert cpp_map_cases <= cases.keys()

cases.update({
    "malformed skipped fixed64": base + varint((1 << 3) | 1) + b"\x00" * 7,
    "malformed skipped length": base + varint((1 << 3) | 2) + b"\x02\x00",
    "malformed skipped group missing end": base + varint((2 << 3) | 3) + scalar(99, 1),
    "malformed skipped group mismatched end": base + group(2, scalar(99, 1), 3),
    "malformed skipped nested group": (
        base + varint((2 << 3) | 3) + varint((99 << 3) | 3) + scalar(1, 1)
        + varint((2 << 3) | 4) + varint((99 << 3) | 4)
    ),
})
records = []
for name, wire in cases.items():
    record = {"name": name, "wire": wire.hex()}
    message = doc_type()
    try:
        message.ParseFromString(wire)
        upb = {"valid": message.IsInitialized()}
        if upb["valid"]:
            upb["fields"] = values(message)
        if name in cpp_map_cases:
            decoded = subprocess.run(
                ["protoc", "--decode=semantics.Doc",
                 f"--descriptor_set_in={root / 'descriptor.bin'}"],
                input=wire,
                capture_output=True,
                check=True,
            )
            cpp_text = decoded.stdout.decode()
            reference = doc_type()
            text_format.Parse(cpp_text, reference, allow_unknown_field=True)
            record["reference"] = "cpp_25_1_text_parse"
            record["observations"] = {
                "upb_6_33_5_binary": upb,
                "cpp_25_1_text": cpp_text,
            }
            record["valid"] = reference.IsInitialized()
            if record["valid"]:
                record["fields"] = values(reference)
        else:
            record.update(upb)
    except google.protobuf.message.DecodeError:
        record["valid"] = False
    records.append(record)
(root / "cases.json").write_text(json.dumps(records, indent=2, sort_keys=True) + "\n")
print(f"Wrote {len(records)} cases using protobuf {google.protobuf.__version__}")
