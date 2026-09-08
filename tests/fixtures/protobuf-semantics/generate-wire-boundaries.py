#!/usr/bin/env python3
"""Regenerate measured proto2/proto3 boundary fixtures with pinned references."""

import argparse
import copy
import json
import re
import os
import pathlib
import shutil
import subprocess
import tempfile

import google.protobuf
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.internal import api_implementation

EXPECTED_PROTOBUF = "6.33.5"
EXPECTED_PROTOC = "libprotoc 25.1"
VOLATILE_CPP_CASES = {
    ("proto3", "utf8_invalid_lone_ff"),
    ("proto3", "utf8_invalid_continuation"),
}
VOLATILE_CPP_PREFIX = re.compile(
    r"^E[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6} [0-9]+ "
    r"wire_format_lite\.cc:603\]"
)
PROTOC = os.environ.get("PROTOC") or shutil.which("protoc")

ACCEPT = {
    "utf8_ascii_control",
    "utf8_multibyte_boundary_control",
    "uint64_zero_canonical_control",
    "uint64_zero_ten_byte_noncanonical_boundary",
    "uint64_max_ten_byte_boundary",
    "int64_negative_one_ten_byte_control",
    "int64_minimum_control",
}
INVALID_UTF8 = {"utf8_invalid_lone_ff", "utf8_invalid_continuation"}
OVERFLOW = {
    "uint64_ten_byte_final_byte_overflow",
    "int64_ten_byte_ff_final_7f",
    "int64_ten_byte_80_final_7f",
}
TRUNCATED = {"uint64_truncated_control"}
assert len(ACCEPT | INVALID_UTF8 | OVERFLOW | TRUNCATED) == 13
assert not (
    (ACCEPT & INVALID_UTF8)
    or (ACCEPT & OVERFLOW)
    or (ACCEPT & TRUNCATED)
    or (INVALID_UTF8 & OVERFLOW)
    or (INVALID_UTF8 & TRUNCATED)
    or (OVERFLOW & TRUNCATED)
)


def printable(value):
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if isinstance(value, str):
        return {"text": value, "utf8_hex": value.encode().hex()}
    return value


def observe(call):
    try:
        return {"success": True, "value": call()}
    except Exception as error:
        return {
            "success": False,
            "error_type": type(error).__name__,
            "error": str(error),
        }


def upb_observation(message_type, wire):
    message = message_type()
    parse = observe(lambda: message.MergeFromString(wire))
    known = observe(
        lambda: {field.name: printable(value) for field, value in message.ListFields()}
    )
    return {
        "parse": parse,
        "initialized": observe(message.IsInitialized),
        "initialization_errors": observe(
            lambda: list(message.FindInitializationErrors())
        ),
        "known_fields": known,
        "serialized_hex": observe(
            lambda: message.SerializePartialToString(deterministic=True).hex()
        ),
    }


def cpp_observation(descriptor, type_name, wire):
    process = subprocess.run(
        [PROTOC, f"--descriptor_set_in={descriptor}", f"--decode={type_name}"],
        input=wire,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return {
        "success": process.returncode == 0,
        "exit_code": process.returncode,
        "text_output": process.stdout.decode("utf-8", "backslashreplace"),
        "stderr": process.stderr.decode("utf-8", "backslashreplace"),
    }


def disposition(syntax, name, upb):
    if name in INVALID_UTF8:
        return {
            "disposition": "refuse",
            "error_contains": "invalid string value",
            "reason": "declared strings require valid UTF-8",
        }
    if name in OVERFLOW:
        return {
            "disposition": "refuse",
            "error_contains": "invalid varint",
            "reason": "varint payload exceeds 64 bits",
        }
    if name in TRUNCATED:
        return {
            "disposition": "refuse",
            "error_contains": "invalid varint",
            "reason": "truncated varint",
        }
    if name not in ACCEPT:
        raise AssertionError(f"unreviewed case has no product disposition: {syntax}/{name}")
    assert upb["parse"]["success"], (syntax, name, upb)
    return {
        "disposition": "accept",
        "expected_fields": upb["known_fields"]["value"],
    }


def normalized_for_comparison(fixture):
    normalized = copy.deepcopy(fixture)
    for case in normalized["cases"]:
        identity = (case["syntax"], case["case"])
        if identity not in VOLATILE_CPP_CASES:
            continue
        stderr = case["cpp_protoc"]["stderr"]
        replacement = "E<date> <time> <pid> wire_format_lite.cc:603]"
        updated, count = VOLATILE_CPP_PREFIX.subn(replacement, stderr, count=1)
        if count != 1:
            raise AssertionError(
                f"unexpected pinned protoc diagnostic prefix for {identity}: {stderr!r}"
            )
        case["cpp_protoc"]["stderr"] = updated
    return normalized


def compare_fixture(existing, fresh):
    expected = json.dumps(
        normalized_for_comparison(existing), sort_keys=True, ensure_ascii=False
    )
    measured = json.dumps(
        normalized_for_comparison(fresh), sort_keys=True, ensure_ascii=False
    )
    if measured != expected:
        raise AssertionError(
            "fresh reference output differs in JSON value or type outside the "
            "two pinned protoc date/time/PID prefixes"
        )


def load_comparison(new_output, existing):
    if existing is None:
        return None
    if new_output.resolve() == existing.resolve():
        raise SystemExit("NEW_OUTPUT and --compare EXISTING must be different paths")
    try:
        return json.loads(existing.read_text())
    except FileNotFoundError:
        raise SystemExit(f"comparison fixture does not exist: {existing}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("new_output", type=pathlib.Path)
    parser.add_argument("--compare", type=pathlib.Path, metavar="EXISTING")
    arguments = parser.parse_args()
    comparison = load_comparison(arguments.new_output, arguments.compare)
    if arguments.new_output.exists():
        raise SystemExit(f"refusing to replace existing output: {arguments.new_output}")
    assert PROTOC, "set PROTOC or put protoc on PATH"
    protoc_version = subprocess.check_output([PROTOC, "--version"], text=True).strip()
    assert protoc_version == EXPECTED_PROTOC, protoc_version
    assert google.protobuf.__version__ == EXPECTED_PROTOBUF
    assert api_implementation.Type() == "upb"

    schemas = {
        "proto2": '''syntax = "proto2"; package wireprobe;
message Proto2Probe {
  required string text = 1;
  optional uint64 number = 2;
  optional int64 signed_number = 3;
}
''',
        "proto3": '''syntax = "proto3"; package wireprobe;
message Proto3Probe { string text = 1; uint64 number = 2; int64 signed_number = 3; }
''',
    }
    empty = bytes.fromhex("0a00")
    cases = {
        "utf8_ascii_control": bytes.fromhex("0a0141"),
        "utf8_multibyte_boundary_control": bytes.fromhex("0a02c3a9"),
        "utf8_invalid_lone_ff": bytes.fromhex("0a01ff"),
        "utf8_invalid_continuation": bytes.fromhex("0a02c328"),
        "uint64_zero_canonical_control": empty + bytes.fromhex("1000"),
        "uint64_zero_ten_byte_noncanonical_boundary": empty + b"\x10" + b"\x80" * 9 + b"\x00",
        "uint64_max_ten_byte_boundary": empty + b"\x10" + b"\xff" * 9 + b"\x01",
        "uint64_ten_byte_final_byte_overflow": empty + b"\x10" + b"\xff" * 9 + b"\x02",
        "uint64_truncated_control": empty + bytes.fromhex("1080"),
        "int64_negative_one_ten_byte_control": empty + b"\x18" + b"\xff" * 9 + b"\x01",
        "int64_ten_byte_ff_final_7f": empty + b"\x18" + b"\xff" * 9 + b"\x7f",
        "int64_minimum_control": empty + b"\x18" + b"\x80" * 9 + b"\x01",
        "int64_ten_byte_80_final_7f": empty + b"\x18" + b"\x80" * 9 + b"\x7f",
    }
    records = []
    with tempfile.TemporaryDirectory(prefix="psearch-p2-p3-") as raw:
        work = pathlib.Path(raw)
        pool = descriptor_pool.DescriptorPool()
        descriptors = {}
        for syntax, source in schemas.items():
            proto = work / f"{syntax}.proto"
            descriptor = work / f"{syntax}.bin"
            proto.write_text(source)
            subprocess.run(
                [PROTOC, f"-I{work}", f"--descriptor_set_out={descriptor}", proto.name],
                cwd=work,
                check=True,
            )
            files = descriptor_pb2.FileDescriptorSet.FromString(descriptor.read_bytes())
            for file_descriptor in files.file:
                pool.Add(file_descriptor)
            descriptors[syntax] = descriptor
        for syntax in ("proto2", "proto3"):
            type_name = f"wireprobe.{syntax.capitalize()}Probe"
            message_type = message_factory.GetMessageClass(
                pool.FindMessageTypeByName(type_name)
            )
            for name, wire in cases.items():
                upb = upb_observation(message_type, wire)
                records.append({
                    "syntax": syntax,
                    "case": name,
                    "input_hex": wire.hex(),
                    "product": disposition(syntax, name, upb),
                    "expected_source_preserved": True,
                    "cpp_protoc": cpp_observation(descriptors[syntax], type_name, wire),
                    "python_upb": upb,
                })
    output = {
        "format_version": 1,
        "reference_runtime": {
            "protoc": protoc_version,
            "python_protobuf": google.protobuf.__version__,
            "python_backend": api_implementation.Type(),
        },
        "schema": {
            "proto2": "required string text = 1; optional uint64 number = 2; optional int64 signed_number = 3;",
            "proto3": "string text = 1; uint64 number = 2; int64 signed_number = 3;",
        },
        "cases": records,
    }
    try:
        with arguments.new_output.open("x") as destination:
            json.dump(output, destination, indent=2, ensure_ascii=False, sort_keys=True)
            destination.write("\n")
    except FileExistsError:
        raise SystemExit(f"refusing to replace existing output: {arguments.new_output}")
    if comparison is not None:
        compare_fixture(comparison, output)
        print(
            "PASS: fresh raw observations match after normalizing only the two "
            "pinned protoc date/time/PID prefixes"
        )


if __name__ == "__main__":
    main()
