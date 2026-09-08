#!/usr/bin/env python3
"""Standard-library unit tests for the narrow boundary-fixture comparator."""
import copy
import importlib.util
import pathlib
import unittest

GENERATOR = pathlib.Path(__file__).with_name("generate-wire-boundaries.py")
spec = importlib.util.spec_from_file_location("wire_generator", GENERATOR)
wire_generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire_generator)


class ComparatorTest(unittest.TestCase):
    def fixture(self):
        return {
            "format_version": 1,
            "cases": [{
                "syntax": "proto3",
                "case": "utf8_invalid_lone_ff",
                "input_hex": "0a01ff",
                "product": {"disposition": "refuse"},
                "cpp_protoc": {"stderr": (
                    "E0907 22:38:33.769752 3544010 wire_format_lite.cc:603] "
                    "String field is invalid UTF-8.\n"
                )},
                "python_upb": {"known_fields": {"value": {"number": 7}}},
            }],
        }

    def assert_changed_fails(self, mutate):
        existing = self.fixture()
        fresh = copy.deepcopy(existing)
        mutate(fresh)
        with self.assertRaises(AssertionError):
            wire_generator.compare_fixture(existing, fresh)

    def test_only_timestamp_and_pid_may_change(self):
        existing = self.fixture()
        fresh = copy.deepcopy(existing)
        fresh["cases"][0]["cpp_protoc"]["stderr"] = (
            "E0908 01:02:19.802298 4066869 wire_format_lite.cc:603] "
            "String field is invalid UTF-8.\n"
        )
        existing_raw = copy.deepcopy(existing)
        fresh_raw = copy.deepcopy(fresh)
        wire_generator.compare_fixture(existing, fresh)
        self.assertEqual(existing, existing_raw)
        self.assertEqual(fresh, fresh_raw)

    def test_changed_prefix_file_line_or_severity_fails(self):
        for old, new in [
            ("wire_format_lite.cc:603]", "wire_format_lite.cc:604]"),
            ("E0907 ", "W0907 "),
        ]:
            with self.subTest(replacement=new):
                self.assert_changed_fails(
                    lambda value, old=old, new=new: value["cases"][0][
                        "cpp_protoc"
                    ].update(
                        stderr=value["cases"][0]["cpp_protoc"]["stderr"].replace(
                            old, new
                        )
                    )
                )

    def test_timestamp_difference_on_untargeted_case_fails(self):
        existing = self.fixture()
        existing["cases"][0]["case"] = "utf8_ascii_control"
        fresh = copy.deepcopy(existing)
        fresh["cases"][0]["cpp_protoc"]["stderr"] = fresh["cases"][0][
            "cpp_protoc"
        ]["stderr"].replace("22:38:33.769752 3544010", "01:02:19.802298 4066869")
        with self.assertRaises(AssertionError):
            wire_generator.compare_fixture(existing, fresh)

    def test_comparison_requires_a_distinct_existing_baseline(self):
        root = pathlib.Path(__file__).parent
        same = root / "same.json"
        with self.assertRaises(SystemExit):
            wire_generator.load_comparison(same, same)
        with self.assertRaises(SystemExit):
            wire_generator.load_comparison(root / "new.json", root / "missing.json")

    def test_changed_diagnostic_fails(self):
        self.assert_changed_fails(
            lambda value: value["cases"][0]["cpp_protoc"].update(
                stderr=value["cases"][0]["cpp_protoc"]["stderr"].replace(
                    "invalid UTF-8", "different diagnostic"
                )
            )
        )

    def test_changed_payload_fails(self):
        self.assert_changed_fails(
            lambda value: value["cases"][0].update(input_hex="0a02ffff")
        )

    def test_changed_projected_numeric_value_fails(self):
        self.assert_changed_fails(
            lambda value: value["cases"][0]["python_upb"]["known_fields"][
                "value"
            ].update(number=8)
        )

    def test_json_number_cannot_be_replaced_by_boolean(self):
        self.assert_changed_fails(
            lambda value: value.update(format_version=True)
        )

    def test_changed_product_disposition_fails(self):
        self.assert_changed_fails(
            lambda value: value["cases"][0]["product"].update(
                disposition="accept"
            )
        )


if __name__ == "__main__":
    unittest.main()
