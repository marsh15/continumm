from __future__ import annotations

import dataclasses
import json

import pytest

from continuum_api.validation.hidden_checks import (
    HiddenCheckCase,
    HiddenCheckError,
    HiddenCheckSuite,
    compare_hidden_output,
)

ARCHIVE = "a" * 64
TREE = "b" * 64


def make_suite() -> HiddenCheckSuite:
    return HiddenCheckSuite(
        suite_id="oracle-v1",
        profile_id="python-pytest",
        profile_version="1",
        candidate_archive_sha256=ARCHIVE,
        candidate_tree_sha256=TREE,
        cases=(
            HiddenCheckCase("basic", '{"z":2,"a":[true,1]}', '{"ok":true,"value":1}'),
            HiddenCheckCase("empty", "null", "[]"),
        ),
    )


def compare(suite: HiddenCheckSuite, stdout: str, **kwargs: object):
    return compare_hidden_output(
        suite,
        candidate_archive_sha256=kwargs.get("candidate_archive_sha256", ARCHIVE),  # type: ignore[arg-type]
        candidate_tree_sha256=kwargs.get("candidate_tree_sha256", TREE),  # type: ignore[arg-type]
        exit_code=kwargs.get("exit_code", 0),  # type: ignore[arg-type]
        stdout=stdout,
        stderr=kwargs.get("stderr", ""),  # type: ignore[arg-type]
    )


def test_suite_canonicalizes_inputs_and_keeps_expected_values_private() -> None:
    suite = make_suite()
    assert suite.cases[0].input_json == '{"a":[true,1],"z":2}'
    assert suite.public_inputs_json() == '[{"a":[true,1],"z":2},null]'
    assert '"ok"' not in suite.public_inputs_json()
    assert '"ok"' not in repr(suite.cases[0])
    assert len(suite.digest) == 64
    with pytest.raises(dataclasses.FrozenInstanceError):
        suite.suite_id = "changed"  # type: ignore[misc]


def test_protocol_pass_and_receipt_never_retains_output_text() -> None:
    suite = make_suite()
    raw = '{"protocol_version":1,"values":[{"value":1.0,"ok":true},[]]}'
    receipt = compare(suite, raw, stderr="private diagnostic")
    assert receipt.outcome == "passed"
    assert receipt.passed_count == 2
    assert receipt.as_dict()["stdout_sha256"]
    assert "private diagnostic" not in json.dumps(receipt.as_dict())
    assert raw not in repr(receipt)


def test_wrong_answer_and_boolean_numeric_distinction_fail() -> None:
    suite = make_suite()
    wrong = compare(suite, '{"protocol_version":1,"values":[{"value":2,"ok":true},[]]}')
    assert (wrong.outcome, wrong.passed_count, wrong.code) == (
        "failed",
        1,
        "hidden_check_mismatch",
    )
    bool_wrong = compare(suite, '{"protocol_version":1,"values":[{"value":1,"ok":1},[]]}')
    assert (bool_wrong.outcome, bool_wrong.passed_count) == ("failed", 1)


@pytest.mark.parametrize(
    ("stdout", "code"),
    [
        (
            '{"protocol_version":1,"values":[{"ok":true,"value":1},[]],"passed":true}',
            "hidden_output_protocol_invalid",
        ),
        (
            '{"protocol_version":1,"protocol_version":1,"values":[{"ok":true,"value":1},[]]}',
            "hidden_output_invalid",
        ),
        ('{"protocol_version":1,"values":[NaN,[]]}', "hidden_output_invalid"),
        ('{"protocol_version":1,"values":[1e999,[]]}', "hidden_output_invalid"),
        ('{"protocol_version":1,"values":[9007199254740992.0,[]]}', "hidden_output_invalid"),
        ('{"protocol_version":1,"values":[9007199254740992,[]]}', "hidden_output_invalid"),
        (
            '{"protocol_version":1,"values":[{"ok":true,"value":1.00000000000000001},[]]}',
            "hidden_output_invalid",
        ),
        (
            '{"protocol_version":1,"values":[{"ok":true,"value":1e-400},[]]}',
            "hidden_output_invalid",
        ),
        (
            '{"protocol_version":true,"values":[{"ok":true,"value":1},[]]}',
            "hidden_output_protocol_invalid",
        ),
        ('{"protocol_version":1,"values":[{"ok":true,"value":1}]}', "hidden_output_values_invalid"),
    ],
)
def test_malformed_or_forged_output_is_never_accepted(stdout: str, code: str) -> None:
    result = compare(make_suite(), stdout)
    assert (result.outcome, result.code) == ("invalid", code)


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("input_json", '{"x":1,"x":2}', "hidden_json_duplicate_key"),
        ("expected_json", "Infinity", "hidden_json_non_finite_number"),
        ("input_json", '"\\ud800"', "hidden_json_unicode_invalid"),
        ("input_json", '{"\\ud800":1}', "hidden_json_unicode_invalid"),
        ("input_json", "1.00000000000000001", "hidden_json_number_precision"),
        ("expected_json", "1.00000000000000001", "hidden_json_number_precision"),
        ("input_json", "1e-400", "hidden_json_number_precision"),
        ("expected_json", "1e-400", "hidden_json_number_precision"),
        ("input_json", "9007199254740992", "hidden_json_integer_unsafe"),
        ("input_json", "[[[[[[[[[[[[[[[[[0]]]]]]]]]]]]]]]]]", "hidden_json_nesting_limit"),
        ("input_json", '"x"' * 30_000, "hidden_json_size_limit"),
    ],
)
def test_suite_rejects_unsafe_json(field: str, value: str, code: str) -> None:
    with pytest.raises(HiddenCheckError) as error:
        HiddenCheckCase(
            "safe-id",
            value if field == "input_json" else "null",
            value if field == "expected_json" else "null",
        )
    assert error.value.code == code


def test_duplicate_ids_and_source_mismatch_are_rejected() -> None:
    one = HiddenCheckCase("same", "null", "null")
    two = HiddenCheckCase("same", "true", "true")
    with pytest.raises(HiddenCheckError, match="hidden_check_id_duplicate"):
        HiddenCheckSuite("suite", "node-npm", "1", ARCHIVE, TREE, (one, two))
    result = compare(
        make_suite(),
        '{"protocol_version":1,"values":[{"ok":true,"value":1},[]]}',
        candidate_archive_sha256="c" * 64,
    )
    assert (result.outcome, result.code) == ("invalid", "candidate_archive_mismatch")


def test_missing_exit_is_unknown_nonzero_is_failed() -> None:
    suite = make_suite()
    assert compare(suite, "", exit_code=None).outcome == "unknown"
    assert compare(suite, "", exit_code=2).outcome == "failed"
    assert compare(suite, "", exit_code=256).code == "exit_code_invalid"
    assert compare(suite, "", stderr="x" * 65_537).code == "hidden_output_size_limit"


@pytest.mark.parametrize(
    "changes",
    [
        {"profile_version": "2"},
        {"profile_id": "custom-profile"},
        {"candidate_tree_sha256": "invalid"},
        {"cases": ()},
        {"cases": tuple(HiddenCheckCase(f"id-{i}", "null", "null") for i in range(33))},
    ],
)
def test_invalid_suite_cannot_be_selected(changes: dict[str, object]) -> None:
    with pytest.raises(HiddenCheckError):
        dataclasses.replace(make_suite(), **changes)


def test_changing_expected_answers_changes_suite_identity() -> None:
    suite = make_suite()
    changed = dataclasses.replace(
        suite, cases=(dataclasses.replace(suite.cases[0], expected_json="false"), suite.cases[1])
    )
    assert changed.digest != suite.digest
    assert changed.public_inputs_json() == suite.public_inputs_json()
    raw = '{"protocol_version":1,"values":[{"ok":true,"value":1},[]]}'
    assert compare(suite, raw).outcome == "passed"
    assert compare(changed, raw).outcome == "failed"


def test_source_tree_mismatch_and_surrogate_output_fail_closed() -> None:
    assert (
        compare(make_suite(), "", candidate_tree_sha256="c" * 64).code == "candidate_tree_mismatch"
    )
    assert (
        compare(make_suite(), '{"protocol_version":1,"values":["\\ud800",[]]}').outcome == "invalid"
    )


def test_invalid_json_error_suppresses_payload_traceback() -> None:
    with pytest.raises(HiddenCheckError) as error:
        HiddenCheckCase("safe-id", '{"private":', "null")
    assert str(error.value) == "hidden_check_input_invalid"
    assert error.value.__suppress_context__ is True
    assert error.value.__cause__ is None
