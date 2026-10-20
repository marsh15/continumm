"""Trusted deterministic fixtures verify harness behavior, not model quality."""

import pytest

from scripts.eval_corpus import Case, corpus, run_case


@pytest.mark.parametrize("case", corpus(), ids=lambda case: case.case_id)
def test_trusted_repair_exposes_regression_and_preserves_baseline(case: Case) -> None:
    report = run_case(case)
    assert report["baseline_passed"] is False
    assert report["candidate_passed"] is True
    assert report["baseline_tree_sha256"] != report["candidate_tree_sha256"]


def test_arbitrary_code_is_not_accepted_by_local_fixture_executor() -> None:
    with pytest.raises(ValueError, match="only_trusted_literal"):
        run_case(Case("untrusted", "python", "", "", ()))
