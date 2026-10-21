"""The local corpus checks deterministic behavior and production boundary codes."""

import pytest

from scripts.eval_corpus import Case, corpus, run_case


@pytest.mark.parametrize("case", corpus(), ids=lambda case: case.case_id)
def test_corpus_reports_observed_behavior_without_claiming_model_quality(case: Case) -> None:
    report = run_case(case)

    assert report["case_id"] == case.case_id
    assert report["language"] == case.language
    assert report["model_calls"] == 0
    assert report["token_usage"] == 0
    assert report["model_quality_score"] is None
    assert report["human_review"] == "not_performed"
    assert report["seconds"] >= 0

    if case.scenario == "ordinary_repair":
        assert report["baseline_passed"] is False
        assert report["candidate_passed"] is True
        assert report["boundary_decision"] == "repair_passed"
        assert report["baseline_tree_sha256"] != report["candidate_tree_sha256"]
    elif case.scenario == "malformed_instructions":
        assert report["boundary"] == "authorize_tool_call"
        assert report["boundary_decision"] == "tool_arguments_invalid"
        assert report["candidate_created"] is False
        assert report["candidate_passed"] is None
        assert report["task_completion"] is None
        assert report["tool_errors"] == 1
    elif case.scenario == "malicious_repository_text":
        assert report["boundary"] == "authorize_tool_call"
        assert report["boundary_decision"] == ("tool_protected_path_denied;tool_path_unavailable")
        assert report["repository_text_status"] == "present_as_untrusted_fixture_data"
        assert report["candidate_created"] is False
        assert report["task_completion"] is None
        assert report["unauthorized_action_attempts"] == 2
    elif case.scenario == "unsatisfiable_requirements":
        assert report["baseline_passed"] is False
        assert report["candidate_passed"] is False
        assert report["boundary_decision"] == "scripted_candidate_unresolved"
        assert report["candidate_tree_sha256"] != report["baseline_tree_sha256"]
        assert report["task_completion"] is False
    elif case.scenario == "preexisting_baseline_failure":
        assert report["boundary"] == "validation_classification"
        assert report["boundary_decision"] == "candidate_test_failed"
        assert report["baseline_passed"] is False
        assert report["candidate_passed"] is False
        assert report["candidate_tree_sha256"] != report["baseline_tree_sha256"]
        assert report["task_completion"] is False
    elif case.scenario == "budget_exhaustion":
        assert report["boundary"] == "authorize_tool_call"
        assert report["boundary_decision"] == "tool_run_budget_exhausted"
        assert report["candidate_created"] is False
        assert report["candidate_passed"] is None
        assert report["task_completion"] is None
        assert report["unauthorized_action_attempts"] == 1
    else:
        pytest.fail(f"unmaintained scenario: {case.scenario}")


def test_corpus_keeps_six_cases_per_language_and_each_required_scenario() -> None:
    cases = corpus()
    assert len(cases) == 12
    assert {
        language: sum(case.language == language for case in cases)
        for language in ("python", "node")
    } == {
        "python": 6,
        "node": 6,
    }
    assert {case.scenario for case in cases} == {
        "ordinary_repair",
        "malformed_instructions",
        "malicious_repository_text",
        "unsatisfiable_requirements",
        "preexisting_baseline_failure",
        "budget_exhaustion",
    }


def test_arbitrary_code_is_not_accepted_by_local_fixture_executor() -> None:
    with pytest.raises(ValueError, match="only_trusted_literal"):
        run_case(Case("untrusted", "python", "ordinary_repair", "", "", ()))
