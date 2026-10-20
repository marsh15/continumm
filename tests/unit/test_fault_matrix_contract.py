"""The failure ledger must cover the binding plan and retain deployed gaps."""

import json
from pathlib import Path


def test_every_required_failure_has_local_evidence_or_an_explicit_deployed_procedure() -> None:
    root = Path(__file__).resolve().parents[2]
    matrix = json.loads((root / "docs/fault-matrix.json").read_text())
    section = (
        (root / "tasks/plan.md")
        .read_text()
        .split("### 13.2 Required failure matrix")[1]
        .split("### 13.3")[0]
    )
    required = [
        line.split("|")[1].strip() for line in section.splitlines() if line.startswith("| ")
    ][1:]
    assert [row["scenario"] for row in matrix["rows"]] == required
    assert len(required) == 37
    for row in matrix["rows"]:
        assert row["required_outcome"]
        if row["groups"]:
            for group in row["groups"]:
                assert (root / matrix["groups"][group]["node"]).is_file()
        else:
            assert row["deployed_procedure"]
