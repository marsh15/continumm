"""Fail-closed entry point for a future controlled live-provider evaluation."""

from __future__ import annotations

import argparse
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=("openai", "anthropic"), required=True)
    parser.parse_args()
    print(
        "live_evaluation_harness_unavailable: live calls require a controlled admitted-run "
        "harness that uses T23 credential dispatch and durable model reservations; "
        "no provider request was sent",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
