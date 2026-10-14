# Source file tools and policy

T19 exposes typed directory listing, text reading, literal search, patch and diff
operations over verified canonical source archives. Repository bytes are data.
The executor does not run repository code, open host paths, launch a shell or
contact a network service.

Requests use strict schemas with a fixed tool name and bounded arguments.
Authorization comes from trusted workflow, profile, account-access, stage and
budget context. Model arguments cannot supply permissions, credentials or the
protected-file inventory. Unknown tools and extra argument fields fail closed.

Dependency manifests and locks, build/profile configuration, CI files, baseline
tests and test harnesses are protected from edits. New regression tests may be
added. Known credential/control paths are excluded from model access, including search
and listings. Paths are archive-relative and retain canonical path constraints.
This is path policy, not a universal classifier of secrets embedded in arbitrary
repository text. Control-plane credentials are never supplied to these tools.

Patch operations use exact preimages and construct a new candidate atomically.
A conflict or rejected edit cannot partially modify source. Candidates remain
bounded by the canonical archive limits and preserve untouched bytes and modes.
Diffs and reads produce bounded text; search is literal rather than a supplied
regular expression.

| Operation | Bound |
|---|---|
| List | 100 entries |
| Read | 16 KiB page from a file no larger than 1 MiB |
| Literal search | 512-byte query, 100 matches, 10 MiB and 1,000 files scanned |
| Patch | Ten files and 64 KiB combined preimage/replacement text |
| Diff | Ten context lines, ten changed files, 2 MiB input and 2,000 lines |
| Serialized arguments/result | 64 KiB each, including JSON escaping |
| Candidate archive | 10 MiB compressed, 50 MiB expanded and 5,000 files |

These are finite per-call limits. Truncated inspection results say so; an
oversized or ambiguous patch fails atomically. Custom harness entrypoints must
be included in the trusted baseline inventory; discovery fails closed where it
cannot establish that inventory. This does not prove arbitrary code's semantics.
Framework configuration can conservatively protect every baseline JS/TS file
when precise selection is unavailable. Unknown command forms, dynamic shell
arguments and unsupported Python doctest/plugin declarations fail closed.
Harness configuration is capped at 256 KiB before parsing.

The result is an ephemeral candidate and policy receipt, not committed progress.
A trusted caller must restore content through T15 authorization and accept a new
checkpoint under the runtime's current-generation fence. Durable tool identity,
execution orchestration and cumulative run budgets belong to T21/T27/T30.
The tools are not yet wired to a live model or public route.

## Local verification

The policy target passed 28 tests and the source executor target passed nine.
They cover strict and reconstructed argument validation, permission intersections,
protected paths, archive pins, UTF-8 pages, literal search limits, atomic and
ambiguous edits, baseline harness discovery, and escaped-result size limits.
They use synthetic source data and execute no repository code.

The complete Python suite passed 400 tests, with 15 optional integration targets
skipped for missing separate database environments. Formatting, lint, typing
across 45 source files and package build passed. No schema, frontend or Edge
changes are included in T19.

```sh
make test-target TARGET=tests/unit/test_tool_policy.py
make test-target TARGET=tests/unit/test_source_tools.py
```

## Sources

- [Pydantic strict validation](https://docs.pydantic.dev/latest/concepts/strict_mode/).
- [Pydantic discriminated unions](https://docs.pydantic.dev/latest/concepts/unions/#discriminated-unions).
- [Vitest configuration and Vite fallback](https://vitest.dev/config/).
- [Canonical source archives](canonical-archives.md).
- [Checkpoint storage and fencing](checkpoint-storage.md).
