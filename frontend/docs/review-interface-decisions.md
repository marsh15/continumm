# Review interface decisions

Build target: the existing Continumm web app (`frontend/src/styles.css`) plus the T28 screen rules in `tasks/plan.md` §10.5. Refero MCP is unavailable in this environment, so the reference lock uses the existing shipped product surface and the Refero bundled craft references.

- Preserve the light neutral canvas, near-black text, restrained blue action color, 6px radius, thin separators, and system sans/monospace split already defined by the app.
- Use a dense, workbench-like review layout: compact run/revision header, file navigation alongside a unified diff, followed by validation evidence and the exact publication manifest/limits. The file selection is the one blue-wash active state; avoid decorative cards and color outside semantic statuses/actions.
- At narrow widths, stack file navigation above the diff and keep decision controls visible after evidence. Use native links/buttons, visible keyboard focus, labels, inline error messages, and polite status announcements.
- Decision forms bind to the displayed manifest digest and expiry. Keep a single idempotency key for each in-flight logical submission so network retries repeat the same mutation; on stale conflicts, refresh authoritative review/run state while retaining the feedback draft.

Sources: plan §10.5 (required screen content, layout, accessibility and idempotence); existing CSS token meanings; Refero bundled `craft-details.md` (focus/form/error patterns), `color.md` (semantic contrast), `typography.md` (compact hierarchy), and `anti-ai-slop.md` (avoid excess cards and generic decorative treatment).

Browser checks use the pinned `@playwright/test` 1.64.1 dev dependency because T28 requires real browser coverage and this repository had no browser runner; route fixtures stub the service boundary, so they do not claim provider or backend integration coverage.
