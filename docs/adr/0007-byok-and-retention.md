---
status: proposed
---
# Use individual BYOK accounts with explicit retention boundaries

The user chose individual accounts, both OpenAI and Anthropic, saved encrypted or per-run credentials, seven-day run-content retention and 48-hour approvals.

Keys are encrypted outside the execution workspace; per-run keys are durably available for recovery and removed from active credential storage at terminal completion. Provider retention and historical backups have separate lifetimes. Minimal non-secret replay/effect tombstones may outlive source content so deletion does not make old commands executable again.

BYOK keeps model charges with the run owner. Application request/token limits remain necessary, but they are not a provider-enforced monetary cap.
