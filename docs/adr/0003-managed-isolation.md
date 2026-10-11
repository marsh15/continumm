---
status: proposed
---
# Isolate repository code in managed compute and commit source outside it

The user chose managed isolation for public repository workloads. Repository commands therefore execute outside the API host with no application, GitHub or model credentials.

The authoritative recovery checkpoint is a verified source artifact referenced by a committed database record. Provider snapshots and drives are disabled initially because their lifetime and session behavior differ from the application's durability contract. Unknown remote allocations retain capacity until observed terminated; local elapsed time alone does not prove delayed creation stopped.
