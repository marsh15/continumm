# Canonical source archives

T14 separates archive identity from source-tree identity. The archive SHA-256
identifies exact compressed bytes. The canonical tree digest identifies a
versioned, sorted manifest of relative file paths, normalized executable modes,
sizes, and content SHA-256 values. A changed executable bit changes tree identity;
timestamps, owners, and compression metadata do not.

The canonical format is strict USTAR in gzip. Generated archives use fixed
metadata and deterministic ordering. Verification accepts bounded bytes and
returns verified file data; it never extracts onto the API host filesystem.
GitHub archive acquisition and conversion are separate later work, so acceptance
of this checkpoint format does not imply support for every external tar format.

Limits are 10 MiB compressed, 50 MiB of file content, and 5,000 files, with a
separate finite allowance for tar headers and padding. Inflation is bounded
before tar parsing, and concatenated gzip members or trailing compressed bytes
are rejected. Extension headers, sparse files, links and special files are
unsupported. Paths must be safe relative source paths with no duplicates,
case-colliding components, or file/directory conflicts. Dependency caches and
repository metadata directories are excluded from the source-only format.

Archive verification establishes byte and tree correctness. An archive held in
worker memory or a successful upload alone is not durable progress; T15 must
verify the stored object and commit its reference under execution fencing.

Local verification: `make test-target TARGET=tests/unit/test_archives.py` passes
51 adversarial unit tests. The complete Python regression suite, formatting,
lint, typing and package build also pass. The strict format permits only regular
files with modes `0644` or `0755`, zero tar ownership/timestamps, NFC UTF-8 paths
up to 255 bytes representable in USTAR, and block-aligned zero endings.

Sources: [Python 3.13 tar formats and extraction behavior](https://docs.python.org/3.13/library/tarfile.html#supported-tar-formats),
[gzip streams and deterministic timestamps](https://docs.python.org/3.13/library/gzip.html#gzip.GzipFile).
