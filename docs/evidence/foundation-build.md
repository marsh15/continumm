# Foundation build from a fresh checkout

**Verified:** 11 October 2026.
**Source:** committed revision `0134299`, cloned locally into an empty temporary
directory with no virtual environment, frontend dependencies or build artifacts.

The following command passed:

```sh
make bootstrap build-package build-frontend
```

It installed the locked backend and frontend dependencies, produced the Python
source distribution and wheel, checked the generated OpenAPI schema/types, and
built the production frontend. The checkout used Python 3.13.12 and Node 26.10.0.
The temporary checkout was removed after verification.

This satisfies the fresh-checkout foundation build item in Checkpoint A. It does
not prove hosted database, object storage or sandbox behavior, and does not close
the rest of Checkpoint A or Checkpoint B.
