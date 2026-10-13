These image-baked control files are copied from trusted application sources by
`profiles/python/Dockerfile`. The archive verifier is the stdlib-only T14
`continuum_api.artifacts.archives` module; the profile bootstrap imports it only
from the fixed root-owned `/opt/continuum/trusted/archives.py` path.

Never add repository-provided files or package data to this directory.
