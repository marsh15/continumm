# Python pytest profile image

This is a local image recipe for the fixed `python-pytest` profile version 1.
The Dockerfile pins CPython 3.13.12 on `linux/amd64` to the official Docker
Library manifest `sha256:3121f8b0804aa3698ab750d9a39ea4a42657a385c9b133722b915e55c51551a6`.
The digest came from the read-only Docker Registry manifest for
`docker.io/library/python:3.13.12-slim-bookworm`; hashing the raw platform
manifest body produced the same SHA256.

The image recipe is not a published Vercel image. T17 performs no image build or
publication, and the runtime must fail closed until an operator configures a
Vercel image reference pinned to a verified image digest.

The image places the trusted bootstrap and source archive verifier under the
root-owned `/opt/continuum` control directory. Repository files are extracted
only to `/vercel/sandbox/workspace`; downloaded wheels and installed packages
use separate `/opt/continuum/wheels` and `/opt/continuum/deps` directories.
