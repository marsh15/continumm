# Official Docker Library Python 3.13.12 slim Bookworm, linux/amd64 manifest.
# Registry endpoint: docker.io/library/python:3.13.12-slim-bookworm
# Platform-specific manifest SHA256 was checked read-only against its raw body.
FROM --platform=linux/amd64 python:3.13.12-slim-bookworm@sha256:3121f8b0804aa3698ab750d9a39ea4a42657a385c9b133722b915e55c51551a6

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN mkdir -p \
        /opt/continuum/trusted \
        /opt/continuum/home \
        /opt/continuum/input \
        /opt/continuum/wheels \
        /opt/continuum/deps \
        /vercel/sandbox/workspace \
    && chown root:root /opt/continuum /opt/continuum/trusted \
    && chmod 0755 /opt/continuum /opt/continuum/trusted \
    && chown 10001:10001 \
        /vercel/sandbox \
        /opt/continuum/home \
        /opt/continuum/input \
        /opt/continuum/wheels \
        /opt/continuum/deps \
        /vercel/sandbox/workspace \
    && chmod 0700 /opt/continuum/home /opt/continuum/input /opt/continuum/wheels \
    && chmod 0755 /vercel/sandbox \
        /opt/continuum/deps \
        /vercel/sandbox/workspace

COPY src/continuum_api/sandbox/profile_bootstrap.py /opt/continuum/profile_bootstrap.py
COPY src/continuum_api/artifacts/archives.py /opt/continuum/trusted/archives.py
COPY profiles/python/trustedfiles/README.md /opt/continuum/trusted/README.md

RUN chown root:root \
        /opt/continuum/profile_bootstrap.py \
        /opt/continuum/trusted/archives.py \
        /opt/continuum/trusted/README.md \
    && chmod 0555 /opt/continuum/profile_bootstrap.py \
    && chmod 0444 /opt/continuum/trusted/archives.py /opt/continuum/trusted/README.md

USER 10001:10001
WORKDIR /opt/continuum
