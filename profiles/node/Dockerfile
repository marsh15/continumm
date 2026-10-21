# Official Node.js 24.21.0 Bookworm slim, linux/amd64 platform manifest.
FROM --platform=linux/amd64 node:24.21.0-bookworm-slim@sha256:51b1100cc2a83d370c6a60952e3f2989c8a43159d0e38586e090f3b3326efefd AS node_runtime

# Keep the same pinned Python and Debian Bookworm foundation used by T17. The
# Node stage supplies only its runtime, npm CLI and the C++ shared libraries
# that the Node ELF may need; it does not run repository or package code.
FROM --platform=linux/amd64 python:3.13.12-slim-bookworm@sha256:3121f8b0804aa3698ab750d9a39ea4a42657a385c9b133722b915e55c51551a6

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=node_runtime /usr/local/bin/node /usr/local/bin/node
COPY --from=node_runtime /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/npm
COPY --from=node_runtime /usr/lib/x86_64-linux-gnu/libstdc++.so.6* /usr/lib/x86_64-linux-gnu/
COPY --from=node_runtime /usr/lib/x86_64-linux-gnu/libgcc_s.so.1* /usr/lib/x86_64-linux-gnu/
COPY --from=node_runtime /usr/lib/x86_64-linux-gnu/libatomic.so.1* /usr/lib/x86_64-linux-gnu/

RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && mkdir -p \
        /opt/continuum/trusted \
        /opt/continuum/home \
        /opt/continuum/input \
        /opt/continuum/npm-cache \
        /vercel/sandbox/workspace \
    && chown root:root /opt/continuum /opt/continuum/trusted \
    && chmod 0755 /opt/continuum /opt/continuum/trusted \
    && chown 10001:10001 \
        /opt/continuum/home \
        /opt/continuum/input \
        /opt/continuum/npm-cache \
        /vercel/sandbox \
        /vercel/sandbox/workspace \
    && chmod 0700 /opt/continuum/home /opt/continuum/input /opt/continuum/npm-cache \
    && chmod 0755 /vercel/sandbox /vercel/sandbox/workspace

COPY src/continuum_api/sandbox/node_bootstrap.py /opt/continuum/node_bootstrap.py
COPY src/continuum_api/sandbox/node_bootstrap_recipe.py /opt/continuum/node_bootstrap_recipe.py
COPY src/continuum_api/artifacts/archives.py /opt/continuum/trusted/archives.py
COPY profiles/node/trustedfiles/README.md /opt/continuum/trusted/README.md

RUN touch /opt/continuum/npm-user-empty.npmrc /opt/continuum/npm-global-empty.npmrc \
    && chmod 0444 /opt/continuum/npm-user-empty.npmrc /opt/continuum/npm-global-empty.npmrc \
    && chown root:root \
        /opt/continuum/node_bootstrap.py \
        /opt/continuum/node_bootstrap_recipe.py \
        /opt/continuum/trusted/archives.py \
        /opt/continuum/trusted/README.md \
    && chmod 0555 /opt/continuum/node_bootstrap.py /opt/continuum/node_bootstrap_recipe.py \
    && chmod 0444 /opt/continuum/trusted/archives.py /opt/continuum/trusted/README.md \
    && if ldd /usr/local/bin/node 2>&1 | grep -F "not found"; then exit 1; fi \
    && test "$(/usr/local/bin/node --version)" = "v24.21.0" \
    && test "$(/usr/local/bin/npm --version)" = "11.19.0" \
    && /usr/local/bin/python3.13 -I -S /opt/continuum/node_bootstrap.py verify-runtime




COPY src/continuum_api/sandbox/hidden_probe.cjs /opt/continuum/hidden_probe.cjs
RUN chown root:root /opt/continuum/hidden_probe.cjs \
    && chmod 0555 /opt/continuum/hidden_probe.cjs

USER 10001:10001
WORKDIR /opt/continuum
