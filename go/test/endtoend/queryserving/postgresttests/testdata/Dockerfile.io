# Runs PostgREST's upstream pytest-based `test/io` suite against an EXTERNAL
# database (the multigateway, or a direct-postgres baseline) instead of the
# temporary DB the upstream nix `withPg` harness provisions.
#
# Unlike the hspec suite (Dockerfile.spec), the io suite does NOT need the
# Haskell test code — it drives a real `postgrest` binary over HTTP and manages
# config reloads, roles and an admin server itself. So this image needs only
# (a) the pinned `postgrest` executable and (b) a Python + pytest environment,
# not a cabal build. We therefore use PostgREST's OFFICIAL prebuilt release
# binary for the pinned tag (checksum-pinned per repo supply-chain rules) on a
# slim Python base — far faster and more reproducible than compiling.
#
# The suite spawns `postgrest` subprocesses with a filtered libpq env
# (PGDATABASE/PGHOST/PGUSER only — see test/io/conftest.py::baseenv), so to
# repoint them at the gateway over TCP with a password we install a tiny
# `postgrest` shim that sources extra libpq vars (PGPORT/PGPASSWORD/PGSSLMODE)
# the harness passes in at `docker run` time. This keeps the upstream test files
# pristine (no patching).
#
# Built and run as linux/amd64 (the harness passes `--platform linux/amd64` to
# both `docker build` and `docker run`; emulated on Apple Silicon, native on CI):
# PostgREST ships a fully STATIC x86-64 release binary that runs on any base with
# no shared-lib deps, whereas the aarch64 build is dynamically linked and would
# need extra runtime libs. This matches how the sibling hspec image already runs
# here. (No `--platform` on FROM — hadolint DL3029; the build flag drives it.)
FROM python:3.11-slim-bookworm

# One-shot build/test image: it runs the io suite to completion against an
# external database, then exits. Not a long-running service.
HEALTHCHECK NONE

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Keep in sync with postgrestTag in postgrest_source.go. Bump both together (and
# re-baseline the divergence report / refresh the checksum below).
ARG POSTGREST_VERSION=v14.16

# SHA256 of the pinned PostgREST release tarball (supply-chain: pin the exact
# checksum, never trust the download blind). Refresh when POSTGREST_VERSION
# changes: `curl -sL <url> | sha256sum`.
#   postgrest-<ver>-linux-static-x86-64.tar.xz (fully static x86-64)
ENV PGRST_SHA256=36b8ae140f188cfcd6003494805bf35a41e895f88c12be9183d60f91782145c6

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl xz-utils ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Download + verify the official postgrest binary, installing it as
# postgrest.real. The `postgrest` name on PATH is a shim (below).
RUN set -eux; \
    url="https://github.com/PostgREST/postgrest/releases/download/${POSTGREST_VERSION}/postgrest-${POSTGREST_VERSION}-linux-static-x86-64.tar.xz"; \
    curl -fsSL "$url" -o /tmp/pgrst.tar.xz; \
    echo "${PGRST_SHA256}  /tmp/pgrst.tar.xz" | sha256sum -c -; \
    tar -xJf /tmp/pgrst.tar.xz -C /usr/local/bin postgrest; \
    mv /usr/local/bin/postgrest /usr/local/bin/postgrest.real; \
    rm /tmp/pgrst.tar.xz; \
    /usr/local/bin/postgrest.real --help >/dev/null

# postgrest shim: the io suite (test/io/postgrest.py::run) launches this binary
# with env=<test dict>, which carries only PGDATABASE/PGHOST/PGUSER (+ PGRST_*).
# Source the harness-supplied connection extras (PGPORT/PGPASSWORD/PGSSLMODE),
# written to /tmp/pgconn.env by the entrypoint, before exec'ing the real binary.
# It only ADDS vars the test dict lacks, so a test that overrides PGUSER/PGHOST/
# PGDATABASE still wins. Absolute paths throughout (the test dict has no PATH).
RUN printf '#!/bin/sh\n[ -r /tmp/pgconn.env ] && . /tmp/pgconn.env\nexec /usr/local/bin/postgrest.real "$@"\n' \
      > /usr/local/bin/postgrest \
    && chmod +x /usr/local/bin/postgrest

# Python deps for the io suite (test/io/conftest.py imports all of these at
# collection time). Pinned for reproducibility; urllib3 held <2 because
# requests-unixsocket 0.3.0 relies on urllib3 v1 connection internals.
RUN pip install --no-cache-dir \
      pytest==8.3.4 \
      pyjwt==2.10.1 \
      pyyaml==6.0.2 \
      requests==2.31.0 \
      urllib3==1.26.20 \
      requests-unixsocket==0.3.0 \
      syrupy==4.8.1

WORKDIR /src
COPY . /src

# Entrypoint: materialize the connection extras the shim sources, then run
# pytest with whatever args the harness passes (test selection + flags). The
# DB connection identity (host/user/db) reaches postgrest via the suite's own
# env plumbing; port/password/sslmode reach it via the shim + this file.
RUN printf '%s\n' \
      '#!/bin/sh' \
      'set -e' \
      ': > /tmp/pgconn.env' \
      '[ -n "$PGPORT" ]    && echo "export PGPORT='"'"'$PGPORT'"'"'" >> /tmp/pgconn.env' \
      '[ -n "$PGPASSWORD" ] && echo "export PGPASSWORD='"'"'$PGPASSWORD'"'"'" >> /tmp/pgconn.env' \
      '[ -n "$PGSSLMODE" ] && echo "export PGSSLMODE='"'"'$PGSSLMODE'"'"'" >> /tmp/pgconn.env' \
      'exec python -m pytest "$@"' \
      > /usr/local/bin/run-io \
    && chmod +x /usr/local/bin/run-io

ENTRYPOINT ["/usr/local/bin/run-io"]
