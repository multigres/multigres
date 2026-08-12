# Compiles PostgREST's `test:spec` hspec suite so it can run against an EXTERNAL
# database (the multigateway, or a direct-postgres baseline) instead of the temp
# DB the upstream `withPg` harness provisions. The suite connects purely via
# libpq env (SpecHelper's configDbUri = "postgresql://"), so the harness injects
# PGHOST/PGPORT/PGUSER/... at run time and loads fixtures itself.
#
# Base GHC matches PostgREST's nix pin (ghc948). Dependencies are solved against
# the Hackage index-state pinned in the checkout's cabal.project.freeze, so the
# build is reproducible for a given PostgREST tag.
FROM haskell:9.4.8

# One-shot build/test image: it compiles the spec suite and runs it to
# completion against an external database, then exits. It is not a long-running
# service, so it deliberately declares no healthcheck.
HEALTHCHECK NONE

# haskell:9.4.8 ships Debian 10 (buster), now EOL — its apt mirrors moved to
# archive.debian.org, so repoint sources before installing build libs. (The libs
# are already present in the base image today; kept explicit so the build does
# not silently depend on that.) apt-get update + install stay in a single layer
# (cache-correct); `cabal update` joins them so the Hackage index is refreshed
# in the same one-time setup layer.
RUN sed -i 's|deb.debian.org|archive.debian.org|g; s|security.debian.org|archive.debian.org|g; /buster-updates/d' /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
       libpq-dev zlib1g-dev libicu-dev pkg-config \
    && cabal update \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . /src

# Compile the test suite at build time and record its binary path, so runtime is
# just "connect + run" with no cabal invocation — which lets the container run
# cleanly as a non-root user (cabal would otherwise want a writable HOME).
RUN cabal build test:spec \
    && cabal list-bin test:spec > /usr/local/bin/spec-binpath

# Exec the prebuilt binary from the repo root (runtime fixtures such as
# openapi.json / images resolve relative to test/spec/fixtures), passing through
# hspec args. The DB connection comes entirely from libpq env.
RUN printf '#!/bin/sh\nexec "$(cat /usr/local/bin/spec-binpath)" "$@"\n' > /usr/local/bin/run-spec \
    && chmod +x /usr/local/bin/run-spec

# The suite's only subprocess call is SpecHelper.analyzeTable, which runs
# `psql -U postgres -c 'ANALYZE test."<t>"'` before the RangeSpec group to
# refresh planner stats. There is no psql client in this image and no `postgres`
# superuser reachable through our proxy, so that ANALYZE is instead run by the Go
# loader (loadFixtures) against the target DB. Provide a psql that exits 0 so the
# now-redundant hook still succeeds; psql is used nowhere else in the suite.
RUN printf '#!/bin/sh\n# ANALYZE is run by the multigres Go loader; see fixtures.go.\nexit 0\n' > /usr/local/bin/psql \
    && chmod +x /usr/local/bin/psql

# Run the suite as a non-root user. The build artifacts and fixtures under /src
# are world-readable and the suite only reads them (it drives PostgREST over
# HTTP, which talks to the external DB), so no ownership change is needed.
RUN useradd --create-home spec
USER spec
ENTRYPOINT ["/usr/local/bin/run-spec"]
