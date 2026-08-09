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

# haskell:9.4.8 ships Debian 10 (buster), now EOL — its apt mirrors moved to
# archive.debian.org, so repoint sources before installing build libs. (The libs
# are already present in the base image today; kept explicit so the build does
# not silently depend on that.)
RUN sed -i 's|deb.debian.org|archive.debian.org|g; s|security.debian.org|archive.debian.org|g; /buster-updates/d' /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
       libpq-dev zlib1g-dev libicu-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . /src

# Compile the test suite at build time so runtime is just "connect + run".
RUN cabal update && cabal build test:spec

# Exec the prebuilt binary from the repo root (runtime fixtures such as
# openapi.json / images resolve relative to test/spec/fixtures), passing through
# hspec args. The DB connection comes entirely from libpq env.
RUN printf '#!/bin/sh\nexec "$(cabal list-bin test:spec)" "$@"\n' > /usr/local/bin/run-spec \
    && chmod +x /usr/local/bin/run-spec
ENTRYPOINT ["/usr/local/bin/run-spec"]
