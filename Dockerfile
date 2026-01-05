# =========================================================================
# Stage 1: The Builder Stage
# =========================================================================
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Install build dependencies like git and make
RUN apk add --no-cache git=2.52.0-r0 make=4.4.1-r3 bash=5.3.3-r1

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build static binaries using Makefile
COPY . .
RUN make build-release

# =========================================================================
# Stage 2: The Final Production Stage
# =========================================================================
FROM debian:bookworm-slim

LABEL org.opencontainers.image.source="https://github.com/multigres/multigres"
LABEL org.opencontainers.image.description="A single container image containing all Multigres components."
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Set pipefail to catch errors in piped commands
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install pgBackRest from PostgreSQL APT repository and procps
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    lsb-release && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/postgresql-archive-keyring.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt-cache policy pgbackrest && \
    apt-get install -y --no-install-recommends \
    pgbackrest \
    procps && \
    pgbackrest version && \
    apt-get remove -y ca-certificates curl gnupg lsb-release && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/multigres/bin:$PATH"
WORKDIR /multigres

# Switch to the default non-root user for enhanced security.
USER 65532:65532

# Copy all the compiled binaries from the builder stage into our namespaced directory.
COPY --from=builder /src/bin/multigres /multigres/bin/
COPY --from=builder /src/bin/multiadmin /multigres/bin/
COPY --from=builder /src/bin/multigateway /multigres/bin/
COPY --from=builder /src/bin/multiorch /multigres/bin/
COPY --from=builder /src/bin/multipooler /multigres/bin/

# Since this container serves multiple binaries depending on the execution it's impossible to have a healthcheck that applies to all at this point.
# The healthcheck below exists only to satisfy Super-linter CHECKOV and it's not a reliable healthcheck for production.
HEALTHCHECK --interval=10s --timeout=3s \
    CMD ["/multigres/bin/multigres"]
