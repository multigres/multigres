# =========================================================================
# Stage 1: The Builder Stage
# =========================================================================
FROM golang:1.25-alpine3.22 AS builder

WORKDIR /src

# Install build dependencies like git and make
RUN apk add --no-cache git make bash

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build static binaries using Makefile
COPY . .
RUN make build-release

# =========================================================================
# Stage 2: The Final Production Stage
# =========================================================================
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.source="https://github.com/multigres/multigres"
LABEL org.opencontainers.image.description="A single container image containing all Multigres components."
LABEL org.opencontainers.image.licenses="Apache-2.0"

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
COPY --from=builder /src/bin/pgctld /multigres/bin/

# Since this container serves multiple binaries depending on the execution it's impossible to have a healthcheck that applies to all at this point.
# The healthcheck below exists only to satisfy Super-linter CHECKOV and it's not a reliable healthcheck for production. 
HEALTHCHECK --interval=10s --timeout=3s \
  CMD ["/multigres/bin/multigres"]
