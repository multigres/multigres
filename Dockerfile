# =========================================================================
# Stage 1: The Builder Stage
# =========================================================================
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Install build dependencies like git
RUN apk add --no-cache git

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build static binaries
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /bin/multigres ./go/cmd/multigres
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /bin/multiadmin ./go/cmd/multiadmin
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /bin/multigateway ./go/cmd/multigateway
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /bin/multiorch ./go/cmd/multiorch
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /bin/multipooler ./go/cmd/multipooler
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /bin/pgctld ./go/cmd/pgctld


# =========================================================================
# Stage 2: The Final Production Stage
# =========================================================================
FROM gcr.io/distroless/static-debian12

LABEL org.opencontainers.image.source="https://github.com/multigres/multigres"
LABEL org.opencontainers.image.description="A single container image containing all Multigres components."
LABEL org.opencontainers.image.licenses="Apache-2.0"

ENV PATH="/multigres/bin:$PATH"
WORKDIR /multigres

# Switch to the default non-root user for enhanced security.
USER 65532:65532

# Copy all the compiled binaries from the builder stage into our namespaced directory.
COPY --from=builder /bin/multigres /multigres/bin/
COPY --from=builder /bin/multiadmin /multigres/bin/
COPY --from=builder /bin/multigateway /multigres/bin/
COPY --from=builder /bin/multiorch /multigres/bin/
COPY --from=builder /bin/multipooler /multigres/bin/
COPY --from=builder /bin/pgctld /multigres/bin/