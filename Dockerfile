FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/multigateway ./go/cmd/multigateway

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/multipooler ./go/cmd/multipooler

FROM alpine:3.20

RUN apk add --no-cache ca-certificates && \
    apk upgrade --no-cache

RUN addgroup -g 1000 multigres && \
    adduser -u 1000 -G multigres -s /bin/sh -D multigres

COPY --from=build /bin/multigateway /usr/local/bin/multigateway
COPY --from=build /bin/multipooler /usr/local/bin/multipooler

RUN chown -R multigres:multigres /usr/local/bin/multigateway /usr/local/bin/multipooler && \
    chmod +x /usr/local/bin/multigateway /usr/local/bin/multipooler

USER multigres

LABEL org.opencontainers.image.title="Multigres Gateway" \
      org.opencontainers.image.description="PostgreSQL horizontal scaling and connection pooling" \
      org.opencontainers.image.source="https://github.com/supabase/multigres"

ENTRYPOINT ["/usr/local/bin/multigateway"]
