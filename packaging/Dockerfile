# Multi-stage, static, non-root. Produces the same binaries the GitHub
# release ships (CGO_ENABLED=0, -trimpath, stripped) — no libc dependency.
# Build:  docker build -t ghcr.io/rahulkj/firn -f packaging/Dockerfile .
# Run:    docker run --rm -p 9460:9460 -p 9461:9461 -v firn-data:/var/lib/firn ghcr.io/rahulkj/firn
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/firnd ./cmd/firnd \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/firn-cli ./cmd/firn-cli

FROM alpine:3.21
LABEL org.opencontainers.image.title="firn" \
      org.opencontainers.image.description="Firn — the durable event broker (streams + queues, one append-only log)" \
      org.opencontainers.image.source="https://github.com/rahulkj/firn" \
      org.opencontainers.image.licenses="MIT"
RUN apk add --no-cache ca-certificates busybox-extras \
 && adduser -D -u 10001 -s /sbin/nologin firn \
 && mkdir -p /var/lib/firn && chown firn:firn /var/lib/firn && chmod 700 /var/lib/firn
WORKDIR /var/lib/firn
COPY --from=build /out/firnd /out/firn-cli /usr/local/bin/
USER firn
VOLUME ["/var/lib/firn"]
EXPOSE 9460 9461 9471
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:9460/healthz || exit 1
ENTRYPOINT ["firnd"]
CMD ["-data-dir", "/var/lib/firn"]
