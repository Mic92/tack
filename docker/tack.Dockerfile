FROM golang:1.25.9-trixie AS build

WORKDIR /app

# Keep dependency downloads in their own layer so ordinary source edits do not
# force Docker users to redownload the full module graph every rebuild.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /app/bin/tack .

FROM debian:trixie-slim AS run

ARG DEBIAN_FRONTEND=noninteractive

# Tack talks to HTTPS endpoints and uses sparse Git checkouts to load workflow
# definitions at the triggered revision.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/bin/tack /app/bin/tack

LABEL org.opencontainers.image.title="Tack" \
    org.opencontainers.image.description="Stitch any CI into Tangled" \
    org.opencontainers.image.authors="Mitchell Hashimoto" \
    org.opencontainers.image.source="https://tangled.org/mitchellh.com/tack" \
    org.opencontainers.image.documentation="https://tangled.org/mitchellh.com/tack" \
    org.opencontainers.image.licenses="MIT" \
    io.atcr.readme="https://tangled.org/mitchellh.com/tack/raw/main/README.md"

CMD ["/app/bin/tack"]
