FROM golang:1.25-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tack .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tack /usr/local/bin/tack

RUN useradd -r -u 1000 -m tack
USER tack

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/tack"]
