FROM golang:1.23-bookworm as builder

ARG ACCESS_TOKEN

RUN git config --global url."https://x-access-token:${ACCESS_TOKEN}@github.com".insteadOf "https://github.com"

WORKDIR /app

COPY . .

RUN go build -o node-proxy cmd/proxy/main.go

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y ca-certificates wget libsnappy-dev libjemalloc-dev
RUN update-ca-certificates

WORKDIR /app

COPY --from=builder /app/node-proxy /app

ENTRYPOINT ["/app/node-proxy"]