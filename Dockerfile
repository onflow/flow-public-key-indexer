# BUILD BIN

# Build stage
FROM golang:1.22.7 AS builder

ARG CGO_ENABLED
ARG CGO_CFLAGS
ARG GOARCH

ENV CGO_ENABLED=$CGO_ENABLED
ENV CGO_CFLAGS=$CGO_CFLAGS
ENV GOARCH=$GOARCH

RUN apt-get update && apt-get install -y gcc-multilib

WORKDIR /key-indexer

COPY go.* ./
RUN go mod download
COPY . ./
RUN go mod verify
RUN GOOS=linux GOARCH=amd64 go build -o key-indexer .

# Final stage
FROM alpine:latest
RUN apk add --no-cache libc6-compat
WORKDIR /key-indexer
COPY --from=builder /key-indexer/key-indexer /key-indexer
EXPOSE 8888
CMD ["./key-indexer"]
