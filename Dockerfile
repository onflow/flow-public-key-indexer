
# BUILD BIN

FROM golang:1.17.7 as builder
# Install go modules
WORKDIR /key-indexer
COPY go.* ./
COPY . ./
RUN go mod download
RUN go mod verify
RUN CGO_ENABLED=0 go build -o key-indexer .
RUN chmod a+x key-indexer


# RUN APP
FROM alpine:latest
WORKDIR /key-indexer
COPY --from=builder /key-indexer /key-indexer
EXPOSE 8888
CMD ["./key-indexer"]
