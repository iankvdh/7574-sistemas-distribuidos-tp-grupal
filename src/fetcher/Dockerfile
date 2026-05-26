FROM golang:1.26-alpine AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
COPY fetcher ./fetcher
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/fetcher ./fetcher

FROM busybox:latest
COPY --from=builder /build/bin/fetcher /fetcher
ENTRYPOINT ["./fetcher"]
