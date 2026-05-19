FROM golang:1.26-alpine AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
COPY serialanalyzer ./serialanalyzer
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/serial-analyzer ./serialanalyzer

FROM busybox:latest
COPY --from=builder /build/bin/serial-analyzer /serial-analyzer
ENTRYPOINT ["./serial-analyzer"]

