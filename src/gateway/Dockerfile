FROM golang:1.26-alpine AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY common ./common
COPY gateway ./gateway
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/gateway ./gateway

FROM busybox:latest
COPY --from=builder /build/bin/gateway /gateway
ENTRYPOINT ["./gateway"]
