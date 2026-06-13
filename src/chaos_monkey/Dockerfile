FROM golang:1.26-alpine AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
COPY chaos_monkey ./chaos_monkey
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/chaos_monkey ./chaos_monkey

FROM alpine:latest
RUN apk add --no-cache docker-cli
COPY --from=builder /build/bin/chaos_monkey /chaos_monkey
ENTRYPOINT ["/chaos_monkey"]
