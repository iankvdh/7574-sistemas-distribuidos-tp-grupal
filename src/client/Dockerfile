FROM golang:1.26-alpine AS builder
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY client ./client
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/client ./client

FROM busybox:latest
COPY --from=builder /build/bin/client /client
ENTRYPOINT ["./client"]
