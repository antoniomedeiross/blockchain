FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o drone ./drones/

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/drone .
CMD ["./drone"]