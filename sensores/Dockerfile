FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o sensor ./sensores/

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/sensor .
CMD ["./sensor"]