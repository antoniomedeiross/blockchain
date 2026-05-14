FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY . .

RUN go build -o teste ./test/main.go

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/teste .

ENTRYPOINT ["./teste"]