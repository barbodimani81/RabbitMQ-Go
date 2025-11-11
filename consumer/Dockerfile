FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/consumer
RUN go build -o /app/app .

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/app .
CMD ["./app"]