FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy module files
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project (so rabbit/ is included)
COPY . .

# Build publisher binary
WORKDIR /app/publisher
RUN go build -o /app/app .

# ---- minimal runtime image ----
FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/app .
CMD ["./app"]