FROM golang:1.26.1 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o datasorter ./cmd/main.go

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/datasorter .
CMD ["./datasorter"]