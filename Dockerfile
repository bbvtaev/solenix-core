FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/solenix ./cmd/solenix

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/bin/solenix /usr/local/bin/solenix

EXPOSE 8731 8080

ENTRYPOINT ["/usr/local/bin/solenix"]
CMD ["serve"]
