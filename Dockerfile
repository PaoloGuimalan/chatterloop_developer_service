# Build
FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module
# graph on every build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Static binary: the runtime stage has no libc to link against.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/developer_service ./cmd/developer

# Runtime
FROM alpine:3.20
# Certificates are needed: Mongo Atlas and a hosted Redis are both TLS.
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 developer
USER developer

COPY --from=build /out/developer_service /usr/local/bin/developer_service

EXPOSE 8890
ENTRYPOINT ["/usr/local/bin/developer_service"]
