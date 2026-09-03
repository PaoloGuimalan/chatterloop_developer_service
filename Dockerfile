# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build

WORKDIR /src

# Manifests first, so the dependency layer caches independently of source edits
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 yields a static binary that runs on a distroless base.
# -s -w strips debug info and symbol tables to shrink it.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/developer_service ./cmd/developer

# static-debian12 ships ca-certificates, which the Supabase, Mongo Atlas and
# hosted Redis TLS connections all need
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/developer_service /app/developer_service

# No secure-connect bundle here, unlike worker_service: this service does not
# talk to Cassandra. See the package docs in internal/connections - Postgres,
# Mongo and Redis are the whole dependency list, deliberately.

EXPOSE 8890
USER nonroot:nonroot

ENTRYPOINT ["/app/developer_service"]
