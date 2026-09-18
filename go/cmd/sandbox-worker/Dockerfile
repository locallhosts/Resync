# Multi-stage build: the final image contains nothing but the static
# binary and a CA bundle (for any real HTTPS integrations you swap the
# mock actions for later). No shell, no package manager, no libc even —
# this is the "isolated execution sandbox" claim made concrete: if this
# container is compromised, there is no shell to get a foothold in.
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/sandbox-worker ./cmd/sandbox-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sandbox-worker /sandbox-worker
USER nonroot:nonroot
ENTRYPOINT ["/sandbox-worker"]
