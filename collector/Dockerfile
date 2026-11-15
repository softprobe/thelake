FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/thelake-collector ./cmd/thelake-collector

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/thelake-collector /app/thelake-collector
COPY config.yaml /etc/thelake-collector/config.yaml

USER 1000:1000
EXPOSE 4318 13133
ENTRYPOINT ["/app/thelake-collector"]
CMD ["--config=/etc/thelake-collector/config.yaml"]
