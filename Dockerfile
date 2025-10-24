FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

RUN apk add --no-cache git

WORKDIR /go/src/github.com/stackvista/sts-opentelemetry-collector
COPY . .

RUN go install go.opentelemetry.io/collector/cmd/builder@v0.100.0
RUN builder --config ./sts-otel-builder.yaml

FROM gcr.io/distroless/static-debian12
USER 10001
COPY --from=builder /go/src/github.com/stackvista/sts-opentelemetry-collector/bin/sts-opentelemetry-collector /usr/bin/sts-opentelemetry-collector
ENTRYPOINT ["/usr/bin/sts-opentelemetry-collector"]
