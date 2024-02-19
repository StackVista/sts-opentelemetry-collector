FROM --platform=$BUILDPLATFORM golang:1.21-alpine AS builder

RUN apk add --no-cache git

WORKDIR /go/src/github.com/stackvista/sts-opentelemetry-collector
COPY . .

RUN go install go.opentelemetry.io/collector/cmd/builder@v0.94.1
RUN builder --config ./sts-otel-builder.yaml

FROM gcr.io/distroless/static-debian11
USER 10001
COPY --from=builder /go/src/github.com/stackvista/sts-opentelemetry-collector/bin/sts-opentelemetry-collector /usr/bin/sts-opentelemetry-collector
ENTRYPOINT ["/usr/bin/sts-opentelemetry-collector"]
