# renovate: datasource=docker depName=registry.suse.com/bci/bci-micro
ARG BASE_IMAGE=registry.suse.com/bci/bci-micro:15.7-61.21

FROM --platform=$BUILDPLATFORM registry.suse.com/bci/golang:1.26 AS builder
# Build only with this image's compiler; a go.work bump waits for a matching builder.
ENV GOTOOLCHAIN=local

ARG TARGETOS=linux
ARG TARGETARCH

RUN zypper --non-interactive refresh \
    && zypper --non-interactive install --no-recommends git-core \
    && zypper clean --all \
    && rm -rf /var/log/zypp /var/log/zypper.log

WORKDIR /go/src/github.com/stackvista/sts-opentelemetry-collector
COPY . .

RUN go install go.opentelemetry.io/collector/cmd/builder@v0.161.0
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH:-$(go env GOARCH)} builder --config ./sts-otel-builder.yaml && \
    compiler="$(go env GOVERSION)" && \
    case "${compiler}" in go1.26.*) ;; *) echo "Unapproved Go compiler: ${compiler}" >&2; exit 1 ;; esac && \
    go version bin/sts-opentelemetry-collector | grep -Fx "bin/sts-opentelemetry-collector: ${compiler}"

FROM ${BASE_IMAGE}
USER 10001
COPY --from=builder /go/src/github.com/stackvista/sts-opentelemetry-collector/bin/sts-opentelemetry-collector /usr/bin/sts-opentelemetry-collector
ENTRYPOINT ["/usr/bin/sts-opentelemetry-collector"]
