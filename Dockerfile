# renovate: datasource=docker depName=registry.suse.com/bci/bci-micro
ARG BASE_IMAGE=registry.suse.com/bci/bci-micro:15.7@sha256:f1427da6f40791aa5a9f73bb339674970ea93a2b158adc81bc99bcd1b7899a74

FROM --platform=$BUILDPLATFORM registry.suse.com/bci/golang:1.26.10@sha256:87dde729781b5347dbd4a5a3d4e084e906c3112b71b51095d9f74a46147de25c AS builder
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
    release_line="go$(awk '/^go / {print $2; exit}' common/go.mod | cut -d. -f1,2)." && \
    case "${compiler}" in "${release_line}"*) ;; *) echo "${compiler} does not match ${release_line}x" >&2; exit 1 ;; esac && \
    go version bin/sts-opentelemetry-collector | grep -Fx "bin/sts-opentelemetry-collector: ${compiler}"

FROM ${BASE_IMAGE}
USER 10001
COPY --from=builder /go/src/github.com/stackvista/sts-opentelemetry-collector/bin/sts-opentelemetry-collector /usr/bin/sts-opentelemetry-collector
ENTRYPOINT ["/usr/bin/sts-opentelemetry-collector"]
