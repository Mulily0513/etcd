ARG BUILDER_IMAGE=golang:1.26-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim
FROM ${BUILDER_IMAGE} AS build

WORKDIR /src
COPY go.mod go.sum ./
COPY api ./api
COPY cache ./cache
COPY client ./client
COPY etcdctl ./etcdctl
COPY pkg ./pkg
COPY server ./server
COPY scripts ./scripts
COPY tests ./tests
COPY tools ./tools

ENV GOTOOLCHAIN=auto
RUN (cd server && go build -mod=mod -trimpath -o /out/etcd .) \
    && (cd etcdctl && go build -mod=mod -trimpath -o /out/etcdctl .) \
    && go build -mod=mod -trimpath -o /out/benchmark ./tools/benchmark \
    && go build -mod=mod -trimpath -o /out/perf-report ./tools/perf/report

FROM ${RUNTIME_IMAGE}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates coreutils curl procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/etcd /usr/local/bin/etcd
COPY --from=build /out/etcdctl /usr/local/bin/etcdctl
COPY --from=build /out/benchmark /usr/local/bin/benchmark
COPY --from=build /out/perf-report /usr/local/bin/perf-report

RUN mkdir -p /var/lib/etcd /results

EXPOSE 2379 2380
ENTRYPOINT ["/usr/local/bin/etcd"]
