#!/usr/bin/env bash
docker run \
  --rm \
  --net=host \
  --volume="$(pwd)/tools/tracing/otel_collector.yaml:/etc/otelcol-contrib/config.yaml" \
  otel/opentelemetry-collector-contrib:latest

