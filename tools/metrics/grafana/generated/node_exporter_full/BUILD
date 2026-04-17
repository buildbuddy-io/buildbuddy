load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

# Stage the upstream dashboard JSON into this package directory so that
# //go:embed (which only sees files under the package dir) can find it.
genrule(
    name = "embedded_dashboard_json",
    srcs = ["//tools/metrics/grafana:dashboards/node-exporter-full.json"],
    outs = ["dashboard.json"],
    cmd = "cp $< $@",
)

go_library(
    name = "node_exporter_full_lib",
    srcs = ["node_exporter_full.go"],
    embedsrcs = [":embedded_dashboard_json"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/node_exporter_full",
)

go_binary(
    name = "node_exporter_full",
    embed = [":node_exporter_full_lib"],
)

genrule(
    name = "node_exporter_full_json",
    outs = ["node-exporter-full-bb.json"],
    cmd = "$(location :node_exporter_full) > $@",
    tools = [":node_exporter_full"],
)
