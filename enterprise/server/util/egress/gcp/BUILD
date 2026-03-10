load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["gcp.csv"])

go_binary(
    name = "gcp",
    embed = [":gcp_lib"],
    visibility = ["//enterprise/server/util/egress:__pkg__"],
)

go_library(
    name = "gcp_lib",
    srcs = ["gcp.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/egress/gcp",
)
