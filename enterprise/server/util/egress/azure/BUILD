load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["azure.csv"])

go_binary(
    name = "azure",
    embed = [":azure_lib"],
    visibility = ["//enterprise/server/util/egress:__pkg__"],
)

go_library(
    name = "azure_lib",
    srcs = ["azure.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/egress/azure",
)
