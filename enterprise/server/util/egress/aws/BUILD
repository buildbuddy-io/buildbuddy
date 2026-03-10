load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

exports_files(["aws.csv"])

go_binary(
    name = "aws",
    embed = [":aws_lib"],
    visibility = ["//enterprise/server/util/egress:__pkg__"],
)

go_library(
    name = "aws_lib",
    srcs = ["aws.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/egress/aws",
)
